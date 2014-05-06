package shardmaster

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "time"
import "strconv"

import "github.com/jmhodges/levigo"
import "bytes"

const Debug = 0
const DebugPersist = 0
const printRPCerrors = false

const persistent = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		fmt.Printf(format, a...)
	}
	return
}

func DPrintfPersist(format string, a ...interface{}) (n int, err error) {
	if DebugPersist > 0 {
		fmt.Printf(format, a...)
	}
	return
}

type ShardMaster struct {
	mu sync.Mutex
	l  net.Listener

	// Network stuff
	me         int
	dead       bool // for testing
	deaf       bool // for testing
	unreliable bool // for testing
	network    bool

	// Shardmaster state
	px           *paxos.Paxos
	configs      map[int]*Config // indexed by config num
	processedSeq int
	maxConfig    int

	// Persistence stuff
	dbReadOptions  *levigo.ReadOptions
	dbWriteOptions *levigo.WriteOptions
	dbOpts         *levigo.Options
	dbName         string
	db             *levigo.DB
	dbLock         sync.Mutex
	dbMaxConfig    int
}

type Op struct {
	Op      int //1 = Query, 2 = Join, 3 = Leave, 4 = Move
	GID     int64
	Servers []string
	Shard   int
}

// Get the desired configuration from memory or disk
// Returns empty Config if it doesn't exist
func (sm *ShardMaster) getConfig(configNum int) Config {
	if configNum < 0 || configNum > sm.maxConfig {
		return Config{}
	}
	var config *Config
	var exists bool
	// Read from memory if possible, otherwise from disk
	if config, exists = sm.configs[configNum]; !exists {
		if config, exists = sm.dbGetConfig(configNum); exists {
			sm.configs[configNum] = config
		}
	}
	return *config
}

// Evenly distribute the given shards over the given groups
func (sm *ShardMaster) balance(gids []int64, shards [NShards]int64) [NShards]int64 {
	DPrintf("%d) Balance with %d and %d\n", sm.me, gids, shards)
	expectedGPS := (NShards / len(gids))
	if expectedGPS <= 0 {
		expectedGPS = 1
	}
	var newShards [NShards]int64
	num := make(map[int64]int)
	var over []int

	// Copy old shard distribution
	// and check which groups have too many shards
	for k, v := range shards {
		num[v] = num[v] + 1
		newShards[k] = v
		// If group no longer exists or is overloaded, mark shard as over
		found := false
		for _, v2 := range gids {
			if v == v2 {
				found = true
			}
		}
		if num[v] > expectedGPS || !found {
			over = append(over, k)
		}
	}
	DPrintf("%d %d\n", len(over), expectedGPS)

	// Move shards from overloaded groups to underloaded groups
	for _, v := range gids {
		if v == 0 {
			continue
		}
		for num[v] < expectedGPS && len(over) > 0 {
			newShards[over[0]] = v
			num[v] += 1
			over = over[1:len(over)]
		}
	}

	DPrintf("%d) Balance returns %d\n", sm.me, newShards)
	return newShards
}

// Create a new configuration which adds the given group
func (sm *ShardMaster) createJoinConfig(gid int64, servers []string) {
	oldConfig := sm.getConfig(sm.maxConfig)
	sm.maxConfig += 1
	newConfig := Config{}
	newConfig.Num = sm.maxConfig
	newConfig.Groups = map[int64][]string{}
	var gids []int64

	// Copy old groups and add new one
	for k, v := range oldConfig.Groups {
		if k != gid && k != 0 {
			gids = append(gids, k)
			newConfig.Groups[k] = v
		}
	}
	gids = append(gids, gid)
	newConfig.Groups[gid] = servers
	// Balance loading
	newConfig.Shards = sm.balance(gids, oldConfig.Shards)
	// Add new configuration
	sm.dbWriteConfig(sm.maxConfig, newConfig)
	sm.configs[sm.maxConfig] = &newConfig
}

// Create a new configuration which removes the given group
func (sm *ShardMaster) createLeaveConfig(gid int64) {
	oldConfig := sm.getConfig(sm.maxConfig)
	sm.maxConfig = sm.maxConfig + 1
	newConfig := Config{}
	newConfig.Num = sm.maxConfig
	newConfig.Groups = map[int64][]string{}
	var gids []int64

	// Copy old groups except for the leaving one
	for k, v := range oldConfig.Groups {
		if k != gid && k != 0 {
			gids = append(gids, k)
			newConfig.Groups[k] = v
		}
	}
	// Balance loading
	newConfig.Shards = sm.balance(gids, oldConfig.Shards)
	// Add the new configuration
	sm.dbWriteConfig(sm.maxConfig, newConfig)
	sm.configs[sm.maxConfig] = &newConfig
}

// Creat configuration with the given shard assigned to the given group
func (sm *ShardMaster) createMoveConfig(gid int64, shard int) {
	oldConfig := sm.getConfig(sm.maxConfig)
	sm.maxConfig = sm.maxConfig + 1
	newConfig := Config{}
	newConfig.Num = sm.maxConfig
	newConfig.Groups = map[int64][]string{}
	// Copy old sharding except for the desired assignment
	for k, v := range oldConfig.Shards {
		if k == shard {
			newConfig.Shards[k] = gid
		} else {
			newConfig.Shards[k] = v
		}
	}
	// Copy old groups
	for k, v := range oldConfig.Groups {
		newConfig.Groups[k] = v
	}
	// Add new configuration
	sm.dbWriteConfig(sm.maxConfig, newConfig)
	sm.configs[sm.maxConfig] = &newConfig
}

// Processes all unprocessed log entries up to the given sequence
func (sm *ShardMaster) processLog(maxSeq int) {
	if maxSeq <= sm.processedSeq+1 {
		return
	}
	DPrintf("%d) Process Log until %d\n", sm.me, maxSeq)

	for i := sm.processedSeq + 1; i < maxSeq; i++ {
		to := 10 * time.Millisecond
		start := false
		// Get the decided value for this sequence
		// Propose a no-op if it has not been decided
		for !sm.dead {
			decided, opp := sm.px.Status(i)
			if decided {
				op := opp.(Op)
				if op.Op == 1 {
					DPrintf("%d) Log %d: QUERY(%d)\n", sm.me, i, op.GID)
				} else if op.Op == 2 {
					DPrintf("%d) Log %d: JOIN(%d, %s)\n", sm.me, i, op.GID, op.Servers)
					sm.createJoinConfig(op.GID, op.Servers)
				} else if op.Op == 3 {
					DPrintf("%d) Log %d: LEAVE(%d)\n", sm.me, i, op.GID)
					sm.createLeaveConfig(op.GID)
				} else if op.Op == 4 {
					DPrintf("%d) Log %d: MOVE(%d -> %d)\n", sm.me, i, op.Shard, op.GID)
					sm.createMoveConfig(op.GID, op.Shard)
				}
				break
			} else if !start {
				sm.px.Start(i, Op{1, -1, nil, 0})
				start = true
			}
			time.Sleep(to)
			if to < 1*time.Second {
				to *= 2
			}
		}
	}
	sm.processedSeq = maxSeq - 1
	sm.dbWriteProcessedSeq(sm.processedSeq)
	sm.px.Done(sm.processedSeq)
	DPrintf("%d) Done Process Log Until %d\n", sm.me, maxSeq)
}

// Accept a Join request
func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	sm.mu.Lock()
	DPrintf("%d) Join: %d -> %s\n", sm.me, args.GID, args.Servers)

	newOp := Op{2, args.GID, args.Servers, 0}

	for !sm.dead {
		// Process any missed log entries
		seq := sm.px.Max() + 1
		sm.processLog(seq)
		// Propose the new op to Paxos
		sm.px.Start(seq, newOp)

		to := 10 * time.Millisecond
		// Wait for a decision and check if it is the desired op
		for !sm.dead {
			decided, theOpp := sm.px.Status(seq)
			if decided {
				theOp := theOpp.(Op)
				sm.processLog(seq + 1)
				if theOp.Op == newOp.Op && theOp.GID == newOp.GID && theOp.Shard == newOp.Shard {
					DPrintf("%d) Join Returns\n", sm.me)
					sm.mu.Unlock()
					return nil
				} else {
					break
				}
			}
			time.Sleep(to)
			if to < 1*time.Second {
				to *= 2
			}
		}
	}

	DPrintf("%d) Join Returns\n", sm.me)
	sm.mu.Unlock()
	return nil
}

// Accept a request to remove a group
func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	sm.mu.Lock()
	DPrintf("%d) Leave: %d\n", sm.me, args.GID)

	newOp := Op{3, args.GID, nil, 0}

	for !sm.dead {
		// Process any missed log entries
		seq := sm.px.Max() + 1
		sm.processLog(seq)
		// Propose the op to Paxos
		sm.px.Start(seq, newOp)

		to := 10 * time.Millisecond
		// Wait for a decision and check if it is the desired op
		for !sm.dead {
			decided, theOpp := sm.px.Status(seq)
			if decided {
				theOp := theOpp.(Op)
				sm.processLog(seq + 1)
				if theOp.Op == newOp.Op && theOp.GID == newOp.GID && theOp.Shard == newOp.Shard {
					DPrintf("%d) Leave Returns\n", sm.me)
					sm.mu.Unlock()
					return nil
				} else {
					break
				}
			}
			time.Sleep(to)
			if to < 1*time.Second {
				to *= 2
			}
		}
	}

	DPrintf("%d) Leave Returns\n", sm.me)
	sm.mu.Unlock()
	return nil
}

// Accept a request to move a shard to a particular group
func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	sm.mu.Lock()
	DPrintf("%d) Move: %d -> %d\n", sm.me, args.Shard, args.GID)

	newOp := Op{4, args.GID, nil, args.Shard}

	for !sm.dead {
		// Process any missed log entries
		seq := sm.px.Max() + 1
		sm.processLog(seq)
		// Propose the op to Paxos
		sm.px.Start(seq, newOp)

		to := 10 * time.Millisecond
		// Wait for a decision and check if it is the desired op
		for !sm.dead {
			decided, theOpp := sm.px.Status(seq)
			if decided {
				theOp := theOpp.(Op)
				sm.processLog(seq + 1)
				if theOp.Op == newOp.Op && theOp.GID == newOp.GID && theOp.Shard == newOp.Shard {
					DPrintf("%d) Move Returns\n", sm.me)
					sm.mu.Unlock()
					return nil
				} else {
					break
				}
			}
			time.Sleep(to)
			if to < 1*time.Second {
				to *= 2
			}
		}
	}

	DPrintf("%d) Move Returns\n", sm.me)
	sm.mu.Unlock()
	return nil
}

// Respond to a query about a particular configuration
func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	sm.mu.Lock()
	DPrintf("%d) Query: %d\n", sm.me, args.Num)

	newOp := Op{1, int64(args.Num), nil, 0}

	for !sm.dead {
		// Process any missed log entries
		seq := sm.px.Max() + 1
		sm.processLog(seq)
		if args.Num > sm.maxConfig {
			newOp = Op{1, -1, nil, 0}
		}
		// Propose this op to Paxos
		sm.px.Start(seq, newOp)

		to := 10 * time.Millisecond
		for !sm.dead {
			// Wait for a decision and then return
			decided, _ := sm.px.Status(seq)
			if decided {
				sm.processLog(seq + 1)
				if args.Num >= 0 && args.Num < sm.maxConfig {
					reply.Config = sm.getConfig(args.Num)
				} else {
					reply.Config = sm.getConfig(sm.maxConfig)
				}
				DPrintf("%d) Query Returns %d\n", sm.me, reply.Config)
				sm.mu.Unlock()
				return nil
			}
			time.Sleep(to)
			if to < 1*time.Second {
				to *= 2
			}
		}
	}

	if args.Num >= 0 && args.Num < sm.maxConfig {
		reply.Config = sm.getConfig(args.Num)
	} else {
		reply.Config = sm.getConfig(sm.maxConfig)
	}
	DPrintf("%d) Query Returns %d\n", sm.me, reply.Config)
	sm.mu.Unlock()
	return nil
}

// please don't change this function.
func (sm *ShardMaster) Kill() {
	// Just double check that Kill isn't called multiple times
	// (trying to close sm.db multiple times causes a panic)
	if sm.dead {
		return
	}

	// Kill the server
	DPrintf("\n%v: Killing the server", sm.me)
	sm.dead = true
	if sm.l != nil {
		sm.l.Close()
	}
	sm.px.Kill()

	// Close the database
	if persistent {
		sm.dbLock.Lock()
		sm.db.Close()
		sm.dbLock.Unlock()
	}

	// Destroy the database
	if persistent {
		DPrintfPersist("\n%v: Destroying database... ", sm.me)
		err := levigo.DestroyDatabase(sm.dbName, sm.dbOpts)
		if err != nil {
			DPrintfPersist("\terror")
		} else {
			DPrintfPersist("\tsuccess")
		}
	}
}

func (sm *ShardMaster) KillSaveDisk() {
	// Just double check that Kill isn't called multiple times
	// (trying to close sm.db multiple times causes a panic)
	if sm.dead {
		return
	}
	// Kill the server
	DPrintf("\n%v: Killing the server", sm.me)
	sm.dead = true
	if sm.l != nil {
		sm.l.Close()
	}
	sm.px.KillSaveDisk()

	// Close the database
	if persistent {
		sm.dbLock.Lock()
		sm.db.Close()
		sm.dbLock.Unlock()
	}
}

// Writes the given instance to the database
func (sm *ShardMaster) dbWriteConfig(configNum int, toWrite Config) {
	if !persistent {
		return
	}
	sm.dbLock.Lock()
	defer sm.dbLock.Unlock()
	if sm.dead {
		return
	}

	toPrint := ""
	toPrint += fmt.Sprintf("\n%v: Writing config %v to database... ", sm.me, configNum)
	// Encode the instance into a byte array
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	err := enc.Encode(toWrite)
	if err != nil {
		DPrintfPersist("\terror encoding: %s", fmt.Sprint(err))
	} else {
		// Write the state to the database
		key := "config_" + strconv.Itoa(configNum)
		err := sm.db.Put(sm.dbWriteOptions, []byte(key), buffer.Bytes())
		if err != nil {
			toPrint += fmt.Sprintf("\terror writing to database")
		} else {
			toPrint += fmt.Sprintf("\tsuccess")
			// Record max instance
			if configNum > sm.dbMaxConfig {
				sm.dbWriteMaxConfig(configNum)
			}
		}
	}
	DPrintfPersist(toPrint)
}

// Tries to get the desired config from the database
// If it doesn't exist, returns empty Config
func (sm *ShardMaster) dbGetConfig(toGet int) (*Config, bool) {
	if !persistent {
		return &Config{}, false
	}
	sm.dbLock.Lock()
	defer sm.dbLock.Unlock()
	if sm.dead {
		return &Config{}, false
	}

	toPrint := ""
	toPrint += fmt.Sprintf("\n%v: Reading config %v from database... ", sm.me, toGet)
	// Read entry from database if it exists
	key := "config_" + strconv.Itoa(toGet)
	entryBytes, err := sm.db.Get(sm.dbReadOptions, []byte(key))

	// Decode the entry if it exists, otherwise return empty
	if err == nil && len(entryBytes) > 0 {
		toPrint += "\tDecoding entry... "
		buffer := *bytes.NewBuffer(entryBytes)
		decoder := gob.NewDecoder(&buffer)
		var entryDecoded Config
		err = decoder.Decode(&entryDecoded)
		if err != nil {
			toPrint += "\terror"
		} else {
			toPrint += "\tsuccess"
			DPrintfPersist(toPrint)
			return &entryDecoded, true
		}
	} else {
		toPrint += fmt.Sprintf("\tNo entry found in database %s", fmt.Sprint(err))
		DPrintfPersist(toPrint)
		return &Config{}, false
	}

	DPrintfPersist(toPrint)
	return &Config{}, false
}

// Writes the max processed sequence number to the database
func (sm *ShardMaster) dbWriteProcessedSeq(seq int) {
	if !persistent {
		return
	}
	if sm.dead {
		return
	}

	toPrint := ""
	toPrint += fmt.Sprintf("\n%v: Writing processed sequence number %v to database... ", sm.me, seq)
	// Encode the number into a byte array
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	err := enc.Encode(seq)
	if err != nil {
		DPrintfPersist("\terror encoding: %s", fmt.Sprint(err))
	} else {
		// Write the state to the database
		key := "processedSequence"
		err := sm.db.Put(sm.dbWriteOptions, []byte(key), buffer.Bytes())
		if err != nil {
			toPrint += fmt.Sprintf("\terror writing to database")
		} else {
			toPrint += fmt.Sprintf("\tsuccess")
		}
	}
	DPrintfPersist(toPrint)
}

// Writes the persisted max config number to the database
func (sm *ShardMaster) dbWriteMaxConfig(max int) {
	if !persistent {
		return
	}
	if sm.dead {
		return
	}

	toPrint := ""
	toPrint += fmt.Sprintf("\n%v: Writing max config %v to database... ", sm.me, max)
	// Encode the number into a byte array
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	err := enc.Encode(max)
	if err != nil {
		DPrintfPersist("\terror encoding: %s", fmt.Sprint(err))
	} else {
		// Write the state to the database
		key := "dbMaxConfig"
		err := sm.db.Put(sm.dbWriteOptions, []byte(key), buffer.Bytes())
		if err != nil {
			toPrint += fmt.Sprintf("\terror writing to database")
		} else {
			toPrint += fmt.Sprintf("\tsuccess")
			sm.dbMaxConfig = max
		}
	}
	DPrintfPersist(toPrint)
}

// Initialize database for persistence
// and load any previously written 'maxConfig' and 'processedSeq' state
func (sm *ShardMaster) dbInit() {
	if !persistent {
		return
	}
	sm.dbLock.Lock()
	defer sm.dbLock.Unlock()
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if sm.dead {
		return
	}

	DPrintfPersist("\n%v: Initializing database", sm.me)

	// Register Proposal struct since we will encode/decode it using gob
	// Calling this probably isn't necessary, but being explicit for now
	gob.Register(Config{})

	// Open database (create it if it doesn't exist)
	sm.dbOpts = levigo.NewOptions()
	sm.dbOpts.SetCache(levigo.NewLRUCache(3 << 30))
	sm.dbOpts.SetCreateIfMissing(true)
	dbDir := "/home/ubuntu/mexos/src/shardmaster/persist/"
	sm.dbName = dbDir + "shardmasterDB_" + strconv.Itoa(sm.me)
	os.MkdirAll(dbDir, 0777)
	DPrintfPersist("\n\t%v: DB Name: %s", sm.me, sm.dbName)
	var err error
	sm.db, err = levigo.Open(sm.dbName, sm.dbOpts)
	if err != nil {
		DPrintfPersist("\n\t%v: Error opening database! \n\t%s", sm.me, fmt.Sprint(err))
	} else {
		DPrintfPersist("\n\t%v: Database opened successfully", sm.me)
	}

	// Create options for reading/writing entries
	sm.dbReadOptions = levigo.NewReadOptions()
	sm.dbWriteOptions = levigo.NewWriteOptions()

	// Read max instance from database if it exists
	sm.dbMaxConfig = 0
	maxConfigBytes, err := sm.db.Get(sm.dbReadOptions, []byte("dbMaxConfig"))
	if err == nil && len(maxConfigBytes) > 0 {
		// Decode the max instance
		DPrintfPersist("\n\t%v: Decoding max config... ", sm.me)
		bufferMax := *bytes.NewBuffer(maxConfigBytes)
		decoder := gob.NewDecoder(&bufferMax)
		var maxDecoded int
		err = decoder.Decode(&maxDecoded)
		if err != nil {
			DPrintfPersist("\terror decoding: %s", fmt.Sprint(err))
		} else {
			sm.maxConfig = maxDecoded
			DPrintfPersist("\tsuccess")
		}
	} else {
		DPrintfPersist("\n\t%v: No stored max instance to load", sm.me)
	}

	// Read processed sequence from database if it exists
	processedSeqBytes, err := sm.db.Get(sm.dbReadOptions, []byte("processedSequence"))
	if err == nil && len(processedSeqBytes) > 0 {
		// Decode the max instance
		DPrintfPersist("\n\t%v: Decoding processed sequence... ", sm.me)
		bufferSeq := *bytes.NewBuffer(processedSeqBytes)
		decoder := gob.NewDecoder(&bufferSeq)
		var processedSeq int
		err = decoder.Decode(&processedSeq)
		if err != nil {
			DPrintfPersist("\terror decoding: %s", fmt.Sprint(err))
		} else {
			sm.processedSeq = processedSeq
			DPrintfPersist("\tsuccess")
		}
	} else {
		DPrintfPersist("\n\t%v: No stored processed sequence to load", sm.me)
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int, network bool) *ShardMaster {
	gob.Register(Op{})

	sm := new(ShardMaster)
	// Network stuff
	sm.me = me
	sm.network = network

	// Shardmaster state
	sm.processedSeq = -1
	sm.maxConfig = 0
	sm.configs = make(map[int]*Config)
	sm.configs[0] = &Config{}
	sm.configs[0].Groups = map[int64][]string{}

	// Persistence stuff
	sm.dbInit()

	rpcs := rpc.NewServer()
	if !printRPCerrors {
		disableLog()
		rpcs.Register(sm)
		enableLog()
	} else {
		rpcs.Register(sm)
	}

	sm.px = paxos.Make(servers, me, rpcs, network, "shardmaster")

	if sm.network {
		port := peers[me][len(peers[me])-6:len(peers[me])-1]
		l, e := net.Listen("tcp", + port)
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		sm.l = l
	} else {
		os.Remove(servers[me])
		l, e := net.Listen("unix", servers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		sm.l = l
	}
	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for sm.dead == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.dead == false {
				if sm.deaf || (sm.unreliable && (rand.Int63()%1000) < 100) {
					// discard the request.
					conn.Close()
				} else if sm.unreliable && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					if sm.network {
						c1 := conn.(*net.TCPConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
					} else {
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && sm.dead == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}

type NullWriter int

func (NullWriter) Write([]byte) (int, error) { return 0, nil }

func enableLog() {
	log.SetOutput(os.Stderr)
}

func disableLog() {
	log.SetOutput(new(NullWriter))
}
