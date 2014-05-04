package shardkv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"
import "strconv"

import "github.com/jmhodges/levigo"
import "bytes"
import "strings"

const Debug = 0
const startport = 2300
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

type Op struct {
	Op    int //1 = Get, 2 = Put, 3 = PutHash, 4 = Reconfigure
	OpID  int64
	Key   string
	Value string

	ConfigNum int
	Store     map[string]string
	Response  map[int64]string
}

type ShardKV struct {
	mu sync.Mutex
	l  net.Listener

	// Network stuff
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	network    bool

	// ShardKV state
	sm       *shardmaster.Clerk
	px       *paxos.Paxos
	gid      int64 // my replica group ID
	config   shardmaster.Config
	store    map[string]string
	response map[int64]string
	minSeq   int

	// Persistence stuff
	dbReadOptions  *levigo.ReadOptions
	dbWriteOptions *levigo.WriteOptions
	dbOpts         *levigo.Options
	dbName         string
	db             *levigo.DB
	dbLock         sync.Mutex
}

// Get the desired response, either from memory or disk
func (kv *ShardKV) getResponse(id int64) (string, bool) {
	response, exists := kv.response[id]
	if !exists {
		response, exists = kv.dbGetResponse(id)
	}
	return response, exists
}

// Get the desired value, either from memory or disk
func (kv *ShardKV) getValue(key string) (string, bool) {
	value, exists := kv.store[key]
	if !exists {
		value, exists = kv.dbGet(key)
	}
	return value, exists
}

// Process log entries up until the given sequence
func (kv *ShardKV) processLog(maxSeq int) {
	if maxSeq <= kv.minSeq+1 {
		return
	}
	DPrintf("%d.%d.%d) Process Log Until %d\n", kv.gid, kv.me, kv.config.Num, maxSeq)

	for i := kv.minSeq + 1; i < maxSeq; i++ {
		to := 10 * time.Millisecond
		start := false
		// Get decided value or propose a no-op
		for !kv.dead {
			decided, opp := kv.px.Status(i)
			if decided {
				op := opp.(Op)
				if op.Op == 1 {
					DPrintf("%d.%d.%d) Log %d: Op #%d - GET(%s)\n", kv.gid, kv.me, kv.config.Num, i, op.OpID, op.Key)
					// Write the response to memory and disk
					val, _ := kv.getValue(op.Key)
					kv.dbWriteResponse(op.OpID, val)
					kv.response[op.OpID] = val
				} else if op.Op == 2 {
					DPrintf("%d.%d.%d) Log %d: Op #%d - PUT(%s, %s)\n", kv.gid, kv.me, kv.config.Num, i, op.OpID, op.Key, op.Value)
					// Write the response to memory and disk
					val, _ := kv.getValue(op.Key)
					kv.dbWriteResponse(op.OpID, val)
					kv.response[op.OpID] = val
					// Write the value to memory and disk
					kv.dbPut(op.Key, op.Value)
					kv.store[op.Key] = op.Value
				} else if op.Op == 3 {
					DPrintf("%d.%d.%d) Log %d: Op #%d - PUTHASH(%s, %s)\n", kv.gid, kv.me, kv.config.Num, i, op.OpID, op.Key, op.Value)
					// Write the response to memory and disk
					val, _ := kv.getValue(op.Key)
					kv.dbWriteResponse(op.OpID, val)
					kv.response[op.OpID] = val
					// Write the value to memory and disk
					val = strconv.Itoa(int(hash(val + op.Value)))
					kv.dbPut(op.Key, val)
					kv.store[op.Key] = val
				} else if op.Op == 4 {
					DPrintf("%d.%d.%d) Log %d: Op #%d - RECONFIGURE(%d)\n", kv.gid, kv.me, kv.config.Num, i, op.OpID, op.ConfigNum)
					// Write the new shard data to memory and disk
					for nk, nv := range op.Store {
						kv.dbPut(nk, nv)
						kv.store[nk] = nv
					}
					// Write the new responses to memory and disk
					for nk, nv := range op.Response {
						kv.dbWriteResponse(nk, nv)
						kv.response[nk] = nv
					}
					// Record the new config in memory and disk
					kv.config = kv.sm.Query(op.ConfigNum)
					kv.dbWriteConfigNum(kv.config.Num)
				}
				break
			} else if !start {
				kv.px.Start(i, Op{})
				start = true
			}
			time.Sleep(to)
			if to < 1*time.Second {
				to *= 2
			}
		}
	}
	// Update the new minSeq in memory and disk
	kv.minSeq = maxSeq - 1
	kv.dbWriteMinSeq(kv.minSeq)
	kv.px.Done(kv.minSeq)
}

// Log the given op and execute it
func (kv *ShardKV) processKV(op Op, reply *KVReply) {
	for !kv.dead {
		// Process any missed log entries
		seq := kv.px.Max() + 1
		kv.processLog(seq)
		// If wrong group for shard, return
		if kv.config.Shards[key2shard(op.Key)] != kv.gid {
			return
		}
		// If duplicate request, use previous response
		if v, seen := kv.getResponse(op.OpID); seen {
			DPrintf("%d.%d.%d) Already Seen Op %d\n", kv.gid, kv.me, kv.config.Num, op.OpID)
			if v == "" {
				reply.Err = ErrNoKey
			} else {
				reply.Err = OK
			}
			reply.Value = v
			return
		}

		// Propose desired op to Paxos log
		kv.px.Start(seq, op)
		to := 10 * time.Millisecond
		for !kv.dead {
			// Check if sequence has been decided
			if decided, _ := kv.px.Status(seq); decided {
				// Process any missed log entries
				seq := kv.px.Max() + 1
				kv.processLog(seq)
				// If wrong group for shard, return
				if kv.config.Shards[key2shard(op.Key)] != kv.gid {
					return
				}
				// If have seen op (duplicate or just decided), return response
				if v, seen := kv.getResponse(op.OpID); seen {
					if v == "" {
						reply.Err = ErrNoKey
					} else {
						reply.Err = OK
					}
					reply.Value = v
					return
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
}

// Log and execute a reconfiguration
func (kv *ShardKV) addReconfigure(num int, store map[string]string, response map[int64]string) {
	defer func() {
		DPrintf("%d.%d.%d) Reconfigure Returns\n", kv.gid, kv.me, kv.config.Num)
	}()

	newOp := Op{}
	newOp.Op = 4
	newOp.OpID = int64(num)
	newOp.ConfigNum = num
	newOp.Store = store
	newOp.Response = response
	DPrintf("%d.%d.%d) Reconfigure: %d\n", kv.gid, kv.me, kv.config.Num, num)

	for !kv.dead {
		// Process any missed log entries
		seq := kv.px.Max() + 1
		kv.processLog(seq)
		// If desired config is now out of date, return
		if kv.config.Num >= num {
			return
		}

		// Propose reconfiguration to Paxos
		kv.px.Start(seq, newOp)

		to := 10 * time.Millisecond
		for !kv.dead {
			// Check if sequence has been decided
			if decided, _ := kv.px.Status(seq); decided {
				// Process any missed log entries
				seq := kv.px.Max() + 1
				kv.processLog(seq)
				// If config is updated, return
				if kv.config.Num >= num {
					return
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
}

// Accept a Get request
func (kv *ShardKV) Get(args *GetArgs, reply *KVReply) error {
	kv.mu.Lock()
	defer func() {
		DPrintf("%d.%d.%d) Get Returns: %s (%s)\n", kv.gid, kv.me, kv.config.Num, reply.Value, reply.Err)
		kv.mu.Unlock()
	}()

	reply.Err = ErrWrongGroup

	newOp := Op{}
	newOp.Op = 1
	newOp.OpID = args.ID
	newOp.Key = args.Key
	DPrintf("%d.%d.%d) Get: %s\n", kv.gid, kv.me, kv.config.Num, args.Key)

	kv.processKV(newOp, reply)
	return nil
}

// Accept a Put request
func (kv *ShardKV) Put(args *PutArgs, reply *KVReply) error {
	kv.mu.Lock()
	defer func() {
		DPrintf("%d.%d.%d) Put Returns: %s (%s)\n", kv.gid, kv.me, kv.config.Num, reply.Value, reply.Err)
		if reply.Err == ErrNoKey {
			reply.Err = OK
		}
		kv.mu.Unlock()
	}()

	reply.Err = ErrWrongGroup

	newOp := Op{}
	if args.DoHash {
		newOp.Op = 3
		DPrintf("%d.%d.%d) PutHash: %s -> %s\n", kv.gid, kv.me, kv.config.Num, args.Key, args.Value)
	} else {
		newOp.Op = 2
		DPrintf("%d.%d.%d) Put: %s -> %s\n", kv.gid, kv.me, kv.config.Num, args.Key, args.Value)
	}
	newOp.OpID = args.ID
	newOp.Key = args.Key
	newOp.Value = args.Value

	kv.processKV(newOp, reply)
	return nil
}

// Respond to a Fetch request
func (kv *ShardKV) Fetch(args *FetchArgs, reply *FetchReply) error {
	//kv.mu.Lock()
	defer func() {
		DPrintf("%d.%d.%d) Fetch Returns: %s\n", kv.gid, kv.me, kv.config.Num, reply.Store)
		//kv.mu.Unlock()
	}()

	DPrintf("%d.%d.%d) Fetch: Shard %d from Config %d\n", kv.gid, kv.me, kv.config.Num, args.Shard, args.Config)

	// If current config is older than requested config, return error
	if kv.config.Num < args.Config {
		reply.Err = ErrNoKey
		return nil
	}

	shardStore := make(map[string]string)
	keysInMemory := make(map[string]bool)
	// Copy key/value pairs for desired shard from memory
	for k, v := range kv.store {
		if key2shard(k) == args.Shard {
			shardStore[k] = v
			keysInMemory[k] = true
		}
	}
	// Copy key/value pairs for desired shard from disk if not in memory
	for k, v := range kv.dbGetShard(args.Shard, keysInMemory) {
		DPrintfPersist("\n\t%v: got shard %v data (%v, %v)", args.Shard, kv.me, k, v)
		shardStore[k] = v
	}

	reply.Err = OK
	reply.Store = shardStore
	reply.Response = kv.response
	return nil
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// Process any missed log entries
	seq := kv.px.Max() + 1
	kv.processLog(seq)

	// Check if current config is latest config
	newConfig := kv.sm.Query(kv.config.Num + 1)
	if newConfig.Num == kv.config.Num {
		return
	}

	DPrintf("%d.%d.%d) Found New Config: %d -> %d\n", kv.gid, kv.me, kv.config.Num, kv.config.Shards, newConfig.Shards)

	var gained []int
	var remoteGained []int
	var lost []int

	// Determine which shards I lost and which shards I gained
	for k, v := range newConfig.Shards {
		if kv.config.Shards[k] == kv.gid && v != kv.gid {
			lost = append(lost, k)
		} else if kv.config.Shards[k] != kv.gid && v == kv.gid {
			gained = append(gained, k)
			if kv.config.Shards[k] > 0 {
				remoteGained = append(remoteGained, k)
			}
		}
	}

	// Get store data and response data for new shards
	newStore := make(map[string]string)
	newResponse := make(map[int64]string)
	if len(remoteGained) != 0 && !kv.dead {
		DPrintf("%d.%d.%d) New Config needs %d\n", kv.gid, kv.me, kv.config.Num, remoteGained)
		for _, shard := range remoteGained {
			otherGID := kv.config.Shards[shard]
			servers := kv.config.Groups[otherGID]
			args := &FetchArgs{newConfig.Num, shard}
			// Keep trying to get new data until success
		srvloop:
			for !kv.dead {
				for sid, srv := range servers {
					DPrintf("%d.%d.%d) Attempting to get Shard %d from %d.%d\n", kv.gid, kv.me, kv.config.Num, shard, otherGID, sid)
					var reply FetchReply
					ok := call(srv, "ShardKV.Fetch", args, &reply, kv.network)
					if ok && (reply.Err == OK) {
						DPrintf("%d.%d.%d) Got Shard %d from %d.%d\n", kv.gid, kv.me, kv.config.Num, shard, otherGID, sid)
						for k, v := range reply.Store {
							newStore[k] = v
						}
						for k, v := range reply.Response {
							newResponse[k] = v
						}
						break srvloop
					} else {
						DPrintf("%d.%d.%d) Failed to get Shard %d from %d.%d\n", kv.gid, kv.me, kv.config.Num, shard, otherGID, sid)
					}
				}
				time.Sleep(10 * time.Millisecond)
			}
		}
	}
	// Log the reconfiguration
	kv.addReconfigure(newConfig.Num, newStore, newResponse)
	DPrintf("%d.%d.%d) New Config adding %d\n", kv.gid, kv.me, kv.config.Num, newStore)
}

// please don't change this function.
func (kv *ShardKV) Kill() {
	// Just double check that Kill isn't called multiple times
	// (trying to close kv.db multiple times causes a panic)
	if kv.dead {
		return
	}

	// Kill the server
	DPrintf("\n%v: Killing the server", kv.me)
	kv.dead = true
	if kv.l != nil {
		kv.l.Close()
	}
	kv.px.Kill()

	// Close the database
	if persistent {
		kv.dbLock.Lock()
		kv.db.Close()
		kv.dbLock.Unlock()
	}

	// Destroy the database
	if persistent {
		DPrintfPersist("\n%v: Destroying database... ", kv.me)
		err := levigo.DestroyDatabase(kv.dbName, kv.dbOpts)
		if err != nil {
			DPrintfPersist("\terror")
		} else {
			DPrintfPersist("\tsuccess")
		}
	}
}

func (kv *ShardKV) KillSaveDisk() {
	// Just double check that Kill isn't called multiple times
	// (trying to close kv.db multiple times causes a panic)
	if kv.dead {
		return
	}
	// Kill the server
	DPrintf("\n%v: Killing the server", kv.me)
	kv.dead = true
	if kv.l != nil {
		kv.l.Close()
	}
	kv.px.KillSaveDisk()

	// Close the database
	if persistent {
		kv.dbLock.Lock()
		kv.db.Close()
		kv.dbLock.Unlock()
	}
}

// Get key/values pairs for given shard from database
// Excludes any of the given keys
func (kv *ShardKV) dbGetShard(shard int, exclude map[string]bool) map[string]string {
	shardStore := make(map[string]string)
	if !persistent {
		return shardStore
	}
	DPrintfPersist("\n%v: dbGetShard Waiting for dbLock", kv.me)
	kv.dbLock.Lock()
	DPrintfPersist("\n%v: dbGetShard Got dbLock", kv.me)
	defer func() {
		kv.dbLock.Unlock()
		DPrintfPersist("\n%v: dbGetShard Released dbLock", kv.me)
	}()
	if kv.dead {
		return shardStore
	}

	toPrint := ""
	toPrint += fmt.Sprintf("\n%v: Reading shard %v from database... ", kv.me, shard)
	// Get database iterator
	iterator := kv.db.NewIterator(kv.dbReadOptions)
	defer iterator.Close()
	iterator.SeekToFirst()
	DPrintfPersist("\n%v: dbGetShard starting iteration", kv.me)
	for iterator.Valid() {
		keyBytes := iterator.Key()
		key := string(keyBytes)
		if strings.Index(key, "KVkey_") < 0 {
			iterator.Next()
			toPrint += "\n\tSkipping key " + key
			continue
		}
		key = key[len("KVkey_"):]
		if key2shard(key) != shard || exclude[key] {
			iterator.Next()
			toPrint += "\n\tSkipping key " + key
			continue
		}

		valueBytes := iterator.Value()
		bufferVal := *bytes.NewBuffer(valueBytes)
		decoderVal := gob.NewDecoder(&bufferVal)
		var value string
		err := decoderVal.Decode(&value)
		if err != nil {
			toPrint += fmt.Sprintf("\n\terror decoding value for %v", key)
			iterator.Next()
			continue
		}
		toPrint += fmt.Sprintf("\n\tRead (%v, %v)", key, value)
		shardStore[key] = value
		iterator.Next()
	}

	DPrintfPersist(toPrint)
	return shardStore
}

// Tries to get the value from the database
// If it doesn't exist, returns empty string
func (kv *ShardKV) dbGet(key string) (string, bool) {
	if !persistent {
		return "", false
	}
	DPrintfPersist("\n%v: dbGet Waiting for dbLock", kv.me)
	kv.dbLock.Lock()
	DPrintfPersist("\n%v: dbGet Got dbLock", kv.me)
	defer func() {
		kv.dbLock.Unlock()
		DPrintfPersist("\n%v: dbGet Released dbLock", kv.me)
	}()
	if kv.dead {
		return "", false
	}

	toPrint := ""
	toPrint += fmt.Sprintf("\n%v: Reading value for %v from database... ", kv.me, key)
	// Read entry from database if it exists
	key = fmt.Sprintf("KVkey_%v", key)
	entryBytes, err := kv.db.Get(kv.dbReadOptions, []byte(key))

	// Decode the entry if it exists, otherwise return empty
	if err == nil && len(entryBytes) > 0 {
		toPrint += "\tDecoding entry... "
		buffer := *bytes.NewBuffer(entryBytes)
		decoder := gob.NewDecoder(&buffer)
		var entryDecoded string
		err = decoder.Decode(&entryDecoded)
		if err != nil {
			toPrint += "\terror"
		} else {
			toPrint += "\tsuccess"
			DPrintfPersist(toPrint)
			return entryDecoded, true
		}
	} else {
		toPrint += fmt.Sprintf("\tNo entry found in database %s", fmt.Sprint(err))
		DPrintfPersist(toPrint)
		return "", false
	}

	DPrintfPersist(toPrint)
	return "", false
}

// Writes the given key/value to the database
func (kv *ShardKV) dbPut(key string, value string) {
	if !persistent {
		return
	}
	DPrintfPersist("\n%v: dbPut Waiting for dbLock", kv.me)
	kv.dbLock.Lock()
	DPrintfPersist("\n%v: dbPut Got dbLock", kv.me)
	defer func() {
		kv.dbLock.Unlock()
		DPrintfPersist("\n%v: dbPut Released dbLock", kv.me)
	}()
	if kv.dead {
		return
	}

	toPrint := ""
	toPrint += fmt.Sprintf("\n%v: Writing (%v, %v) to database... ", kv.me, key, value)
	// Encode the value into a byte array
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	err := enc.Encode(value)
	if err != nil {
		DPrintfPersist("\terror encoding: %s", fmt.Sprint(err))
	} else {
		// Write the state to the database
		key := fmt.Sprintf("KVkey_%v", key)
		err := kv.db.Put(kv.dbWriteOptions, []byte(key), buffer.Bytes())
		if err != nil {
			toPrint += fmt.Sprintf("\terror writing to database")
		} else {
			toPrint += fmt.Sprintf("\tsuccess")
		}
	}
	DPrintfPersist(toPrint)
}

// Tries to get the desired response from the database
// If it doesn't exist, returns empty string
func (kv *ShardKV) dbGetResponse(toGet int64) (string, bool) {
	if !persistent {
		return "", false
	}
	DPrintfPersist("\n%v: dbGetResponse Waiting for dbLock", kv.me)
	kv.dbLock.Lock()
	DPrintfPersist("\n%v: dbGetResponse Got dbLock", kv.me)
	defer func() {
		kv.dbLock.Unlock()
		DPrintfPersist("\n%v: dbGetResponse Released dbLock", kv.me)
	}()
	if kv.dead {
		return "", false
	}

	toPrint := ""
	toPrint += fmt.Sprintf("\n%v: Reading response %v from database... ", kv.me, toGet)
	// Read entry from database if it exists
	key := fmt.Sprintf("response_%v", toGet)
	entryBytes, err := kv.db.Get(kv.dbReadOptions, []byte(key))

	// Decode the entry if it exists, otherwise return empty
	if err == nil && len(entryBytes) > 0 {
		toPrint += "\tDecoding entry... "
		buffer := *bytes.NewBuffer(entryBytes)
		decoder := gob.NewDecoder(&buffer)
		var entryDecoded string
		err = decoder.Decode(&entryDecoded)
		if err != nil {
			toPrint += "\terror"
		} else {
			toPrint += "\tsuccess"
			DPrintfPersist(toPrint)
			return entryDecoded, true
		}
	} else {
		toPrint += fmt.Sprintf("\tNo entry found in database %s", fmt.Sprint(err))
		DPrintfPersist(toPrint)
		return "", false
	}

	DPrintfPersist(toPrint)
	return "", false
}

// Writes the given client response to the database
func (kv *ShardKV) dbWriteResponse(id int64, response string) {
	if !persistent {
		return
	}
	DPrintfPersist("\n%v: dbWriteResponse Waiting for dbLock", kv.me)
	kv.dbLock.Lock()
	DPrintfPersist("\n%v: dbWriteResponse Got dbLock", kv.me)
	defer func() {
		kv.dbLock.Unlock()
		DPrintfPersist("\n%v: dbWriteResponse Released dbLock", kv.me)
	}()
	if kv.dead {
		return
	}

	toPrint := ""
	toPrint += fmt.Sprintf("\n%v: Writing response %v -> %v to database... ", kv.me, id, response)
	// Encode the response into a byte array
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	err := enc.Encode(response)
	if err != nil {
		DPrintfPersist("\terror encoding: %s", fmt.Sprint(err))
	} else {
		// Write the state to the database
		key := fmt.Sprintf("response_%v", id)
		err := kv.db.Put(kv.dbWriteOptions, []byte(key), buffer.Bytes())
		if err != nil {
			toPrint += fmt.Sprintf("\terror writing to database")
		} else {
			toPrint += fmt.Sprintf("\tsuccess")
		}
	}
	DPrintfPersist(toPrint)
}

// Writes the min sequence number to the database
func (kv *ShardKV) dbWriteMinSeq(seq int) {
	if !persistent {
		return
	}
	DPrintfPersist("\n%v: dbWriteMinSeq Waiting for dbLock", kv.me)
	kv.dbLock.Lock()
	DPrintfPersist("\n%v: dbWriteMinSeq Got dbLock", kv.me)
	defer func() {
		kv.dbLock.Unlock()
		DPrintfPersist("\n%v: dbWriteMinSeq Released dbLock", kv.me)
	}()
	if kv.dead {
		return
	}

	toPrint := ""
	toPrint += fmt.Sprintf("\n%v: Writing min sequence num %v to database... ", kv.me, seq)
	// Encode the number into a byte array
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	err := enc.Encode(seq)
	if err != nil {
		DPrintfPersist("\terror encoding: %s", fmt.Sprint(err))
	} else {
		// Write the state to the database
		key := "minSeq"
		err := kv.db.Put(kv.dbWriteOptions, []byte(key), buffer.Bytes())
		if err != nil {
			toPrint += fmt.Sprintf("\terror writing to database")
		} else {
			toPrint += fmt.Sprintf("\tsuccess")
		}
	}
	DPrintfPersist(toPrint)
}

// Writes the config number to the database
func (kv *ShardKV) dbWriteConfigNum(configNum int) {
	if !persistent {
		return
	}
	DPrintfPersist("\n%v: dbWriteConfigNum Waiting for dbLock", kv.me)
	kv.dbLock.Lock()
	DPrintfPersist("\n%v: dbWriteConfigNum Got dbLock", kv.me)
	defer func() {
		kv.dbLock.Unlock()
		DPrintfPersist("\n%v: dbWriteConfigNum Released dbLock", kv.me)
	}()
	if kv.dead {
		return
	}

	toPrint := ""
	toPrint += fmt.Sprintf("\n%v: Writing config num %v to database... ", kv.me, configNum)
	// Encode the number into a byte array
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	err := enc.Encode(configNum)
	if err != nil {
		DPrintfPersist("\terror encoding: %s", fmt.Sprint(err))
	} else {
		// Write the state to the database
		key := "configNum"
		err := kv.db.Put(kv.dbWriteOptions, []byte(key), buffer.Bytes())
		if err != nil {
			toPrint += fmt.Sprintf("\terror writing to database")
		} else {
			toPrint += fmt.Sprintf("\tsuccess")
		}
	}
	DPrintfPersist(toPrint)
}

// Initialize database for persistence
// and load any previously written 'minSeq' and 'configNum' state
func (kv *ShardKV) dbInit() {
	if !persistent {
		return
	}
	DPrintfPersist("\n%v: dbInit Waiting for dbLock", kv.me)
	kv.dbLock.Lock()
	DPrintfPersist("\n%v: dbInit Got dbLock", kv.me)
	defer func() {
		kv.dbLock.Unlock()
		DPrintfPersist("\n%v: dbInit Released dbLock", kv.me)
	}()
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.dead {
		return
	}

	DPrintfPersist("\n%v: Initializing database", kv.me)

	// Open database (create it if it doesn't exist)
	kv.dbOpts = levigo.NewOptions()
	kv.dbOpts.SetCache(levigo.NewLRUCache(3 << 30))
	kv.dbOpts.SetCreateIfMissing(true)
	dbDir := "/home/ubuntu/mexos/src/shardkv/persist/"
	kv.dbName = dbDir + "shardkvDB_" + fmt.Sprint(kv.gid) + "_" + strconv.Itoa(kv.me)
	os.MkdirAll(dbDir, 0777)
	DPrintfPersist("\n\t%v: DB Name: %s", kv.me, kv.dbName)
	var err error
	kv.db, err = levigo.Open(kv.dbName, kv.dbOpts)
	if err != nil {
		DPrintfPersist("\n\t%v: Error opening database! \n\t%s", kv.me, fmt.Sprint(err))
	} else {
		DPrintfPersist("\n\t%v: Database opened successfully", kv.me)
	}

	// Create options for reading/writing entries
	kv.dbReadOptions = levigo.NewReadOptions()
	kv.dbWriteOptions = levigo.NewWriteOptions()

	// Read minSeq from database if it exists
	minSeqBytes, err := kv.db.Get(kv.dbReadOptions, []byte("minSeq"))
	if err == nil && len(minSeqBytes) > 0 {
		// Decode the max instance
		DPrintfPersist("\n\t%v: Decoding min seqeunce... ", kv.me)
		bufferMinSeq := *bytes.NewBuffer(minSeqBytes)
		decoder := gob.NewDecoder(&bufferMinSeq)
		var minSeqDecoded int
		err = decoder.Decode(&minSeqDecoded)
		if err != nil {
			DPrintfPersist("\terror decoding: %s", fmt.Sprint(err))
		} else {
			kv.minSeq = minSeqDecoded
			DPrintfPersist("\tsuccess")
		}
	} else {
		DPrintfPersist("\n\t%v: No stored min sequence to load", kv.me)
	}

	// Read config number from database if it exists
	configNumBytes, err := kv.db.Get(kv.dbReadOptions, []byte("configNum"))
	if err == nil && len(configNumBytes) > 0 {
		// Decode the max instance
		DPrintfPersist("\n\t%v: Decoding config num... ", kv.me)
		bufferConfigNum := *bytes.NewBuffer(configNumBytes)
		decoder := gob.NewDecoder(&bufferConfigNum)
		var configNumDecoded int
		err = decoder.Decode(&configNumDecoded)
		if err != nil {
			DPrintfPersist("\terror decoding: %s", fmt.Sprint(err))
		} else {
			kv.config = kv.sm.Query(configNumDecoded)
			if kv.config.Num != configNumDecoded {
				kv.dbLock.Unlock()
				kv.dbWriteConfigNum(kv.config.Num)
				kv.dbLock.Lock()
			}
			DPrintfPersist("\tsuccess")
		}
	} else {
		DPrintfPersist("\n\t%v: No stored config num to load", kv.me)
	}
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
	servers []string, me int, network bool) *ShardKV {
	gob.Register(Op{})

	kv := new(ShardKV)
	// Network stuff
	kv.me = me
	kv.network = network

	// ShardKV state
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters, kv.network)
	kv.config = kv.sm.Query(0)
	kv.store = make(map[string]string)
	kv.response = make(map[int64]string)
	kv.minSeq = -1

	// Peristence stuff
	kv.dbInit()

	rpcs := rpc.NewServer()
	if !printRPCerrors {
		disableLog()
		rpcs.Register(kv)
		enableLog()
	} else {
		rpcs.Register(kv)
	}

	kv.px = paxos.Make(servers, me, rpcs, kv.network, "shardkv_"+fmt.Sprint(kv.gid))

	if kv.network {
		l, e := net.Listen("tcp", strconv.Itoa(startport+me))
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		kv.l = l
	} else {
		os.Remove(servers[me])
		l, e := net.Listen("unix", servers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		kv.l = l
	}

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.dead == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.dead == false {
				if kv.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.unreliable && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					if kv.network {
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
			if err != nil && kv.dead == false {
				fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
				kv.Kill()
			}
		}
	}()

	go func() {
		for kv.dead == false {
			kv.tick()
			time.Sleep(250 * time.Millisecond)
		}
	}()

	return kv
}

type NullWriter int

func (NullWriter) Write([]byte) (int, error) { return 0, nil }

func enableLog() {
	log.SetOutput(os.Stderr)
}

func disableLog() {
	log.SetOutput(new(NullWriter))
}
