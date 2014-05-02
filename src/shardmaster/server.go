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

const Debug = 0
const printRPCerrors = false
const startport = 2100

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	px         *paxos.Paxos
	network    bool
	configs      []Config // indexed by config num
	processedSeq int
	maxConfig    int
}

type Op struct {
	Op      int //1 = Query, 2 = Join, 3 = Leave, 4 = Move
	GID     int64
	Servers []string
	Shard   int
}

func (sm *ShardMaster) balance(gids []int64, shards [NShards]int64) [NShards]int64 {
	DPrintf("%d) Balance with %d and %d\n", sm.me, gids, shards)
	expectedGPS := (NShards / len(gids))
	if expectedGPS <= 0 {
		expectedGPS = 1
	}
	var newShards [NShards]int64
	num := make(map[int64]int)
	var over []int

	for k, v := range shards {
		num[v] = num[v] + 1
		newShards[k] = v
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
	for _, k := range gids {
		if k == 0 {
			continue
		}
		for num[k] < expectedGPS && len(over) > 0 {
			newShards[over[0]] = k
			num[k] += 1
			over = over[1:len(over)]
		}
	}

	DPrintf("%d) Balance returns %d\n", sm.me, newShards)
	return newShards
}

func (sm *ShardMaster) createJoinConfig(gid int64, servers []string) {
	oldConfig := sm.configs[sm.maxConfig]
	sm.maxConfig = sm.maxConfig + 1
	newConfig := Config{}
	newConfig.Num = sm.maxConfig
	newConfig.Groups = map[int64][]string{}
	var gids []int64

	for k, v := range oldConfig.Groups {
		if k != gid && k != 0 {
			gids = append(gids, k)
			newConfig.Groups[k] = v
		}
	}
	gids = append(gids, gid)
	newConfig.Groups[gid] = servers

	newConfig.Shards = sm.balance(gids, oldConfig.Shards)

	sm.configs = append(sm.configs, newConfig)
}

func (sm *ShardMaster) createLeaveConfig(gid int64) {
	oldConfig := sm.configs[sm.maxConfig]
	sm.maxConfig = sm.maxConfig + 1
	newConfig := Config{}
	newConfig.Num = sm.maxConfig
	newConfig.Groups = map[int64][]string{}
	var gids []int64

	for k, v := range oldConfig.Groups {
		if k != gid && k != 0 {
			gids = append(gids, k)
			newConfig.Groups[k] = v
		}
	}

	newConfig.Shards = sm.balance(gids, oldConfig.Shards)

	sm.configs = append(sm.configs, newConfig)
}

func (sm *ShardMaster) createMoveConfig(gid int64, shard int) {
	oldConfig := sm.configs[sm.maxConfig]
	sm.maxConfig = sm.maxConfig + 1
	newConfig := Config{}
	newConfig.Num = sm.maxConfig
	newConfig.Groups = map[int64][]string{}

	for k, v := range oldConfig.Shards {
		if k == shard {
			newConfig.Shards[k] = gid
		} else {
			newConfig.Shards[k] = v
		}
	}

	for k, v := range oldConfig.Groups {
		newConfig.Groups[k] = v
	}

	sm.configs = append(sm.configs, newConfig)
}

func (sm *ShardMaster) processLog(maxSeq int) {
	if maxSeq <= sm.processedSeq+1 {
		return
	}
	DPrintf("%d) Process Log until %d\n", sm.me, maxSeq)

	for i := sm.processedSeq + 1; i < maxSeq; i++ {
		to := 10 * time.Millisecond
		start := false
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
			if to < 10*time.Second {
				to *= 2
			}
		}
	}
	sm.processedSeq = maxSeq - 1
	sm.px.Done(sm.processedSeq)
	DPrintf("%d) Done Process Log Until %d\n", sm.me, maxSeq)
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	sm.mu.Lock()
	DPrintf("%d) Join: %d -> %s\n", sm.me, args.GID, args.Servers)

	newOp := Op{2, args.GID, args.Servers, 0}

	for !sm.dead {
		seq := sm.px.Max() + 1
		sm.processLog(seq)

		sm.px.Start(seq, newOp)

		to := 10 * time.Millisecond
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
			if to < 10*time.Second {
				to *= 2
			}
		}
	}

	DPrintf("%d) Join Returns\n", sm.me)
	sm.mu.Unlock()
	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	sm.mu.Lock()
	DPrintf("%d) Leave: %d\n", sm.me, args.GID)

	newOp := Op{3, args.GID, nil, 0}

	for !sm.dead {
		seq := sm.px.Max() + 1
		sm.processLog(seq)

		sm.px.Start(seq, newOp)

		to := 10 * time.Millisecond
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
			if to < 10*time.Second {
				to *= 2
			}
		}
	}

	DPrintf("%d) Leave Returns\n", sm.me)
	sm.mu.Unlock()
	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	sm.mu.Lock()
	DPrintf("%d) Move: %d -> %d\n", sm.me, args.Shard, args.GID)

	newOp := Op{4, args.GID, nil, args.Shard}

	for !sm.dead {
		seq := sm.px.Max() + 1
		sm.processLog(seq)

		sm.px.Start(seq, newOp)

		to := 10 * time.Millisecond
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
			if to < 10*time.Second {
				to *= 2
			}
		}
	}

	DPrintf("%d) Move Returns\n", sm.me)
	sm.mu.Unlock()
	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	sm.mu.Lock()
	DPrintf("%d) Query: %d\n", sm.me, args.Num)

	newOp := Op{1, int64(args.Num), nil, 0}

	for !sm.dead {
		seq := sm.px.Max() + 1
		sm.processLog(seq)
		if args.Num > sm.maxConfig {
			newOp = Op{1, -1, nil, 0}
		}
		sm.px.Start(seq, newOp)

		to := 10 * time.Millisecond
		for !sm.dead {
			decided, _ := sm.px.Status(seq)
			if decided {
				sm.processLog(seq + 1)
				if args.Num >= 0 && args.Num < sm.maxConfig {
					reply.Config = sm.configs[args.Num]
				} else {
					reply.Config = sm.configs[sm.maxConfig]
				}
				DPrintf("%d) Query Returns %d\n", sm.me, reply.Config)
				sm.mu.Unlock()
				return nil
			}
			time.Sleep(to)
			if to < 10*time.Second {
				to *= 2
			}
		}
	}

	if args.Num >= 0 && args.Num < sm.maxConfig {
		reply.Config = sm.configs[args.Num]
	} else {
		reply.Config = sm.configs[sm.maxConfig]
	}
	DPrintf("%d) Query Returns %d\n", sm.me, reply.Config)
	sm.mu.Unlock()
	return nil
}

// please don't change this function.
func (sm *ShardMaster) Kill() {
	sm.dead = true
	sm.l.Close()
	sm.px.Kill()
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
	sm.me = me
	sm.network = network
	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}

	sm.processedSeq = -1
	sm.maxConfig = 0

	rpcs := rpc.NewServer()
	if !printRPCerrors {
		disableLog()
		rpcs.Register(sm)
		enableLog()
	} else {
		rpcs.Register(sm)
	}

	sm.px = paxos.Make(servers, me, rpcs, network)

	if sm.network {
		l, e := net.Listen("tcp", ":"+strconv.Itoa(startport+me))
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
				if sm.unreliable && (rand.Int63()%1000) < 100 {
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
