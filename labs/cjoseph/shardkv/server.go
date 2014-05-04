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

//import "errors"
import "strings"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const (
	Get     = "Get"
	Put     = "Put"
	PutHash = "PutHash"
	Reconf  = "Reconf"
	Noop    = "Noop"
	Old     = "Old"
)

type Op struct {
	// Your definitions here.
	Type       string
	ID         string
	GetArgs    *GetArgs
	PutArgs    *PutArgs
	ReconfArgs *ReconfArgs

	GetReply *GetReply
	PutReply *PutReply
	Replay   bool
}

type ShardKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos

	gid int64 // my replica group ID

	contOK chan bool
	// Your definitions here.
	config         *shardmaster.Config
	shards         *shards
	states         *clientStates
	highestApplied int              //the highest paxos operation applied to our loggy log
	outstanding    map[int]chan *Op //requests we have yet to respond to
	count          int
}

func (kv *ShardKV) Lock(reason string) {
	//DPrintf("%d.%d locking %s", kv.me, kv.gid,reason)
	kv.mu.Lock()
	//DPrintf("%d.%d locking %s SUCCESS", kv.me, kv.gid,reason)
}

func (kv *ShardKV) Unlock(reason string) {
	//DPrintf("%d.%d UNlocking %s", kv.me, kv.gid, reason)
	kv.mu.Unlock()
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	kv.Lock("beginning of Get")
	if kv.config.Num == 0 {
		reply.Err = Err(ErrWrongGroup)
		kv.Unlock("leaving Geet because config is 0")
		return nil
	}
	kv.Unlock("Get about to call SeqChanID")
	seq, c := kv.seqChan()
	ID := args.From + "-" + strconv.Itoa(args.CliSeq)
	DPrintf("%d.%d got GET(%s) ID=%s", kv.me, kv.gid, args.Key, ID)
	proposedVal := Op{Type: Get, ID: ID, GetArgs: args, GetReply: reply, Replay: false}

	kv.px.Start(seq, proposedVal)
	acceptedVal := <-c

	//start over if it wasn't what we proposed
	if proposedVal.ID != acceptedVal.ID { //is this comparison sufficient?
		kv.Lock("Get deleting outstanding chan entry for recall")
		delete(kv.outstanding, seq)
		kv.Unlock("Get being re-called because proposed ID != accepted ID")
		kv.contOK <- true
		kv.Get(args, reply)
		return nil
	}

	reply.Err = acceptedVal.GetReply.Err
	reply.Value = acceptedVal.GetReply.Value
	if acceptedVal.Replay {
		reply.ValueHistory = "replay " + acceptedVal.GetReply.ValueHistory
	} else {
		reply.ValueHistory = acceptedVal.GetReply.ValueHistory
	}

	//housecleaning
	kv.Lock("Get deleting outstanding chan for RETURN")
	delete(kv.outstanding, seq)
	DPrintf("%d.%d returning from GET(%s) during %s  with '%s' ID=%s", kv.me, kv.gid, args.Key, kv.config.Num, reply.Value, ID)
	kv.Unlock("Get releasing and RETURNING")
	kv.contOK <- true

	return nil
}

func (kv *ShardKV) Put(args *PutArgs, reply *PutReply) error {
	kv.Lock("beginning of Put")
	if kv.config.Num == 0 {
		reply.Err = Err(ErrWrongGroup)
		kv.Unlock("leaving Put because config was 0")
		return nil
	}
	kv.Unlock("Put about to call SeqChanID")
	seq, c := kv.seqChan()
	ID := args.From + "-" + strconv.Itoa(args.CliSeq)

	t := Put
	if args.DoHash {
		t = PutHash
		DPrintf("%d.%d got PUTHASH(%s, %s) ID=%s", kv.me, kv.gid, args.Key,
			args.Value, ID)
	} else {
		DPrintf("%d.%d got PUT(%s, %s) ID=%s", kv.me, kv.gid, args.Key,
			args.Value, ID)
	}
	proposedVal := Op{Type: t, ID: ID, PutArgs: args, PutReply: reply, Replay: false}
	kv.px.Start(seq, proposedVal)
	acceptedVal := <-c

	//start over if it wasn't what we proposed
	if proposedVal.ID != acceptedVal.ID { //is this comparison sufficient?
		kv.Lock("Put about to delete outstanding chan for recall")
		delete(kv.outstanding, seq)
		kv.Unlock("Put about to be recalled because proposed != accepted")
		kv.contOK <- true
		kv.Put(args, reply)
		return nil
	}

	reply.Err = acceptedVal.PutReply.Err
	reply.PreviousValue = acceptedVal.PutReply.PreviousValue
	if acceptedVal.Replay {
		reply.ValueHistory = "replay " + acceptedVal.PutReply.ValueHistory
	} else {
		reply.ValueHistory = acceptedVal.PutReply.ValueHistory
	}

	//housecleaning
	kv.Lock("Put about to delete outstanding for RETURN")
	delete(kv.outstanding, seq)
	DPrintf("%d.%d returning from %s(%s, %s) during %d with '%s' ID=%s", kv.me,
		kv.gid, t, args.Key, args.Value, kv.config.Num, reply.PreviousValue, ID)

	kv.Unlock("Put releasing for last time because RETURNING")
	kv.contOK <- true

	return nil
}

func (kv *ShardKV) Reconf(args *ReconfArgs, reply *ReconfReply) error {
	seq, c := kv.seqChan()
	ID := "reconf" + strconv.Itoa(args.Config.Num)
	proposedVal := Op{Type: Reconf, ID: ID, ReconfArgs: args, Replay: false}
	kv.px.Start(seq, proposedVal)
	acceptedVal := <-c

	//start over if it wasn't what we proposed
	if proposedVal.ID != acceptedVal.ID { //is this comparison sufficient?
		kv.Lock("Reconf deleting outstanding for rerun")
		delete(kv.outstanding, seq)
		kv.Unlock("Reconf about to rerun because proposed != accepted")
		kv.contOK <- true

		kv.Reconf(args, reply)
		return nil
	}

	//housecleaning
	kv.Lock("Reconf deleting outstanding for RETURN")
	delete(kv.outstanding, seq)
	kv.Unlock("reconf RETURNING")
	kv.contOK <- true
	return nil
}

func (kv *ShardKV) GetShard(args *GetShardArgs, reply *GetShardReply) error {
	kv.Lock("GetShard obtaining lock")
	defer kv.Unlock("GetShard releasing lock")
	//return if we've
	if kv.config.Num < args.Config {
		reply.Err = ErrNoKey
		return nil
	}
	if args.Shard <= shardmaster.NShards {
		reply.Shard = kv.shards.getShard(args.Shard, args.Config)
		reply.ClientState = kv.states.getState(args.Shard, args.Config)
	} else {
		log.Fatalf("Fatal! Invalid shard number %s", args.Shard)
	}
	return nil
}

func (kv *ShardKV) makeID() string {
	kv.Lock("makeID start")
	defer kv.Unlock("makeID end")
	ID := strconv.Itoa(kv.me) + "." + strconv.FormatInt(kv.gid, 10) + "-" + strconv.Itoa(kv.count)
	kv.count++
	return ID
}

func (kv *ShardKV) seqChan() (int, chan *Op) {
	kv.Lock("SeqChanID start")
	defer kv.Unlock("SeqChanID end")
	seq := kv.px.Max() + 1
	for _, ok := kv.outstanding[seq]; ok || kv.highestApplied >= seq; {
		seq++
		_, ok = kv.outstanding[seq]
	}
	c := make(chan *Op)
	kv.outstanding[seq] = c
	return seq, c
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
	kv.Lock("tick starting")
	latest := kv.sm.Query(-1)
	for kv.config.Num < latest.Num {
		next := kv.sm.Query(kv.config.Num + 1)
		var r ReconfReply
		kv.Unlock("tick unlocking to run reconf" + strconv.Itoa(next.Num))
		kv.Reconf(&ReconfArgs{next}, &r)
		kv.Lock("tick locking because returned reconf" + strconv.Itoa(next.Num))
	}
	kv.Unlock("tick ending")
}

//only call this while lock is held
func (kv *ShardKV) checkKey(key string) (int, bool) {
	shard := key2shard(key)
	gid := kv.config.Shards[shard]
	if gid == kv.gid {
		return shard, true
	} else {
		return 0, false
	}
}

//only call this while lock is held
func (kv *ShardKV) apply(seq int, op *Op) {
	if dup, d := kv.duplicateCheck(op); !dup {
		switch op.Type {
		case Get:
			key := op.GetArgs.Key
			if s, ok := kv.checkKey(key); ok {
				DPrintf("%d.%d decided Get(%s) ID = %s",
					kv.me, kv.gid, op.GetArgs.Key, op.ID)

				v, err := kv.shards.get(key, op.ID)
				hist := kv.shards.getHistory(key)
				if err != nil {
					if err.Error() == ErrNoKey {
						op.GetReply.Value = ""
						op.GetReply.Err = Err(ErrNoKey)
					} else {
						DPrintf("Should never happen?")
					}
				} else {
					op.GetReply.Value = v
					op.GetReply.ValueHistory = hist
					kv.states.put(s, op.GetArgs.From, op)
				}
			} else {
				op.GetReply.Err = Err(ErrWrongGroup)
			}
		case Put:
			key := op.PutArgs.Key
			val := op.PutArgs.Value
			if s, ok := kv.checkKey(key); ok {
				DPrintf("%d.%d decided Put(%s,%s) ID = %s", kv.me,
					kv.gid, op.PutArgs.Key, op.PutArgs.Value, op.ID)
				src := strconv.Itoa(kv.me) + "." + strconv.FormatInt(kv.gid, 10)
				err := kv.shards.put(key, val, op.ID, src)
				hist := kv.shards.getHistory(key)
				if err != nil {
					DPrintf("Should never happen?")
				}
				op.PutReply.ValueHistory = hist
				kv.states.put(s, op.PutArgs.From, op)
			} else { //wrong group, reply with err
				op.PutReply.Err = Err(ErrWrongGroup)
			}
		case PutHash:
			key := op.PutArgs.Key
			val := op.PutArgs.Value
			if s, ok := kv.checkKey(key); ok {
				DPrintf("%d.%d decided PutHash(%s,%s) ID = %s", kv.me,
					kv.gid, op.PutArgs.Key, op.PutArgs.Value, op.ID)
				src := strconv.Itoa(kv.me) + "." + strconv.FormatInt(kv.gid, 10)
				prev, err := kv.shards.puthash(key, val, op.ID, src)
				if err != nil {
					DPrintf("Should never happen?")
				}
				op.PutReply.PreviousValue = prev
				hist := kv.shards.getHistory(key)
				op.PutReply.ValueHistory = hist
				kv.states.put(s, op.PutArgs.From, op)
			} else {
				op.PutReply.Err = Err(ErrWrongGroup)
			}
		case Reconf:
			old := kv.config
			fresh := op.ReconfArgs.Config
			if old.Num+1 < fresh.Num {
				log.Fatalf("apply() Fatal! Trying to skip too many configs! %d to %d", old.Num, fresh.Num)
			} else if old.Num+1 == fresh.Num {
				kv.reconfigure(old, &fresh)
				kv.config = &fresh
			} //if old.Num == fresh.Num, do nothing
		case Noop: //do nothing
		default:
			log.Fatalf("Fatal! Invalid Op type %s", op.Type)
		}
	} else {
		if d.Type == Old { //a very stale request, cannot replay
			///some sort of error?
			op.GetReply = &GetReply{Err: Err(ErrAncient)}
			op.PutReply = &PutReply{Err: Err(ErrAncient)}
		} else {
			//			DPrintf("%d.%d about to PutHash (%s, %s) during config %d",
			//				kv.me, kv.gid, key, val, kv.config.Num)
			//DPrintf("%d.%d found a duplicate. dID = %s pID=%s",
			//				kv.me, kv.gid, d, op)
			if op.ID == d.ID {
				DPrintf("%d.%d replayed request %s", kv.me, kv.gid, d.ID)
			}
			//copy committed op fields
			op.Type = d.Type
			op.ID = d.ID
			op.GetArgs = d.GetArgs
			op.PutArgs = d.PutArgs
			op.ReconfArgs = d.ReconfArgs
			op.GetReply = d.GetReply
			op.PutReply = d.PutReply
			op.Replay = true
		}
	}
}

func extractCliSeq(ID string) int {
	seq, _ := strconv.Atoi(ID[strings.IndexAny(ID, "-")+1 : len(ID)])
	return seq
}

//only call this while lock is held
func (kv *ShardKV) duplicateCheck(op *Op) (bool, *Op) {
	if kv.config.Num == 0 {
		return false, &Op{}
	}

	var from string
	var CliSeq int
	DupCliSeq := -1
	var err error
	var duplicate *Op

	switch op.Type {
	case Get:
		from = op.GetArgs.From
		CliSeq = op.GetArgs.CliSeq
		key := op.GetArgs.Key
		duplicate, err = kv.states.get(key2shard(key), from, "")
		if err == nil {
			DupCliSeq = extractCliSeq(duplicate.ID)
		} else if err.Error() != ErrNoKey {
			DPrintf("Weird status check error in dupCheck of a Get")
		}

	case Put:
		from = op.PutArgs.From
		CliSeq = op.PutArgs.CliSeq
		key := op.PutArgs.Key
		duplicate, err = kv.states.get(key2shard(key), from, "")
		if err == nil {
			DupCliSeq = extractCliSeq(duplicate.ID)
		} else if err.Error() != ErrNoKey {
			DPrintf("Weird status check error in dupCheck of a Put")
		}
	case PutHash:
		from = op.PutArgs.From
		CliSeq = op.PutArgs.CliSeq
		key := op.PutArgs.Key
		duplicate, err = kv.states.get(key2shard(key), from, fmt.Sprintf("%d.%d checking for duplicate PutHash(%s,%s) during config %d",
			kv.me, kv.gid, key, op.PutArgs.Value, kv.config.Num))
		if err == nil {
			DupCliSeq = extractCliSeq(duplicate.ID)
		} else if err.Error() != ErrNoKey {
			DPrintf("Weird status check error in dupCheck of a PutHash")
		}

	case Reconf:
		if op.ReconfArgs.Config.Num <= kv.config.Num {
			DPrintf("Duplicate found for config %d. Current is %d", op.ReconfArgs.Config.Num, kv.config.Num)
			return true, &Op{ID: op.ID}
		} else {
			return false, &Op{}
		}
	case Noop:
		return false, &Op{Type: Old}
	default:
		log.Fatalf("Fatal! Invalid Op type %s", op.Type)
	}

	if err == nil {
		//check responses map
		if DupCliSeq == CliSeq {
			return true, duplicate
		} else if CliSeq < DupCliSeq {
			return true, &Op{}
		}
	}
	return false, &Op{}
}

//only call this while lock is held
func (kv *ShardKV) reconfigure(old *shardmaster.Config, fresh *shardmaster.Config) {
	shards := make([]map[string]string, shardmaster.NShards)
	states := make([]map[string]*Op, shardmaster.NShards)
	src := strconv.Itoa(kv.me) + "." + strconv.FormatInt(kv.gid, 10)
	//just change it w/out shard xfer, if this is the first real config
	if old.Num == 0 {
		kv.shards.updateLatest(fresh.Num, shards, src)
		kv.states.updateLatest(fresh.Num, states, src)
		return
	}
	DPrintf("collecting shards for update from %d to %d. Current is %d", old.Num, fresh.Num, kv.config.Num)
	//figure out which shards need to be requested, then send RPC
	for s, g := range fresh.Shards {
		oldGID := old.Shards[s]

		if g == kv.gid && oldGID != g {
			args := &GetShardArgs{Config: fresh.Num, Shard: s}
			var reply *GetShardReply
			servers := old.Groups[oldGID]

			ok := false
			err := Err("")
			for !ok || err != "" { //keep going till we get our shard
				i := rand.Int() % len(servers)
				reply = &GetShardReply{}
				kv.Unlock("reconfugre releasing before getshard rpc")
				ok = call(servers[i], "ShardKV.GetShard",
					args, &reply)
				if ok {
					err = reply.Err
				} else {
					time.Sleep(250 * time.Millisecond)
				}
				kv.Lock("reconfigure reacquiring lock")
			}
			shards[s] = reply.Shard
			states[s] = reply.ClientState
		}

	}
	DPrintf("about to updateLatest from %d to %d. Current is %d", old.Num, fresh.Num, kv.config.Num)
	kv.shards.updateLatest(fresh.Num, shards, src)
	kv.states.updateLatest(fresh.Num, states, src)
}

func (kv *ShardKV) getStatus() {
	seq := 0
	restarted := false
	//TA suggests 10-20ms max before proposing noop
	to := 10 * time.Millisecond
	for !kv.dead {
		decided, r := kv.px.Status(seq)
		if decided {
			//add to log and notify channels
			op, isOp := r.(Op)
			if isOp {
				kv.Lock("GetStatus checking outstanding for channel for" + op.Type)
				kv.apply(seq, &op)
				if ch, ok := kv.outstanding[seq]; ok {
					kv.Unlock("GetStatus unlocking to notify channel" + op.Type)
					ch <- &op //notify handler
					<-kv.contOK
				} else {
					kv.Unlock("GetStatus unlocking because no channel found" + op.Type)
				}
			} else {
				log.Fatal("Fatal!could not cast Op")
			}
			seq++
			restarted = false
		} else {
			time.Sleep(to)
			if to < 25*time.Millisecond {
				to *= 2
			} else { //if we've slept long enough, propose a noop
				if !restarted {
					to = 10 * time.Millisecond
					kv.px.Start(seq, Op{Type: Noop})
					time.Sleep(time.Millisecond)
					restarted = true
				}
			}
		}
	}
}

// tell the server to shut itself down.
func (kv *ShardKV) kill() {
	kv.dead = true
	kv.l.Close()
	kv.px.Kill()
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
	servers []string, me int) *ShardKV {
	gob.Register(Op{})
	DPrintf("%d.%d starting", me, gid)
	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)
	kv.contOK = make(chan bool)

	// Your initialization code here.
	// Don't call Join().
	kv.config = &shardmaster.Config{}
	kv.shards = &shards{shards: make([]map[int]map[string]string,
		shardmaster.NShards), latest: 0}
	for i, _ := range kv.shards.shards {
		kv.shards.shards[i] = make(map[int]map[string]string)
	}
	kv.states = &clientStates{states: make([]map[int]map[string]*Op,
		shardmaster.NShards), latest: 0}
	for i, _ := range kv.states.states {
		kv.states.states[i] = make(map[int]map[string]*Op)
	}
	kv.outstanding = make(map[int]chan *Op)
	kv.highestApplied = -1
	kv.count = 0

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

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
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
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
				kv.kill()
			}
		}
	}()

	go func() {
		for kv.dead == false {
			kv.tick()
			time.Sleep(250 * time.Millisecond)
		}
	}()

	go kv.getStatus()
	return kv
}
