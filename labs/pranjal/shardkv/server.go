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

import crand "crypto/rand"
import "math/big"
func nrand() int64 {
  max := big.NewInt(int64(1) << 62)
  bigx, _ := crand.Int(crand.Reader, max)
  x := bigx.Int64()
  return x
}


const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	Type string
	UID int64
	Key string
	Value string
	DoHash bool
	Config shardmaster.Config
	Map map[string]string
}

type ShardKV struct {
	mu         sync.Mutex
	send_mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos

	gid int64 // my replica group ID
	uid string
	
	// Your definitions here.
	config     shardmaster.Config
	logSize int
	logged map[int64]bool
	values map[string]string
	servers []string
	inTick int
}

func (kv *ShardKV) update(seq int) (string, int) {
	err := 0
//	fmt.Println(kv.uid, "entering update")
	if seq == -1 {
		seq = kv.px.Max()
	}
	
	if seq <= kv.logSize {
		return "invalid", 1
	}
	var retval string
	for kv.logSize < seq {
		err = 0
		i := kv.logSize + 1
		kv.logSize = i
		found, tmp := kv.StatusBackingOff(i - 1)
		for !found {
			if kv.dead { return "dead", 1; }
			kv.px.Start(i, Op {Type: "Nop"})
			time.Sleep(100*time.Millisecond)
			found, _ = kv.StatusBackingOff(i - 1)
		}
		op := tmp.(Op)
		fmt.Println(kv.uid, "Processing", i, op)
		if kv.logged[op.UID] {
			continue
		}
		kv.logged[op.UID] = true
		if op.Type == "Get" {
			fmt.Println(kv.uid, op.Key, "from shard", key2shard(op.Key), kv.gid, kv.values[op.Value])
			if kv.config.Shards[key2shard(op.Key)] == kv.gid {
				retval = kv.values[op.Value]
			}  else {
				fmt.Println(kv.uid, "Wrong shard")
				err = 1
				retval = "Wrong shard"
			}
		} else if op.Type == "Put" {			
			fmt.Println(kv.uid, op.Key, "from shard", key2shard(op.Key), kv.gid)
			if kv.config.Shards[key2shard(op.Key)] == kv.gid {
				if op.DoHash {
					newhash := uint64(hash(kv.values[op.Key] + op.Value))
					retval = kv.values[op.Key]
					//				fmt.Println("retval: ", retval)
					kv.values[op.Key] = strconv.FormatUint(newhash, 10)
				} else {
					kv.values[op.Key] = op.Value
				}
			} else {
				fmt.Println(kv.uid, "Wrong shard")
				err = 1
				retval = "Wrong shard"
			}
		} else if op.Type == "Reconfig" {
			kv.config = op.Config
			for k, v := range(op.Map) {
				kv.values[k] = v
			}
		}
		kv.px.Done(kv.logSize)
	}
	fmt.Println(kv.uid, "Done Processing", kv.logSize)
	return retval, err
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.update(-1)
	seq := kv.SendOp(Op{Type:"Get", Key:args.Key})
	kv.update(seq + 1)
	reply.Value = kv.values[args.Key]
	reply.Err = OK
	return nil
}

func (kv *ShardKV) Put(args *PutArgs, reply *PutReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.update(-1)
	// Your code here.
	op := Op{Type:"Put", Key:args.Key, Value:args.Value, DoHash:args.DoHash}
	fmt.Println("sending op", op)
	seq := kv.SendOp(op)
	rv, err := kv.update(seq+1)
	if err > 0 {
		reply.Err = ErrWrongGroup
	} else {
		reply.PreviousValue = rv
		reply.Err = OK
	}
	return nil
}

type GetShardArgs struct {
	Id int
	Config shardmaster.Config
}
type GetShardReply struct {
	Vals map[string]string
}

func (kv *ShardKV) GetShard(args *GetShardArgs, reply *GetShardReply) error {	
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if (kv.config.Num < args.Config.Num) {
		kv.update(-1)
		for kv.sm.Query(-1).Num > kv.config.Num {
			newconf := kv.sm.Query(kv.config.Num + 1)
			kv.inTick = newconf.Num
			kv.reconfig(newconf)
		}
	}
	kv.update(-1)
	reply.Vals = make(map[string]string)
	for k, v := range(kv.values) {
		if key2shard(k) == args.Id{
			reply.Vals[k] = v
		}
	}
	return nil
}

func (kv *ShardKV) fetchShard(servers []string, shard int, conf shardmaster.Config) map[string]string {
	reply := GetShardReply{}
	args := GetShardArgs{Id: shard, Config: conf}
	output := make(map[string]string)
	for _, server := range(servers) {
		if kv.servers[kv.me] != server && call(server, "ShardKV.GetShard", args, &reply) {
			for k, v := range(reply.Vals) {
				output[k] = v
			}
		}
	}
	fmt.Println(kv.uid, "got from fetch", shard, output)
	return output
}


func (kv *ShardKV) reconfig(newconf shardmaster.Config) {
	current := kv.config
	newvals := make(map[string]string)
	for shard, gid := range(newconf.Shards) {
		if gid == kv.gid && kv.config.Shards[shard] != kv.gid {
			
			m := kv.fetchShard(current.Groups[current.Shards[shard]], shard, newconf)
//			kv.mu.Lock()
			for k, v := range(m) {
				newvals[k] = v
			}
//			kv.mu.Unlock()
		} 
	}
	kv.SendOp(Op{Type:"Reconfig", Map:newvals, Config:newconf})
	fmt.Println(kv.uid, "Updated config from",  kv.config, "to", newconf, "with", newvals)
//	kv.mu.Lock()
	kv.update(-1)
//	kv.mu.Unlock()	
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {	
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.update(-1)
	for kv.sm.Query(-1).Num > kv.config.Num {
		newconf := kv.sm.Query(kv.config.Num + 1)
		kv.inTick = newconf.Num
		kv.reconfig(newconf)
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

	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.uid = strconv.Itoa(me) + string("-") + strconv.FormatInt(gid, 10)
	kv.sm = shardmaster.MakeClerk(shardmasters)
	kv.servers = servers

	// Your initialization code here.
	kv.logged = make(map[int64]bool)
	kv.values = make(map[string]string)
	// Don't call Join().

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

	return kv
}
//stuff from previous lab...


func (kv *ShardKV) SendOp(op Op) int {
	ch :=make(chan int)	
	s := kv.DoSendOp(op, ch)
	if kv.dead {return 0}

	return s
}
func (kv *ShardKV) StatusBackingOff(seq int) (bool, interface{}) {
	decided := false	
	to := 10 * time.Millisecond
	var v interface{}
	for !decided {	
		if kv.dead {return false, nil}
		decided, v = kv.px.Status(seq)
		if (decided) {
			break
		}	
		time.Sleep(to)
		if to < 250 * time.Millisecond {
			to *= 2
		} else { 
			return false, nil; 
		}
	}
	return true, v
}

func (kv *ShardKV) DoSendOp(op Op, ch chan int) int {
	done := false
	var v Op
	var seq int
	for !done {
		if kv.dead {break}
		seq = kv.px.Max() + 1
		op.UID = nrand()
		kv.px.Start(seq, op)
		found, tmp := kv.StatusBackingOff(seq)
//		if (!found) { 
//			break
//		}
		if (found) {
			v = tmp.(Op)
			if (op.UID == v.UID) { done = true;  }
		}
	}
//	fmt.Println("Sending", seq, op.UID, v.UID)
	return seq
}


func min(x int, y int) int {
	if x < y {return x}
	return y
}

func max( x int, y int) int {
	if x > y {return x}
	return y
}
