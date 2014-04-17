package kvpaxos

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

import crand "crypto/rand"
import "math/big"
func nrand() int64 {
  max := big.NewInt(int64(1) << 62)
  bigx, _ := crand.Int(crand.Reader, max)
  x := bigx.Int64()
  return x
}

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Name string
	Seq int
	Key string
	Val string
	Token int64
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	px         *paxos.Paxos

	// Your definitions here.
	data       map[string]string
	pvs        map[string]string

	logSize   int

	ops        map[int64]string
	logOps     map[int64]bool

	logLock sync.Mutex	

	oldestUpdateNeeded int
}

func min(x int, y int) int {
	if x < y {return x}
	return y
}

func max( x int, y int) int {
	if x > y {return x}
	return y
}

func (kv *KVPaxos) UpdateLog(seq int) {
	kv.logLock.Lock()
	defer kv.logLock.Unlock()
	if kv.px.Max() == kv.logSize {
		return
	}
	for i := kv.logSize + 1; i <= min(seq, kv.px.Max());  {
		found, tmp := kv.StatusBackingOff(i)
		if !found {
			if kv.dead { return; }
			kv.px.Start(i, Op{Name:"Nop", Seq:i})
			time.Sleep(100*time.Millisecond)
			continue
		}
		v := tmp.(Op)
		if !kv.logOps[v.Token] {

			if v.Name == "Put" { 
//				kv.pvs[v.Key] = kv.data[v.Key]
				kv.data[v.Key] = v.Val 
			}
			if v.Name == "PutHash" { 
				newhash := uint64(hash(kv.data[v.Key] + v.Val))
				kv.pvs[v.Key] = kv.data[v.Key]
				kv.data[v.Key] = strconv.FormatUint(newhash, 10)
			}
			if kv.me == 0 {
//				fmt.Println("reading log with token ", v.Token, v.Key, v.Val, kv.data[v.Key], i)
			}
		} else {
			if kv.me == 0 {
//				fmt.Println("skipping log with token ", v.Token, v.Key, kv.data[v.Key], i)
			}
		}
		kv.logOps[v.Token] = true
		kv.logSize = i		
		kv.px.Done(i)
		i++
	}
}

func (op *Op) matches(other *Op) bool {
	if (op.Seq != other.Seq) { return false; }
	if (op.Name != other.Name) { return false; }
	if (op.Key != other.Key) { return false; }
	if (op.Name == "Get") { return true; }
	if (op.Val != other.Val) { return false; }
	return true;
}

func (kv *KVPaxos) SendOp(op Op) int {
	ch :=make(chan int)
	go kv.DoSendOp(op, ch)
	if kv.dead {return 0}
	s := <-ch
	return s
}
func (kv *KVPaxos) StatusBackingOff(seq int) (bool, interface{}) {
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
		if to < 10 * time.Second {
			to *= 2
		} else { 
			return false, nil; 
		}
	}
	return true, v
}

func (kv *KVPaxos) DoSendOp(op Op, ch chan int) {
	done := false
	var v Op
	var seq int
	for !done {
		if kv.dead {break}
		seq = kv.px.Max() + 1
		op.Seq = seq
		kv.px.Start(seq, op)
		found, tmp := kv.StatusBackingOff(seq)
		if (!found) { 
			break
		}
		v = tmp.(Op)
		if (op.matches(&v)) { done = true;  }
	}
	ch <- seq
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
//	fmt.Println("Get", args)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Done = true
	val, retry := kv.ops[args.Token]
	if retry {
		reply.Value = val
		reply.Err = ""
		return nil
	}
	op := Op{Name:"Get", Seq:0, Key:args.Key, Token:args.Token}
	seq := kv.SendOp(op)
	if kv.dead {return nil}
	kv.UpdateLog(seq)
	reply.Value = kv.data[args.Key]	
	kv.ops[args.Token] = reply.Value
	reply.Err = ""
	// Your code here.
	return nil
 }

func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
//	fmt.Println("Put", args)
	reply.Done = true
	pv, retry := kv.ops[args.Token]
	if retry {
		reply.Err = ""
		reply.PreviousValue = pv
		fmt.Println("skipping")
		return nil
	}
	op := Op{Name:"Put", Seq:0, Key:args.Key, Val:args.Value, Token:args.Token}
	if args.DoHash {
		op.Name = "PutHash"
	}
	seq := kv.SendOp(op)
	if kv.dead {return nil}
//	if args.DoHash {
//		fmt.Println(args, kv.pvs[args.Key], seq, kv.logSize, kv.me)
//	}
	kv.UpdateLog(seq)
	reply.PreviousValue = kv.pvs[args.Key]
	kv.ops[args.Token] = reply.PreviousValue
	reply.Err = ""
	return nil
}

// tell the server to shut itself down.
// please do not change this function.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	kv.dead = true
	kv.l.Close()
	kv.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me

	// Your initialization code here.
	kv.data = make(map[string]string)
	kv.pvs = make(map[string]string)
	kv.ops = make(map[int64]string)
	kv.logOps = make(map[int64]bool)
	kv.logSize = -1

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
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
