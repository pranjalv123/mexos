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

import crand "crypto/rand"
import "math/big"
func nrand() int64 {
  max := big.NewInt(int64(1) << 62)
  bigx, _ := crand.Int(crand.Reader, max)
  x := bigx.Int64()
  return x
}


type ShardMaster struct {
	mu         sync.Mutex
	sendmu     sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	px         *paxos.Paxos

	configs []Config // indexed by config num
	logSize int
	
	logged     map[int64]bool
	
}

type Op struct {
	// Your data here.
	Type string
	GID int64
	Shard int
	Servers []string
	UID int64
}

func counts(shards []int64, groups []int64) map[int64]int {
	counts := make(map[int64]int)
	for _, j := range groups {
		counts[j] = 0
		for _, x := range shards {
			if x == j {
				counts[j] ++
			}
		}
	}
	return counts
}

func minval(counts map[int64]int) int {
	m := 9999
	for _, v := range counts {
		m = min(m, v)
	}
	return m
}

func maxval(counts map[int64]int) int {
	m := 0
	for _, v := range counts {
		m = max(m, v)
	}
	return m
}


func minind(counts map[int64]int) int64 {
	m := 9999
	var i int64
	for k, v := range counts {
		if m > v {
			i = k
			m = v
		}
	}
	return i
}

func maxind(counts map[int64]int) int64 {
	m := 0
	var i int64
	for k, v := range counts {
		if m < v {
			i = k
			m = v
		}
	}
	return i
}

func haszero(shards []int64) bool {
	for _, x := range shards {
		if x == 0 { return true }
	}
	return false
}

func rebalance(shards []int64, groups []int64) []int64 {
	c := counts(shards, groups)
	for i, j := range shards {
		if c[j] == 0 {
			shards[i] = 0
		}
	}
//	fmt.Println("rebalance")
//	fmt.Println(shards, groups, c)
	if (len(groups) == 1) {
		for i := range shards {
			shards[i] = groups[0]
		}
	}
	for (maxval(c) - minval(c) > 1) || (haszero(shards)) {
		increase := minind(c)
		decrease := maxind(c)
		if haszero(shards) {
			decrease = 0
		}
		for i, j := range shards {
			if j == decrease {
				shards[i] = increase
				break
			}
		}
		c = counts(shards, groups)
	}
//	fmt.Println(shards)
	return shards
}

func (sm *ShardMaster) update() {
	if sm.px.Max() == sm.logSize {
		return
	}
	for sm.logSize < sm.px.Max() {
		i := sm.logSize + 1
		sm.logSize = i
		found, tmp := sm.StatusBackingOff(i - 1) 
		if !found {
			if sm.dead { return; }
			sm.px.Start(i, Op {Type:"Nop"})
			time.Sleep(100*time.Millisecond) 
			continue
		}
		op := tmp.(Op)
		if sm.logged[op.UID] {
			continue
		} 
		sm.logged[op.UID] = true

		if op.Type == "Join" {	
			prev := sm.configs[len(sm.configs) - 1]
			c := Config{Num:prev.Num + 1}			
			c.Groups = make(map[int64][]string)
			for key, value := range prev.Groups {
				c.Groups[key] = value
			}			
			c.Groups[op.GID] = op.Servers
			keys := make([]int64, 0)
			for k, _ := range c.Groups {
				keys = append(keys, k)
			}
			copy(c.Shards[:], rebalance(prev.Shards[:], keys)[0:NShards])

			sm.configs = append(sm.configs, c)
//			fmt.Println(c)
		} else if op.Type == "Leave" {
			prev := sm.configs[len(sm.configs) - 1]
			c := Config{Num:prev.Num + 1}			
			c.Groups = make(map[int64][]string)
			for key, value := range prev.Groups {
				if key != op.GID {
					c.Groups[key] = value
				}
			}
			keys := make([]int64, 0)
			for k, _ := range c.Groups {
				keys = append(keys, k)
			} 
			copy(c.Shards[:], rebalance(prev.Shards[:], keys)[0:NShards])

			sm.configs = append(sm.configs, c)
//			fmt.Println(c)
			
		} else if op.Type == "Move" {
			prev := sm.configs[len(sm.configs) - 1]
			c := Config{Num:prev.Num + 1}			
			c.Groups = make(map[int64][]string)
			for key, value := range prev.Groups {
				c.Groups[key] = value
			}
			for i, j := range prev.Shards {
				c.Shards[i] = j
			}
			c.Shards[op.Shard] = op.GID
			sm.configs = append(sm.configs, c)
//			fmt.Println(c)
		} else if op.Type == "Query" {
		}

		sm.px.Done(i)
	}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	// Your code here.
	seq := sm.SendOp(Op{Type:"Join", GID:args.GID, Servers:args.Servers})
//	fmt.Println("Join", seq, args)
	if seq == 0 {
//		fmt.Println("Couldn't send")
		return nil
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.update()
	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	// Your code here.
//	fmt.Println("Leave", args)
	if sm.SendOp(Op{Type:"Leave", GID:args.GID}) == 0 {
//		fmt.Println("Couldn't send")
		return nil
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.update()
	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	// Your code here.
//	fmt.Println("Move", args)
	if sm.SendOp(Op{Type:"Move", Shard:args.Shard, GID:args.GID}) == 0 {
//		fmt.Println("Couldn't send")
		return nil
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.update()
	
	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	// Your code here.

	if sm.SendOp(Op{Type:"Query"}) == 0 {

		return nil
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.update()

	if args.Num == -1 {
		reply.Config = sm.configs[len(sm.configs) - 1]
	} else {
		reply.Config = sm.configs[args.Num]
	}
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
func StartServer(servers []string, me int) *ShardMaster {
	gob.Register(Op{})

	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}
	sm.logged = make(map[int64]bool)
	rpcs := rpc.NewServer()
	rpcs.Register(sm)

	sm.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

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
			if err != nil && sm.dead == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}






//stuff from previous lab...


func (kv *ShardMaster) SendOp(op Op) int {
	kv.sendmu.Lock()
	defer kv.sendmu.Unlock()
	s := kv.DoSendOp(op)
	if kv.dead {return 0}

	return s
}
func (kv *ShardMaster) StatusBackingOff(seq int) (bool, interface{}) {
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

func (kv *ShardMaster) DoSendOp(op Op) int {
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
		for t := rand.Intn(10); t > 0; t-- {
			time.Sleep(10)
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
