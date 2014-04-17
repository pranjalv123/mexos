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
import "strconv"
import "time"

const Debug=0

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
  From string
  CSeq int
  PSeq int //paxos sequence number
  Proposer int //for debug

  Type string
  Key string
  Value string
  PreviousValue string
  Err Err
}

type KVPaxos struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  px *paxos.Paxos
 
  // Your definitions here
  log map[int]*Op
  responses map[string]*Op
  db map[string]string
  outstanding map[int]chan *Op
  highestApplied int
}


func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	seq, c := kv.seqChan()

	proposedVal := Op{From: args.From, CSeq: args.SeqNum, PSeq: seq, 
		Proposer: kv.me, Type:  Get, Key: args.Key}
	
	kv.px.Start(seq, proposedVal)
	acceptedVal := <- c

	//start over if it wasn't what we proposed
	if acceptedVal.From != args.From || acceptedVal.CSeq != args.SeqNum {
		reply.Err = Err(ErrGet)
		//housecleaning
		kv.mu.Lock()
		delete(kv.outstanding, seq)
		c <- &Op{}
		kv.mu.Unlock()
		return nil
	}
	reply.Err = acceptedVal.Err
	reply.Value = acceptedVal.Value		
	
	//housecleaning
	kv.mu.Lock()
	delete(kv.outstanding, seq)
	c <- &Op{}
	kv.mu.Unlock()
	return nil
}

func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
	seq, c := kv.seqChan()
	t := Put
	if args.DoHash {
		t = PutHash
	}
	proposedVal := Op{From: args.From, CSeq: args.SeqNum, PSeq: seq, 
		Proposer: kv.me, Type: t, Key: args.Key, Value: args.Value}
	kv.px.Start(seq, proposedVal)
	acceptedVal := <- c
	//retry if it wasn't what we proposed
	if acceptedVal.From != args.From  || acceptedVal.CSeq != args.SeqNum {
		reply.Err = Err(ErrPut)
		//housecleaning
		kv.mu.Lock()
		delete(kv.outstanding, seq)
		c <- &Op{}
		kv.mu.Unlock()
		return nil
	}
	
	reply.Err = acceptedVal.Err
	reply.PreviousValue = acceptedVal.PreviousValue
	
	//housecleaning
	kv.mu.Lock()
	delete(kv.outstanding, seq)
	c <- &Op{}
	kv.mu.Unlock()
	return nil
}

func (kv *KVPaxos) seqChan() (int, chan *Op) {
	kv.mu.Lock()
	seq := kv.px.Max() + 1
	for _,ok := kv.outstanding[seq]; ok || kv.highestApplied >= seq; {
		seq++
		_,ok = kv.outstanding[seq]
	}
	c := make(chan *Op)
	kv.outstanding[seq] = c
	kv.mu.Unlock()
	return seq,c
}

func (kv *KVPaxos) duplicateCheck(from string, CSeq int) (bool, *Op) {
	//check responses map
	if duplicate, ok := kv.responses[from]; ok {
		if duplicate.CSeq == CSeq {
			return true, duplicate
		} else if CSeq < duplicate.CSeq {
			return true, &Op{From:Nobody}
		}
	}
	return false, &Op{}
}

func (kv *KVPaxos) getStatus() {
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
				kv.mu.Lock()
				if op.From != Nobody {
					kv.apply(seq, &op)
				} else {
					kv.highestApplied = seq
					kv.px.Done(seq)
				}
				if ch, ok := kv.outstanding[seq]; ok {
					ch <- &op //notify handler 
					kv.mu.Unlock()
					<- ch
				} else {
					kv.mu.Unlock()
				}
			} else {
				log.Fatal("Fatal!could not cast Op");
			}
			seq++
			restarted = false
		} else {
			time.Sleep(to)
			if to < 25 * time.Millisecond {
				to *= 2
			} else { //if we've slept long enough, propose a noop
				if !restarted {
					to = 10 * time.Millisecond
					kv.px.Start(seq, Op{From: Nobody})
					time.Sleep(time.Millisecond)
					restarted = true
				}
			}
		}
	}
}

func (kv *KVPaxos) apply(seq int, op *Op) {
	if dup, d := kv.duplicateCheck(op.From, op.CSeq); !dup {
		switch op.Type {
		case Put :
			op.PreviousValue = kv.db[op.Key]
			kv.db[op.Key] = op.Value
		case PutHash:
			prev := kv.db[op.Key]
			value := strconv.Itoa(int(hash(prev + op.Value)))
			op.PreviousValue = prev
			kv.db[op.Key] = value
		case Get:
			if v, ok := kv.db[op.Key]; ok {		
				op.Value = v
			} else {
				op.Err = Err(ErrNoKey)
			}
		}
		kv.responses[op.From] = op
	} else {
		if d.From != Nobody {
			//copy all the things
			op.From = d.From
			op.CSeq = d.CSeq
			op.PSeq = d.PSeq
			op.Proposer = d.Proposer
			op.Type = d.Type
			op.Key = d.Key
			op.Value = d.Value 
			op.PreviousValue = d.PreviousValue 
			op.Err = d.Err
		} else { //this is a very stale request
			op.Err = Err(ErrGet)
		}
	}	
	kv.highestApplied = seq
	kv.px.Done(seq)
}

//concatenates source and seq
//func (kv *KVPaxos) makeID(source string, seq int) string {
//	str := source + strconv.Itoa(seq)
//	return str
//}

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
  kv.db = make(map[string]string)
  kv.log = make(map[int]*Op)
  kv.responses = make(map[string]*Op)
  kv.outstanding = make(map[int]chan *Op)
  kv.highestApplied = -1

  rpcs := rpc.NewServer()
  rpcs.Register(kv)

  kv.px = paxos.Make(servers, me, rpcs)

  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me]);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  kv.l = l


  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for kv.dead == false {
      conn, err := kv.l.Accept()
      if err == nil && kv.dead == false {
        if kv.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if kv.unreliable && (rand.Int63() % 1000) < 200 {
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

  //this goroutine starts a thread to monitor paxos
  go kv.getStatus()	
  return kv
}

