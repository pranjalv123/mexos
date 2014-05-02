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

const Debug=0
const startport = 2300

func DPrintf(format string, a ...interface{}) (n int, err error) {
        if Debug > 0 {
                log.Printf(format, a...)
        }
        return
}


type Op struct {
  Op int //1 = Get, 2 = Put, 3 = PutHash, 4 = Reconfigure
  OpID int64
  Key string
  Value string

  ConfigNum int
  Store map[string]string
  Response map[int64]string
}


type ShardKV struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  sm *shardmaster.Clerk
  px *paxos.Paxos
  network bool
  gid int64 // my replica group ID

  config shardmaster.Config

  store map[string]string
  response map[int64]string
  minSeq int
}

func (kv *ShardKV) processLog(maxSeq int) {
    if maxSeq <= kv.minSeq + 1 {
    return
  }
  DPrintf("%d.%d.%d) Process Log Until %d\n", kv.gid, kv.me, kv.config.Num, maxSeq)

  for i := kv.minSeq + 1; i < maxSeq; i++ {
    to := 10 * time.Millisecond
    start := false
    for !kv.dead {
      decided, opp := kv.px.Status(i)
      if decided {
        op := opp.(Op)
        if op.Op == 1 {
          DPrintf("%d.%d.%d) Log %d: Op #%d - GET(%s)\n", kv.gid, kv.me, kv.config.Num, i, op.OpID, op.Key)
          kv.response[op.OpID] = kv.store[op.Key]
        } else if op.Op == 2 {
          DPrintf("%d.%d.%d) Log %d: Op #%d - PUT(%s, %s)\n", kv.gid, kv.me, kv.config.Num, i, op.OpID, op.Key, op.Value)
          kv.response[op.OpID] = kv.store[op.Key]
          kv.store[op.Key] = op.Value
        } else if op.Op == 3 {
          DPrintf("%d.%d.%d) Log %d: Op #%d - PUTHASH(%s, %s)\n", kv.gid, kv.me, kv.config.Num, i, op.OpID, op.Key, op.Value)
          kv.response[op.OpID] = kv.store[op.Key]
          kv.store[op.Key] = strconv.Itoa(int(hash(kv.store[op.Key] + op.Value)))
        } else if op.Op == 4 {
          DPrintf("%d.%d.%d) Log %d: Op #%d - RECONFIGURE(%d)\n", kv.gid, kv.me, kv.config.Num, i, op.OpID, op.ConfigNum)
          for nk,nv := range op.Store {
            kv.store[nk] = nv
          }
          for nk,nv := range op.Response {
            kv.response[nk] = nv
          }
          kv.config = kv.sm.Query(op.ConfigNum)
        }
        break
      } else if !start {
        kv.px.Start(i, Op{})
        start = true
      }
      time.Sleep(to)
      if to < 10 * time.Second {
        to *= 2
      }
    }
  }
  kv.minSeq = maxSeq - 1
  kv.px.Done(kv.minSeq)
}

func (kv *ShardKV) processKV(op Op, reply *KVReply) {
  for !kv.dead {
    seq := kv.px.Max() + 1
    kv.processLog(seq)
    if kv.config.Shards[key2shard(op.Key)] != kv.gid {
      return
    }
    if v,seen := kv.response[op.OpID]; seen {
      DPrintf("%d.%d.%d) Already Seen Op %d\n", kv.gid, kv.me, kv.config.Num, op.OpID)
      if v == "" {
        reply.Err = ErrNoKey
      } else {
        reply.Err = OK
      }
      reply.Value = v
      return
    }

    kv.px.Start(seq, op)

    to := 10 * time.Millisecond
    for !kv.dead {

      if decided,_ := kv.px.Status(seq); decided {
        seq := kv.px.Max() + 1
        kv.processLog(seq)
        if kv.config.Shards[key2shard(op.Key)] != kv.gid {
          return
        }

        if v,seen := kv.response[op.OpID]; seen {
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
      if to < 10 * time.Second {
        to *= 2
      }
    }
  }
}

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
    seq := kv.px.Max() + 1
    kv.processLog(seq)
    if kv.config.Num >= num {
      return
    }

    kv.px.Start(seq, newOp)

    to := 10 * time.Millisecond
    for !kv.dead {
      if decided,_ := kv.px.Status(seq); decided {
        seq := kv.px.Max() + 1
        kv.processLog(seq)
        if kv.config.Num >= num {
          return
        } else {
          break
        }
      }

      time.Sleep(to)
      if to < 10 * time.Second {
        to *= 2
      }
    }
  }
}


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

func (kv *ShardKV) Fetch(args *FetchArgs, reply *FetchReply) error {
  //kv.mu.Lock()
  defer func() {
    DPrintf("%d.%d.%d) Fetch Returns: %s\n", kv.gid, kv.me, kv.config.Num, reply.Store)
    //kv.mu.Unlock()
  }()

  DPrintf("%d.%d.%d) Fetch: Shard %d from Config %d\n", kv.gid, kv.me, kv.config.Num, args.Shard, args.Config)

  if kv.config.Num < args.Config {
    reply.Err = ErrNoKey
    return nil
  }

  shardStore := make(map[string]string)

  for k,v := range kv.store {
    if key2shard(k) == args.Shard {
      shardStore[k] = v
    }
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

  seq := kv.px.Max() + 1
  kv.processLog(seq)

  newConfig := kv.sm.Query(kv.config.Num + 1)
  if newConfig.Num == kv.config.Num {
    return
  }

  DPrintf("%d.%d.%d) Found New Config: %d -> %d\n", kv.gid, kv.me, kv.config.Num, kv.config.Shards, newConfig.Shards)

  var gained []int
  var remoteGained []int
  var lost []int

  for k,v := range newConfig.Shards {
    if kv.config.Shards[k] == kv.gid && v != kv.gid {
      lost = append(lost, k)
    } else if kv.config.Shards[k] != kv.gid && v == kv.gid {
      gained = append(gained, k)
      if kv.config.Shards[k] > 0 {
        remoteGained = append(remoteGained, k)
      }
    }
  }

  newStore := make(map[string]string)
  newResponse := make(map[int64]string)

  if len(remoteGained) != 0 && !kv.dead {
    DPrintf("%d.%d.%d) New Config needs %d\n", kv.gid, kv.me, kv.config.Num, remoteGained)
    for _,shard := range remoteGained {
      otherGID := kv.config.Shards[shard]
      servers := kv.config.Groups[otherGID]
      args := &FetchArgs{newConfig.Num, shard}
    srvloop:
      for !kv.dead {
        for sid, srv := range servers {
          DPrintf("%d.%d.%d) Attempting to get Shard %d from %d.%d\n", kv.gid, kv.me, kv.config.Num, shard, otherGID, sid)
          var reply FetchReply
		ok := call(srv, "ShardKV.Fetch", args, &reply, kv.network)
          if ok && (reply.Err == OK) {
            DPrintf("%d.%d.%d) Got Shard %d from %d.%d\n", kv.gid, kv.me, kv.config.Num, shard, otherGID, sid)
            for k,v := range reply.Store {
              newStore[k] = v
            }
            for k,v := range reply.Response {
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

  kv.addReconfigure(newConfig.Num, newStore, newResponse)
  DPrintf("%d.%d.%d) New Config adding %d\n", kv.gid, kv.me, kv.config.Num, newStore)
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
	servers []string, me int, network bool) *ShardKV {
  gob.Register(Op{})
  kv := new(ShardKV)
  kv.me = me
  kv.network = network
  kv.gid = gid
  kv.sm = shardmaster.MakeClerk(shardmasters, kv.network)

  kv.config = kv.sm.Query(0)
		//fmt.Println("Got here!")
  kv.store = make(map[string]string)
  kv.response = make(map[int64]string)
  kv.minSeq = -1

  rpcs := rpc.NewServer()
  rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs, kv.network)


	if kv.network {
		l, e := net.Listen("tcp", strconv.Itoa(startport+me));
		if e != nil {
			log.Fatal("listen error: ", e);
		}
		kv.l = l
	} else {
		os.Remove(servers[me])
		l, e := net.Listen("unix", servers[me]);
		if e != nil {
			log.Fatal("listen error: ", e);
		}
		kv.l = l
	}

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
