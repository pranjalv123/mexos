package shardkv

import "shardmaster"
import "net/rpc"
import "time"
import "sync"
import "fmt"
import "crypto/rand"
import "math/big"
import "strconv"

type Clerk struct {
  mu sync.Mutex // one RPC at a time
  sm *shardmaster.Clerk
  config shardmaster.Config
  // You'll have to modify Clerk.
  me string
  seq int
}



func MakeClerk(shardmasters []string) *Clerk {
  ck := new(Clerk)
  ck.sm = shardmaster.MakeClerk(shardmasters)
  // You'll have to modify MakeClerk.
  ck.me = strconv.FormatInt(nrand(), 10)
  ck.seq = 0
  return ck
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it doesn't get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
          args interface{}, reply interface{}) bool {
  c, errx := rpc.Dial("unix", srv)
  if errx != nil {
    return false
  }
  defer c.Close()
    
  err := c.Call(rpcname, args, reply)
  if err == nil {
    return true
  }

  fmt.Println(err)
  return false
}

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
  shard := 0
  if len(key) > 0 {
    shard = int(key[0])
  }
  shard %= shardmaster.NShards
  return shard
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
func (ck *Clerk) Get(key string) string {
  ck.mu.Lock()
  defer ck.mu.Unlock()
	DPrintf("Clerk %s sending seq %d (GET)",ck.me, ck.seq)
  // You'll have to modify Get().
	seq := ck.seq
  for {
    shard := key2shard(key)

    gid := ck.config.Shards[shard]

    servers, ok := ck.config.Groups[gid]

    if ok {

      // try each server in the shard's replication group.
      for _, srv := range servers {
	      args := &GetArgs{Key: key, CliSeq: seq, From: ck.me}
	      var reply GetReply
	      ok := call(srv, "ShardKV.Get", args, &reply)
	      if ok && (reply.Err == "" || reply.Err == ErrNoKey) {
		      ck.seq++
		      return reply.Value
	      }
	      if ok && (reply.Err == ErrWrongGroup) {
		      break 
	      }
      }
    }
	  
	  time.Sleep(100 * time.Millisecond)
	  
	  // ask master for a new configuration.
	  ck.config = ck.sm.Query(-1)
  }
  return ""
}

func (ck *Clerk) PutExt(key string, value string, dohash bool) string {
  ck.mu.Lock()
  defer ck.mu.Unlock()
	DPrintf("Clerk %s sending seq %d (PUT)",ck.me, ck.seq)
  // You'll have to modify Put().
	seq := ck.seq
  for {
    shard := key2shard(key)

    gid := ck.config.Shards[shard]

    servers, ok := ck.config.Groups[gid]

    if ok {

      // try each server in the shard's replication group.
      for _, srv := range servers {
	      args := &PutArgs{Key: key, Value: value, DoHash: dohash, 
		      CliSeq: seq, From: ck.me}
	      args.DoHash = dohash
	      var reply PutReply
	      ok := call(srv, "ShardKV.Put", args, &reply)
	      if ok && reply.Err == "" {
		      if dohash {
			      DPrintf("PutHash (%s, %s) returned '%s'. History: %s", 
				      key, value, reply.PreviousValue, reply.ValueHistory)
		      }
		      ck.seq++ //only increment upon success
		      return reply.PreviousValue
	      }
	      if ok && (reply.Err == ErrWrongGroup) {
		      break
	      }
      }
    }
	  
	  time.Sleep(100 * time.Millisecond)
	  
	  // ask master for a new configuration.
	  ck.config = ck.sm.Query(-1)
  }
}

func (ck *Clerk) Put(key string, value string) {
  ck.PutExt(key, value, false)
}
func (ck *Clerk) PutHash(key string, value string) string {
	v := ck.PutExt(key, value, true)
	return v
}

func nrand() int64 {
        max := big.NewInt(int64(1) << 62)
        bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

