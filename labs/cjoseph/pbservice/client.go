package pbservice

import "viewservice"
import "net/rpc"
import "fmt"

// You'll probably need to uncomment these:
import "time"
import "crypto/rand"
import "math/big"
//import "log"

type Clerk struct {
  vs *viewservice.Clerk
  // Your declarations here
  me string
}


func MakeClerk(vshost string, me string) *Clerk {
  ck := new(Clerk)
  ck.vs = viewservice.MakeClerk(me, vshost)
  // Your ck.* initializations here
  ck.me = me
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
// fetch a key's value from the current primary;
// if they key has never been set, return "".
// Get() must keep trying until it either the
// primary replies with the value or the primary
// says the key doesn't exist (has never been Put().
//
func (ck *Clerk) Get(key string) string {
	var reply GetReply 
	args := &GetArgs{key, nrand(), ck.me}
	ok := call(ck.vs.Primary(), "PBServer.Get", args, &reply)
	for !ok || reply.Err == ErrWrongServer {//retry loop
		reply.Err = ""
		time.Sleep(viewservice.PingInterval)
		ok = call(ck.vs.Primary(), "PBServer.Get", args, &reply)
	}
	if reply.Err == ErrNoKey {
		return ""
	}
	return reply.Value
}

//
// tell the primary to update key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) PutExt(key string, value string, dohash bool) string {
	var reply PutReply 
	args := &PutArgs{key, value, dohash, nrand(), ck.me}
	ok := call(ck.vs.Primary(), "PBServer.Put", args, &reply)
	for !ok || reply.Err != "" { //retry loop
		reply.Err=""
		time.Sleep(viewservice.PingInterval)
		ok = call(ck.vs.Primary(), "PBServer.Put", args, &reply)
	}
	return reply.PreviousValue
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
