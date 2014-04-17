package pbservice

import "viewservice"
import "net/rpc"
import "fmt"

// You'll probably need to uncomment these:
import "time"
import "strconv"

type Clerk struct {
	vs *viewservice.Clerk
	// Your declarations here
	primary string // The current primary
}

func MakeClerk(vshost string, me string) *Clerk {
	ck := new(Clerk)
	ck.vs = viewservice.MakeClerk(me, vshost)

	// Your ck.* initializations here
	view, _ := ck.vs.Ping(0)
	newPrimary := view.Primary
	ck.primary = newPrimary
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

	// Your code here

	DPrintf("\nClient got Get(%s)", key)

	// Create args with a unique XID and tagged with current time
	args := new(GetArgs)
	args.Xid = strconv.Itoa(int(nrand()))
	args.Key = key
	args.SendTime = time.Now().UnixNano()

	reply := new(GetReply)
	ok := false
	// Keep trying to send message until it succeeds
	for !(ok && (reply.Err == "")) {
		tempReply := new(GetReply)
		ok = call(ck.primary, "PBServer.Get", args, &tempReply)
		reply.Err = tempReply.Err
		reply.Value = tempReply.Value
		DPrintf("\n\tok: %v, replyErr: <%s> %s", ok, reply.Err, args.Xid)
		if !(ok && (reply.Err == "")) {
			view, _ := ck.vs.Ping(0)
			ck.primary = view.Primary
			time.Sleep(viewservice.PingInterval)
			DPrintf("\n\tResending Get(%s) to %s", key, ck.primary)
		}
	}
	DPrintf("\n\tClient returning Get(%s) = %s", key, reply.Value)
	return reply.Value
}

//
// tell the primary to update key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) PutExt(key string, value string, doHash bool) string {

	// Your code here.

	DPrintf("\nClient got Put(%s, %s)", key, value)
	if doHash {
		DPrintf("   (hash)")
	}

	// If PutHash, compute new value to use
	if doHash {
		prevValue := ck.Get(key) // Will keep retrying until value is obtained
		_, err := strconv.Atoi(prevValue)
		if (prevValue != "") && (err != nil) {
			return "Error: Could not get previous value"
		}
		hashRes := hash(prevValue + value)
		value = strconv.Itoa(int(hashRes))
	}

	// Make PutArgs with new arguments, a unique XID, and tagged with the time
	args := new(PutArgs)
	args.DoHash = doHash
	args.Xid = strconv.Itoa(int(nrand()))
	args.SendTime = time.Now().UnixNano()
	args.Key = key
	args.Value = value

	// Keep sending request until success
	reply := new(PutReply)
	ok := false
	for !(ok && (reply.Err == "")) {
		tempReply := new(PutReply)
		ok = call(ck.primary, "PBServer.Put", args, &tempReply)
		reply.Err = tempReply.Err
		reply.PreviousValue = tempReply.PreviousValue
		DPrintf("\n\tok: %v, replyErr: <%s> %s", ok, reply.Err, args.Xid)
		if !(ok && (reply.Err == "")) {
			view, _ := ck.vs.Ping(0)
			ck.primary = view.Primary
			time.Sleep(viewservice.PingInterval)
			DPrintf("\n\tResending Put(%s, %s) to %s", key, value, ck.primary)
		}
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
