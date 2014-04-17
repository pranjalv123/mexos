package kvpaxos

import "net/rpc"
import "fmt"
import "strconv"
import "time"

type Clerk struct {
	servers []string
	// You will have to modify this struct.
	clientID int64
}

func MakeClerk(servers []string) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientID = nrand() // Unique ID for this client
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
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.

	DPrintf("\nClient %v received Get(%s)", ck.clientID, key)

	// Create GetArgs with uniqe XID
	args := new(GetArgs)
	args.Key = key
	args.XID = nrand()
	args.Sender = ck.clientID
	args.SendTime = time.Now().UnixNano()

	DPrintf("\n\tClient sending Get(%s), XID:%v, to %s", key, args.XID, ck.servers[0])

	// Send GetArgs to a server
	// If it times out, try another server
	success := false
	reply := new(GetReply)
	s := int64(0)
	tries := 0
	for !success && (tries < 1000) {
		tempReply := new(GetReply)
		success = call(ck.servers[s], "KVPaxos.Get", args, &tempReply)
		reply.Err = tempReply.Err
		reply.Value = tempReply.Value
		// See if command succeeded
		success = success && (reply.Err == "")
		// Choose which server to try next in case it failed
		s = (s + 1) % int64(len(ck.servers))
		if !success {
			time.Sleep(50 * time.Millisecond)
			DPrintf("\n\tClient resending Get(%s), XID:%v, to %s", key, args.XID, ck.servers[s])
		}
		tries++
	}

	if tries >= 1000 {
		DPrintf("\n\t*** Client reached max tries! ***")
		DPrintf("\n\tClient returning Get(%s), XID:%v", key, args.XID)
		return ""
	}
	DPrintf("\n\tClient returning Get(%s), XID:%v, value <%s>, tries %v", key, args.XID, ck.val2print(reply.Value), tries)
	return reply.Value
}

//
// set the value for a key.
// keeps trying until it succeeds.
//
func (ck *Clerk) PutExt(key string, value string, dohash bool) string {
	// You will have to modify this function.

	DPrintf("\nClient %v received Put(%s, %s), HASH:%v", ck.clientID, key, ck.val2print(value), dohash)

	// If PutHash, compute new value to use
	if dohash {
		prevValue := ck.Get(key) // Will keep retrying until value is obtained
		_, err := strconv.Atoi(prevValue)
		if (prevValue != "") && (err != nil) {
			return "Error: Could not parse previous value"
		}
		hashRes := hash(prevValue + value)
		value = strconv.Itoa(int(hashRes))
	}

	// Create PutArgs with unique XID
	args := new(PutArgs)
	args.DoHash = dohash
	args.XID = nrand()
	args.Key = key
	args.Value = value
	args.Sender = ck.clientID
	args.SendTime = time.Now().UnixNano()

	DPrintf("\n\tClient sending Put(%s, %s), XID:%v, to %s", key, ck.val2print(args.Value), args.XID, ck.servers[0])

	// Send PutArgs to a server
	// If it times out, try another server
	success := false
	reply := new(PutReply)
	s := int64(0)
	tries := 0
	for !success && (tries < 1000) {
		tempReply := new(PutReply)
		success = call(ck.servers[s], "KVPaxos.Put", args, &tempReply)
		reply.Err = tempReply.Err
		reply.PreviousValue = tempReply.PreviousValue
		// See if command succeeded
		success = success && (reply.Err == "")
		// Choose next server to try in case it failed
		s = (s + 1) % int64(len(ck.servers))
		if !success {
			time.Sleep(50 * time.Millisecond)
			DPrintf("\n\tClient resending Put(%s, %s), XID:%v, to %s", key, ck.val2print(args.Value), args.XID, ck.servers[s])
		}
		tries++
	}

	if tries >= 1000 {
		DPrintf("\n\t*** Client reached max tries! ***")
		DPrintf("\n\tClient returning Put(%s, %s), XID:%v", key, ck.val2print(args.Value), args.XID)
		return ""
	}

	DPrintf("\n\tClient returning Put(%s, %s), XID:%v, prevValue <%s>, tries %v", key, ck.val2print(args.Value), args.XID, ck.val2print(reply.PreviousValue), tries)
	return reply.PreviousValue
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutExt(key, value, false)
}
func (ck *Clerk) PutHash(key string, value string) string {
	v := ck.PutExt(key, value, true)
	return v
}

//
// Function to get a printable variable of an instance value
// Basically just here to deal with Test 03 where 1MB values are chosen
//
// If value is a super big string, returns a string stating its length
// otherwise just returns the original value
//
func (ck *Clerk) val2print(v interface{}) interface{} {
	// If it is a huge string, don't print it
	vString, isString := v.(string)
	if isString && (len(vString) > 500) {
		return fmt.Sprintf("<reallyBigString length %v>", len(vString))
	}
	return v
}
