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

//import "strconv"
//import "strings"

//
// ****** QUESTION FOR REVIEWER ******
// I didn't actually use any no-op commands, nor did I use Paxos.Min() or Paxos.Max()
//
// On Piazza it looks like people used Min and Max to choose sequence numbers, and
// use no-ops to learn of log entries, and tune some sort of delay
//
// Instead, I just store the last sequence committed on my disk, and use that to
// propose sequence numbers - I learn about what is actually in the log as I go along.
// This way there should be no holes, no delay tuning, and no special cases for
// updating myself with missed log entries.
//
// The code passed all tests over 200 times in a row, so it seems to work...
//
// If you can spot any cases which would cause my code to fail but which would
// be fixed by using no-ops, I would really appreciate it since I'm a bit confused as
// to why they would be needed (they sound more complicated than my approach).
//
// Thanks!!
//

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		fmt.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Command  string // Either "Put" or "Get"
	Key      string // The key of the record in question
	Value    string // The new value - will be set to "" for a Get command
	XID      int64  // The unique XID of the command
	Sender   int64  // The unique Client ID that sent this command
	SendTime int64  // The time at which the client first sent this XID
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	px         *paxos.Paxos

	// Your definitions here.
	maxSequenceCommitted int               // The highest log sequence which has been applied on this server
	kvDatabase           map[string]string // The key/value database
	seenXID              map[int64]bool    // A list of all XIDs processed
	clientResponses      map[int64]string  // The most recent response sent to each client
	lastPut              map[string]int64  // The time of the most recent Put command for each Key
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.

	// Acquire locks since we will be accessing various maps
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// Will create a long string to print and print it all at the end,
	// so this method's output won't be interleaved with that of concurrent methods
	toPrint := ""
	toPrint += fmt.Sprintf("\n\tServer %v got Get(%s), XID %v, Sender %v, Time %v", kv.me, args.Key, args.XID, args.Sender, args.SendTime)

	// If this is a duplicate command, respond with last reply sent to that client
	if kv.seenXID[args.XID] {
		toPrint += "\n\t\tDuplicate XID"
		reply.Value = kv.clientResponses[args.Sender]
		reply.Err = ""
		toPrint += fmt.Sprintf("\n\t\tReturning <%s>", kv.val2print(reply.Value))
		DPrintf(toPrint)
		return nil
	}

	// If there has been a Put since this Get was issued, return error
	//   Not really clear what the client would want
	//   Using at-most-once semantics, so we can just reject it
	// This case is apparently never reached in this lab, but it seems good
	// to have in case messages get delayed in the network right?
	if kv.lastPut[args.Key] > args.SendTime {
		toPrint += "\n\t\tIgnoring old Get command!"
		reply.Value = ""
		reply.Err = "Old"
		DPrintf(toPrint)
		return nil
	}

	// Create Op structure for Get command
	var op Op
	op.Command = "Get"
	op.Key = args.Key
	op.Value = ""
	op.XID = args.XID
	op.Sender = args.Sender

	// Start with the sequence just after the last one this server executed
	// Keep calling paxos.Start(), increasing sequence number each time
	// Execute each decided command that we discover as we go along
	// Eventually the decided one will be ours, and then we're done and up to date!
	// If we discover that our command is old or a duplicate, exit
	oldGet := false
	var curSequence int
	for !oldGet && !kv.seenXID[args.XID] && !kv.dead {
		curSequence = kv.maxSequenceCommitted + 1
		toPrint += fmt.Sprintf("\n\t\tSet sequence to %v", curSequence)
		// Start Paxos instance on the current Op
		kv.px.Start(curSequence, op)

		// Wait until its Status is decided
		decided := false
		var decidedValue interface{}
		toWait := 25 * time.Millisecond
		for !decided {
			time.Sleep(toWait)
			if toWait < 2*time.Second {
				toWait *= 2
			}
			decided, decidedValue = kv.px.Status(curSequence)
		}

		// Get the decided operation for this sequence
		decidedOp := decidedValue.(Op)
		isPut := (decidedOp.Command == "Put")
		isGet := (decidedOp.Command == "Get")
		nextKey := decidedOp.Key
		nextVal := decidedOp.Value
		nextSender := decidedOp.Sender
		nextXID := decidedOp.XID
		nextSendTime := decidedOp.SendTime

		// Execute this log entry
		if isPut {
			toPrint += fmt.Sprintf("\tPut(%s, %s) XID %v", nextKey, kv.val2print(nextVal), nextXID)
			kv.seenXID[nextXID] = true                              // Mark this XID as seen
			kv.clientResponses[nextSender] = kv.kvDatabase[nextKey] // Store PreviousValue that client would have received
			kv.lastPut[nextKey] = nextSendTime
			oldGet = (kv.lastPut[args.Key] > args.SendTime)
			kv.kvDatabase[nextKey] = nextVal
			kv.maxSequenceCommitted = curSequence
		} else if isGet {
			toPrint += fmt.Sprintf("\tGet(%s) XID %v", nextKey, nextXID)
			kv.seenXID[nextXID] = true                              // Mark this XID as seen
			kv.clientResponses[nextSender] = kv.kvDatabase[nextKey] // Store the Value that the client would have received
			kv.maxSequenceCommitted = curSequence
		}
	}
	if kv.dead {
		return nil
	}

	// Loop is ended, so the possibilities are:
	//  - we successfully got a new instance to log our command
	//  - we discovered that this command was actually a duplicate XID
	//  - we discovered that this command is actually old
	// In any case, we are up-to-date up to maxSequenceCommitted, and have
	// stored the proper response to send to the client

	// Call Done to let Paxos free memory
	kv.px.Done(kv.maxSequenceCommitted)

	// If there has been a Put since this Get was issued, return error
	//   Not really clear what the client would want
	//   Using at-most-once semantics, so we can just reject it
	// This case is apparently never reached in this lab, but it seems good
	// to have in case messages get delayed in the network right?
	if oldGet {
		toPrint += "\n\t\tIgnoring old Get command!"
		reply.Value = ""
		reply.Err = "Old"
		DPrintf(toPrint)
		return nil
	}

	// All done! Return to the client
	reply.Value = kv.clientResponses[args.Sender]
	reply.Err = ""
	toPrint += fmt.Sprintf("\n\t\tReturning <%s>", kv.val2print(reply.Value))
	DPrintf(toPrint)
	return nil
}

func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
	// Your code here.

	// Note that this method is basically identical to the above Get method
	// since the commands are executed in the loop in exactly the same way

	// Acquire locks since we will be accessing various maps
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// Will create a long string to print and print it all at the end,
	// so this method's output won't be interleaved with that of concurrent methods
	toPrint := ""
	toPrint += fmt.Sprintf("\n\tServer %v got Put(%s, %s), XID %v, Sender %v, Time %v", kv.me, args.Key, kv.val2print(args.Value), args.XID, args.Sender, args.SendTime)

	// If this is a duplicate command, respond with last reply sent to that client
	if kv.seenXID[args.XID] {
		toPrint += "\n\t\tDuplicate XID"
		reply.PreviousValue = kv.clientResponses[args.Sender]
		reply.Err = ""
		toPrint += fmt.Sprintf("\n\t\tReturning <%s>", kv.val2print(reply.PreviousValue))
		DPrintf(toPrint)
		return nil
	}

	// If there has been a Put since this Put was issued, return error
	// This case is apparently never reached in this lab, but it seems good
	// to have in case messages get delayed in the network right?
	if kv.lastPut[args.Key] > args.SendTime {
		toPrint += "\n\t\tIgnoring old Put command!"
		reply.PreviousValue = ""
		reply.Err = "Old"
		DPrintf(toPrint)
		return nil
	}

	// Create Op structure for Put command
	var op Op
	op.Command = "Put"
	op.Key = args.Key
	op.Value = args.Value
	op.XID = args.XID
	op.Sender = args.Sender

	// Start with the sequence just after the last one this server executed
	// Keep calling paxos.Start(), increasing sequence number each time
	// Execute each decided command that we discover as we go along
	// Eventually the decided one will be ours, and then we're done and up to date!
	// If we discover that our command is old or a duplicate, exit
	oldPut := false
	var curSequence int
	for !oldPut && !kv.seenXID[args.XID] && !kv.dead {
		curSequence = kv.maxSequenceCommitted + 1
		curSequence = kv.maxSequenceCommitted + 1
		// Start Paxos instance on the current Op
		kv.px.Start(curSequence, op)

		// Wait until its Status is decided
		decided := false
		var decidedValue interface{}
		toWait := 25 * time.Millisecond
		for !decided {
			time.Sleep(toWait)
			if toWait < 2*time.Second {
				toWait *= 2
			}
			decided, decidedValue = kv.px.Status(curSequence)
		}

		// Get the decided operation for this sequence
		decidedOp := decidedValue.(Op)
		isPut := (decidedOp.Command == "Put")
		isGet := (decidedOp.Command == "Get")
		nextKey := decidedOp.Key
		nextVal := decidedOp.Value
		nextSender := decidedOp.Sender
		nextXID := decidedOp.XID
		nextSendTime := decidedOp.SendTime

		//Execute this log entry
		if isPut {
			toPrint += fmt.Sprintf("\tPut(%s, %s) XID %v", nextKey, kv.val2print(nextVal), nextXID)
			kv.seenXID[nextXID] = true                              // Mark this XID as seen
			kv.clientResponses[nextSender] = kv.kvDatabase[nextKey] // Store PreviousValue that client would have received
			kv.lastPut[nextKey] = nextSendTime
			oldPut = (kv.lastPut[args.Key] > args.SendTime)
			kv.kvDatabase[nextKey] = nextVal
			kv.maxSequenceCommitted = curSequence
		} else if isGet {
			toPrint += fmt.Sprintf("\tGet(%s) XID %v", nextKey, nextXID)
			kv.seenXID[nextXID] = true                              // Mark this XID as seen
			kv.clientResponses[nextSender] = kv.kvDatabase[nextKey] // Store the Value that the client would have received
			kv.maxSequenceCommitted = curSequence
		}
	}
	if kv.dead {
		return nil
	}

	// Loop is ended, so the possibilities are:
	//  - we successfully got a new instance to log our command
	//  - we discovered that this command was actually a duplicate XID
	//  - we discovered that this command is actually old
	// In any case, we are up-to-date up to maxSequenceCommitted, and have
	// stored the proper response to send to the client

	// Call Done to let Paxos free memory
	kv.px.Done(kv.maxSequenceCommitted)

	// If there has been a Put since this Put was issued, return error
	// This case is apparently never reached in this lab, but it seems good
	// to have in case messages get delayed in the network right?
	if oldPut {
		toPrint += "\n\t\tIgnoring old Put command!"
		reply.PreviousValue = ""
		reply.Err = "Old"
		DPrintf(toPrint)
		return nil
	}

	// All done! Return to the client
	reply.PreviousValue = kv.clientResponses[args.Sender]
	reply.Err = ""
	toPrint += fmt.Sprintf("\n\t\tReturning <%s>", kv.val2print(reply.PreviousValue))
	DPrintf(toPrint)
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

	kv.maxSequenceCommitted = -1
	kv.kvDatabase = make(map[string]string)
	kv.seenXID = make(map[int64]bool)
	kv.clientResponses = make(map[int64]string)
	kv.lastPut = make(map[string]int64)

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

//
// Function to get a printable variable of an instance value for debugging
// Basically just exists to deal with Test 03 where 1MB values are chosen
//
// If value is a super big string, returns a string stating its length
// otherwise just returns the original value
//
func (kv *KVPaxos) val2print(v interface{}) interface{} {
	// If it is a huge string, don't print it
	vString, isString := v.(string)
	if isString && (len(vString) > 500) {
		return fmt.Sprintf("<reallyBigString length %v>", len(vString))
	}
	return v
}

// Function to write an Op struct to a string
// was used to just send strings instead of structs to Paxos for debugging
//func (kv *KVPaxos) op2string(op *Op) string {
//	res := ""
//	res += "<Command>" + op.Command + "<Command>"
//	res += "<Key>" + op.Key + "<Key>"
//	res += "<Value>" + op.Value + "<Value>"
//	res += "<XID>" + strconv.FormatInt(op.XID, 10) + "<XID>"
//	res += "<Sender>" + strconv.FormatInt(op.Sender, 10) + "<Sender>"
//	res += "<SendTime>" + strconv.FormatInt(op.SendTime, 10) + "<SendTime>"
//	return res
//}

// Function to create an Op from a string
// was used to just send strings instead of structs to Paxos for debugging
//func (kv *KVPaxos) string2op(opString string) *Op {
//	res := new(Op)
//	res.Command = strings.Split(opString, "<Command>")[1]
//	res.Key = strings.Split(opString, "<Key>")[1]
//	res.Value = strings.Split(opString, "<Value>")[1]
//	res.XID, _ = strconv.ParseInt(strings.Split(opString, "<XID>")[1], 10, 64)
//	res.Sender, _ = strconv.ParseInt(strings.Split(opString, "<Sender>")[1], 10, 64)
//	res.SendTime, _ = strconv.ParseInt(strings.Split(opString, "<SendTime>")[1], 10, 64)
//	return res
//}
