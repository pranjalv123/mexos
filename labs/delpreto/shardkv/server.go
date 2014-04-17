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

const Debug = 0

// Create a map of debug messages, indexed by XID
// This way all messages related to a particular XID can be printed out at once
// so other operations' messages don't intermingle
var toPrint map[int64]string

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func DPrintfLater(xid int64, format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		toPrint[xid] += fmt.Sprintf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	Command       string             // Either "Put" or "Get" or "Config"
	Key           string             // The key of the record in question
	Value         string             // The new value - will be set to "" for a Get command
	PreviousValue string             // The previous value for the key (used for Put operations)
	DoHash        bool               // Whether it is a Put or Puthash
	Config        shardmaster.Config // The configuration being proposed (used for Config operations)
	Err           Err                // Error sent back to the client
	XID           int64              // The unique XID of the command
	Sender        int64              // The unique ID of the Client that sent this command
}

type ShardKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos

	gid int64 // my replica group ID

	// Your definitions here.
	kvDatabase           map[int]map[string]string         // The key-value database
	currentConfig        shardmaster.Config                // The current shard configuration being used by this replica
	myShards             map[int]bool                      // The shards I am responsible for (only 'true' entries will exist)
	shardsToGet          map[int]bool                      // The shards I need to receive from other groups to finish reconfiguring (only 'true' entries will exist)
	receivedShards       map[int]*ShardArgs                // The shards I have received from other groups (indexed by configuration number)
	receivedShardsLock   sync.Mutex                        // To protect editing of receivedShards
	maxSequenceCommitted int                               // The max Paxos sequence committed on this replica
	seenXID              map[int]map[int64]bool            // Which operations have been seen on this replica, indexed by shard and then XID (only 'true' entries will exist)
	clientResponses      map[int]map[int64]*ClientResponse // Past client responses, indexed by shard and then client ID
	replyChannels        map[int64]chan Op                 // Channels used for the log-applier to send responses back to the waiting client operation method
}

// Propose the operation to Paxos
// Will execute all operations found in the log along the way
// The results will be put in the appropriate replyChannels if anyone is waiting for them
func (kv *ShardKV) proposeOp(opToPropose Op) {
	// Start proposing at the sequence after the last committed sequence
	// so we see all logged operations we may have missed
	var curSequence int
	success := false
	for !success && !kv.dead {
		// Start Paxos instance on the desired Op
		curSequence = kv.maxSequenceCommitted + 1
		DPrintf("\n%v-%v: proposing %v for sequence %v", kv.gid, kv.me, opToPropose.XID, curSequence)
		kv.px.Start(curSequence, opToPropose)

		// Wait until its Status is decided
		DPrintfLater(opToPropose.XID, "\n\t\tWaiting for decision on sequence %v", curSequence)
		decided := false
		var decidedValue interface{}
		for !decided && !kv.dead {
			time.Sleep(50 * time.Millisecond)
			decided, decidedValue = kv.px.Status(curSequence)
		}
		if kv.dead {
			break
		}

		// Get the decided operation for this sequence
		decidedOp := *new(Op)
		decidedOp.Command = decidedValue.(Op).Command
		decidedOp.Config = decidedValue.(Op).Config
		decidedOp.DoHash = decidedValue.(Op).DoHash
		decidedOp.Err = decidedValue.(Op).Err
		decidedOp.Key = decidedValue.(Op).Key
		decidedOp.PreviousValue = decidedValue.(Op).PreviousValue
		decidedOp.Sender = decidedValue.(Op).Sender
		decidedOp.Value = decidedValue.(Op).Value
		decidedOp.XID = decidedValue.(Op).XID

		DPrintfLater(opToPropose.XID, "\n\t\tDecided on XID %v", decidedOp.XID)

		// Execute this log entry
		if decidedOp.Command == "Get" {
			DPrintfLater(opToPropose.XID, "\n\t\tExecuting a Get")
			DPrintf("\n\t\t%v-%v: Executing Get(%v) %v for sequence %v", kv.gid, kv.me, decidedOp.Key, decidedOp.XID, curSequence)
			kv.getHandler(&decidedOp)
		} else if decidedOp.Command == "Put" {
			DPrintfLater(opToPropose.XID, "\n\t\tExecuting a Put")
			DPrintf("\n\t\t%v-%v: Executing Put(%v, %v) %v for sequence %v", kv.gid, kv.me, decidedOp.Key, decidedOp.Value, decidedOp.XID, curSequence)
			kv.putHandler(&decidedOp)
		} else if decidedOp.Command == "Config" {
			DPrintfLater(opToPropose.XID, "\n\t\tExecuting a Config")
			DPrintf("\n\t\t%v-%v: Executing a Config %v for sequence %v", kv.gid, kv.me, decidedOp.Config.Num, curSequence)
			//kv.configHandler(&decidedOp, decidedOp.XID == opToPropose.XID)
			kv.configHandler(&decidedOp)
		}
		// Record that we committed this operation
		kv.maxSequenceCommitted = curSequence

		// Put response in channel if someone is waiting for it
		// Note the channels are buffered so this method does not wait for value to be read out
		replyChannel, exists := kv.replyChannels[decidedOp.XID]
		if exists {
			DPrintfLater(opToPropose.XID, "\n\t\tPutting response in channel")
			DPrintf("\n\t%v-%v: decided on %v, putting in channel %v", kv.gid, kv.me, decidedOp.XID, replyChannel)
			replyChannel <- decidedOp
		} else {
			if _, printExists := toPrint[decidedOp.XID]; printExists {
				DPrintfLater(opToPropose.XID, toPrint[decidedOp.XID])
				delete(toPrint, decidedOp.XID)
			}
			DPrintfLater(opToPropose.XID, "\n\t\tNo response channel to use for %v", decidedOp.XID)
			DPrintf("\n\t%v-%v: decided on %v, no reply channel", kv.gid, kv.me, decidedOp.XID)
			// Nobody is waiting for the reply, so it was an instance we had not proposed ourselves
			// If trying to propose a configuration and have reached
			// that configuration, stop (no need to keep going since desired configuration has been reached)
			if (opToPropose.Command == "Config") && (kv.currentConfig.Num == opToPropose.Config.Num) {
				DPrintf("\n\t%v-%v: finished with proposal (early break)", kv.gid, kv.me)
				DPrintfLater(opToPropose.XID, "\n\t\tFinished with proposal (early break)")
				kv.replyChannels[opToPropose.XID] <- decidedOp
			}
		}
		// See if we are done (if we should break out of the proposal loop)
		if opToPropose.Command == "Config" {
			success = (kv.currentConfig.Num == opToPropose.Config.Num)
		} else {
			_, seenShard := kv.seenXID[key2shard(opToPropose.Key)]
			if seenShard {
				success = kv.seenXID[key2shard(opToPropose.Key)][opToPropose.XID]
			}
		}
	}
	// Either we were successfull, or we have been killed
	// Tell Paxos we are done so it can free memory
	kv.px.Done(kv.maxSequenceCommitted)

	DPrintf("\n\t%v-%v: finished with proposal for %v", kv.gid, kv.me, opToPropose.XID)
	DPrintfLater(opToPropose.XID, "\n\tFinished with proposal")
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.

	DPrintf("\n%v-%v: Get(%s) waiting for lock, from <%v>, XID <%v>", kv.gid, kv.me, args.Key, args.Sender, args.XID)

	kv.mu.Lock()
	defer kv.mu.Unlock()

	DPrintfLater(args.XID, "\n%v-%v: Get(%s) got lock, from <%v>, XID <%v>", kv.gid, kv.me, args.Key, args.Sender, args.XID)

	// If this is a duplicate command,
	// respond with last reply sent to that client
	// By checking here, we ensure that duplicate commands will not be logged in Paxos
	if kv.seenXID[key2shard(args.Key)][args.XID] {
		reply.Value = kv.clientResponses[key2shard(args.Key)][args.Sender].Value
		reply.Err = kv.clientResponses[key2shard(args.Key)][args.Sender].Err
		DPrintfLater(args.XID, "\n\tDuplicate command, responding with <%s>, Err=<%s>", reply.Value, reply.Err)
		return nil
	}

	// Create Op to propose
	var op Op
	op.Command = "Get"
	op.Key = args.Key
	op.Value = ""
	op.XID = args.XID
	op.Sender = args.Sender

	// Create new channel in which to place reply
	// Note it will be buffered so the propose method doesn't need to wait for our read
	replyChannel := make(chan Op, 1)
	kv.replyChannels[op.XID] = replyChannel

	// Call proposeOp with our Get operation
	DPrintfLater(args.XID, "\n\tCalling propose, waiting for reply")
	DPrintf("\n%v-%v: Calling propose for %v, waiting on channel %v", kv.gid, kv.me, op.XID, replyChannel)
	kv.proposeOp(op)

	// Wait for reply (for the proposal to complete and the operation to execute)
	opReply := <-replyChannel
	reply.Err = opReply.Err
	reply.Value = opReply.Value

	// Delete channel and return
	delete(kv.replyChannels, op.XID)
	DPrintfLater(args.XID, "\n\tGot reply, responding with <%s>, Err=<%s>", reply.Value, reply.Err)
	DPrintf(toPrint[args.XID])
	delete(toPrint, args.XID)
	return nil
}

func (kv *ShardKV) getHandler(op *Op) {

	DPrintfLater(op.XID, "\n\t\t\tGet(%s) handler, from <%v>, XID <%v>", op.Key, op.Sender, op.XID)

	// Determine which shard is being requested
	keyShard := key2shard(op.Key)

	// If not our shard, return error
	if !kv.myShards[keyShard] {
		op.Value = ""
		op.Err = ErrWrongGroup
		DPrintfLater(op.XID, "\n\t\t\t\tNot our shard")
	} else if kv.seenXID[keyShard][op.XID] {
		// If this is a duplicate command,
		// respond with last reply sent to that client
		op.Value = kv.clientResponses[keyShard][op.Sender].Value
		op.Err = kv.clientResponses[keyShard][op.Sender].Err
		DPrintfLater(op.XID, "\n\t\t\t\tDuplicate command, responding with <%s>, Err=<%s>", op.Value, op.Err)
	} else {
		// Perform the Get operation
		op.Err = OK
		value, exists := kv.kvDatabase[keyShard][op.Key]
		op.Value = value
		if !exists {
			op.Err = ErrNoKey
		}
		DPrintfLater(op.XID, "\n\t\t\t\tValue is <%s>", op.Value)
	}

	// Record that this XID has been seen
	if _, seenXIDExists := kv.seenXID[keyShard]; !seenXIDExists {
		kv.seenXID[keyShard] = make(map[int64]bool)
	}
	kv.seenXID[keyShard][op.XID] = true

	// Store this response in list of previous client responses
	if _, responseShardExists := kv.clientResponses[keyShard]; !responseShardExists {
		kv.clientResponses[keyShard] = make(map[int64]*ClientResponse)
	}
	kv.clientResponses[keyShard][op.Sender] = new(ClientResponse) // Store the Value that the client would have received
	kv.clientResponses[keyShard][op.Sender].Err = op.Err
	kv.clientResponses[keyShard][op.Sender].PreviousValue = op.PreviousValue
	kv.clientResponses[keyShard][op.Sender].Value = op.Value
	kv.clientResponses[keyShard][op.Sender].Sender = op.Sender
}

func (kv *ShardKV) Put(args *PutArgs, reply *PutReply) error {
	// Your code here.

	DPrintf("\n%v-%v: Put(%s, %s, %v) waiting for lock, from <%v>, XID <%v>", kv.gid, kv.me, args.Key, args.Value, args.DoHash, args.Sender, args.XID)

	kv.mu.Lock()
	defer kv.mu.Unlock()

	DPrintfLater(args.XID, "\n%v-%v: Put(%s, %s, %v) got lock, from <%v>, XID <%v>", kv.gid, kv.me, args.Key, args.Value, args.DoHash, args.Sender, args.XID)
	// If this is a duplicate command,
	// respond with last reply sent to that client
	// Checking here ensures that no duplicate operations will be logged in Paxos
	if kv.seenXID[key2shard(args.Key)][args.XID] {
		reply.PreviousValue = kv.clientResponses[key2shard(args.Key)][args.Sender].PreviousValue
		reply.Err = kv.clientResponses[key2shard(args.Key)][args.Sender].Err
		DPrintfLater(args.XID, "\n\tDuplicate command, responding with <%s>, Err=<%s>", reply.PreviousValue, reply.Err)
		return nil
	}

	// Create Op to propose
	var op Op
	op.Command = "Put"
	op.Key = args.Key
	op.Value = args.Value
	op.DoHash = args.DoHash
	op.XID = args.XID
	op.Sender = args.Sender

	// Create new channel in which to place reply
	// Note it will be buffered so the propose method doesn't need to wait for our read
	replyChannel := make(chan Op, 1)
	kv.replyChannels[op.XID] = replyChannel

	// Call proposeOp for the desired Put
	DPrintfLater(op.XID, "\n\tCalling propose, waiting for reply")
	DPrintf("\n%v-%v: Calling propose for %v, waiting on channel %v", kv.gid, kv.me, op.XID, replyChannel)
	kv.proposeOp(op)

	// Wait for reply (for operation to be logged and executed)
	opReply := <-replyChannel
	reply.Err = opReply.Err
	reply.PreviousValue = opReply.PreviousValue

	// Delete channel and return
	delete(kv.replyChannels, op.XID)
	DPrintfLater(op.XID, "\n\tGot reply, responding with <%s>, Err=<%s>", reply.PreviousValue, reply.Err)
	DPrintf(toPrint[op.XID])
	delete(toPrint, op.XID)
	return nil
}

func (kv *ShardKV) putHandler(op *Op) {

	DPrintfLater(op.XID, "\n\t\t\tPut(%s, %s, %v) handler, from <%v>, XID <%v>", op.Key, op.Value, op.DoHash, op.Sender, op.XID)

	// Determine which shard is being acted upon
	keyShard := key2shard(op.Key)

	// If not our shard, return error
	if !kv.myShards[keyShard] {
		op.PreviousValue = ""
		op.Err = ErrWrongGroup
		DPrintfLater(op.XID, "\n\t\t\t\tNot our shard")
	} else if kv.seenXID[keyShard][op.XID] {
		// If this is a duplicate command,
		// respond with last reply sent to that client
		op.PreviousValue = kv.clientResponses[keyShard][op.Sender].PreviousValue
		op.Err = kv.clientResponses[keyShard][op.Sender].Err
		DPrintfLater(op.XID, "\n\t\t\t\tDuplicate command, responding with <%s>, Err=<%s>", op.PreviousValue, op.Err)
	} else {
		// Create map for shard if haven't seen it yet
		_, shardExists := kv.kvDatabase[keyShard]
		if !shardExists {
			kv.kvDatabase[keyShard] = make(map[string]string)
		}
		// Perform the desired Put operation
		op.Err = OK
		op.PreviousValue = kv.kvDatabase[keyShard][op.Key]
		if op.DoHash {
			hashRes := hash(op.PreviousValue + op.Value)
			kv.kvDatabase[keyShard][op.Key] = strconv.Itoa(int(hashRes))
		} else {
			kv.kvDatabase[keyShard][op.Key] = op.Value
		}
		DPrintfLater(op.XID, "\n\t\t\t\tPrevious Value is <%s>, New Value is <%s>", op.PreviousValue, kv.kvDatabase[keyShard][op.Key])
	}

	// Record that we have seen this XID
	if _, seenXIDExists := kv.seenXID[keyShard]; !seenXIDExists {
		kv.seenXID[keyShard] = make(map[int64]bool)
	}
	kv.seenXID[keyShard][op.XID] = true

	// Store this response as a previous client response
	if _, responseShardExists := kv.clientResponses[keyShard]; !responseShardExists {
		kv.clientResponses[keyShard] = make(map[int64]*ClientResponse)
	}
	kv.clientResponses[keyShard][op.Sender] = new(ClientResponse) // Store the Value that the client would have received
	kv.clientResponses[keyShard][op.Sender].Err = op.Err
	kv.clientResponses[keyShard][op.Sender].PreviousValue = op.PreviousValue
	kv.clientResponses[keyShard][op.Sender].Value = op.Value
	kv.clientResponses[keyShard][op.Sender].Sender = op.Sender
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
	// Get the newest configuration
	latestConfig := kv.sm.Query(-1)
	DPrintf("\n%v-%v: Tick sees config %v, shards %v", kv.gid, kv.me, latestConfig.Num, latestConfig.Shards)

	// If different than ours, call proposeConfiguration
	if latestConfig.Num == kv.currentConfig.Num+1 {
		DPrintf("\n\t%v-%v: Tick proposing %v", kv.gid, kv.me, latestConfig.Num)
		kv.proposeConfiguration(latestConfig)
	} else if latestConfig.Num > kv.currentConfig.Num {
		// We have missed some intermediate configurations
		// One by one, propose all configurations between ours and the updated one
		nextNum := kv.currentConfig.Num + 1
		for nextNum < latestConfig.Num && !kv.dead {
			DPrintf("\n\t%v-%v: Tick proposing %v", kv.gid, kv.me, nextNum)
			kv.proposeConfiguration(kv.sm.Query(nextNum))
			latestConfig = kv.sm.Query(-1)
			nextNum = kv.currentConfig.Num + 1
		}
	}
}

func (kv *ShardKV) proposeConfiguration(config shardmaster.Config) {

	DPrintf("\n%v-%v: proposeConfiguration(%v) waiting for lock", kv.gid, kv.me, config.Num)

	kv.mu.Lock()
	defer kv.mu.Unlock()

	// Create Op to propose with a new, unique XID
	var op Op
	op.Command = "Config"
	op.XID = nrand()
	op.Sender = int64(kv.me)
	op.Config = config

	DPrintfLater(op.XID, "\n%v-%v: ProposeConfig for config %v", kv.gid, kv.me, config.Num)

	// If this is an old configuration, do nothing
	// This may occur if intermediate configuration proposals were found in the Paxos log
	// while this method was waiting for the lock
	if op.Config.Num <= kv.currentConfig.Num {
		DPrintfLater(op.XID, "\n\tIgnoring old configuration")
		DPrintf("\n%v-%v: proposeConfig for config %v - ignoring old config", kv.gid, kv.me, op.Config.Num)
		return
	}

	// Create new channel in which to place reply
	// Note it will be buffered so the proposal method does not need to wait for our read
	replyChannel := make(chan Op, 1)
	kv.replyChannels[op.XID] = replyChannel

	// Call proposeOp for the reconfiguration
	DPrintfLater(op.XID, "\n\tCalling propose, waiting for reply on XID %v", op.XID)
	kv.proposeOp(op)

	// Wait for reply (for the reconfiguration to complete)
	<-replyChannel

	// Delete channel and return
	delete(kv.replyChannels, op.XID)
	DPrintfLater(op.XID, "\n\tProposal complete")
	DPrintf(toPrint[op.XID])
	delete(toPrint, op.XID)
}

func (kv *ShardKV) configHandler(op *Op) {
	DPrintf("\n%v-%v: Config handler for config %v", kv.gid, kv.me, op.Config.Num)
	DPrintfLater(op.XID, "\n\t\t\tConfig handler for config %v (GID %v)", op.Config.Num, kv.gid)
	DPrintfLater(op.XID, "\n\t\t\tShards: %v", op.Config.Shards)

	newConfig := op.Config
	// Make sure this is actually a new configuration
	// (in case intermediate entries in the Paxos log updated the config before this one was reached)
	if newConfig.Num <= kv.currentConfig.Num {
		DPrintfLater(op.XID, "\n\t\t\tIgnoring old configuration")
		DPrintf("\n%v-%v: Config handler for config %v - ignoring old config", kv.gid, kv.me, op.Config.Num)
		return
	}

	// Determine which shards are now our responsibility
	// And which of our shards need to be sent to others
	myNewShards := make(map[int]bool)          // only 'true' entries will exist
	shardsToSend := make(map[int64]*ShardArgs) // indexed by the GID that need the shards
	for shard, shardGID := range newConfig.Shards {
		wasMyShard := kv.myShards[shard]
		// If this shard is now assigned to my GID, record it
		if shardGID == kv.gid {
			myNewShards[shard] = true
		} else if wasMyShard {
			// It is not assigned to me now, but it used to be
			// so record who now needs the data now
			_, exists := shardsToSend[shardGID]
			if !exists {
				shardsToSend[shardGID] = new(ShardArgs)
				shardsToSend[shardGID].Sender = fmt.Sprintf("%v-%v", kv.gid, kv.me)
				shardsToSend[shardGID].ConfigNum = newConfig.Num
				shardsToSend[shardGID].Shards = make(map[int]map[string]string)
				shardsToSend[shardGID].ClientResponses = make(map[int]map[int64]ClientResponse)
				shardsToSend[shardGID].SeenXIDs = make(map[int]map[int64]bool)
			}
			// By initializing these maps, we 'mark' that they need to be populated/sent
			shardsToSend[shardGID].Shards[shard] = make(map[string]string)
			shardsToSend[shardGID].ClientResponses[shard] = make(map[int64]ClientResponse)
			shardsToSend[shardGID].SeenXIDs[shard] = make(map[int64]bool)
		}
	}
	DPrintfLater(op.XID, "\n\t\t\t\tmyShards:    %v (len %v)", kv.myShards, len(kv.myShards))
	DPrintfLater(op.XID, "\n\t\t\t\tmyNewShards: %v (len %v)", myNewShards, len(myNewShards))

	// If NewConfig number is 1, just update currentConfig and return (no need to send shards / wait for shards)
	if newConfig.Num == 1 {
		kv.currentConfig = newConfig
		kv.myShards = myNewShards
		DPrintfLater(op.XID, "\n\t\t\t\tConfiguration complete")
		DPrintf("\n\t%v-%v: config handler finished with XID %v for config %v", kv.gid, kv.me, op.XID, op.Config.Num)
		return
	}

	// Loop through GIDs that need to get shards from us
	// and send them to all servers in that group
	// Having this immediately send them seems simpler than actively requesting the shards
	DPrintf("\n%v-%v: Config handler for config %v looping through GIDs", kv.gid, kv.me, op.Config.Num)
	for destGID, shardArgs := range shardsToSend {
		// Look up which shards this GID needs, and populate the key/value data for it
		DPrintfLater(op.XID, "\n\t\t\t\tSending %v [", destGID)
		for shard, shardValues := range shardArgs.Shards { // Will find the maps initialized above
			DPrintfLater(op.XID, "%v ", shard)
			for key, value := range kv.kvDatabase[shard] {
				shardValues[key] = value
			}
		}
		DPrintfLater(op.XID, "]")
		// Look up which shards this GID needs, and populate the client response data for it
		for shard, responses := range shardArgs.ClientResponses { // Will find the maps initialized above
			for gid, response := range kv.clientResponses[shard] {
				responses[gid] = *response
			}
		}
		// Look up which shards this GID needs, and populate which XIDs have been seen for it
		for shard, seenXIDs := range shardArgs.SeenXIDs { // Will find the maps initialized above
			for gid, _ := range kv.seenXID[shard] {
				seenXIDs[gid] = true
			}
		}

		// Send the shards to everyone in the destination GID
		// resending until success (in case of unreliable connections)
		waitChan := make(chan int)
		for _, nextServer := range newConfig.Groups[destGID] {
			go func(inServer string, args ShardArgs) {
				// Make a clone of all needed data so we don't risk
				// these getting wrong data when this replica continues with new operations
				destServer := inServer
				var sendArgs ShardArgs
				sendArgs.ConfigNum = args.ConfigNum
				sendArgs.Sender = args.Sender
				sendArgs.Shards = make(map[int]map[string]string)
				sendArgs.ClientResponses = make(map[int]map[int64]ClientResponse)
				sendArgs.SeenXIDs = make(map[int]map[int64]bool)
				// Clone any needed key/value data
				for shard, values := range args.Shards {
					sendArgs.Shards[shard] = make(map[string]string)
					for key, value := range values {
						sendArgs.Shards[shard][key] = value
					}
				}
				// Clone any needed client response data
				for shard, responses := range args.ClientResponses {
					sendArgs.ClientResponses[shard] = make(map[int64]ClientResponse)
					for gid, response := range responses {
						responseClone := *new(ClientResponse)
						responseClone.Err = response.Err
						responseClone.PreviousValue = response.PreviousValue
						responseClone.Sender = response.Sender
						responseClone.Value = response.Value
						sendArgs.ClientResponses[shard][gid] = responseClone
					}
				}
				// Clone any needed XID data
				for shard, xids := range args.SeenXIDs {
					sendArgs.SeenXIDs[shard] = make(map[int64]bool)
					for gid, _ := range xids {
						sendArgs.SeenXIDs[shard][gid] = true
					}
				}
				var reply ShardReply
				var ok bool
				// Have cloned all needed data, so allow next go func to be called
				waitChan <- 1

				// Send the data!
				rpcTries := 0
				for !ok && !kv.dead {
					localPrint := fmt.Sprintf("\n%v-%v: Config handler for config %v sending to %v", kv.gid, kv.me, sendArgs.ConfigNum, destServer)
					ok = call(destServer, "ShardKV.NewShards", sendArgs, &reply)
					localPrint += fmt.Sprintf("\n\tOk after call: %v", ok)
					if !ok {
						rpcTries += 1
					} else {
						rpcTries = 0
					}
					if ok && (reply.Err != OK) {
						ok = false
						localPrint += fmt.Sprintf("\n\tError reply: %v", reply.Err)
					}
					localPrint += fmt.Sprintf("\n\tOk: %v", ok)
					DPrintf(localPrint)
					time.Sleep(150 * time.Millisecond)
					// Decide replica is dead at some point
					// Probably not needed (and in fact is never reached), but seems nice to have
					if rpcTries > 150 {
						DPrintf("\n%v-%v: ** GIVING UP RESEND TO %v**", destServer)
						break
					}
				}
			}(nextServer, *shardArgs)
			<-waitChan
		}
	} // Have now finished sending all of our data to those that will need it

	// Determine shards that we need to get from others
	DPrintf("\n%v-%v: Config handler for config %v determining our shards", kv.gid, kv.me, op.Config.Num)
	kv.shardsToGet = make(map[int]bool)
	DPrintfLater(op.XID, "\n\t\t\t\tWe need to get shards [")
	for newShard := range myNewShards {
		if !kv.myShards[newShard] {
			kv.shardsToGet[newShard] = true
			DPrintfLater(op.XID, "%v ", newShard)
		}
	}
	DPrintfLater(op.XID, "]")

	// If we need to get any shards from others, wait until we have received them
	// We may have already received them in the past (before we knew we needed them)
	// in which case they will be stored in receivedShards already (see NewShards method below)
	// Note that the lock is held, so all operations are stalled until reconfiguration is complete
	if len(kv.shardsToGet) > 0 {
		reconfiguring := true
		// Wait until all of these shards have been received
		DPrintf("\n%v-%v: Config handler for config %v waiting for our shards", kv.gid, kv.me, op.Config.Num)
		for reconfiguring {
			time.Sleep(25 * time.Millisecond)
			kv.receivedShardsLock.Lock() // Coordinate usage with the NewShards method
			// See if have received any shards for this configuration
			shardArgs, haveShardArgs := kv.receivedShards[newConfig.Num]
			if haveShardArgs {
				// See if we have received any of the shards we are waiting for
				for shard, _ := range kv.shardsToGet {
					_, haveShard := shardArgs.Shards[shard]
					if haveShard {
						// Copy key/value data for this shard
						_, dataShardExists := kv.kvDatabase[shard]
						if !dataShardExists {
							kv.kvDatabase[shard] = make(map[string]string)
						}
						for key, value := range shardArgs.Shards[shard] {
							kv.kvDatabase[shard][key] = value
						}
						// Copy client response data for this shard
						// Note that we delete any previous responses we already had for this shard
						// since they must have been stale (from at least the previous configuration)
						kv.clientResponses[shard] = make(map[int64]*ClientResponse)
						for gid, response := range shardArgs.ClientResponses[shard] {
							responseClone := new(ClientResponse)
							responseClone.Err = response.Err
							responseClone.PreviousValue = response.PreviousValue
							responseClone.Value = response.Value
							responseClone.Sender = response.Sender
							kv.clientResponses[shard][gid] = responseClone
						}
						// Copy seen XID data for this shard
						// Note that we delete any previous records we already had for this shard
						// since they must have been stale (from at least the previous configuration)
						kv.seenXID[shard] = make(map[int64]bool)
						for gid, _ := range shardArgs.SeenXIDs[shard] {
							kv.seenXID[shard][gid] = true
						}

						// Mark that we no longer need to receive this shard
						delete(kv.shardsToGet, shard)
					}
				}
			}
			// Check if we have received all needed data
			if len(kv.shardsToGet) == 0 {
				reconfiguring = false
				delete(kv.receivedShards, kv.currentConfig.Num)
				kv.currentConfig = newConfig
				kv.myShards = myNewShards
			}
			kv.receivedShardsLock.Unlock()
		}
	} else {
		DPrintfLater(op.XID, "\n\t\t\t\tReconfiguration complete")
		delete(kv.receivedShards, kv.currentConfig.Num)
		kv.currentConfig = newConfig
		kv.myShards = myNewShards
	}

	// Have now received all my data,
	// and started threads to give data to others

	// All done!
}

func (kv *ShardKV) NewShards(args *ShardArgs, reply *ShardReply) error {

	localToPrint := fmt.Sprintf("\n%v-%v: In NewShards for config %v from %s", kv.gid, kv.me, args.ConfigNum, args.Sender)

	// If already have these, respond with OK
	if args.ConfigNum <= kv.currentConfig.Num {
		reply.Err = OK
		localToPrint += "\n\tDon't need the shards, returning OK"
		DPrintf(localToPrint)
		return nil
	}

	// Now know the data is for a higher configuration than ours
	// (possibly one we are currently reconfiguring to achieve, possibly an even higher one we don't know about yet)

	// Store the data
	kv.receivedShardsLock.Lock()
	_, configEntryExists := kv.receivedShards[args.ConfigNum]
	if !configEntryExists {
		kv.receivedShards[args.ConfigNum] = new(ShardArgs)
		kv.receivedShards[args.ConfigNum].ConfigNum = args.ConfigNum
		kv.receivedShards[args.ConfigNum].Shards = make(map[int]map[string]string)
		kv.receivedShards[args.ConfigNum].ClientResponses = make(map[int]map[int64]ClientResponse)
		kv.receivedShards[args.ConfigNum].SeenXIDs = make(map[int]map[int64]bool)
	}
	// Copy key/value data for each shard
	for shard, shardValues := range args.Shards {
		_, shardEntryExists := kv.receivedShards[args.ConfigNum].Shards[shard]
		if !shardEntryExists {
			kv.receivedShards[args.ConfigNum].Shards[shard] = make(map[string]string)
		}
		for key, value := range shardValues {
			kv.receivedShards[args.ConfigNum].Shards[shard][key] = value
		}
	}
	// Copy client response data for each shard
	for shard, responses := range args.ClientResponses {
		_, shardEntryExists := kv.receivedShards[args.ConfigNum].ClientResponses[shard]
		if !shardEntryExists {
			kv.receivedShards[args.ConfigNum].ClientResponses[shard] = make(map[int64]ClientResponse)
		}
		for gid, response := range responses {
			responseClone := *new(ClientResponse)
			responseClone.Err = response.Err
			responseClone.PreviousValue = response.PreviousValue
			responseClone.Value = response.Value
			responseClone.Sender = response.Sender
			kv.receivedShards[args.ConfigNum].ClientResponses[shard][gid] = responseClone
		}
	}
	// Copy seen XID data for each shard
	for shard, xids := range args.SeenXIDs {
		_, shardEntryExists := kv.receivedShards[args.ConfigNum].SeenXIDs[shard]
		if !shardEntryExists {
			kv.receivedShards[args.ConfigNum].SeenXIDs[shard] = make(map[int64]bool)
		}
		for gid, _ := range xids {
			kv.receivedShards[args.ConfigNum].SeenXIDs[shard][gid] = true
		}
	}
	kv.receivedShardsLock.Unlock()

	// Have now stored all shard data that was being sent in the appropriate configuration index

	localToPrint += fmt.Sprintf("\n\t%v-%v: In NewShards for config %v, copied shards", kv.gid, kv.me, args.ConfigNum)
	reply.Err = OK
	DPrintf(localToPrint)
	return nil
}

// tell the server to shut itself down.
func (kv *ShardKV) kill() {
	DPrintf("\n%v-%v: *** KILLED ***", kv.gid, kv.me)
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
	gob.Register(ClientResponse{})

	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)

	// Your initialization code here.
	// Don't call Join().
	DPrintf("\n\n%v-%v: My servers are %v\n\nMy shardmasters are %v\n\n", kv.gid, kv.me, servers, shardmasters)
	toPrint = make(map[int64]string)
	kv.kvDatabase = make(map[int]map[string]string)
	kv.currentConfig = shardmaster.Config{}
	kv.myShards = make(map[int]bool)
	kv.receivedShards = make(map[int]*ShardArgs)
	kv.maxSequenceCommitted = -1
	kv.seenXID = make(map[int]map[int64]bool)
	kv.clientResponses = make(map[int]map[int64]*ClientResponse)
	kv.replyChannels = make(map[int64]chan Op)

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
