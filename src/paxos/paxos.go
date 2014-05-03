package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"
import "os"
import "syscall"
import "sync"
import "fmt"
import "math/rand"
import "time"
import "strconv"

import "github.com/jmhodges/levigo"
import "encoding/gob"
import "bytes"

const startport = 2100
const printRPCerrors = false
const persistent = true

const Debug = 0
const DebugPersist = 0

func (px *Paxos) DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		fmt.Printf(format, a...)
	}
	return
}

func (px *Paxos) DPrintfPersist(format string, a ...interface{}) (n int, err error) {
	if DebugPersist > 0 {
		fmt.Printf(format, a...)
	}
	return
}

// Structure for a proposal status
// Will be written to disk for each sequence
// (note Paxos persistence requires writing Np, Na, and Va so using Proposal just has an extra bool)
type Proposal struct {
	Prepare int
	Accept  int
	Value   interface{}
	Decided bool
}

type Paxos struct {
	mu   sync.Mutex
	l    net.Listener
	dead bool

	// Paxos state
	instances    map[int]Proposal
	maxInstance  int
	done         map[int]int
	doneChannels map[int]chan bool

	// Networking stuff
	unreliable bool
	network    bool
	deaf       bool
	rpcCount   int
	reachable  []bool
	peers      []string
	me         int // index into peers[]

	// Persistence stuff
	dbReadOptions  *levigo.ReadOptions
	dbWriteOptions *levigo.WriteOptions
	dbOpts         *levigo.Options
	dbName         string
	db             *levigo.DB
	dbLock         sync.Mutex
	dbMaxInstance  int
}

type PrepareArgs struct {
	Instance int
	PID      int
}

type PrepareReply struct {
	Err   bool
	PID   int
	Value interface{}
	Done  int
}

type AcceptArgs struct {
	Instance int
	PID      int
	Value    interface{}
}

type AcceptReply struct {
	Err  bool
	PID  int
	Done int
}

type DecideArgs struct {
	Instance int
	PID      int
	Value    interface{}
}

type DecideReply struct {
	Err  bool
	Done int
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func (px *Paxos) callWrap(srv string, name string, args interface{},
	reply interface{}) bool {
	//check if unreachable, for partition tests
	for i, p := range px.peers {
		if p == srv && !px.reachable[i] {
			return false
		}
	}
	return call(srv, name, args, reply, px.network)
}

func call(srv string, name string, args interface{}, reply interface{},
	network bool) bool {
	if network {
		c, err := rpc.Dial("tcp", srv)
		if err != nil {
			err1 := err.(*net.OpError)
			if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
				fmt.Printf("paxos Dial() failed: %v\n", err1)
			}
			return false
		}
		defer c.Close()

		err = c.Call(name, args, reply)
		if err == nil {
			return true
		}

		if printRPCerrors {
			fmt.Println(err)
		}
		return false
	} else {
		c, err := rpc.Dial("unix", srv)
		if err != nil {
			err1 := err.(*net.OpError)
			if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
				fmt.Printf("paxos Dial() failed: %v\n", err1)
			}
			return false
		}
		defer c.Close()

		err = c.Call(name, args, reply)
		if err == nil {
			return true
		}

		if printRPCerrors {
			fmt.Println(err)
		}
		return false
	}
}

// Wrapper for calling prepare, accept, or decide
// Uses local function if calling myself, otherwise uses RPC
func (px *Paxos) callAcceptor(index int, name string, args interface{}, reply interface{}) bool {
	if index == px.me {
		if name == "Paxos.Prepare" {
			return px.Prepare(args.(*PrepareArgs), reply.(*PrepareReply)) == nil
		} else if name == "Paxos.Accept" {
			return px.Accept(args.(*AcceptArgs), reply.(*AcceptReply)) == nil
		} else if name == "Paxos.Decide" {
			return px.Decide(args.(*DecideArgs), reply.(*DecideReply)) == nil
		}
	}
	return px.callWrap(px.peers[index], name, args, reply)
}

// Get the instance of the given sequence number
// If haven't heard about it, creats a new instance for that sequence
// Also updates px.maxInstance
func (px *Paxos) getInstance(seq int) Proposal {
	// Load instance from memory if available
	// otherwise look in database
	if _, ok := px.instances[seq]; !ok {
		px.instances[seq] = px.dbGetInstance(seq)
	} else {
		if seq > px.maxInstance {
			px.maxInstance = seq
		}
	}
	prop := px.instances[seq]
	return prop
}

// Continually check if old instances can be deleted
// Note that Min() actually processes the Done messages
func (px *Paxos) doneCollector() {
	oldMin := -1 // Index of highest instance that has been deleted already
	for !px.dead {
		// Find out min instance to preserve
		min := px.Min()
		// Delete any instances not already deleted
		if min > oldMin {
			px.mu.Lock()
			for k := range px.instances {
				if k < min {
					px.instances[k] = Proposal{-1, -1, nil, false}
					delete(px.instances, k)
					px.dbDeleteInstance(k)
				}
			}
			px.mu.Unlock()
			oldMin = min
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// Respond to a Prepare request
func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
	px.mu.Lock()
	px.DPrintf("\n%v: Received prepare for sequence %v", px.me, args.Instance)
	prop := px.getInstance(args.Instance)
	reply.Err = true
	reply.Done = px.done[px.me]

	// Check if proposal number is high enough
	if args.PID > prop.Prepare {
		px.instances[args.Instance] = Proposal{args.PID, prop.Accept, prop.Value, prop.Decided}
		px.dbWriteInstance(args.Instance, px.instances[args.Instance])
		reply.Err = false
		reply.PID = prop.Accept
		reply.Value = prop.Value
	}
	px.mu.Unlock()
	return nil
}

// Respond to an Accept request
func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
	px.mu.Lock()
	px.DPrintf("\n%v: Received accept for sequence %v", px.me, args.Instance)
	// Create instance if needed (note prepare request may have been lost in network)
	prop := px.getInstance(args.Instance)
	reply.Err = true
	reply.Done = px.done[px.me]

	if args.PID >= prop.Prepare {
		px.instances[args.Instance] = Proposal{args.PID, args.PID, args.Value, prop.Decided}
		px.dbWriteInstance(args.Instance, px.instances[args.Instance])
		reply.Err = false
		reply.PID = args.PID
	}
	px.mu.Unlock()
	return nil
}

// Respond to a Decide request
func (px *Paxos) Decide(args *DecideArgs, reply *DecideReply) error {
	px.mu.Lock()
	px.DPrintf("\n%v: Received decide for sequence %v", px.me, args.Instance)
	px.instances[args.Instance] = Proposal{args.PID, args.PID, args.Value, true}
	px.dbWriteInstance(args.Instance, px.instances[args.Instance])
	if args.Instance > px.maxInstance {
		px.maxInstance = args.Instance
	}
	px.mu.Unlock()
	reply.Err = false
	reply.Done = px.done[px.me]
	// If someone is listening for this sequence to finish, let them know
	// (used for benchmark speed tests to avoid sleeping)
	if channel, exists := px.doneChannels[args.Instance]; exists {
		channel <- true
		delete(px.doneChannels, args.Instance)
	}
	return nil
}

// Propose a value for a sequence (will do prepare, accept, decide)
func (px *Paxos) Propose(seq int, v interface{}) {
	px.DPrintf("\n%v: Starting proposal for sequence %v", px.me, seq)
	px.mu.Lock()
	prop := px.getInstance(seq)
	px.mu.Unlock()
	nPID := 0
	for !prop.Decided && !px.dead {
		total := len(px.peers)
		hPID := -1
		hValue := v
		ok := 0

		// Send Prepare requests to everyone (and record piggybacked done response)
		px.DPrintf("\n%v: Sending prepare for sequence %v", px.me, seq)
		for i := 0; i < len(px.peers); i++ {
			args := &PrepareArgs{seq, nPID}
			var reply PrepareReply
			if px.callAcceptor(i, "Paxos.Prepare", args, &reply) && !reply.Err {
				px.recordDone(i, reply.Done)
				ok += 1
				// Record highest prepare number / value among resonses
				if reply.PID > hPID {
					hPID = reply.PID
					hValue = reply.Value
				}
			}
		}
		// If prepare was rejected, start over with new proposal value
		if ok <= total/2 {
			px.mu.Lock()
			prop = px.getInstance(seq)
			px.mu.Unlock()
			if hPID > nPID {
				nPID = hPID + 1
			} else {
				nPID = nPID + 1
			}
			continue
		}

		// Send Accept requests to everyone (and record piggybacked done response)
		px.DPrintf("\n%v: Sending accept for sequence %v", px.me, seq)
		ok = 0
		for i := 0; i < len(px.peers); i++ {
			args := &AcceptArgs{seq, nPID, hValue}
			var reply AcceptReply
			if px.callAcceptor(i, "Paxos.Accept", args, &reply) && !reply.Err {
				px.recordDone(i, reply.Done)
				ok += 1
			}
		}
		// If accept was rejected, start over with new proposal value
		if ok <= total/2 {
			px.mu.Lock()
			prop = px.getInstance(seq)
			px.mu.Unlock()
			if hPID > nPID {
				nPID = hPID + 1
			} else {
				nPID = nPID + 1
			}
			continue
		}

		// Send Decided messages to everyone (and record piggybacked done response)
		px.DPrintf("\n%v: Sending decided for sequence %v", px.me, seq)
		for i := 0; i < len(px.peers); i++ {
			args := &DecideArgs{seq, nPID, hValue}
			var reply DecideReply
			if px.callAcceptor(i, "Paxos.Decide", args, &reply) && !reply.Err {
				px.recordDone(i, reply.Done)
			}
		}
		break
	}
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	go px.Propose(seq, v)
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	px.recordDone(px.me, seq)
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	return px.maxInstance
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	minDone := px.done[px.me]
	for _, v := range px.done {
		if v < minDone {
			minDone = v
		}
	}
	return minDone + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {
	px.mu.Lock()
	prop := px.getInstance(seq)
	px.mu.Unlock()
	return prop.Decided, prop.Value
}

// Record a "done" value from a peer
func (px *Paxos) recordDone(peer int, val int) {
	px.mu.Lock()
	defer px.mu.Unlock()
	px.DPrintf("\n%v: recording done %v, %v", px.me, peer, val)
	oldVal := px.done[peer]
	if val > oldVal {
		px.done[peer] = val
		px.dbWriteDone()
	}
}

// Set a channel which can be used to listen for when a sequence is decided
// Used by benchmark speed tests to avoid sleep times
func (px *Paxos) SetDoneChannel(seq int, channel chan bool) {
	px.doneChannels[seq] = channel
}

//
// tell the peer to shut itself down.
// deletes disk contents
//
func (px *Paxos) Kill() {
	// Just double check that Kill isn't called multiple times
	// (trying to close px.db multiple times causes a panic)
	if px.dead {
		return
	}

	// Kill the server
	px.KillSaveDisk()

	// Destroy the database
	if persistent {
		px.DPrintfPersist("\n%v: Destroying database... ", px.me)
		err := levigo.DestroyDatabase(px.dbName, px.dbOpts)
		if err != nil {
			px.DPrintfPersist("\terror")
		} else {
			px.DPrintfPersist("\tsuccess")
		}
	}
}

func (px *Paxos) KillSaveDisk() {
	// Just double check that Kill isn't called multiple times
	// (trying to close px.db multiple times causes a panic)
	if px.dead {
		return
	}
	// Kill the server
	px.DPrintf("\n%v: Killing the server", px.me)
	px.dead = true
	if px.l != nil {
		px.l.Close()
	}
	// Close the database
	if persistent {
		px.dbLock.Lock()
		px.db.Close()
		px.dbLock.Unlock()
	}
}

// Writes the given instance to the database
func (px *Paxos) dbWriteInstance(seq int, toWrite Proposal) {
	if !persistent {
		return
	}
	px.dbLock.Lock()
	defer px.dbLock.Unlock()
	if px.dead {
		return
	}

	toPrint := ""
	toPrint += fmt.Sprintf("\n%v: Writing instance %v to database... ", px.me, seq)
	// Encode the instance into a byte array
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	err := enc.Encode(toWrite)
	if err != nil {
		px.DPrintfPersist("\terror encoding: %s", fmt.Sprint(err))
	} else {
		// Write the state to the database
		key := "instance_" + strconv.Itoa(seq)
		err := px.db.Put(px.dbWriteOptions, []byte(key), buffer.Bytes())
		if err != nil {
			toPrint += fmt.Sprintf("\terror writing to database")
		} else {
			toPrint += fmt.Sprintf("\tsuccess")
			// Record max instance
			if seq > px.dbMaxInstance {
				px.dbWriteMaxInstance(seq)
			}
		}
	}
	px.DPrintfPersist(toPrint)
}

// Tries to get the desired instance from the database
// If it doesn't exist, returns empty Proposal
func (px *Paxos) dbGetInstance(toGet int) Proposal {
	if !persistent {
		return Proposal{-1, -1, nil, false}
	}
	px.dbLock.Lock()
	defer px.dbLock.Unlock()
	if px.dead {
		return Proposal{-1, -1, nil, false}
	}

	toPrint := ""
	toPrint += fmt.Sprintf("\n%v: Reading instance %v from database... ", px.me, toGet)
	// Read entry from database if it exists
	key := "instance_" + strconv.Itoa(toGet)
	entryBytes, err := px.db.Get(px.dbReadOptions, []byte(key))

	// Decode the entry if it exists, otherwise return empty
	if err == nil && len(entryBytes) > 0 {
		toPrint += "\tDecoding entry... "
		buffer := *bytes.NewBuffer(entryBytes)
		decoder := gob.NewDecoder(&buffer)
		var entryDecoded Proposal
		err = decoder.Decode(&entryDecoded)
		if err != nil {
			toPrint += "\terror"
		} else {
			toPrint += "\tsuccess"
			px.DPrintfPersist(toPrint)
			return entryDecoded
		}
	} else {
		toPrint += fmt.Sprintf("\tNo entry found in database %s", fmt.Sprint(err))
		px.DPrintfPersist(toPrint)
		return Proposal{-1, -1, nil, false}
	}

	px.DPrintfPersist(toPrint)
	return Proposal{-1, -1, nil, false}
}

// Deletes the given instance from the database
func (px *Paxos) dbDeleteInstance(seq int) {
	if !persistent {
		return
	}
	px.dbLock.Lock()
	defer px.dbLock.Unlock()
	if px.dead {
		return
	}

	px.DPrintfPersist("\n%v: Deleting instance %v from the database... ", px.me, seq)
	// Delete entry if it exists
	key := "instance_" + strconv.Itoa(seq)
	err := px.db.Delete(px.dbWriteOptions, []byte(key))
	if err != nil {
		px.DPrintfPersist("\terror")
	} else {
		px.DPrintfPersist("\tsuccess")
	}
}

// Write the "done" state to the database
func (px *Paxos) dbWriteDone() {
	if !persistent {
		return
	}
	px.dbLock.Lock()
	defer px.dbLock.Unlock()
	if px.dead {
		return
	}

	px.DPrintfPersist("\n%v: Writing 'done' state to database... ", px.me)
	// Encode the "done" map into a byte array
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	err := enc.Encode(px.done)
	if err != nil {
		px.DPrintfPersist("\terror encoding")
	} else {
		// Write the state to the database
		err := px.db.Put(px.dbWriteOptions, []byte("done"), buffer.Bytes())
		if err != nil {
			px.DPrintfPersist("\terror writing to database")
		} else {
			px.DPrintfPersist("\tsuccess %v", px.done)
		}
	}
}

// Writes the persisted max instance number to the database
func (px *Paxos) dbWriteMaxInstance(max int) {
	if !persistent {
		return
	}
	if px.dead {
		return
	}

	toPrint := ""
	toPrint += fmt.Sprintf("\n%v: Writing max instance %v to database... ", px.me, max)
	// Encode the number into a byte array
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	err := enc.Encode(max)
	if err != nil {
		px.DPrintfPersist("\terror encoding: %s", fmt.Sprint(err))
	} else {
		// Write the state to the database
		key := "dbMaxInstance"
		err := px.db.Put(px.dbWriteOptions, []byte(key), buffer.Bytes())
		if err != nil {
			toPrint += fmt.Sprintf("\terror writing to database")
		} else {
			toPrint += fmt.Sprintf("\tsuccess")
			px.dbMaxInstance = max
		}
	}
	px.DPrintfPersist(toPrint)
}

// Initialize database for persistence
// and load any previously written "done" state
func (px *Paxos) dbInit() {
	if !persistent {
		return
	}
	px.dbLock.Lock()
	defer px.dbLock.Unlock()
	px.mu.Lock()
	defer px.mu.Unlock()
	if px.dead {
		return
	}

	px.DPrintfPersist("\n%v: Initializing database", px.me)

	// Register Proposal struct since we will encode/decode it using gob
	// Calling this probably isn't necessary, but being explicit for now
	gob.Register(Proposal{})

	// Open database (create it if it doesn't exist)
	px.dbOpts = levigo.NewOptions()
	px.dbOpts.SetCache(levigo.NewLRUCache(3 << 30))
	px.dbOpts.SetCreateIfMissing(true)
	px.dbName = "/home/ubuntu/mexos/src/paxos/persist/paxosDB_" + strconv.Itoa(px.me)
	px.DPrintfPersist("\n\t%v: DB Name: %s", px.me, px.dbName)
	var err error
	px.db, err = levigo.Open(px.dbName, px.dbOpts)
	if err != nil {
		px.DPrintfPersist("\n\t%v: Error opening database! \n\t%s", px.me, fmt.Sprint(err))
	} else {
		px.DPrintfPersist("\n\t%v: Database opened successfully", px.me)
	}

	// Create options for reading/writing entries
	px.dbReadOptions = levigo.NewReadOptions()
	px.dbWriteOptions = levigo.NewWriteOptions()

	// Read Paxos "done" state from database if it exists
	doneBytes, err := px.db.Get(px.dbReadOptions, []byte("done"))
	if err == nil && len(doneBytes) > 0 {
		// Decode the "done" state
		px.DPrintfPersist("\n\t%v: Decoding stored 'done' state... ", px.me)
		buffer := *bytes.NewBuffer(doneBytes)
		decoder := gob.NewDecoder(&buffer)
		var doneDecoded map[int]int
		err = decoder.Decode(&doneDecoded)
		if err != nil {
			px.DPrintfPersist("\terror decoding: %s", fmt.Sprint(err))
		} else {
			for peer, doneVal := range doneDecoded {
				px.done[peer] = doneVal
			}
			px.DPrintfPersist("\tsuccess %v --- %v", doneDecoded, px.done)
		}
	} else {
		px.DPrintfPersist("\n\t%v: No stored 'done' state to load", px.me)
	}

	// Read max instance from database if it exists
	px.dbMaxInstance = -1
	maxInstanceBytes, err := px.db.Get(px.dbReadOptions, []byte("dbMaxInstance"))
	if err == nil && len(maxInstanceBytes) > 0 {
		// Decode the max instance
		px.DPrintfPersist("\n\t%v: Decoding max instance... ", px.me)
		bufferMax := *bytes.NewBuffer(maxInstanceBytes)
		decoder := gob.NewDecoder(&bufferMax)
		var maxDecoded int
		err = decoder.Decode(&maxDecoded)
		if err != nil {
			px.DPrintfPersist("\terror decoding: %s", fmt.Sprint(err))
		} else {
			px.maxInstance = maxDecoded
			px.DPrintfPersist("\tsuccess")
		}
	} else {
		px.DPrintfPersist("\n\t%v: No stored max instance to load", px.me)
	}
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server, network bool) *Paxos {
	px := &Paxos{}
	// Network stuff
	px.peers = peers
	px.me = me
	px.network = network
	px.reachable = make([]bool, len(px.peers))
	for i, _ := range px.peers {
		px.reachable[i] = true
	}
	px.deaf = false

	// Paxos state
	px.instances = make(map[int]Proposal)
	px.maxInstance = -1
	px.done = make(map[int]int)
	for i := 0; i < len(px.peers); i++ {
		px.done[i] = -1
	}
	px.doneChannels = make(map[int]chan bool)

	// Persistence stuff
	px.dbInit()

	go px.doneCollector()

	if rpcs != nil {
		// caller will create socket &c
		if !printRPCerrors {
			disableLog()
			rpcs.Register(px)
			enableLog()
		} else {
			rpcs.Register(px)
		}
	} else {
		rpcs = rpc.NewServer()
		if !printRPCerrors {
			disableLog()
			rpcs.Register(px)
			enableLog()
		} else {
			rpcs.Register(px)
		}

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		if px.network {
			l, e := net.Listen("tcp", ":"+strconv.Itoa(startport+me))
			if e != nil {
				log.Fatal("listen error: ", e)
			}
			px.l = l
		} else {
			os.Remove(peers[me]) // only needed for "unix"
			l, e := net.Listen("unix", peers[me])
			if e != nil {
				log.Fatal("listen error: ", e)
			}
			px.l = l
		}

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.dead == false {
				conn, err := px.l.Accept()
				if err == nil && px.dead == false {
					if px.deaf || (px.unreliable && (rand.Int63()%1000) < 100) {
						// discard the request.
						conn.Close()
					} else if px.unreliable && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						if !px.network {
							c1 := conn.(*net.UnixConn)
							f, _ := c1.File()
							err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
							if err != nil {
								fmt.Printf("shutdown: %v\n", err)
							}

						} else {
							//respond to imaginary port?
							c1 := conn.(*net.TCPConn)
							f, _ := c1.File()
							err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
							if err != nil {
								fmt.Printf("shutdown: %v\n", err)
							}
						}
						px.rpcCount++
						go rpcs.ServeConn(conn)
					} else {
						px.rpcCount++
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.dead == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}

func enableLog() {
	log.SetOutput(os.Stderr)
}

func disableLog() {
	log.SetOutput(new(NullWriter))
}

type NullWriter int

func (NullWriter) Write([]byte) (int, error) { return 0, nil }
