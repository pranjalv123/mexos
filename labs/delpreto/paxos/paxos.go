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
import "encoding/gob"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		n, err = fmt.Printf(format, a...)
	}
	time.Sleep(time.Millisecond * 3)
	return
}

// A structure for a Proposal, used for both prepares and accepts
type Proposal struct {
	Sequence int         // Which paxos instance
	Number   uint64      // Number of proposal
	Value    interface{} // Value of proposal
	Sender   string      // Advocate of proposal (which paxos peer)
}

// A structure for a Decision, indicating a final value for a sequence
type Decision struct {
	Sequence int         // Which paxos instance
	Number   uint64      // Number of proposal
	Value    interface{} // Value of proposal
}

// A structure for representing that sequences <= a number can be forgotten
type ForgetSequence struct {
	Sequence int    // Highest paxos instance that can be forgotten
	Sender   string // Sender of this sequence number
}

// A structure for the status of a paxos instance
type InstanceStatus struct {
	Npromised uint64      // Highest prepare request received
	Naccepted uint64      // Highest prepare request accepted
	Vaccepted interface{} // Value of highest accepted proposal
	Decided   bool        // Whether Vaccepted has been decided upon
}

// A Paxos instance!
type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       bool
	unreliable bool
	rpcCount   int
	peers      []string
	me         int // index into peers[]

	// Your data here.
	instances     map[int]InstanceStatus // Status of each sequence number
	doneResponses map[string]int         // Highest sequence to forget from each sender
	minSequence   int                    // Min sequence currently stored
	maxSequence   int                    // Max sequence currently stored
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
func call(srv string, name string, args interface{}, reply interface{}) bool {
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

	fmt.Println(err)
	return false
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	// Your code here.

	// Obtain the lock since we will be using the px.instances map
	px.mu.Lock()
	defer px.mu.Unlock()

	// Create one long string to print rather than printing separate messages
	//   This will print whole method output at once, so concurrent threads'
	//   outputs won't intermingle with outputs from this method
	toPrint := ""
	toPrint += fmt.Sprintf("\n%v starting, seq: %v value: %v", px.me, seq, px.val2print(v))

	// If it's already decided, do nothing
	_, seenSequence := px.instances[seq]
	if seenSequence {
		if px.instances[seq].Decided {
			toPrint += fmt.Sprintf("\n\tAlready decided this sequence! Doing nothing.")
			DPrintf(toPrint)
			return
		}
	}

	// Update min and max sequence numbers
	if (seq > px.maxSequence) || (px.maxSequence == -1) {
		px.maxSequence = seq
	}
	if (seq < px.minSequence) || (px.minSequence == -1) {
		px.minSequence = seq
	}

	// Start thread for advocating the proposed value
	toPrint += fmt.Sprintf("\n\tStarting go function to advocate proposal!")
	go func() {
		toAccept := px.prepare(seq, v)  // Send prepare requests to all
		toDecide := px.accept(toAccept) // Send accept requests to all
		px.decide(toDecide)             // Send decision to all
	}()

	DPrintf(toPrint)
}

//
// sends prepare requests with the desired value to all servers
// waits for majority response
// returns resultant proposal that should be sent as an accept request
// returns nil if proposal was rejected
//
func (px *Paxos) prepare(seq int, value interface{}) *Proposal {

	// Create one long string to print rather than printing separate messages
	//   This will print whole method output at once, so concurrent threads'
	//   outputs won't intermingle with outputs from this method
	toPrint := ""

	// Create a Proposal for advocating the given value
	var proposal Proposal
	proposal.Sender = px.peers[px.me]
	proposal.Sequence = seq
	proposal.Value = value
	// Concatenate first 16 digits of current time with
	// first 3 digits of px.me
	//   Using 3 digits of px.me allows for a thousand paxos peers
	//   Using 16 digits of the time means it won't repeat for about 115 days
	//   Using both time and "me" means it's probably unique
	//   Using 19 digits means it will fit in a uint64
	timeDigits := uint64(time.Now().UnixNano() % 10000000000000000)
	meDigits := uint64(px.me % 1000)
	proposal.Number = timeDigits*1000 + meDigits

	toPrint += fmt.Sprintf("\nStarting prepare(%v, %v, %s) on server %v", seq, proposal.Number, px.val2print(value), px.me)

	// Send prepare requests to every peer (including me)
	waitChannel := make(chan int)          // For ensuring go functions launched
	responseChannel := make(chan Proposal) // For storing peers' responses
	numInChannel := 0                      // Number of responses expected to be in the channel
	numPeers := len(px.peers)
	for i := 0; i < numPeers; i++ {
		go func(index int) {
			curPeer := px.peers[index]
			toPrint += fmt.Sprintf("\n\tSending prepare to %v", index)
			waitChannel <- 1 // Indicate we stored the index value (make sure indeces aren't shared across go functions)
			// Send request locally to me and over RPC to everyone else
			var replyProposal Proposal
			numInChannel++
			if curPeer == px.peers[px.me] {
				px.ProcessPrepare(proposal, &replyProposal)
			} else {
				ok := false
				// Probably shouldn't have a timeout for this lab, but it feels nice
				tries := 0
				for !ok && (tries < 5000) && !px.dead {
					ok = call(curPeer, "Paxos.ProcessPrepare", proposal, &replyProposal)
					time.Sleep(time.Millisecond * 50)
					tries++
				}
				if tries >= 5000 {
					fmt.Println("*** HIT MAX TRIES ***")
				}
			}
			// Put response into the channel
			responseChannel <- replyProposal
		}(i)
		<-waitChannel
	}

	// Tally requests until we get a majority of either accepts or rejects
	numPromises := 0
	numRejections := 0
	var highestResponse Proposal
	highestResponse.Number = 0
	peersResponded := make(map[string]int)
	for ((numPromises < numPeers/2+1) && (numRejections < numPeers/2+1)) && !px.dead {
		nextResponse := <-responseChannel
		numInChannel--
		// Tally their response as positive
		// if they sent the desired promise and
		// if we haven't heard from them already (to filter duplicate RPCs)
		// They indicate a rejection by setting reply sequence to -1
		_, seenSender := peersResponded[nextResponse.Sender]
		if !seenSender && (nextResponse.Sequence == seq) {
			toPrint += fmt.Sprintf("\n\tGot promise from %s on prepare(%v, %v, %s)", nextResponse.Sender, seq, proposal.Number, px.val2print(value))
			peersResponded[nextResponse.Sender] = 1
			numPromises++
			// Keep track of highest proposal number received
			if nextResponse.Number > highestResponse.Number {
				highestResponse.Number = nextResponse.Number
				highestResponse.Value = nextResponse.Value
				toPrint += fmt.Sprintf("\n\t\tHighest response number now %v, value %v", highestResponse.Number, px.val2print(highestResponse.Value))
			}
		} else {
			toPrint += fmt.Sprintf("\n\t%v got response from %s but seenSender = %v and their sequence = %v", px.me, nextResponse.Sender, seenSender, nextResponse.Sequence)
			// Record rejection
			if nextResponse.Sequence == -1 {
				numRejections++
			}
		}
	}

	// Have majority (either or promises or of rejections), or have been killed
	// We don't need any future responses, so keep flushing the channel (frees memory)
	go func() {
		for numInChannel > 0 {
			<-responseChannel
			numInChannel--
		}
	}()
	// If proposal was rejected, return nil
	if numPromises < numPeers/2+1 {
		toPrint += fmt.Sprintf("\n\tReturning nil, majority rejected")
		DPrintf(toPrint)
		return nil
	}
	// If majority sent promises, return a Proposal to send in accept requests
	// If highestResponse number > 0, then we need to use previously accepted value
	// Otherwise we can use our desired value
	if highestResponse.Number > 0 {
		toPrint += fmt.Sprintf("\n\tGot majority on new prepare(%v, %v, %s)", seq, highestResponse.Number, px.val2print(highestResponse.Value))

		highestResponse.Sequence = proposal.Sequence
		highestResponse.Sender = px.peers[px.me]
		highestResponse.Number = proposal.Number
		DPrintf(toPrint)
		return &highestResponse
	} else {
		toPrint += fmt.Sprintf("\n\tGot majority on my prepare(%v, %v, %s)", seq, proposal.Number, px.val2print(proposal.Value))
		DPrintf(toPrint)
		return &proposal
	}
}

//
// RPC for receiving prepare requests
//
func (px *Paxos) ProcessPrepare(args Proposal, reply *Proposal) error {

	// Get lock since we will be using px.instances map
	px.mu.Lock()
	defer px.mu.Unlock()

	// Create one long string to print rather than printing separate messages
	//   This will print whole method output at once, so concurrent threads'
	//   outputs won't intermingle with outputs from this method
	toPrint := ""
	toPrint += fmt.Sprintf("\n\t\t%v received prepare(%v, %v, %s)... ", px.me, args.Sequence, args.Number, px.val2print(args.Value))

	// If haven't seen sequence yet, initialize it
	// Use temp variable since apparently can't assign stuff to fields of px.instances[args.Sequence] directly
	// (since map is of objects rather than pointers, but this way was much more reliable)
	temp, haveSeen := px.instances[args.Sequence]
	if !haveSeen {
		temp = *new(InstanceStatus)
	}
	// If proposal has a higher number than our previous promises, accept it
	//  and reply with highest-numbered previously accepted value
	if args.Number >= px.instances[args.Sequence].Npromised {
		temp.Npromised = args.Number
		px.instances[args.Sequence] = temp
		reply.Sender = px.peers[px.me] // For filtering out duplicate RPCs
		reply.Number = px.instances[args.Sequence].Naccepted
		reply.Value = px.instances[args.Sequence].Vaccepted
		reply.Sequence = args.Sequence

		toPrint += fmt.Sprintf("responding with na=%v va=%s", reply.Number, px.val2print(reply.Value))
	} else {
		toPrint += fmt.Sprintf("responding with rejection")
		reply.Sequence = -1
	}

	DPrintf(toPrint)
	return nil
}

//
// sends accept requests to all servers
// waits for majority
// returns a Decision indicating the chosen value
// returns nil if accept was rejected by majority
//
func (px *Paxos) accept(inProposal *Proposal) *Decision {

	// If proposal is nil do nothing
	// this should only happen when px.dead has been set or the prepare was rejected
	if inProposal == nil {
		return nil
	}
	proposal := *inProposal

	// Create one long string to print rather than printing separate messages
	//   This will print whole method output at once, so concurrent threads'
	//   outputs won't intermingle with outputs from this method
	toPrint := ""
	toPrint += fmt.Sprintf("\nStarting accept(%v, %v, %s) on server %v", proposal.Sequence, proposal.Number, px.val2print(proposal.Value), px.me)

	// Send accept requests to every peer (including me)
	waitChannel := make(chan int)          // For ensuring go functions launched
	responseChannel := make(chan Proposal) // For storing peers' responses
	numInChannel := 0                      // Number of responses in the channel
	numPeers := len(px.peers)
	for i := 0; i < numPeers; i++ {
		go func(index int) {
			curPeer := px.peers[index]
			toPrint += fmt.Sprintf("\n\tSending accept(%v, %v, %s) to %v", proposal.Sequence, proposal.Number, px.val2print(proposal.Value), index)
			waitChannel <- 1 // Indicate we stored the index value (make sure indeces aren't shared across go functions)
			// Send request locally to me and over RPC to everyone else
			var replyProposal Proposal
			numInChannel++
			if curPeer == px.peers[px.me] {
				px.ProcessAccept(proposal, &replyProposal)
			} else {
				ok := false
				// Probably shouldn't have a timeout for this lab, but it feels nice
				tries := 0
				for !ok && (tries < 5000) && !px.dead {
					ok = call(curPeer, "Paxos.ProcessAccept", proposal, &replyProposal)
					time.Sleep(time.Millisecond * 50)
					tries++
				}
				if tries >= 5000 {
					fmt.Println("*** HIT MAX TRIES ***")
				}
			}
			// Store response in channel
			responseChannel <- replyProposal
		}(i)
		<-waitChannel
	}

	// Tally requests until we get a majority of either accepts or rejects
	numPositives := 0
	numRejections := 0
	peersResponded := make(map[string]int)
	DPrintf("%v waiting for majority on accept(%v, %v)", px.me, proposal.Sequence, proposal.Number)
	for ((numPositives < numPeers/2+1) && (numRejections < numPeers/2+1)) && !px.dead {
		nextResponse := <-responseChannel
		numInChannel--
		// Tally their response as positive
		// if they responded positively and
		// if we haven't heard from them already (to filter duplicate RPCs)
		// They indicate a rejection by setting reply sequence to -1
		_, seenSender := peersResponded[nextResponse.Sender]
		if !seenSender && (nextResponse.Sequence == proposal.Sequence) {
			toPrint += fmt.Sprintf("\n\tGot response from %s on accept(%v, %v, %s)", nextResponse.Sender, proposal.Sequence, proposal.Number, px.val2print(proposal.Value))
			peersResponded[nextResponse.Sender] = 1
			numPositives++
		} else if nextResponse.Sequence == -1 {
			numRejections++
		}
	}

	// Have majority (either or promises or of rejections), or have been killed
	// We don't need any future responses, so keep flushing the channel (frees memory)
	go func() {
		for numInChannel > 0 {
			<-responseChannel
			numInChannel--
		}
	}()

	// If proposal was rejected, return nil
	if numPositives < numPeers/2+1 {
		toPrint += fmt.Sprintf("\n\tReturning nil, majority rejected")
		DPrintf(toPrint)
		return nil
	}
	toPrint += fmt.Sprintf("\n\tGot majority on accept(%v, %v, %s)", proposal.Sequence, proposal.Number, px.val2print(proposal.Value))

	// If majority sent promises, return a Decision to send in broadcasts
	var decided Decision
	decided.Sequence = proposal.Sequence
	decided.Number = proposal.Number
	decided.Value = proposal.Value

	DPrintf(toPrint)
	return &decided
}

//
// RPC for processing an accept request
//
func (px *Paxos) ProcessAccept(args Proposal, reply *Proposal) error {

	// Get lock since we will be using the px.instances map
	px.mu.Lock()
	defer px.mu.Unlock()

	// Create one long string to print rather than printing separate messages
	//   This will print whole method output at once, so concurrent threads'
	//   outputs won't intermingle with outputs from this method
	toPrint := ""
	toPrint += fmt.Sprintf("\n\t\t%v received accept(%v, %v, %s)... ", px.me, args.Sequence, args.Number, px.val2print(args.Value))

	// If haven't seen sequence yet, initialize it
	// Use temp variable since apparently can't assign stuff to fields of px.instances[args.Sequence] directly
	// (since map is of objects rather than pointers, but this way was much more reliable)
	temp, haveSeen := px.instances[args.Sequence]
	if !haveSeen {
		temp = *new(InstanceStatus)
	}
	// If we haven't promised anyone with a higher number than this accept, accept it
	// Note we do accept it if it equals a previous promise
	//   This avoids rejection in case when this reply is lost in network, and proposer retries
	//   In that case we would have duplicate accept request, and should accept it again
	if args.Number >= px.instances[args.Sequence].Npromised {
		temp.Npromised = args.Number
		temp.Naccepted = args.Number
		temp.Vaccepted = args.Value
		px.instances[args.Sequence] = temp
		reply.Sender = px.peers[px.me]
		reply.Number = args.Number
		reply.Value = args.Value
		reply.Sequence = args.Sequence

		toPrint += fmt.Sprintf("responding with na=%v va=%v", reply.Number, px.val2print(reply.Value))
	} else {
		toPrint += fmt.Sprintf("responding with rejection")
		reply.Sequence = -1
	}

	DPrintf(toPrint)
	return nil
}

//
// sends decided value to all peers
// doesn't do anything with the responses
//
func (px *Paxos) decide(inDecision *Decision) {

	// If decision is nil, do nothing
	// This should only happen if pb.dead has been set or accept request was rejected
	if inDecision == nil {
		return
	}
	decision := *inDecision

	// Create one long string to print rather than printing separate messages
	//   This will print whole method output at once, so concurrent threads'
	//   outputs won't intermingle with outputs from this method
	toPrint := ""
	toPrint += fmt.Sprintf("\nStarting decide(%v, %v, %s) on server %v", decision.Sequence, decision.Number, px.val2print(decision.Value), px.me)

	// Send decision to every peer (including me)
	waitChannel := make(chan int) // For indicating that go function launched
	numPeers := len(px.peers)
	for i := 0; i < numPeers; i++ {
		go func(index int) {
			curPeer := px.peers[index]
			toPrint += fmt.Sprintf("\n\tSending decide(%v, %v, %s) to %v", decision.Sequence, decision.Number, px.val2print(decision.Value), index)
			waitChannel <- 1 // Indicate that arguments have been processed (avoid sharing across go functions)
			// Send to me locally and over RPC to everyone else
			replyDecision := new(Decision)
			if curPeer == px.peers[px.me] {
				px.ProcessDecide(decision, replyDecision)
			} else {
				ok := false
				// Probably shouldn't have timeout for this lab, but it feels nice
				tries := 0
				for !ok && (tries < 5000) && !px.dead {
					ok = call(curPeer, "Paxos.ProcessDecide", decision, replyDecision)
					time.Sleep(time.Millisecond * 50)
					tries++
				}
				if tries >= 5000 {
					fmt.Println("*** HIT MAX TRIES ***")
				}
			}
		}(i)
		<-waitChannel
	}
	DPrintf(toPrint)
}

//
// RPC for processing a decided value
//
func (px *Paxos) ProcessDecide(args Decision, reply *Decision) error {

	// Get lock since we will be using px.instances
	px.mu.Lock()
	defer px.mu.Unlock()

	// Create one long string to print rather than printing separate messages
	//   This will print whole method output at once, so concurrent threads'
	//   outputs won't intermingle with outputs from this method
	toPrint := ""
	toPrint += fmt.Sprintf("\n\t\t%v received decide(%v, %v, %s)...", px.me, args.Sequence, args.Number, px.val2print(args.Value))

	// If haven't seen sequence yet, initialize it
	// Use temp variable since apparently can't assign stuff to fields of px.instances[args.Sequence] directly
	// (since map is of objects rather than pointers, but this way was much more reliable)
	temp, haveSeen := px.instances[args.Sequence]
	if !haveSeen {
		temp = *new(InstanceStatus)
	}
	// Update promised number and accepted number to be at least decided number
	//   basically simulate receiving prepare/accept for decision if we haven't
	if px.instances[args.Sequence].Npromised < args.Number {
		temp.Npromised = args.Number
	}
	if px.instances[args.Sequence].Naccepted < args.Number {
		temp.Naccepted = args.Number
	}
	// Record decided value
	temp.Vaccepted = args.Value
	temp.Decided = true
	px.instances[args.Sequence] = temp
	toPrint += fmt.Sprintf("\n\t\t\tupdated value to %v", px.val2print(args.Value))

	DPrintf(toPrint)
	return nil
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {

	// Your code here.

	// Create one long string to print rather than printing separate messages
	//   This will print whole method output at once, so concurrent threads'
	//   outputs won't intermingle with outputs from this method
	toPrint := ""
	toPrint += fmt.Sprintf("\nStarting Done(%v) on server %v", seq, px.me)

	// Create ForgetSequence for indicating min sequence needed
	var forgetSequence ForgetSequence
	forgetSequence.Sequence = seq
	forgetSequence.Sender = px.peers[px.me]

	// Let all other peers know (including me)
	waitChannel := make(chan int)
	numPeers := len(px.peers)
	for i := 0; i < numPeers; i++ {
		go func(index int) {
			curPeer := px.peers[index]
			toPrint += fmt.Sprintf("\n\tSending forget(%v) to %v", forgetSequence.Sequence, index)
			waitChannel <- 1
			// Send to me locally and everyone else over RPC
			replyForget := new(ForgetSequence)
			if curPeer == px.peers[px.me] {
				px.ProcessDone(forgetSequence, replyForget)
			} else {
				ok := false
				// Probably don't need timeout for this lab, but it feels nice
				tries := 0
				for !ok && (tries < 5000) && !px.dead {
					ok = call(curPeer, "Paxos.ProcessDone", forgetSequence, replyForget)
					time.Sleep(time.Millisecond * 50)
					tries++
				}
				if tries >= 5000 {
					fmt.Println("*** HIT MAX TRIES ***")
				}
			}
		}(i)
		<-waitChannel
	}
	DPrintf(toPrint)
}

//
// RPC for processing new minimum sequence from a peer
//
func (px *Paxos) ProcessDone(args ForgetSequence, reply *ForgetSequence) error {

	// Get lock since we will be using px.instances map
	px.mu.Lock()
	defer px.mu.Unlock()

	// Create one long string to print rather than printing separate messages
	//   This will print whole method output at once, so concurrent threads'
	//   outputs won't intermingle with outputs from this method
	toPrint := ""
	toPrint += fmt.Sprintf("\n%v received done(%v)", px.me, args.Sequence)

	// Record new value from this peer
	px.doneResponses[args.Sender] = args.Sequence
	// Check on my value
	mySequence := px.doneResponses[px.peers[px.me]]
	// Check if we have heard from all peers, and if so find min
	minSequence := mySequence
	numPeers := len(px.peers)
	numResponses := 0
	for i := 0; i < numPeers; i++ {
		sequence, seenPeer := px.doneResponses[px.peers[i]]
		if seenPeer {
			numResponses++
			if sequence < minSequence {
				minSequence = sequence
			}
		}
	}
	if numResponses == numPeers {
		toPrint += fmt.Sprintf("\n\tHeard from everyone, updating min to %v", minSequence+1)
		// Delete all instances less than minimum of doneResponses
		for s := px.minSequence; s <= minSequence; s++ {
			_, exists := px.instances[s]
			if exists {
				toPrint += fmt.Sprintf("\n\t\tDeleting %v (whose value was %v)", s, px.val2print(px.instances[s].Vaccepted))
			}
			delete(px.instances, s)
		}
		px.minSequence = minSequence + 1
	}

	DPrintf(toPrint)
	return nil
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	// Your code here.
	return px.maxSequence
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
// It is illegal to call Done(i) on a peer and
// then call Start(j) on that peer for any j <= i.
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
	// You code here.
	return px.minSequence
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {

	// Your code here.

	// Get lock since we will use px.instances map
	px.mu.Lock()
	defer px.mu.Unlock()

	// Return status if have seen sequence
	_, haveSequence := px.instances[seq]
	if haveSequence {
		return px.instances[seq].Decided, px.instances[seq].Vaccepted
	}
	// Return false, nil if haven't seen sequence
	return false, nil
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change this function.
//
func (px *Paxos) Kill() {
	px.dead = true
	if px.l != nil {
		px.l.Close()
	}
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me

	// Your initialization code here.

	px.instances = make(map[int]InstanceStatus) // Status of each sequence number
	px.doneResponses = make(map[string]int)     // Responses from each peer from Done method
	px.minSequence = 0                          // Minimum stored sequence
	px.maxSequence = 0                          // Maximum stored sequence

	// Not sure exactly what gob.Register does, so not sure if these are needed...
	gob.Register(Proposal{})
	gob.Register(Decision{})
	gob.Register(ForgetSequence{})

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.dead == false {
				conn, err := px.l.Accept()
				if err == nil && px.dead == false {
					if px.unreliable && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.unreliable && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
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

//
// Function to get a printable variable of an instance value
// Basically just here to deal with Test 08 where 1MB values are chosen
//
// If value is super big, returns a string stating its length
// otherwise just returns the original value
//
func (px *Paxos) val2print(v interface{}) interface{} {
	// If it is a huge string, don't print it
	vString, isString := v.(string)
	if isString && (len(vString) > 500) {
		return fmt.Sprintf("<reallyBigString length %v>", len(vString))
	}
	return v
}
