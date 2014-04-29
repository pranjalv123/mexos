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
import "math"
import "math/rand"
import "sort"
import "time"

type Err string

//a paxos peer
type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       bool
	unreliable bool
	rpcCount   int
	peers      []string
	me         int // index into peers[]

	// Your data here.
	majority int //floor(npeers)/2 + 1
	//THE BELOW REQUIRE LOCKING BEFORE ACCESS
	instances map[int]*PaxInst //maps instance number to status
	done      map[int]int      //the highest number each peer has marked as done
	k         int              //for the proposal number
}

type PaxInst struct {
	decided  bool //true if consensus
	n_p      int  //highest proposal seen
	n_a      int
	v_a      interface{}
	n_pRumor int //highest a peer has seen
}

type Accept struct {
	N_A int
	V_A interface{}
}

type PrepareArgs struct {
	Seq  int //paxos instance number
	N_P  int //proposal number
	From int
	Done int
}

type PrepareReply struct {
	OK   bool
	N_P  int
	N_A  int
	V_A  interface{}
	From int
	Done int
}

type AcceptArgs struct {
	Seq  int //paxos instance number
	N_A  int
	V_A  interface{}
	From int
	Done int
}

type AcceptReply struct {
	OK   bool
	N    int
	V_A  interface{} //TODO; get rid of this
	From int
	Done int
}

type DecideArgs struct {
	Seq int         //paxos instance number
	V   interface{} //the chosen value
}

type DecideReply struct {
	OK bool //return True if everything went OK?
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

//for debugging
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	From     string
	CSeq     int //TODO: necessary?
	PSeq     int //paxos sequence number
	Proposer int

	Type          string
	Key           string
	Value         string
	PreviousValue string
	Err           Err
}

//but I just met 'er!
func (px *Paxos) proposer(seq int, val interface{}) {

	decided, _ := px.Status(seq)
	for !decided && !px.dead {
		//get a unique proposal number
		num := px.getPropNum(seq)
		px.mu.Lock()
		px.mu.Unlock()
		//initialize response array
		responses := make([]*PrepareReply, len(px.peers))
		for i, _ := range responses {
			responses[i] = &PrepareReply{false, -1, -1, nil, -1, -1}
		}

		//piggyback done info
		done := -1
		px.mu.Lock()
		if s, ok := px.done[px.me]; ok {
			done = s
		}
		px.mu.Unlock()

		var pr PrepareReply
		prepArgs := &PrepareArgs{seq, num, px.me, done}
		//send proposals!

		for i, p := range px.peers {
			if i == px.me { //send prepare to self
				if px.RcvPrepare(prepArgs, &pr) == nil {
					responses[pr.From] = &pr
					px.setDone(pr.From, pr.Done)
				}
			} else { //send prepares to peers
				if ok, r := px.sendPrepare(seq, p, num); ok {
					responses[r.From] = r
					px.setDone(r.From, r.Done)
				}
			}
		}

		reject := 0
		//look at our lovely responses
		for _, r := range responses {
			if !r.OK {
				reject++
				//if n_p was too low, try again...
				if px.setHighestRumoredProp(seq, r.N_P) {
					time.Sleep(time.Duration(rand.Int()%
						30) * time.Millisecond)
					px.Start(seq, val)
					return
				}
			}
		}

		//if not enough peers responded to prepares, start over.
		if reject >= px.majority {
			time.Sleep(time.Duration(rand.Int()%30) * time.Millisecond)
			px.Start(seq, val)
			return
		}
		//val = v_a with highest n_a; choose original val otherwise
		val = px.chooseHighest(responses, val)

		//piggyback Done info
		done = -1
		px.mu.Lock()
		if s, ok := px.done[px.me]; ok {
			done = s
		}
		px.mu.Unlock()

		accept := 0
		var ar AcceptReply
		acceptArgs := &AcceptArgs{seq, num, val, px.me, done}
		//send the accept to all peers
		for i, p := range px.peers {
			if i == px.me { //send to self
				if px.RcvAccept(acceptArgs, &ar) == nil {
					if ar.OK {
						accept++
						px.setDone(ar.From, ar.Done)
					}
				}
			} else { //send to peers
				if ok, r := px.sendAccept(seq, p, num, val); ok {
					px.setDone(r.From, r.Done)
					if r.OK {
						accept++
					}
				}
			}
		}

		//if not enough accepts succeeded, start over!
		if accept < px.majority {
			time.Sleep(time.Duration(rand.Int()%30) * time.Millisecond)
			px.Start(seq, val)
			return
		}

		accept = 0
		//send decided messages to all
		for i, p := range px.peers {
			if i == px.me { //send to self
				dr := &DecideReply{}
				if px.RcvDecide(&DecideArgs{seq, val}, dr) == nil {
					if dr.OK {
						accept++
					}
				}
			} else { //send to peers
				if ok, r := px.sendDecide(seq, p, val); ok {
					if r.OK {
						accept++
					}
				}
			}
		}

		if accept < px.majority {
			time.Sleep(time.Duration(rand.Int()%30) * time.Millisecond)
			px.Start(seq, val)
			return
		}
		decided, _ = px.Status(seq)
	}
	return
}

func (px *Paxos) getPropNum(seq int) int {
	px.mu.Lock()
	num := px.k*len(px.peers) + px.me
	px.mu.Unlock()

	for h := px.getHighestProp(seq); num <= h; {
		px.mu.Lock()
		px.k++
		num = px.k*len(px.peers) + px.me
		px.mu.Unlock()
	}
	return num
}

func (px *Paxos) chooseHighest(r []*PrepareReply, orig interface{}) interface{} {
	max := &Accept{-1, nil}
	for _, v := range r {
		if v.OK && v.N_A > max.N_A {
			max = &Accept{v.N_A, v.V_A}
		}
	}
	//set to original value if responses were empty
	if max.V_A == nil {
		max.V_A = orig
	}
	return max.V_A
}

//returns the greater of the actual seen vs rumored n_p
func (px *Paxos) getHighestProp(seq int) int {
	result := -1
	px.mu.Lock()
	if px.instances[seq] != nil {
		result = int(math.Max(float64(px.instances[seq].n_p),
			float64(px.instances[seq].n_pRumor)))
	}
	px.mu.Unlock()
	return result
}

//sets n_pRumor if n_p > n_pRumor, and returns true
func (px *Paxos) setHighestRumoredProp(seq int, n_p int) bool {
	success := false
	px.mu.Lock()
	if px.instances[seq] != nil && n_p > px.instances[seq].n_pRumor {
		px.instances[seq].n_pRumor = n_p
		success = true
	}
	px.mu.Unlock()
	return success
}

func (px *Paxos) getHighestAcceptNum(seq int) int {
	result := -1
	px.mu.Lock()
	if px.instances[seq] != nil {
		result = px.instances[seq].n_a
	}
	px.mu.Unlock()
	return result
}

func (px *Paxos) getHighestAcceptValue(seq int) interface{} {
	var result interface{} = nil
	px.mu.Lock()
	if px.instances[seq] != nil {
		result = px.instances[seq].v_a
	}
	px.mu.Unlock()
	return result
}

func (px *Paxos) sendPrepare(seq int, dest string, num int) (bool, *PrepareReply) {
	var reply PrepareReply
	done := -1
	px.mu.Lock()
	if val, ok := px.done[px.me]; ok {
		done = val
	}
	px.mu.Unlock()
	args := &PrepareArgs{seq, num, px.me, done}
	ok := call(dest, "Paxos.RcvPrepare", args, &reply)
	return ok, &reply
}

func (px *Paxos) RcvPrepare(args *PrepareArgs, reply *PrepareReply) error {
	reply.From = px.me
	px.setDone(args.From, args.Done)
	px.mu.Lock()
	if val, ok := px.done[px.me]; ok {
		reply.Done = val
	} else {
		reply.Done = 0
	}
	i := px.instances[args.Seq]
	if i == nil { //add to table if we've never heard of it
		px.instances[args.Seq] = &PaxInst{false, -1, -1, nil, -1}
		i = px.instances[args.Seq]
	}

	if args.N_P > i.n_p {
		i.n_p = args.N_P
		reply.OK = true
	} else {
		reply.OK = false
	}

	reply.N_A = i.n_a
	reply.V_A = i.v_a
	reply.N_P = i.n_p
	px.mu.Unlock()
	return nil
}

func (px *Paxos) sendAccept(seq int, dest string, n_a int, v_a interface{}) (bool, *AcceptReply) {
	var reply AcceptReply
	done := 0
	px.mu.Lock()
	if val, ok := px.done[px.me]; ok {
		done = val
	}
	px.mu.Unlock()
	args := &AcceptArgs{seq, n_a, v_a, px.me, done}
	ok := call(dest, "Paxos.RcvAccept", args, &reply)
	return ok, &reply
}

func (px *Paxos) RcvAccept(args *AcceptArgs, reply *AcceptReply) error {
	reply.From = px.me
	px.setDone(args.From, args.Done)
	px.mu.Lock()

	//piggyback highest done
	if val, ok := px.done[px.me]; ok {
		reply.Done = val
	} else {
		reply.Done = 0
	}

	i := px.instances[args.Seq]
	if i == nil { //reject if we've never heard of it
		reply.OK = false
	} else {
		if args.N_A >= i.n_p {
			i.n_p = args.N_A
			i.n_a = args.N_A
			i.v_a = args.V_A
			reply.OK = true
			reply.N = i.n_a
			reply.V_A = i.v_a
		} else {
			reply.OK = false
		}
	}
	px.mu.Unlock()
	return nil
}

func (px *Paxos) sendDecide(seq int, dest string, val interface{}) (bool, *DecideReply) {
	var reply DecideReply
	ok := call(dest, "Paxos.RcvDecide", &DecideArgs{seq, val}, &reply)
	return ok, &reply
}

func (px *Paxos) RcvDecide(args *DecideArgs, reply *DecideReply) error {
	px.mu.Lock()
	i := px.instances[args.Seq]
	if i != nil {
		i.v_a = args.V
		i.decided = true
		reply.OK = true
	} else { //reject if we've never heard of it
		reply.OK = false
	}
	px.mu.Unlock()
	return nil
}

//sets the done map and clears entries. Caller must hold lock.
func (px *Paxos) setDone(peer int, seq int) {
	px.mu.Lock()
	px.done[peer] = seq
	px.mu.Unlock()
	min := px.Min()
	keys := px.sortedSeqs()
	for _, k := range keys {
		if k < min {
			px.mu.Lock()
			delete(px.instances, k)
			px.mu.Unlock()
		}
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
	if seq >= px.Min() {
		px.mu.Lock()
		px.mu.Unlock()
		go px.proposer(seq, v)
	}

}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	px.mu.Lock()
	px.done[px.me] = seq
	px.mu.Unlock()
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	keys := px.sortedSeqs()
	if len(keys) == 0 {
		return -1
	} else {
		sort.Ints(keys)
	}
	return keys[len(keys)-1]
}

func (px *Paxos) sortedSeqs() []int {
	px.mu.Lock()
	keys := make([]int, len(px.instances))
	i := 0
	for k, _ := range px.instances {
		keys[i] = k
		i++
	}
	px.mu.Unlock()
	return keys
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
	px.mu.Lock()
	//abort if we haven't heard from all peers
	if len(px.done) < len(px.peers) {
		px.mu.Unlock()
		return 0
	}
	min := px.done[0]
	for _, n := range px.done {
		if n < min {
			min = n
		}
	}
	px.mu.Unlock()
	return min + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {
	status := false
	var value interface{} = nil
	px.mu.Lock()
	if px.instances[seq] != nil {
		status = px.instances[seq].decided
		value = px.instances[seq].v_a
	}
	px.mu.Unlock()
	return status, value
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
	px.instances = make(map[int]*PaxInst)
	px.done = make(map[int]int)
	px.majority = len(px.peers)/2 + 1
	px.k = 0

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
