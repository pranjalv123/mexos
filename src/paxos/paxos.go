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

const startport = 2100
const printRPCerrors = false

type Proposal struct {
	prepare int
	accept  int
	value   interface{}
	decided bool
}

type Paxos struct {
	mu           sync.Mutex
	l            net.Listener
	dead         bool
	unreliable   bool
	network      bool
	deaf         bool
	rpcCount     int
	peers        []string
	reachable    []bool
	me           int // index into peers[]
	instances    map[int]Proposal
	maxInstance  int
	done         map[int]int
	doneChannels map[int]chan bool
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
	for i,p := range px.peers {
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

func (px *Paxos) getInstance(seq int) Proposal {
	px.mu.Lock()
	if _, ok := px.instances[seq]; !ok {
		px.instances[seq] = Proposal{-1, -1, nil, false}
	} else {
		if seq > px.maxInstance {
			px.maxInstance = seq
		}
	}
	prop := px.instances[seq]
	px.mu.Unlock()
	return prop
}

func (px *Paxos) doneCollector() {
	oldMin := -1
	for !px.dead {
		min := px.Min()
		if min > oldMin {
			px.mu.Lock()
			for k := range px.instances {
				if k < min {
					px.instances[k] = Proposal{-1, -1, nil, false}
					delete(px.instances, k)
				}
			}
			px.mu.Unlock()
			oldMin = min
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
	px.mu.Lock()
	if _, ok := px.instances[args.Instance]; !ok {
		px.instances[args.Instance] = Proposal{-1, -1, nil, false}
		if args.Instance > px.maxInstance {
			px.maxInstance = args.Instance
		}
	}
	prop := px.instances[args.Instance]
	reply.Err = true
	reply.Done = px.done[px.me]

	if args.PID > prop.prepare {
		px.instances[args.Instance] = Proposal{args.PID, prop.accept, prop.value, prop.decided}
		reply.Err = false
		reply.PID = prop.accept
		reply.Value = prop.value
	}
	px.mu.Unlock()
	return nil
}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
	px.mu.Lock()
	prop := px.instances[args.Instance]
	reply.Err = true
	reply.Done = px.done[px.me]

	if args.PID >= prop.prepare {
		px.instances[args.Instance] = Proposal{args.PID, args.PID, args.Value, prop.decided}
		reply.Err = false
		reply.PID = args.PID
	}
	px.mu.Unlock()
	return nil
}

func (px *Paxos) Decide(args *DecideArgs, reply *DecideReply) error {
	px.mu.Lock()
	px.instances[args.Instance] = Proposal{args.PID, args.PID, args.Value, true}
	px.mu.Unlock()
	reply.Err = false
	reply.Done = px.done[px.me]
	if channel, exists := px.doneChannels[args.Instance]; exists {
		channel <- true
		delete(px.doneChannels, args.Instance)
	}
	return nil
}

func (px *Paxos) Propose(seq int, v interface{}) {
	prop := px.getInstance(seq)
	nPID := 0
	for !prop.decided && !px.dead {
		total := len(px.peers)
		hPID := -1
		hValue := v
		ok := 0

		for i := 0; i < len(px.peers); i++ {
			args := &PrepareArgs{seq, nPID}
			var reply PrepareReply
			if px.callAcceptor(i, "Paxos.Prepare", args, &reply) && !reply.Err {
				px.done[i] = reply.Done
				ok += 1
				if reply.PID > hPID {
					hPID = reply.PID
					hValue = reply.Value
				}
			}
		}

		if ok <= total/2 {
			prop = px.getInstance(seq)
			if hPID > nPID {
				nPID = hPID + 1
			} else {
				nPID = nPID + 1
			}
			continue
		}

		ok = 0
		for i := 0; i < len(px.peers); i++ {
			args := &AcceptArgs{seq, nPID, hValue}
			var reply AcceptReply
			if px.callAcceptor(i, "Paxos.Accept", args, &reply) && !reply.Err {
				px.done[i] = reply.Done
				ok += 1
			}
		}

		if ok <= total/2 {
			prop = px.getInstance(seq)
			if hPID > nPID {
				nPID = hPID + 1
			} else {
				nPID = nPID + 1
			}
			continue
		}

		for i := 0; i < len(px.peers); i++ {
			args := &DecideArgs{seq, nPID, hValue}
			var reply DecideReply
			if px.callAcceptor(i, "Paxos.Decide", args, &reply) && !reply.Err {
				px.done[i] = reply.Done
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
	px.done[px.me] = seq
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
	prop := px.getInstance(seq)
	return prop.decided, prop.value
}

func (px *Paxos) SetDoneChannel(seq int, channel chan bool) {
	px.doneChannels[seq] = channel
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
func Make(peers []string, me int, rpcs *rpc.Server, network bool) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.reachable = make([]bool, len(px.peers))
	for i, _ := range px.peers {
		px.reachable[i] = true
	}
	px.me = me
	px.deaf = false
	px.network = network
	px.instances = make(map[int]Proposal)
	px.maxInstance = -1
	px.done = make(map[int]int)
	for i := 0; i < len(px.peers); i++ {
		px.done[i] = -1
	}
	px.doneChannels = make(map[int]chan bool)

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

type NullWriter int

func (NullWriter) Write([]byte) (int, error) { return 0, nil }

func enableLog() {
	log.SetOutput(os.Stderr)
}

func disableLog() {
	log.SetOutput(new(NullWriter))
}
