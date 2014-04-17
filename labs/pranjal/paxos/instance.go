package paxos

import "time"
import "fmt"

import "crypto/rand"
import "math/big"
func nrand() int64 {
  max := big.NewInt(int64(1) << 62)
  bigx, _ := rand.Int(rand.Reader, max)
  x := bigx.Int64()
  return x
}

func nevercall() {
	fmt.Println("blah")
}
type PrepareArgs struct {
	Seq int64
	N   int64
	Min int64
	Sender int
	
	Uid int64

}

type PrepareReply struct {
	Ok  bool
	Seq int64
	
	Accepted bool
	Decided bool
	N   int64
	V   interface{}
	Min int64
	Sender int

	Reached bool
}

type AcceptArgs struct {
	Seq int64
	N   int64
	V   interface{}
	Min int64
	Sender int

	Uid int64
}

type AcceptReply struct {
	Seq int64
	Ok  bool
	N   int64
	Min int64
	Sender int

	Reached bool
}

type DecideArgs struct {
	Seq int64
	N   int64
	V   interface{}
	Min int64
	Sender int

	Uid int64
}

type DecideReply struct {
	Min int64
	Sender int

	Reached bool
}

func max(x, y int64) int64 {
	if x > y { return x}
	return y
}
func min(x, y int64) int64 {
	if x < y { return x}
	return y
}

func (px *Paxos) Forget() {
	m := px.Min()
	for x := range px.instances {
		if int(x) < m - 1 {
			px.instances[x].RWLock.Lock()
//			px.instances[x].Lock.Lock()
			delete(px.instances, x)
//			fmt.Println("Forgetting", x, "(min is", m,")")
		}
	}
}



func (px *Paxos) HandlePrepare(args *PrepareArgs, reply *PrepareReply) error {	
	if (px.instances[args.Seq] == nil) {
		px.mu.Lock()
		if (px.instances[args.Seq] == nil) {
			px.instances[args.Seq] = new(PaxosInstance)
			px.instances[args.Seq].Np = 0
			px.instances[args.Seq].Na = 0
			px.instances[args.Seq].Accepted = false
		}
		px.mu.Unlock()
	}
	px.instances[args.Seq].Lock.Lock()
	defer px.Forget()
	defer px.instances[args.Seq].Lock.Unlock()
	retval, found := px.prepared[args.Uid]
	if found {
		*reply = retval
		return nil
	}
	reply.Reached = true
	px.done_mu.Lock()
	px.done[args.Sender] = args.Min
	reply.Min = px.done[px.me]
	px.done_mu.Unlock()
	reply.Sender = px.me
	reply.Seq = args.Seq
	if args.N > px.instances[args.Seq].Np {

		reply.Ok = true
		px.instances[args.Seq].Np = args.N
		if px.instances[args.Seq].Accepted {
			reply.N = px.instances[args.Seq].Na
			reply.V = px.instances[args.Seq].V
			reply.Accepted = true
		} 
		if px.instances[args.Seq].Decided {
			reply.N = px.instances[args.Seq].Na
			reply.V = px.instances[args.Seq].V
			reply.Accepted = true
			reply.Decided = true
		}
//		fmt.Println(px.me, "successful prepare", args, px.instances[args.Seq].Np, reply, args.N > px.instances[args.Seq].Np)
	} else {
//		fmt.Println(px.me, "reject prepare", args, px.instances[args.Seq].Np, args.N > px.instances[args.Seq].Np)
		reply.Ok = false
	}
	px.prepared[args.Uid] = *reply
	return nil
}

func (px *Paxos) HandleAccept(args *AcceptArgs, reply *AcceptReply) error {
	if (px.instances[args.Seq] == nil) {		
		px.mu.Lock()
		if (px.instances[args.Seq] == nil) {		
			px.instances[args.Seq] = new(PaxosInstance)
			px.instances[args.Seq].Np = 0
			px.instances[args.Seq].Na = 0
			px.instances[args.Seq].Accepted = false
		}
		px.mu.Unlock()
	}
	px.instances[args.Seq].RWLock.RLock()
	px.instances[args.Seq].Lock.Lock()
	defer px.Forget()
	defer px.instances[args.Seq].RWLock.RUnlock()
	defer px.instances[args.Seq].Lock.Unlock()
	retval, found := px.accepted[args.Uid]
	if found {		
		*reply = retval
		return nil
	}
	reply.Reached = true
	px.done_mu.Lock()
	px.done[args.Sender] = args.Min
	reply.Min = px.done[px.me]
	px.done_mu.Unlock()
	reply.Sender = px.me
	reply.Seq = args.Seq
	
	if args.N >= px.instances[args.Seq].Np { 
		px.instances[args.Seq].Np = args.N
		px.instances[args.Seq].Na = args.N
		px.instances[args.Seq].V  = args.V
		px.instances[args.Seq].Accepted = true
		reply.Ok = true
//		fmt.Println(px.me, "successful accept request", args, instance.Np)
//		fmt.Println("new np", args, instance.Np, reply)
	} else {
//		fmt.Println(px.me, "rejecting accept request", args, instance.Np)
		reply.Ok = false
//		fmt.Println("args.N too small", args, instance.Np)
	}	
	px.accepted[args.Uid] = *reply
	return nil
}


func (px *Paxos) HandleDecide(args *DecideArgs, reply *DecideReply) error {
	if (px.instances[args.Seq] == nil) {
		px.mu.Lock()
		if (px.instances[args.Seq] == nil) {
			px.instances[args.Seq] = new(PaxosInstance)
			px.instances[args.Seq].Np = 0
			px.instances[args.Seq].Na = 0
			px.instances[args.Seq].Accepted = false
		}
		px.mu.Unlock()
	}
	px.instances[args.Seq].RWLock.RLock()
	defer px.instances[args.Seq].RWLock.RUnlock()
	px.instances[args.Seq].Lock.Lock()
	defer px.Forget()
	defer px.instances[args.Seq].Lock.Unlock()
	retval, found := px.decided[args.Uid]
	if found {
		*reply = retval
		return nil
	}
	px.HandleDecide_nolock(args , reply)
	px.decided[args.Uid] = *reply
	
	return nil
}
func (px *Paxos) HandleDecide_nolock(args *DecideArgs, reply *DecideReply) error {
	reply.Reached = true
	px.done_mu.Lock()
	px.done[args.Sender] = args.Min
	reply.Min = px.done[px.me]
	px.done_mu.Unlock()
	reply.Sender = px.me
	px.mu.Lock()
	if (px.instances[args.Seq] == nil) {
		px.instances[args.Seq] = new(PaxosInstance)
		px.instances[args.Seq].Np = 0
		px.instances[args.Seq].Na = 0
		px.instances[args.Seq].Accepted = false
	}
	px.mu.Unlock()
	
	px.instances[args.Seq].Np = args.N
	px.instances[args.Seq].Na = args.N
	px.instances[args.Seq].V  = args.V
	px.instances[args.Seq].Accepted = true
	px.max = max(args.Seq, px.max)
	
	if (!px.instances[args.Seq].Decided) {
		px.instances[args.Seq].Decided = true
		for peer, _ := range px.peers {
			if peer != args.Sender && (peer > px.me || args.Sender==px.me) {
				go px.asyncDecide(peer, args)
			}
		}
	}

	return nil
}



func (px *Paxos) asyncPrepare(srv string, args *PrepareArgs, replyChan chan PrepareReply) {
	reply := PrepareReply{Ok : false}
	reply.Reached = false
	ntries := 20
	call(srv, "Paxos.HandlePrepare", args, &reply)
	for !reply.Reached && ntries > 0 {
		if px.dead { 
			replyChan <- reply
			return
		}		
		call(srv, "Paxos.HandlePrepare", args, &reply)
		time.Sleep(10*time.Millisecond)
		ntries --
	}
	replyChan <- reply
}

func (px *Paxos) asyncAccept(srv string, args *AcceptArgs, replyChan chan AcceptReply) {
	var reply AcceptReply
	reply.Reached = false
	ntries := 20
	for !reply.Reached && ntries > 0 {
		call(srv, "Paxos.HandleAccept", args, &reply)
		if px.dead { 
			replyChan <- reply
			return 
		}
		if reply.Reached {break} 
		time.Sleep(10*time.Millisecond)
		ntries --
	}
	replyChan <- reply
}


func (px *Paxos) asyncDecide(srv int, args *DecideArgs) {
	var reply DecideReply
	ntries := 20
	for !reply.Reached && ntries > 0 {
		call(px.peers[srv], "Paxos.HandleDecide", args, &reply)
		if px.dead {break}
		if reply.Reached {break} 
		time.Sleep(10*time.Millisecond)
		ntries --
	}
	px.done_mu.Lock()
	px.done[reply.Sender] = reply.Min
	px.done_mu.Unlock()
	px.Forget()

}

func current_time() int64 {
	tn := time.Now()
	return int64(tn.Hour()) * 60 * 60000000000 + int64(tn.Minute()) * 60000000000 +int64(tn.Second()) * 1000000000 + int64(tn.Nanosecond())
}

func countTrue(counts map[int]bool) int {
	ct := 0
	for _, v := range counts {
		if v {
			ct++
		}
	}
	return ct
}

func (px *Paxos) waitForPrepareReplies (args PrepareArgs, replyChan *chan PrepareReply, V interface{}) (int64, interface{}, int) {
	count := make(map[int]bool)
	failCt := 0
	v := V
	var na int64
	na = -1
	seq := args.Seq
	for countTrue(count) < len(px.peers)/2 + 1 {
		px.instances[seq].Lock.Unlock()
		pr := <- *replyChan
//		fmt.Println(px.me, "got reply", pr)
		if px.dead || px.instances[seq] == nil { 
			if (px.instances[seq] != nil) { 
				px.instances[seq].Lock.Lock()
			}
			return -1, v, countTrue(count);
		}
		px.instances[seq].Lock.Lock()
		px.done_mu.Lock()
		px.done[pr.Sender] = pr.Min
		px.done_mu.Unlock()
		if pr.Ok {
			if (count[pr.Sender] == false) {
//				fmt.Println(px.me, "prepare_ok from", pr.Sender, seq, pr)
			}
			count[pr.Sender] = true
			if pr.Accepted && pr.N >= na {
				v = pr.V
				na = pr.N
				if pr.Decided {
					px.instances[seq].V = v
					px.instances[seq].Na = na
					px.instances[seq].Accepted = true
					px.instances[seq].Decided = true
					px.max = max(seq, px.max)
//					fmt.Println("side channel", v, seq)
					return -1, v, countTrue(count)
				}
			} 
		} else {
			failCt += 1
			if failCt > len(px.peers)/2 {
				return -1, v, countTrue(count)
			}
		}	
	}	
	if na == -1 {
		na = args.N
	}
	if (countTrue(count) >= len(px.peers)/2 + 1) {
		return na, v, countTrue(count)
	}
	fmt.Println("something went wrong")
	return na, v, countTrue(count)
}

func (px *Paxos) waitForAcceptReplies (replyChan *chan AcceptReply, seq int64, v interface{}) bool {
	count := make(map[int]bool)
	failCt := 0
	for countTrue(count) < len(px.peers)/2 + 1 {
		px.instances[seq].Lock.Unlock()
		ar := <- *replyChan
		if px.dead || px.instances[seq] == nil { 
			if (px.instances[seq] != nil) { 
				px.instances[seq].Lock.Lock()
			}
			return false; 
		}
		px.instances[seq].Lock.Lock()
		px.done_mu.Lock()
		px.done[ar.Sender] = ar.Min
		px.done_mu.Unlock()
		if ar.Ok {
			if (count[ar.Sender] == false) {
//				fmt.Println(px.me, "accept_ok from", ar.Sender, seq, ar, v)
			}
			count[ar.Sender] = true
		} else {
			failCt += 1
			if failCt > len(px.peers)/2 {
				return false
			}
		}
	}
	if (countTrue(count) >= len(px.peers)/2 + 1) {
		return true
	}
	fmt.Println("something went wrong")
	return false
}

func (px *Paxos) propose(seq int64, V interface{})  {
	
	replyChan := make(chan PrepareReply, len(px.peers))
	px.done_mu.Lock()
	np := current_time() * int64(len(px.peers)) + int64(px.me)
//	fmt.Println(px.me, "proposing", V, seq, np)
	args := PrepareArgs{Seq : seq, N : np, Min : px.done[px.me], Sender : px.me, Uid : nrand() }
	if (np <= px.instances[seq].Np) {
//		fmt.Println("np too small!", np, px.instances[seq].Np)
	}
//	fmt.Println(seq, args.N, current_time())
	px.done_mu.Unlock()
	
	for _, peer := range px.peers {
		go px.asyncPrepare(peer, &args, replyChan)
	}
	na, v, _ := px.waitForPrepareReplies(args, &replyChan, V)
	if na == -1 {
//		fmt.Println(seq, v, "couldn't get enough prepares")
		return
	}
//	fmt.Println("got na", na, "v", v, "seq", seq, "V", v, "Np", np, "Ct", ct, "/", len(px.peers)/2 + 1)
	replyChan2 := make(chan AcceptReply, len(px.peers))
	px.done_mu.Lock()
	args2 := AcceptArgs{Seq : seq, N : np, V : v, Sender : px.me, Min : px.done[px.me], Uid : nrand()}
	px.done_mu.Unlock()


	for _, peer := range px.peers {
		go px.asyncAccept(peer, &args2, replyChan2)
	}
	
	if (!px.waitForAcceptReplies(&replyChan2, seq, v)) {
//		fmt.Println(seq, v, "couldn't get enough accepts")
		return
	}

	px.done_mu.Lock()
	args3 := DecideArgs{N : na, V : v, Seq : seq, Min : px.done[px.me], Sender : px.me, Uid : nrand()}
	px.done_mu.Unlock()

	var reply3 DecideReply
//	fmt.Println(px.me, "successfully proposed", v, seq)
	px.HandleDecide_nolock(&args3, &reply3)


}
