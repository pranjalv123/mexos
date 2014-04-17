package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "os"
import "syscall"
import "math/rand"
import "sync"

import "strconv"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		n, err = fmt.Printf(format, a...)
	}
	return
}

type Command struct {
	name       string
	key        string
	value      string
	senderView viewservice.View
}

type Result struct {
	err        string
	value      string	
}

type PBServer struct {
	l          net.Listener
	dead       bool // for testing
	unreliable bool // for testing
	me         string
	vs         *viewservice.Clerk
	done       sync.WaitGroup
	finish     chan interface{}
	// Your declarations here.
	view       viewservice.View
	values     map[string]string
	processed  map[int64]string
	valuesLock sync.Mutex
	processedLock sync.Mutex
	putLock    sync.Mutex
	getLock sync.Mutex
	// channels
	commands   chan Command	
	results    chan Result
	// put fifo
	fifoLock sync.Mutex
	lastReqProcessed int
	nextReqQueued int
}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
	pb.fifoLock.Lock()
	myCount := pb.nextReqQueued
	pb.nextReqQueued += 1
	pb.fifoLock.Unlock()

	for ; ; {
		pb.fifoLock.Lock()
		if (pb.lastReqProcessed >= myCount)  {
			pb.fifoLock.Unlock()
			break
		}
		pb.fifoLock.Unlock()
		
	}

//	fmt.Println("CALLED PUT")
	// Your code here.
	for ; (args.Viewnum > pb.view.Viewnum) ; {
		pb.tick()
	}

	pb.putLock.Lock()
	pb.processedLock.Lock()
	if (pb.processed[args.Token] != "") {
		pb.processedLock.Unlock()
		reply.Err = "ok"
		reply.PreviousValue = pb.processed[args.Token]
		pb.putLock.Unlock()
		pb.lastReqProcessed = myCount + 1

		return nil
	}
	pb.processedLock.Unlock()
	reply.Viewnum = pb.view.Viewnum;
	reply.Err = "ok"
	if (pb.me == pb.view.Backup && args.Backup) {
		pb.valuesLock.Lock()
		pb.processedLock.Lock()
		reply.PreviousValue = pb.values[args.Key]
		pb.processed[args.Token] = reply.PreviousValue
		pb.values[args.Key] = args.Value
		pb.processedLock.Unlock()
		pb.valuesLock.Unlock()
	} else if (pb.me == pb.view.Primary) {

//		fmt.Println("Putting", pb.values[args.Key], "For", args.Key)
		if (pb.view.Backup != "") {		
//			fmt.Println("Fwding", args.Key,"to backup") 
			var backupReply PutReply
			args.Backup = true
			callPutExactlyOnce(func() string {return pb.view.Backup}, *args, &backupReply)
			if backupReply.Viewnum > pb.view.Viewnum {
				pb.putLock.Unlock()
				pb.lastReqProcessed = myCount + 1

				return nil
			}
		}
		pb.valuesLock.Lock()
		pb.processedLock.Lock()
		reply.PreviousValue = pb.values[args.Key]
		pb.processed[args.Token] = reply.PreviousValue
		pb.processedLock.Unlock()
		if args.DoHash {
			newhash := uint64(hash(reply.PreviousValue + args.Value))
			pb.values[args.Key] = strconv.FormatUint(newhash, 10)
		} else {
			pb.values[args.Key] = args.Value 
		}
		pb.valuesLock.Unlock()
	} else {
		reply.Err = "Not the primary"
	}
	pb.putLock.Unlock()
	pb.lastReqProcessed = myCount + 1

	return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	//	fmt.Println("CALLED GET")
	pb.getLock.Lock()	
	for ; (args.Viewnum > pb.view.Viewnum) ; {
		pb.tick()
	}	
	thisView := pb.view
//	fmt.Println(args.Key, pb.me, pb.view)
	pb.processedLock.Lock()
	if (pb.processed[args.Token] == "got") {
		reply.Err = "ok"
		pb.processedLock.Unlock()
		pb.getLock.Unlock()
		return nil
	}
	pb.processedLock.Unlock()
//	pb.processed[args.Token] = "got"
//	fmt.Println(pb.view, pb.me)
	reply.Err = ""
	if (thisView.Primary == pb.me) {
		pb.valuesLock.Lock()
		reply.Value = pb.values[args.Key]
		pb.valuesLock.Unlock()
		reply.Err = "ok"
	} else {
		fmt.Println(pb.me, thisView, args.Viewnum, args)
		reply.Err =  "Not the primary"
	}
	pb.getLock.Unlock()
//	fmt.Println("Returning", reply.Value, "For", args.Key)
	return nil
}

func (pb *PBServer) GetAll(args *GetAllArgs, reply *GetAllReply) error {
//	fmt.Println("Sending vals to backup")
	pb.valuesLock.Lock()
	if args.View.Viewnum > pb.view.Viewnum {
		pb.view = args.View
	}
	reply.Value = pb.values
	pb.valuesLock.Unlock()
	reply.Err = "ok"

	return nil
}

// ping the viewserver periodically.
func (pb *PBServer) tick() {

	newv, err := pb.vs.Ping(pb.view.Viewnum)	
	
	if (err == nil) {
		if (newv.Backup == pb.me && pb.view.Backup != pb.me && newv.Backup != ""){

			var backupArgs GetAllArgs
			var backupReply GetAllReply
			backupArgs.View = newv
			pb.putLock.Lock()
			callGetAllExactlyOnce(func() string{return newv.Primary}, &backupArgs, &backupReply)
			pb.putLock.Unlock()
			pb.values = backupReply.Value

		}
		pb.view = newv
	} else {
		pb.view.Primary = ""
		pb.view.Backup = ""
	}
}

func (*PBServer) processor() {
	for ;; {
		select {
			
		}
	}
}

// tell the server to shut itself down.
// please do not change this function.
func (pb *PBServer) kill() {
	pb.dead = true
	pb.l.Close()
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	pb.finish = make(chan interface{})
	pb.values = make(map[string]string)
	pb.processed = make(map[int64]string)
	// Your pb.* initializations here.

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.dead == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.dead == false {
				if pb.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.unreliable && (rand.Int63()%1000) < 200 {					
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					pb.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						pb.done.Done()
					}()
				} else {
					pb.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						pb.done.Done()
					}()
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && pb.dead == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
		DPrintf("%s: wait until all request are done\n", pb.me)
		pb.done.Wait()
		// If you have an additional thread in your solution, you could
		// have it read to the finish channel to hear when to terminate.
		close(pb.finish)
	}()

	pb.done.Add(1)
	go func() {
		for pb.dead == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
		pb.done.Done()
	}()

	return pb
}
