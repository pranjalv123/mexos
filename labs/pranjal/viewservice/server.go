package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"

type ViewServer struct {
	mu   sync.Mutex
	l    net.Listener
	dead bool
	me   string

	// Your declarations here.
	downtime map[string]int
	view View
	nextView View
	hasNextView bool
	viewAcked bool
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {	
//	addr := args.Me[len(args.Me)-1] - 48
	vs.downtime[args.Me] = 0
//	fmt.Println("got ping from ", args.Me[len(args.Me)-1], " with viewnum ", args.Viewnum);
	if (vs.view.Viewnum - args.Viewnum > 1) {

		if args.Me == vs.view.Primary && vs.viewAcked {
//			fmt.Println("Killing", addr, "because view is too old", vs.view.Viewnum, args.Viewnum)
			vs.killPrimary()
		}
	}
	// See if the primary has acknowledged
	if (args.Me == vs.view.Primary && args.Viewnum == vs.view.Viewnum) {
		vs.viewAcked = true
		vs.view = vs.nextView
//		fmt.Println ("Primary ", addr,  " has acknowledged view ", args.Viewnum)
//		fmt.Println ("Replacing view ", vs.view.Viewnum, " with ", vs.nextView.Viewnum)
	}
	
	// add new Primary or Backup if needed 
	if (vs.view.Primary == "") {
		vs.view.Primary = args.Me
		vs.view.Viewnum = vs.view.Viewnum + 1
		vs.nextView = vs.view
		vs.viewAcked = false
	} else if (vs.view.Backup == "" && args.Me != vs.view.Primary && vs.viewAcked) {		
		vs.nextView.Backup = args.Me
		vs.nextView.Viewnum = vs.view.Viewnum + 1
		vs.hasNextView = true
	}

	if (vs.viewAcked && vs.hasNextView) {
		vs.view = vs.nextView
		vs.viewAcked = false
		vs.hasNextView = false
//		fmt.Println("New view", vs.nextView)
	}
	reply.View = vs.view

//	fmt.Println("replying to   ", addr, " with viewnum ", reply.View.Viewnum, reply.View);		
	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	reply.View = vs.view
	return nil
}

func (vs *ViewServer) killPrimary() {
//	fmt.Println("Primary ", vs.view.Primary[len(vs.view.Primary)-1] - 48, " has disappeared!")
	vs.nextView = vs.view
	vs.nextView.Primary = vs.view.Backup
	vs.nextView.Backup = ""
	vs.nextView.Viewnum ++
	vs.hasNextView = true
}

func (vs *ViewServer) killBackup() {
//	fmt.Println("Backup ", vs.view.Backup[len(vs.view.Backup)-1] - 48, " has disappeared!")
	vs.nextView.Backup = ""
	vs.nextView.Viewnum = vs.view.Viewnum + 1
	vs.viewAcked = false
//	fmt.Println("Backup  has disappeared!")
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {
	vs.downtime[vs.view.Primary] ++
	vs.downtime[vs.view.Backup] ++
	if vs.view.Primary != "" && vs.downtime[vs.view.Primary] > DeadPings && vs.viewAcked {
		vs.killPrimary()
	}
	if vs.view.Backup != "" && vs.downtime[vs.view.Backup] > DeadPings  && vs.viewAcked {
		vs.killBackup()
	}
//	fmt.Println("Current state", vs)
}

//
// tell the server to shut itself down.
// for testing.
// please don't change this function.
//
func (vs *ViewServer) Kill() {
	vs.dead = true
	vs.l.Close()
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.downtime = make(map[string]int)

	vs.viewAcked = false
	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.dead == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.dead == false {
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.dead == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.dead == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
