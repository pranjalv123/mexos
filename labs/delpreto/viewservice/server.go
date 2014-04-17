package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"

const Debug = 0

func DebugPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		n, err = fmt.Printf(format, a...)
	}
	return
}

type ViewServer struct {
	mu   sync.Mutex
	l    net.Listener
	dead bool
	me   string

	// Your declarations here.
	currentView          View                 // The current view state
	currentViewConfirmed bool                 // Whether the current primary has confirmed view
	pingTimes            map[string]time.Time // Times of last ping from each server
	availableServers     map[string]bool      // Servers known to be alive
	updatedServers       map[string]bool      // Servers known to be up to date (correct view)
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// Your code here.

	// See if this is a new server or if we have a pingTime
	_, existingServer := vs.pingTimes[args.Me]
	if existingServer {
		DebugPrintf("Ping: Existing Server: %s\n", args.Me)
		DebugPrintf("\tPing: currentViewNum = %v\n", vs.currentView.Viewnum)
		DebugPrintf("\tPing: requestViewNum = %v\n", args.Viewnum)
		// Record ping time and mark server as alive
		vs.pingTimes[args.Me] = time.Now()
		vs.availableServers[args.Me] = true
		// Record whether server has updated view
		if args.Viewnum == vs.currentView.Viewnum {
			vs.updatedServers[args.Me] = true
		} else {
			delete(vs.updatedServers, args.Me)
		}
		// If waiting for acknowledgment, see if this is it
		if !vs.currentViewConfirmed {
			DebugPrintf("\tPing: Need acknowledgment\n")
			if (args.Me == vs.currentView.Primary) && (args.Viewnum == vs.currentView.Viewnum) {
				DebugPrintf("\tPing: Got acknowledgement of new primary\n")
				vs.currentViewConfirmed = true
			}
		} else {
			// View is confirmed
			// If primary is out of date, mark it as dead
			if (args.Me == vs.currentView.Primary) && (args.Viewnum != vs.currentView.Viewnum) {
				delete(vs.availableServers, args.Me)
				delete(vs.pingTimes, args.Me)
			}
		}
	} else { // new server - record ping time, will be marked alive next time
		DebugPrintf("Ping: New Server: %s\n", args.Me)
		vs.pingTimes[args.Me] = time.Now()
	}

	// Respond with the current view
	reply.View = vs.currentView

	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.

	// Respond with the current view
	reply.View = vs.currentView

	return nil
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

	// Your code here.

	// If any have missed too many pings, mark them as dead
	for server, lastPingTime := range vs.pingTimes {
		interval := time.Since(lastPingTime).Nanoseconds()
		if interval >= DeadPings*PingInterval.Nanoseconds() {
			// If dead, delete all traces of it
			// It will be treated as new if it restarts
			DebugPrintf("Tick: deleting %s\n", server)
			delete(vs.availableServers, server)
			delete(vs.updatedServers, server)
			delete(vs.pingTimes, server)
		}
	}
	// Update the current view
	vs.updateView()

	DebugPrintf("Status: %v\n", vs.currentView.Viewnum)
	DebugPrintf("\tprimary: %s\n", vs.currentView.Primary)
	DebugPrintf("\tbackup: %s\n", vs.currentView.Backup)
	DebugPrintf("\tconfirmed: %b\n", vs.currentViewConfirmed)
	DebugPrintf("\tavailable: %v\n", len(vs.availableServers))
}

//
// Update the view if needed
// Should be called after availableServers and updatedServers are set
// Only updates the view if the current view has been acknowledged
//
func (vs *ViewServer) updateView() {
	// Check if our primary and backup are operational
	_, primaryAvailable := vs.availableServers[vs.currentView.Primary]
	_, backupAvailable := vs.availableServers[vs.currentView.Backup]
	// Only update the view if the current view has been confirmed
	assignNewPrimary := !primaryAvailable && vs.currentViewConfirmed
	assignNewBackup := !backupAvailable && vs.currentViewConfirmed
	incrementViewNum := false

	// Assign new primary if appropriate
	if assignNewPrimary {
		DebugPrintf("NewView: assigning new primary to ")
		// If backup is working and updated, promote it to primary
		if backupAvailable && vs.updatedServers[vs.currentView.Backup] {
			DebugPrintf(vs.currentView.Backup)
			vs.currentView.Primary = vs.currentView.Backup
			vs.currentView.Backup = ""
			assignNewBackup = true
			incrementViewNum = true
			vs.currentViewConfirmed = false
		} else {
			// No updated backup available
			// Look for an updated server to use as primary
			for server, _ := range vs.updatedServers {
				if (server != vs.currentView.Primary) && (server != vs.currentView.Backup) {
					DebugPrintf(server)
					vs.currentView.Primary = server
					incrementViewNum = true
					vs.currentViewConfirmed = false
					break
				}
			}
		}
		DebugPrintf("\n")
	}

	// Assign new backup if appropriate
	if assignNewBackup {
		DebugPrintf("NewView: assigning new backup to ")
		// Look for an available server to use as backup
		// (not necessarily an updated one)
		for server, _ := range vs.availableServers {
			if (server != vs.currentView.Primary) && (server != vs.currentView.Backup) {
				DebugPrintf(server)
				vs.currentView.Backup = server
				incrementViewNum = true
				vs.currentViewConfirmed = false
				break
			}
		}
		DebugPrintf("\n")
	}
	// Increment viewNum if new view has been assigned
	// (this way it only increments once even if both primary and backup changed)
	if incrementViewNum {
		vs.currentView.Viewnum++
	}
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
	vs.currentView.Backup = ""
	vs.currentView.Primary = ""
	vs.currentView.Viewnum = 0
	vs.currentViewConfirmed = true

	vs.pingTimes = make(map[string]time.Time)
	vs.availableServers = make(map[string]bool)
	vs.updatedServers = make(map[string]bool)

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
