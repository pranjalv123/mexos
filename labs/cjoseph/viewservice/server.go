
package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"

const (
  Primary = "Primary"
  Backup = "Backup"
  Add = "Add"
  Remove = "Remove"
)


type ViewServer struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  me string


  // Your declarations here.
  statuses map[string] *StatusInfo //tracks most recent ping
  view *View //tracks the curent view
  acked bool //tracks whether current primary has ack'd
}

type StatusInfo struct {
  time time.Time
  Viewnum uint
}

/*
Protocol Outline:

tick():
Upon receiving a ping, add it to the statuses map, creating a new entry if necessary.
Go through the map, taking note of any hosts we have not heard from in DeadPings and cull them.
(Do not cull if it is a primary or secondary, go to an atomic function to fix?)


*/

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	//multiple ping handlers can run at once. NEED LOCKING
	
	//make sure incoming ping isn't a crash
	vs.mu.Lock()
	if args.Me == vs.view.Primary || args.Me == vs.view.Backup {
		s := vs.statuses[args.Me]
		if s.Viewnum > args.Viewnum { //a crash!
			//pick a new viewfix crash sending response
			vs.dispatchChange(args.Me)
		} 
		if args.Me == vs.view.Primary {
			if args.Viewnum == vs.view.Viewnum {
				vs.acked = true
			} else {
				//problem?
			}
		}
	}

	vs.statuses[args.Me] = &StatusInfo{time.Now(), args.Viewnum}

	//add a primary if we're missing one
	if vs.view.Primary == "" {
		log.Printf("Adding the  first primary.")
		vs.changeViews(Primary, Add) //bad abstraction to call this directly...
	}
	
	//add a secondary if we're missing one
	if vs.view.Backup == "" {
		vs.changeViews(Backup, Add) //bad abstraction to call this directly...
	}

	//reply with current view
	reply.View = *vs.view //do we need to do anything else to respond?

	vs.mu.Unlock()
	return nil
}

// 
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
	vs.mu.Lock()
	reply.View = *vs.view //do we need to do anything else to respond?
	vs.mu.Unlock()
	return nil
}


//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {
	vs.mu.Lock()
	for k,v := range vs.statuses {
		if time.Now().Sub(v.time) > DeadPings*PingInterval {
			r:=true
			//if the dead server is in the current view
			if k == vs.view.Primary || k == vs.view.Backup {
				r  = vs.dispatchChange(k)
			}
			if r { //only remove if view changes are successful
				delete(vs.statuses, k)
			} 
		}
	}
	vs.mu.Unlock()
}

func (vs * ViewServer) dispatchChange(v string) bool{
	r := true
	switch v{
	case vs.view.Primary:
		r = vs.changeViews(Primary, Remove)
	case vs.view.Backup:
		r = vs.changeViews(Backup, Remove)
	default:
		log.Println("Error! Should never get here (dispatchChange)")
	}
	return r
}

func (vs *ViewServer) changeViews(class string, action string) bool {
        //need to make sure mutex is held when this is called
        r  := true
	switch {
	case class == Primary && action == Add:
		//should only ever explicitly add a primary if view is empty
		if vs.view.Primary == "" {
			vs.view.Primary = vs.findServer()
		} else { r = false}
	case class == Primary && action == Remove:
		if vs.acked{
			if vs.view.Backup == "" {
				vs.view.Backup = vs.findServer()
			}
			vs.view.Primary = vs.view.Backup
			vs.view.Backup = vs.findServer()
		} else {r = false}
	case class == Backup && action == Add:
		if vs.acked{
			b := vs.findServer()

			if b == "" { //backup wasn't found
				r = false
			} else
			{
				vs.view.Backup = b
			}
		} else {
			r = false}
	case class == Backup && action == Remove:
		if vs.acked{
			vs.view.Backup = ""
		} else {r = false}
	default:
		log.Printf("Error! Should never get here. (changeViews)")
	}
	if r { //we successfully changed the view
		vs.view.Viewnum++
		vs.acked = false
	}
	return r
}

//find a live server that is not the current primary or backup
func (vs *ViewServer) findServer() string {
	for k,v := range vs.statuses {
		live := time.Now().Sub(v.time) < DeadPings*PingInterval
		if live && k != vs.view.Primary && k!= vs.view.Backup {
			return k
		}
	}
	return "" //we found none :(
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
  vs.statuses = make(map[string] *StatusInfo)
  vs.view = &View{0, "", ""}
  vs.acked = false

  // tell net/rpc about our RPC server and handlers.
  rpcs := rpc.NewServer()
  rpcs.Register(vs)

  // prepare to receive connections from clients.
  // change "unix" to "tcp" to use over a network.
  os.Remove(vs.me) // only needed for "unix"
  l, e := net.Listen("unix", vs.me);
  if e != nil {
    log.Fatal("listen error: ", e);
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
