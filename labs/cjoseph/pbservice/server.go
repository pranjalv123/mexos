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

type PBServer struct {
  l net.Listener
  dead bool // for testing
  unreliable bool // for testing
  me string
  vs *viewservice.Clerk
  done sync.WaitGroup
  finish chan interface{}
  // Your declarations here.
  db map[string]string
  requests map[string]*ReplyArchive //a backlog of past request resplies
  view *viewservice.View //tracks the curent view
  mu sync.Mutex //use this lock when accessing the maps?
  valid bool //only turn this true if we have successfully sync'd
}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
	pb.mu.Lock()
	//check to see if we are even the primary, dude.
	if pb.me != pb.view.Primary || !pb.valid {
		pb.mu.Unlock()
		//return nil
		return fmt.Errorf("%s Got Put() request when not valid primary! Current view is %s", pb.me, pb.view)
	}
	value := args.Value
	//check if this is a duplicate request
	requestID := args.Name + strconv.FormatInt(args.SeqNum, 10)
	if r, ok := pb.requests[requestID]; !ok { //not a duplicate, respond.
		if args.DoHash {
			prev := pb.db[args.Key]
			reply.PreviousValue = prev
			value = strconv.Itoa(int(hash(prev + value)))
			args.Value = value
		}
		if err := pb.FwdPut(pb.view.Backup, args, reply); err != nil {
			pb.mu.Unlock()
			reply.Err = Err(ErrFwdFailed)
			return nil
		}
		pb.db[args.Key] = value
		pb.requests[requestID] = &ReplyArchive{Put, nil, reply}
	} else {
		reply.Err = r.Put.Err
		reply.PreviousValue = r.Put.PreviousValue
		pb.mu.Unlock()
		return nil
	}
	pb.mu.Unlock()
	return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	pb.mu.Lock()
	//check to see if we are even the primary, dude.
	if pb.me != pb.view.Primary || !pb.valid {
		pb.mu.Unlock()
		return fmt.Errorf("%s Got Get() request when not valid primary!", pb.me)
	}
	
	key := args.Key
	//check if this is a duplicate request
	requestID := args.Name + strconv.FormatInt(args.SeqNum, 10)
	if r, ok := pb.requests[requestID]; !ok {
		//not a duplicate, respond.
		reply.Value = pb.db[key]

		v,ok := pb.db[key]
		//send "intent to respond" to backup and wait for ACK
		if ok {
			reply.Value = v
		} else {
			reply.Err = ErrNoKey
		}

		if err := pb.FwdGet(pb.view.Backup, args, reply); err != nil {
			//if there's an error, return!
			pb.mu.Unlock()
			return err
		} 
		pb.requests[requestID] = &ReplyArchive{Get, reply, nil}
	} else{
		reply.Err = r.Get.Err
		reply.Value = r.Get.Value
		pb.mu.Unlock()
		return nil
	}
 	pb.mu.Unlock()
	return nil
}

func (pb *PBServer) FwdGet(backup string, args *GetArgs, reply *GetReply) error {
	if backup=="" { //do nothing if there is no backup yet
		return nil
	}
	fargs := FwdGet{args, reply}
	freply := FwdReply{}
	ok := call(pb.view.Backup, "PBServer.RcvGetFwd", fargs, &freply)
	if !ok {
		return fmt.Errorf("Backup nonresponsive while forwarding Get")
	}
	if reply.Err != "" {
		return fmt.Errorf("Some weird error occured while forwarding: %s", reply.Err)
	}
	return nil
}

func (pb *PBServer) RcvGetFwd(args *FwdGet, reply *FwdReply) error {
	pb.mu.Lock()
	//make sure that the current view matches
	if pb.view.Backup != pb.me {
		reply.Err = Err(ErrWrongServer)
		pb.mu.Unlock()
		return fmt.Errorf(ErrWrongServer)
	}
	//since it is a get, all we have to do is record the request
	requestID := args.Fargs.Name + strconv.FormatInt(args.Fargs.SeqNum, 10)
	pb.requests[requestID] = &ReplyArchive{Get, args.IntendedReply, nil}
	pb.mu.Unlock()
	return nil
}

func (pb *PBServer) FwdPut(backup string, args *PutArgs, reply *PutReply) error {
	if backup=="" { //do nothing if there is no backup yet
		return nil
	}
	fargs := FwdPut{args, reply}
	freply := &FwdReply{}
	ok := call(pb.view.Backup, "PBServer.RcvFwdPut", fargs, &freply)
	if !ok {
		return fmt.Errorf("Backup nonresponsive while forwarding Put")
	}
	if reply.Err != "" {
		return fmt.Errorf("Some weird error occured while forwarding: %s", reply.Err)
	}
	return nil
}

func (pb *PBServer) RcvFwdPut(args *FwdPut, reply *FwdReply) error {
	pb.mu.Lock()
	if pb.view.Backup != pb.me {
		reply.Err = Err(ErrWrongServer)
		pb.mu.Unlock()
		return fmt.Errorf(ErrWrongServer)
	}
	//record the request and add to db
	requestID := args.Fargs.Name + strconv.FormatInt(args.Fargs.SeqNum, 10)
	pb.requests[requestID] = &ReplyArchive{Put, nil, args.IntendedReply}

	pb.db[args.Fargs.Key] = args.Fargs.Value
	pb.mu.Unlock()
	return nil
}

// ping the viewserver periodically.
// Each p/b server should send a Ping RPC once per PingInterval.
// The view server replies with a description of the current
// view. The Pings let the view server know that the p/b
// server is still alive; inform the p/b server of the current
// view; and inform the view server of the most recent view
// that the p/b server knows about.
func (pb *PBServer) tick() {
	pb.mu.Lock()
	v,e := pb.vs.Ping(pb.view.Viewnum)
	if e == nil {
		old := pb.view.Viewnum
		var syncErr error
		//if the view has changed and we are primary, sync
		if v.Primary == pb.me && old != v.Viewnum {
			syncErr = pb.Sync(v.Backup)

			if syncErr == nil {
				if old == 0 { //first primary ever must be valid
					pb.valid = true
				}
			}
		} 
		//de-validates any former PBs
		if v.Primary != pb.me && v.Backup != pb.me && pb.valid {
			pb.valid = false
		}
		if syncErr == nil {
			pb.view = &v
		}
	}
	pb.mu.Unlock()
}

func (pb *PBServer) Sync(backup string) error {
	if backup=="" { //do nothing if there is no backup yet
		return nil
	}
	//sync the database AND the outstanding requests. 
	args := &SyncArgs{pb.me, pb.db, pb.requests}
	var reply SyncReply
	ok := call(backup, "PBServer.RcvSync", args, &reply)
	if !ok {
		return fmt.Errorf("Error syncing %s to %s", pb.me, backup)
	}
	return nil
}

func (pb *PBServer) RcvSync(args *SyncArgs, reply *SyncReply) error {
	pb.mu.Lock()
	if pb.view.Backup != pb.me || args.Primary != pb.view.Primary {
		reply.Err = Err(ErrWrongServer)
		pb.mu.Unlock()
		return fmt.Errorf(ErrWrongServer)
	}
	pb.requests = args.Requests
	pb.db = args.DB
	pb.valid=true
	pb.mu.Unlock()
	return nil
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
  // Your pb.* initializations here.
  pb.db = make(map[string]string)
  pb.requests = make(map[string]*ReplyArchive)
  pb.view = &viewservice.View{}
  pb.valid = false
  
  rpcs := rpc.NewServer()
  rpcs.Register(pb)

  os.Remove(pb.me)
  l, e := net.Listen("unix", pb.me);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  pb.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for pb.dead == false {
      conn, err := pb.l.Accept()
      if err == nil && pb.dead == false {
        if pb.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if pb.unreliable && (rand.Int63() % 1000) < 200 {
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
