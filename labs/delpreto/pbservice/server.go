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

//import "strconv"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		n, err = fmt.Printf(format, a...)
	}
	return
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

	viewnum      uint   // The current view number
	primary      string // The current primary
	backup       string // The current backup
	amPrimary    bool   // Whether this server is the primary
	amBackup     bool   // Whether this server is the backup
	updateBackup bool   // Whether the backup's database needs to be updated

	kvLock     sync.Mutex        // Mutex object to deal with concurrent editing
	kvDatabase map[string]string // Database of key value pairs

	xidReplies map[string]string   // Past replies to requests (indexed by XID of request)
	xidPutArgs map[string]*PutArgs // Past arguments to Put requests (indexed by XID of request)
	lastPut    map[string]int64    // The last time that a Put was made to a given key
}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
	// Your code here.

	// Gain control of mutex object to avoid concurrent editing issues
	// Note we lock for the whole method, so our view cannot change during the method
	pb.kvLock.Lock()
	defer pb.kvLock.Unlock()

	// Record any previous replies to this request, and the current value
	oldReply, seenXid := pb.xidReplies[args.Xid]
	previousValue := pb.kvDatabase[args.Key]

	if pb.amPrimary {
		DPrintf("\nPrimary")
	} else if pb.amBackup {
		DPrintf("\n\tBackup")
	} else {
		DPrintf("\nNeither")
	}
	DPrintf(" %s got Put(%s, %s) xid %s", pb.me, args.Key, args.Value, args.Xid)

	// If not primary or backup, return error
	if !pb.amPrimary && !pb.amBackup {
		reply.Err = "Got Put request, not primary or backup"
		reply.PreviousValue = ""
		DPrintf("... Done, with error (not p/b)")
		return nil
	}

	// If primary and request is forwarded, return error (have split brain)
	if pb.amPrimary && args.Forwarded {
		reply.Err = "Error: Am primary but got forwarded request"
		reply.PreviousValue = ""
		DPrintf("... Done, with error (primary got forwarded request)")
		return nil
	}

	// If duplicate request, respond with old reply
	if seenXid {
		reply.Err = ""
		reply.PreviousValue = oldReply
		DPrintf("... Done, used previous reply")
		return nil
	}

	// If older than a previous Put, ignore it (delayed Put request)
	if args.SendTime < pb.lastPut[args.Key] {
		reply.Err = ""
		reply.PreviousValue = ""
		DPrintf("... Ignoring old command")
		return nil
	}

	// If primary, forward to backup and then execute
	if pb.amPrimary {
		// Forward to backup and wait for response
		if pb.backup != "" {
			backupReply := new(PutReply)
			args.Forwarded = true // Let the backup know this is a forwarded request
			DPrintf("... Forwarding to backup")
			backupResponded := call(pb.backup, "PBServer.Put", args, &backupReply)
			args.Forwarded = false // Reset the arguments (for future storing)
			// If backup timed out, got error, or is out of date
			// respond with error (either I am outdated or backup failed to be updated)
			// and mark that we should update the backup database (might be out of date)
			if !backupResponded || (backupReply.Err != "") || (backupReply.PreviousValue != previousValue) {
				pb.updateBackup = true
				reply.PreviousValue = ""
				reply.Err = "Error forwarding to backup"
				DPrintf(" Error forwarding to backup (%v, %s, %s)", backupResponded, backupReply.Err, backupReply.PreviousValue)
				return nil
			}
		}
		// Execute command (and store replies / record XID)
		reply.PreviousValue = pb.kvDatabase[args.Key]
		pb.kvDatabase[args.Key] = args.Value
		pb.xidReplies[args.Xid] = reply.PreviousValue
		pb.xidPutArgs[args.Xid] = args
		pb.lastPut[args.Key] = args.SendTime
		reply.Err = ""
		DPrintf("... Primary done, no error (Put %s, %s)", args.Key, args.Value)
	} else if pb.amBackup {
		// If backup, execute only if caller was primary
		if args.Forwarded {
			reply.PreviousValue = pb.kvDatabase[args.Key]
			pb.kvDatabase[args.Key] = args.Value
			pb.xidReplies[args.Xid] = reply.PreviousValue
			pb.xidPutArgs[args.Xid] = args
			pb.lastPut[args.Key] = args.SendTime
			reply.Err = ""
			DPrintf("... Backup done, no error (Put %s, %s)", args.Key, args.Value)
		} else {
			DPrintf("... Ignoring command, primary is %s", pb.primary)
			reply.Err = "Error: Command not from primary"
			reply.PreviousValue = ""
		}
	}
	return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.

	// Gain control of mutex object to avoid concurrent editing issues
	// Note we lock for the whole method, so our view cannot change during the method
	pb.kvLock.Lock()
	defer pb.kvLock.Unlock()

	// Record any previous replies to this request, and the current value
	oldReply, seenXid := pb.xidReplies[args.Xid]
	currentValue := pb.kvDatabase[args.Key]

	if pb.amPrimary {
		DPrintf("\nPrimary")
	} else if pb.amBackup {
		DPrintf("\n\tBackup")
	} else {
		DPrintf("\nNeither")
	}
	DPrintf(" %s got Get(%s) xid %s", pb.me, args.Key, args.Xid)

	// If not primary or backup, return error
	if !pb.amPrimary && !pb.amBackup {
		reply.Err = "Got Get request, not primary or backup"
		reply.Value = ""
		DPrintf("... Done, with error (not p/b)")
		return nil
	}

	// If primary and request is forwarded, return error (split brain)
	if pb.amPrimary && args.Forwarded {
		DPrintf("... Error, I got forwarded request")
		reply.Err = "Error: Primary got forwarded request"
		reply.Value = ""
		return nil
	}

	// If duplicate, respond with old reply
	if seenXid {
		reply.Err = ""
		reply.Value = oldReply
		DPrintf("... Done, used previous reply (%s)", oldReply)
		return nil
	}

	// If primary, forward to backup and then execute
	if pb.amPrimary {
		// Forward to backup and wait for response
		if pb.backup != "" {
			backupReply := new(GetReply)
			args.Forwarded = true
			DPrintf("... Forwarding to backup")
			backupResponded := call(pb.backup, "PBServer.Get", args, &backupReply)
			args.Forwarded = false
			// If backup timed out, got error, or is out of date
			// respond with error (either I am outdated or backup failed to be updated)
			// and mark that we should update the backup database (might be out of date)
			if !backupResponded || (backupReply.Err != "") || (backupReply.Value != currentValue) {
				pb.updateBackup = true
				reply.Value = ""
				reply.Err = "Error forwarding to backup"
				DPrintf(" Error forwarding to backup (%v, %s, %s)", backupResponded, backupReply.Err, backupReply.Value)
				return nil
			}
		}
		// Execute the command (and store reply / XID)
		reply.Value = pb.kvDatabase[args.Key]
		pb.xidReplies[args.Xid] = reply.Value
		reply.Err = ""
		DPrintf("... Primary done, no error, reply is %s", reply.Value)
	} else if pb.amBackup {
		// If backup, execute only if caller was primary
		if args.Forwarded {
			reply.Value = pb.kvDatabase[args.Key]
			pb.xidReplies[args.Xid] = reply.Value
			reply.Err = ""
			DPrintf("... Backup done, no error, reply is %s", reply.Value)
		} else {
			DPrintf("... Ignoring command, primary is %s", pb.primary)
			reply.Err = "Error: Command not from primary"
			reply.Value = ""
		}
	}
	return nil
}

// ping the viewserver periodically.
func (pb *PBServer) tick() {
	// Your code here.

	// Gain control of the mutex object to avoid concurrency issues
	// No Puts or Gets can ocur while the view is being updated
	pb.kvLock.Lock()
	defer pb.kvLock.Unlock()

	// Get the new view (but don't change instance variables yet)
	view, _ := pb.vs.Ping(pb.viewnum)
	newViewnum := view.Viewnum
	newPrimary := view.Primary
	newBackup := view.Backup

	pb.amPrimary = (newPrimary == pb.me)
	pb.amBackup = (newBackup == pb.me)

	// If we're primary and the view changed, update backup database
	// (this includes cases where we just became primary, where backup changed, etc.)
	if pb.amPrimary && (newViewnum != pb.viewnum) && (newBackup != "") {
		pb.updateBackup = true
	}

	// Store the new view
	pb.viewnum = newViewnum
	pb.primary = newPrimary
	pb.backup = newBackup

	// Update the backup if needed
	// This may have been set because view changed
	// or because an incorrect response was received from a forwarded message
	if pb.updateBackup {
		pb.updateBackupDatabase(pb.backup)
	}

}

func (pb *PBServer) updateBackupDatabase(backup string) {

	// Note mutex object is not used here, but it is assumed that
	// this is called from a method which currently has it locked

	DPrintf("\nUpdating backup server %s", backup)
	updateArgs := new(UpdateDatabaseArgs)
	updateArgs.Caller = pb.me
	updateArgs.KvDatabase = pb.kvDatabase
	updateArgs.LastPut = pb.lastPut
	updateArgs.XidPutArgs = pb.xidPutArgs
	updateArgs.XidReplies = pb.xidReplies

	updateReply := new(UpdateDatabaseReply)
	call(backup, "PBServer.UpdateDatabase", updateArgs, &updateReply)
	if updateReply.Err != "" {
		DPrintf("\n\t%s", updateReply.Err)
	} else {
		DPrintf("\n\tDone updating backup %s", backup)
		pb.updateBackup = false
	}
}

func (pb *PBServer) UpdateDatabase(args *UpdateDatabaseArgs, reply *UpdateDatabaseReply) error {

	pb.kvLock.Lock()
	defer pb.kvLock.Unlock()

	// Get the latest view
	// But don't store it yet - let that happen on next tick() to avoid issues
	view, _ := pb.vs.Ping(pb.viewnum)
	newPrimary := view.Primary
	newBackup := view.Backup

	// If I'm not the backup, return error
	if newBackup != pb.me {
		reply.Err = "Error: In UpdateDatabase but I'm not the backup"
		return nil
	}
	// If caller is not primary, return error
	if newPrimary != args.Caller {
		reply.Err = "Error: In UpdateDatabase but caller is not primary"
		return nil
	}
	// Update our database
	pb.kvDatabase = args.KvDatabase
	pb.xidPutArgs = args.XidPutArgs
	pb.xidReplies = args.XidReplies
	pb.lastPut = args.LastPut
	reply.Err = ""
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
	pb.viewnum = 0
	pb.primary = ""
	pb.backup = ""
	pb.amPrimary = false
	pb.amBackup = false
	pb.updateBackup = false

	pb.kvDatabase = make(map[string]string)

	pb.xidReplies = make(map[string]string)
	pb.xidPutArgs = make(map[string]*PutArgs)
	pb.lastPut = make(map[string]int64)

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
