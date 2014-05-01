package shardmaster

//
// Shardmaster clerk.
// Please don't change this file.
//

import "net/rpc"
import "time"
import "fmt"

type Clerk struct {
	servers []string // shardmaster replicas
	network bool
}

func MakeClerk(servers []string, network bool) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.network = network
	return ck
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it doesn't get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string, args interface{}, 
	reply interface{}, network bool) bool {
	if network {
		c, errx := rpc.Dial("tcp", srv)
		if errx != nil {
			return false
		}
		defer c.Close()
		
		err := c.Call(rpcname, args, reply)
		if err == nil {
			return true
		}
		
		if printRPCerrors {
			fmt.Println(err)
		}
		return false
	} else {
		c, errx := rpc.Dial("unix", srv)
		if errx != nil {
			return false
		}
		defer c.Close()
		
		err := c.Call(rpcname, args, reply)
		if err == nil {
			return true
		}
		
		if printRPCerrors {
			fmt.Println(err)
		}
		return false
	}
}

func (ck *Clerk) Query(num int) Config {
	for {
		// try each known server.
		for _, srv := range ck.servers {
			args := &QueryArgs{}
			args.Num = num
			var reply QueryReply
			ok := call(srv, "ShardMaster.Query", args, &reply, ck.network)
			if ok {
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return Config{}
}

func (ck *Clerk) Join(gid int64, servers []string) {
	for {
		// try each known server.
		for _, srv := range ck.servers {
			args := &JoinArgs{}
			args.GID = gid
			args.Servers = servers
			var reply JoinReply
			ok := call(srv, "ShardMaster.Join", args, &reply, ck.network)
			if ok {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gid int64) {
	for {
		// try each known server.
		for _, srv := range ck.servers {
			args := &LeaveArgs{}
			args.GID = gid
			var reply LeaveReply
			ok := call(srv, "ShardMaster.Leave", args, &reply, ck.network)
			if ok {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int64) {
	for {
		// try each known server.
		for _, srv := range ck.servers {
			args := &MoveArgs{}
			args.Shard = shard
			args.GID = gid
			var reply LeaveReply
			ok := call(srv, "ShardMaster.Move", args, &reply, ck.network)
			if ok {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
