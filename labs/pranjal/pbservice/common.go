package pbservice

import "hash/fnv"
import "viewservice"


const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
)

type Err string


func callPutExactlyOnce(getSrv func()string, 
	args PutArgs, reply *PutReply) bool {
	reply.SetErr("Couldn't reach server yet")
	for ; 	reply.GetErr() == "Couldn't reach server yet";  {
		srv := getSrv()
		call(srv, "PBServer.Put", args, &reply)
	}
	return true
}
func callGetExactlyOnce(getSrv func()string, 
	args GetArgs, reply *GetReply, ck *Clerk) bool {
	reply.SetErr("Couldn't reach server yet")
	for ; 	reply.GetErr() == "Couldn't reach server yet";  {
		srv := getSrv()
		args.Viewnum = ck.vs.Viewnum()
		call(srv, "PBServer.Get", args, &reply)
	}
	return true
}
func callGetAllExactlyOnce(getSrv func()string, 
	args *GetAllArgs, reply *GetAllReply) bool {
	reply.Err = "Couldn't reach server yet"
	for ; 	reply.Err == "Couldn't reach server yet";  {
		srv := getSrv()
		call(srv, "PBServer.GetAll", args, &reply)
	}
	return true
}


type PutArgs struct {
	Key    string
	Value  string
	DoHash bool // For PutHash
	// You'll have to add definitions here.

	// Field names must start with capital letters,
	// otherwise RPC will break.
	Backup bool
	Token int64
	Viewnum uint
}

type PutReply struct {
	Err           Err
	PreviousValue string // For PutHash
	Viewnum uint
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Token int64
	Viewnum uint
}

type GetReply struct {
	Err   Err
	Value string
}


type GetAllArgs struct {
	View viewservice.View
}

type GetAllReply struct {
	Err   Err
	Value map[string]string
}

func (reply *GetReply) GetErr() Err {
	return reply.Err
}
func (reply *PutReply) GetErr() Err {
	return reply.Err
}


func (reply *GetReply) SetErr(s Err)  {
	reply.Err = s
}
func (reply *PutReply) SetErr(s Err)  {
	reply.Err = s
}


// Your RPC definitions here.

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
