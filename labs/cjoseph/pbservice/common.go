package pbservice

import "hash/fnv"

const (
  OK = "OK"
  ErrNoKey = "ErrNoKey"
  ErrWrongServer = "ErrWrongServer"
	ErrFwdFailed = "ErrFwdFailed"
  Put = "Put"
  Get = "Get"
)
type Err string

type PutArgs struct {
  Key string
  Value string
  DoHash bool // For PutHash
  // You'll have to add definitions here.
  SeqNum int64
  Name string
  // Field names must start with capital letters,
  // otherwise RPC will break.
}

type PutReply struct {
  Err Err
  PreviousValue string // For PutHash
}

type GetArgs struct {
  Key string
  SeqNum int64
  Name string
}

type GetReply struct {
  Err Err
  Value string
}

// Your RPC definitions here.
type FwdGet struct {
	Fargs *GetArgs
	IntendedReply *GetReply
	
}

type FwdPut struct {
	Fargs *PutArgs
	IntendedReply *PutReply
}

type FwdReply struct {
	Err Err
}

type SyncArgs struct {
	Primary string
	DB map[string]string
	Requests map[string]*ReplyArchive
}

type ReplyArchive struct {
	Type string
	Get *GetReply
	Put *PutReply
}

type SyncReply struct {
	Err Err
}

func hash(s string) uint32 {
  h := fnv.New32a()
  h.Write([]byte(s))
  return h.Sum32()
}

