package kvpaxos

import "hash/fnv"

const (
  OK = "OK"
  ErrNoKey = "ErrNoKey"
  ErrPut = "Undefined Put Error"
  ErrGet = "Undefined Get Error"
  Nobody = "Nobody"
  Put = "Put"
  PutHash = "PutHash"
  Get = "Get"
)
type Err string

type PutArgs struct {
  // You'll have to add definitions here.
  Key string
  Value string
  DoHash bool  // For PutHash
  // You'll have to add definitions here.
  // Field names must start with capital letters,
  // otherwise RPC will break.
  From string
  SeqNum int
}

type PutReply struct {
  Err Err
  PreviousValue string   // For PutHash
}

type GetArgs struct {
  Key string
  // You'll have to add definitions here.
  From string
  SeqNum int
}

type GetReply struct {
  Err Err
  Value string
}

func hash(s string) uint32 {
  h := fnv.New32a()
  h.Write([]byte(s))
  return h.Sum32()
}
