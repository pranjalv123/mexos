package pbservice

import "hash/fnv"
import "crypto/rand"
import "math/big"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
)

type Err string

type PutArgs struct {
	Key    string
	Value  string
	DoHash bool // For PutHash
	// You'll have to add definitions here.

	// Field names must start with capital letters,
	// otherwise RPC will break.
	Forwarded bool   //Whether request is forwarded from primary
	Xid       string // Unique XID for detecting duplicates
	SendTime  int64  // The time of request for detecting delayed requests
}

type PutReply struct {
	Err           Err
	PreviousValue string // For PutHash
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Forwarded bool   //Whether request is forwarded from primary
	Xid       string // Unique XID for detecting duplicates
	SendTime  int64  // The time of request for detecting delayed requests
}

type GetReply struct {
	Err   Err
	Value string
}

// Your RPC definitions here.
type UpdateDatabaseArgs struct {
	Caller     string              // To check that the primary is calling
	KvDatabase map[string]string   // The entire key value database
	XidReplies map[string]string   // The entire history of replies
	XidPutArgs map[string]*PutArgs // The entire history of Put requests
	LastPut    map[string]int64    // The entire log of last Put time for each key
}

type UpdateDatabaseReply struct {
	Err Err
}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}
