package shardkv

import "math/big"
import "crypto/rand"
import "hash/fnv"

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
)

type Err string

type PutArgs struct {
	Key    string
	Value  string
	DoHash bool // For PutHash
	ID     int64
}

type GetArgs struct {
	Key string
	ID  int64
}

type KVReply struct {
	Err   Err
	Value string
}

type FetchArgs struct {
	Config int
	Shard  int
}

type FetchReply struct {
	Err      Err
	Store    map[string]string
	Response map[int64]string
	Seen     map[int64]bool
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
