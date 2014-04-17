package shardkv

//import "shardmaster"
import "hash/fnv"
import "crypto/rand"
import "math/big"

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
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Sender int64 // unique Client ID that sent the command
	XID    int64 // unique XID of the command
}

type PutReply struct {
	Err           Err
	PreviousValue string // For PutHash
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Sender int64 // unique Client ID that sent the command
	XID    int64 // unique XID of the command
}

type GetReply struct {
	Err   Err
	Value string
}

// For sending shard data between servers
type ShardArgs struct {
	Sender          string                           // Not really needed/used but it's here just in case
	ConfigNum       int                              // The configuration with which this data is associated
	Shards          map[int]map[string]string        // The key/value data (indexed by shard)
	ClientResponses map[int]map[int64]ClientResponse // The past responses sent to clients (indexed by shard then client ID)
	SeenXIDs        map[int]map[int64]bool           // The XIDs seen already (indexed by shard and then xid)
}

// For indicating that the shard data was received
type ShardReply struct {
	Err Err
}

type ClientResponse struct {
	Sender        int64
	Value         string
	PreviousValue string
	Err           Err
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
