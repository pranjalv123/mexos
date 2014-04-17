package shardkv

import "hash/fnv"
import "shardmaster"
//import "sync"
import "log"
import "strconv"
import "errors"
import "bytes"

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
  OK = "OK"
  ErrNoKey = "ErrNoKey"
  ErrWrongGroup = "ErrWrongGroup"
  ErrAncient = "ErrAncient"
)

type Err string

type PutArgs struct {
  Key string
  Value string
  DoHash bool  // For PutHash
  // You'll have to add definitions here.
  // Field names must start with capital letters,
  // otherwise RPC will break.
  CliSeq int
  From string
}

type PutReply struct {
  Err Err
  PreviousValue string   // For PutHash
  ValueHistory string
}

type GetArgs struct {
  Key string
  // You'll have to add definitions here.
  CliSeq int
  From string
}

type GetReply struct {
  Err Err
  Value string
  ValueHistory string
}

type ReconfArgs struct {
  Config shardmaster.Config
}

type ReconfReply struct {
	//I think an err here is unnecessary, as paxos will take care
	//of any errors that my need handling
}

type GetShardArgs struct {
	Config int //config num we want the shard for
	Shard int //shard that is being requested
}

type GetShardReply struct {
	Err Err 
	Shard map[string]string
	ClientState map[string]*Op
	
}

func hash(s string) uint32 {
  h := fnv.New32a()
  h.Write([]byte(s))
  return h.Sum32()
}

//--------------------------------shard struct----------------------------------

type shards struct {
	shards[] map[int]map[string]string //keys and values	
	latest int //set to 0
}

func (sh *shards) updateLatest(config int, shards []map[string]string, from string) error {
	DPrintf("%s updatelatest to config %d (current = %d)", from, config, sh.latest)
	if config != sh.latest +1 {
		log.Fatalf("shards.updateLatest: cannot skip configs!  w/ config %d, latest known was %d", config, sh.latest);
	}
	sh.latest = config
	for n, s := range shards{
		if s != nil {
			//put our new shard in
			sh.putShard(n, config, s, from)
		} else {
			//copy the old one
			if config == 1 {
				sh.putShard(n, config, make(map[string]string), from)
			} else {
				sh.putShard(n, config, sh.getShard(n, config-1), from)
			}
		}
	}
	return nil
}

func (sh *shards) getShard(shard int, config int) map[string]string {
	if config > sh.latest || config == 0 {
		log.Fatalf("shard.getShard Fatal! Must call updateConfig before getShard! Called w/ config %d, latest known was %d", config, sh.latest);
	}
	//create a new db on the fly, if necessary
	if _, ok := sh.shards[shard][config]; !ok {
		log.Fatalf("Missing shard %d for config %d!",shard, config)
	} 
	return sh.shards[shard][config]
}

func (sh *shards) putShard(shard int, config int, d map[string]string, from string) error {
	DPrintf("%s Putshard(%d) happening during %d.", from, shard, config)
	if config != sh.latest || config == 0 {
		log.Fatalf("shard.putShard Fatal! Must call updateConfig before putShard! Called w/ config %d, latest known was %d", config, sh.latest);
	}
	sh.shards[shard][config] = d
	return nil
}

func (sh *shards) put(key string, val string, ID string, from string) error {
	shard := key2shard(key)
	s := sh.getShard(shard, sh.latest)	
	DPrintf("%s.shards about to Put(%s, %s) ID = %s during config %d",
		from, key, val, ID, sh.latest)
	
	s[key] = val
	return nil
}

func (sh *shards) puthash(key string, val string, ID string, from string) (string, error) {
	shard := key2shard(key)
	s := sh.getShard(shard, sh.latest)
	prev := s[key]
	hashed := strconv.Itoa(int(hash(prev + val)))
	s[key] = hashed	
	DPrintf("%s.shards about to PutHash(%s, %s) ID = %s during config %d. Prev = %s, Hashed = %s",
		from,key, val, ID, sh.latest, prev, hashed)
	
	return prev, nil
}

func (sh *shards) get(key string, ID string) (string, error) {
	shard := key2shard(key)
	s := sh.getShard(shard, sh.latest)
	DPrintf("shards about to Get(%s) ID = %s during config %d",
		key, ID, sh.latest)
	
	if v, ok := s[key]; ok {
		return v, nil
	} else {
		return "", errors.New(ErrNoKey)
	}
}

func (sh *shards) getHistory(key string) string {
	shard := key2shard(key)
	var b bytes.Buffer
	b.WriteString("[")
	for i:= 1; i <= sh.latest; i++ {
		s := sh.getShard(shard, i)
		if v, ok := s[key]; ok {
			b.WriteString(v+",")
		} else {
			b.WriteString(" ,")
		}
	}
	b.WriteString("]")
	return b.String()
}

//-------------------------clientState struct----------------------------------

type clientStates struct {
	states[] map[int]map[string]*Op //ecent responses to requests
	latest int //set to 0
}

func (cs *clientStates) updateLatest(config int, states[]map[string]*Op, server string) error {
	
	if config != cs.latest +1 {
		log.Fatalf("clientStates.updateLatest: cannot skip configs!  w/ config %d, latest known was %d", config, cs.latest);
	}
	cs.latest = config
	for n, s := range states {
		if s != nil {
			//put our new shard in
			DPrintf("%s updating state for config %d  shard %d: %s", server, config, n, s)
			cs.putState(n, config, s)
		} else {//copy the old one
			if config == 1 {
				DPrintf("%s updating state for config %d  shard %d: %s", server, config, n, "[]")
				cs.putState(n, config, make(map[string]*Op))
			} else {
				old := cs.getState(n, config-1)
				DPrintf("%s updating state for config %d  shard %d: %s", server, config, n, old)
				cs.putState(n, config, old)
			}
		}
	}

	return nil
}

func (cs *clientStates) getState(shard int, config int) map[string]*Op {

	if config > cs.latest || config == 0 {
		log.Fatalf("clientStates.getState Fatal! Must call updateConfig before getState! Called w/ config %d, latest known was %d", config, cs.latest);
	}
	//create a new db on the fly, if necessary
	if _, ok := cs.states[shard][config]; !ok {
		log.Fatalf("Missing shard %d for config %d!",shard, config)
	}
	return cs.states[shard][config]
}

func (cs *clientStates) putState(shard int,config int,d map[string]*Op) error {
	if config != cs.latest || config == 0 {
		log.Fatalf("clientStaes.putState: Fatal! Must call updateConfig before putState! Called w/ config %d, latest known was %d", config, cs.latest);
	}
	cs.states[shard][config] = d
	return nil
}

//put into most recent config
func (cs *clientStates) put(shard int, from string, op *Op) error {
	s := cs.getState(shard, cs.latest)
	if op.Type == PutHash {
		DPrintf("clientStates.put(): %s %s  during config %d",
			op.ID, op.Type, cs.latest)
	}
	if op.Type == Get {
		DPrintf("clientStates.put(): %s Getduring config %d",
			op.ID,  cs.latest)
	}
	

	s[from] = op
	return nil
}

//get record from most recent config
func (cs *clientStates) get(shard int, from string, log string) (*Op, error) {	
	s := cs.getState(shard, cs.latest)
	DPrintf("%d from %s in shard %d: %s",log,from,shard, s)
	if v,ok := s[from]; ok {
		return v, nil
	} else { 
		return &Op{}, errors.New(ErrNoKey)
	} 		
}
