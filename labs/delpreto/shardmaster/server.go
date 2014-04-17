package shardmaster

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "time"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		fmt.Printf(format, a...)
	}
	return
}

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	px         *paxos.Paxos

	configs              []Config // indexed by config num
	maxSequenceCommitted int      // The highest Paxos sequence committed on this server (including no-ops)
}

type Op struct {
	// Your data here.
	ProposedConfig Config // The configuration being proposed
	ID             uint64 // A unique ID of the Op
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	// Your code here.

	// Obtain the lock
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Will creat one debug string to print at the end
	// to ensure that other methods' outputs don't interleave with those of this method
	toPrint := ""
	toPrint += fmt.Sprintf("\nShardMaster got Join(%v)", args.GID)

	// Get the latest configuration by consulting Paxos
	toPrint += fmt.Sprintf("\n\tGetting latest configuration...")
	sm.update()
	toPrint += fmt.Sprintf(" got config %v", sm.configs[len(sm.configs)-1].Num)

	// While the current configuration doesn't include the new GID,
	// propose a new configuration that adds the new GID
	// (so if it is already included we do nothing, and otherwise we retry until success)
	for !sm.haveGroup(sm.configs[len(sm.configs)-1], args.GID) && !sm.dead {
		// Determine a desired new configuration
		newConfig := sm.cloneCurrentConfig()      // Clone the current configuration
		newConfig.Groups[args.GID] = args.Servers // Add the new GID to the group list
		newConfig.Num++
		if newConfig.Num == 1 {
			// First real configuration - assign new group to all shards
			for i := 0; i < NShards; i++ {
				newConfig.Shards[i] = args.GID
				toPrint += fmt.Sprintf("\n\tAssigning new group to shard %v", i)
			}
		} else {
			// Will adjust current configuration to accomodate new group
			// Keep assigning slices to new group until it has NShards/numGroups slices
			//   where numGroups is the number of known groups, plus the new group
			// At each step, take a shard away from the max-loaded group
			// This will maintain balance, distribute shards evenly, and only move NShards/numGroups slices

			// Find a shard belonging to the max-loaded group and move it to the new group
			_, shardMax := sm.maxLoadedGroup(newConfig)
			newConfig.Shards[shardMax] = args.GID
			toPrint += fmt.Sprintf("\n\tAssigning new group to shard %v", shardMax)
			// Keep doing this until the new group has enough shards
			for i := 1; i < NShards/len(newConfig.Groups); i++ {
				_, shardMax := sm.maxLoadedGroup(newConfig)
				newConfig.Shards[shardMax] = args.GID
				toPrint += fmt.Sprintf("\n\tAssigning new group to shard %v", shardMax)
			}
		}

		// Propose new configuration to Paxos
		// Note that unique ID is only needed for no-ops
		// since here we don't really care if our proposal was accepted
		// (we only care if the accepted proposal includes the new group)
		var op Op
		op.ProposedConfig = newConfig
		toPrint += fmt.Sprintf("\n\tProposing config %v to Paxos", newConfig.Num)
		sm.px.Start(sm.maxSequenceCommitted+1, op)
		// Wait until its Status is decided
		decided := false
		var decidedValue interface{}
		toWait := 25 * time.Millisecond
		for !decided && !sm.dead {
			time.Sleep(toWait)
			if toWait < 100*time.Millisecond {
				toWait *= 2
			}
			decided, decidedValue = sm.px.Status(sm.maxSequenceCommitted + 1)
		}

		if sm.dead {
			break
		}

		// Get the decided configuration for this sequence
		decidedConfig := decidedValue.(Op).ProposedConfig
		// If it's not a no-op (number 0), store the new configuration
		if decidedConfig.Num > 0 {
			sm.addConfig(decidedConfig)
			toPrint += fmt.Sprintf("\n\t\tShards: %v", decidedConfig.Shards)
			for gid, _ := range decidedConfig.Groups {
				toPrint += fmt.Sprintf("\n\t\t%v", gid)
			}
		}
		sm.maxSequenceCommitted++
	}

	// All done (have configuration using new group)
	// Call Paxos.Done() to free memory, and return
	sm.px.Done(sm.maxSequenceCommitted)

	toPrint += "\n\tReturning"
	DPrintf(toPrint)
	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	// Your code here.

	// Obtain the lock
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Will creat one debug string to print at the end
	// to ensure that other methods' outputs don't interleave with those of this method
	toPrint := ""
	toPrint += fmt.Sprintf("\nShardMaster got Leave(%v)", args.GID)

	// Get the latest configuration by consulting Paxos
	toPrint += fmt.Sprintf("\n\tGetting latest configuration...")
	sm.update()
	toPrint += fmt.Sprintf(" got config %v", sm.configs[len(sm.configs)-1].Num)

	// While the current configuration includes the GID,
	// propose a new configuration that doesn't use the GID
	// (so if it is already excluded we do nothing, and otherwise we retry until success)
	// Note that the haveGroup method returns true even if the GID is in the group
	// list but not assigned any shards
	for sm.haveGroup(sm.configs[len(sm.configs)-1], args.GID) && !sm.dead {
		// Determine a desired new configuration
		newConfig := sm.cloneCurrentConfig()
		delete(newConfig.Groups, args.GID)
		newConfig.Num++

		// Will adjust current configuration to remove given group
		// Keep assigning its slices to other groups until it has none left
		// Always give a shard to the min-loaded group to maintain balance
		// This should maintain even distribution and only move the number of shards that the GID currently has
		for sm.usingGroup(newConfig, args.GID) {
			// Find the min-loaded group and a shard assigned to the group we're deleting
			gidMin, shardToMove := sm.minLoadedGroup(newConfig, args.GID)
			newConfig.Shards[shardToMove] = gidMin
			toPrint += fmt.Sprintf("\n\tAssigning shard %v to group %v", shardToMove, gidMin)
		}

		// Propose new configuration to Paxos
		// Note that unique ID is only needed for no-ops
		// since here we don't really care if our proposal was accepted
		// (we only care if the accepted proposal excludes the new group)
		var op Op
		op.ProposedConfig = newConfig
		toPrint += fmt.Sprintf("\n\tProposing config %v to Paxos", newConfig.Num)
		sm.px.Start(sm.maxSequenceCommitted+1, op)
		// Wait until its Status is decided
		decided := false
		var decidedValue interface{}
		toWait := 25 * time.Millisecond
		for !decided && !sm.dead {
			time.Sleep(toWait)
			if toWait < 100*time.Millisecond {
				toWait *= 2
			}
			decided, decidedValue = sm.px.Status(sm.maxSequenceCommitted + 1)
		}

		if sm.dead {
			break
		}

		// Get the decided configuration for this sequence
		decidedConfig := decidedValue.(Op).ProposedConfig
		// If it's not a no-op (number 0), store the new configuration
		if decidedConfig.Num > 0 {
			sm.addConfig(decidedConfig)
			toPrint += fmt.Sprintf("\n\t\tShards: %v", decidedConfig.Shards)
			for gid, _ := range decidedConfig.Groups {
				toPrint += fmt.Sprintf("\n\t\t%v", gid)
			}
		}
		sm.maxSequenceCommitted++
	}

	// All done (have configuration excluding given group)
	// Call Paxos.Done() to free memory, and return
	sm.px.Done(sm.maxSequenceCommitted)

	toPrint += "\n\tReturning"
	DPrintf(toPrint)
	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	// Your code here.

	// Obtain the lock
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Will creat one debug string to print at the end
	// to ensure that other methods' outputs don't interleave with those of this method
	toPrint := ""
	toPrint += fmt.Sprintf("\nShardMaster got Move(%v, %v)", args.Shard, args.GID)

	// Get the latest configuration from Paxos
	toPrint += fmt.Sprintf("\n\tGetting latest configuration...")
	sm.update()
	toPrint += fmt.Sprintf(" got config %v", sm.configs[len(sm.configs)-1].Num)

	// While current configuration has desired shard somewhere else
	// propose a new configuration that has the correct assignment
	for sm.configs[len(sm.configs)-1].Shards[args.Shard] != args.GID && !sm.dead {
		// Determine desired new configuration
		newConfig := sm.cloneCurrentConfig()
		newConfig.Shards[args.Shard] = args.GID
		newConfig.Num++

		// Propose new configuration to Paxos
		// Note that unique ID is only needed for no-ops
		// since here we don't really care if our proposal was accepted
		// (we only care if the accepted proposal has the given shard assignment)
		var op Op
		op.ProposedConfig = newConfig
		toPrint += fmt.Sprintf("\n\tProposing config %v to Paxos", newConfig.Num)
		sm.px.Start(sm.maxSequenceCommitted+1, op)
		// Wait until its Status is decided
		decided := false
		var decidedValue interface{}
		toWait := 25 * time.Millisecond
		for !decided && !sm.dead {
			time.Sleep(toWait)
			if toWait < 100*time.Millisecond {
				toWait *= 2
			}
			decided, decidedValue = sm.px.Status(sm.maxSequenceCommitted + 1)
		}

		if sm.dead {
			break
		}

		// Get the decided configuration for this sequence
		decidedConfig := decidedValue.(Op).ProposedConfig
		// If it's not a no-op (number 0), store the new configuration
		if decidedConfig.Num > 0 {
			sm.addConfig(decidedConfig)
			toPrint += fmt.Sprintf("\n\tDecided on config %v", decidedConfig.Num)
		}
		sm.maxSequenceCommitted++
	}

	// All done (have configuration with given shard assignment)
	// Call Paxos.Done() to free memory, and return
	sm.px.Done(sm.maxSequenceCommitted)

	toPrint += "\n\tReturning"
	DPrintf(toPrint)
	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	// Your code here.

	// Obtain the lock
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Get the latest configuration from Paxos if needed
	if (args.Num == -1) || (args.Num >= len(sm.configs)) {
		sm.update()
	}

	// Return the requested configuration or the most recent one
	if (args.Num == -1) || (args.Num >= len(sm.configs)) {
		reply.Config = sm.cloneConfig(sm.configs[len(sm.configs)-1])
	} else {
		reply.Config = sm.cloneConfig(sm.configs[args.Num])
	}

	return nil
}

// Determine which group has the lowest load
// Returns the GID of that group, and a shard which is assigned to the given gidExclude group
// Will consider all known groups except for the given gidExclude
// For sample usage, see the Leave() function above
func (sm *ShardMaster) minLoadedGroup(config Config, gidExlcude int64) (int64, int) {
	minLoad := -1
	var minLoadGID int64 = -1
	shards := make(map[int64]int)

	// Determine the load on each group
	numShards := make(map[int64]int)
	// First initialize to 0 for all known groups
	// This ensures that we consider known groups with no shards currently assigned
	for gid, _ := range config.Groups {
		numShards[gid] = 0
	}
	for shard, gid := range config.Shards {
		numShards[gid]++
		shards[gid] = shard
	}

	// Determine lowest load
	for gid, load := range numShards {
		if (gid != gidExlcude) && ((load < minLoad) || (minLoad == -1)) {
			minLoad = load
			minLoadGID = gid
		}
	}

	// Return answer
	return minLoadGID, shards[gidExlcude]
}

// Determine which group has the highest load
// Returns the GID of that group, and a shard which is assigned to that group
// For sample usage, see the Join() function above
func (sm *ShardMaster) maxLoadedGroup(config Config) (int64, int) {
	maxLoad := -1
	var maxLoadGID int64 = -1
	shards := make(map[int64]int)
	// Determine the load on each group
	numShards := make(map[int64]int)
	for shard, gid := range config.Shards {
		numShards[gid]++
		shards[gid] = shard
	}
	// Determine highest load
	for gid, load := range numShards {
		if (load > maxLoad) || (maxLoad == -1) {
			maxLoad = load
			maxLoadGID = gid
		}
	}
	// Return answer and a shard it services
	return maxLoadGID, shards[maxLoadGID]
}

// Determine if the given group is currently known to the given configuration
// Returns true even if the group is not assigned any shards
func (sm *ShardMaster) haveGroup(config Config, gid int64) bool {
	for gidUsed, _ := range config.Groups {
		if gid == gidUsed {
			return true
		}
	}
	return false
}

// Determine if the given group is currently being utilized by the given configuration
func (sm *ShardMaster) usingGroup(config Config, gid int64) bool {
	for _, gidUsed := range config.Shards {
		if gid == gidUsed {
			return true
		}
	}
	return false
}

// Create a clone of the given configuration
// Addresses issue of maps being references
func (sm *ShardMaster) cloneConfig(config Config) Config {
	var newConfig Config
	newConfig.Groups = make(map[int64][]string)
	newConfig.Num = config.Num
	for gid, servers := range config.Groups {
		newConfig.Groups[gid] = servers
	}
	for shard, gid := range config.Shards {
		newConfig.Shards[shard] = gid
	}
	return newConfig
}

// Create a clone of the current configuration
// Addresses issue of maps being references
func (sm *ShardMaster) cloneCurrentConfig() Config {
	return sm.cloneConfig(sm.configs[len(sm.configs)-1])
}

// Add a new configuration to this server's list of configurations
func (sm *ShardMaster) addConfig(newConfig Config) {
	newList := make([]Config, len(sm.configs)+1)
	for index, next := range sm.configs {
		newList[index] = sm.cloneConfig(next)
	}
	newList[len(newList)-1] = sm.cloneConfig(newConfig)
	sm.configs = newList
}

// Make sure Paxos is up to date by proposing no-ops
func (sm *ShardMaster) update() {
	var noop Op
	noop.ProposedConfig.Num = 0
	// Concatenate first 16 digits of current time with
	// first 3 digits of sm.me
	//   Using 3 digits of sm.me allows for a thousand peers
	//   Using 16 digits of the time means it won't repeat for about 115 days
	//   Using both time and "me" means it's probably unique
	//   Using 19 digits means it will fit in a uint64
	timeDigits := uint64(time.Now().UnixNano() % 10000000000000000)
	meDigits := uint64(sm.me % 1000)
	noop.ID = timeDigits*1000 + meDigits

	updated := false
	for !updated && !sm.dead {
		sm.px.Start(sm.maxSequenceCommitted+1, noop)
		// Wait until its Status is decided
		decided := false
		var decidedValue interface{}
		toWait := 25 * time.Millisecond
		for !decided && !sm.dead {
			decided, decidedValue = sm.px.Status(sm.maxSequenceCommitted + 1)
			if !decided {
				time.Sleep(toWait)
				//if toWait < 2*time.Second {
				//	toWait *= 2
				//}
			}
		}

		if sm.dead {
			break
		}

		// Get the decided configuration for this sequence
		decidedConfig := decidedValue.(Op).ProposedConfig
		// If the decided value has the chosen unique ID, ours was accepted and we are updated
		// Otherwise, store decided configuration (if it's not another no-op)
		if decidedValue.(Op).ID == noop.ID {
			updated = true
		} else {
			if decidedConfig.Num > 0 {
				sm.addConfig(decidedConfig)
			}
		}
		sm.maxSequenceCommitted++
	}
	sm.px.Done(sm.maxSequenceCommitted)
}

// please don't change this function.
func (sm *ShardMaster) Kill() {
	sm.dead = true
	sm.l.Close()
	sm.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
	gob.Register(Op{})

	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}

	rpcs := rpc.NewServer()
	rpcs.Register(sm)

	sm.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for sm.dead == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.dead == false {
				if sm.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.unreliable && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && sm.dead == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}
