package shardmaster

import "testing"
import "runtime"
import "strconv"
import "os"

import "time"
import "fmt"
import "math/rand"

const onlyBenchmarks = false
const runOldTests = true
const runNewTests = true

// Make a port using the given tag and host number
func makePort(tag string, host int) string {
	s := "/var/tmp/824-"
	s += strconv.Itoa(os.Getuid()) + "/"
	os.Mkdir(s, 0777)
	s += "sm-"
	s += strconv.Itoa(os.Getpid()) + "-"
	s += tag + "-"
	s += strconv.Itoa(host)
	return s
}

// Kill the given servers
func cleanup(shardMasterServers []*ShardMaster) {
	for i := 0; i < len(shardMasterServers); i++ {
		if shardMasterServers[i] != nil {
			shardMasterServers[i].Kill()
		}
	}
}

//
// maybe should take a cka[] and find the server with
// the highest Num.
//
func checkConfig(test *testing.T, knownGroups []int64, masterClerk *Clerk) {
	config := masterClerk.Query(-1)
	if len(config.Groups) != len(knownGroups) {
		test.Fatalf("wanted %v groups, got %v", len(knownGroups), len(config.Groups))
	}

	// Are the groups in the configuration valid groups?
	for _, nextGroup := range knownGroups {
		_, haveGroup := config.Groups[nextGroup]
		if !haveGroup {
			test.Fatalf("config is missing group %v", nextGroup)
		}
	}

	// Any un-allocated shards?
	if len(knownGroups) > 0 {
		for shard, group := range config.Shards {
			_, validGroup := config.Groups[group]
			if !validGroup {
				test.Fatalf("shard %v assigned to invalid group %v", shard, group)
			}
		}
	}

	// More or less balanced sharding?
	numShards := map[int64]int{}
	for _, group := range config.Shards {
		numShards[group] += 1
	}
	min := 257
	max := 0
	for group, _ := range config.Groups {
		if numShards[group] > max {
			max = numShards[group]
		}
		if numShards[group] < min {
			min = numShards[group]
		}
	}
	if max > min+1 {
		test.Fatalf("max %v too much larger than min %v", max, min)
	}
}

func TestFileBasic(test *testing.T) {
	if onlyBenchmarks || !runOldTests {
		return
	}
	runtime.GOMAXPROCS(4)

	const numServers = 3
	var shardMasterServers []*ShardMaster = make([]*ShardMaster, numServers)
	var shardMasterPorts []string = make([]string, numServers)
	defer cleanup(shardMasterServers)

	// Make ports for servers
	for i := 0; i < numServers; i++ {
		shardMasterPorts[i] = makePort("basic", i)
	}
	for i := 0; i < numServers; i++ {
		shardMasterServers[i] = StartServer(shardMasterPorts, i, false)
	}

	// Make clerks to communicate with shardmaster
	masterClerk := MakeClerk(shardMasterPorts, false)
	var clerks [numServers]*Clerk
	for i := 0; i < numServers; i++ {
		clerks[i] = MakeClerk([]string{shardMasterPorts[i]}, false)
	}

	fmt.Printf("\nTest: Basic leave/join ...")

	// Check empty initial configuration
	configs := make([]Config, 6)
	configs[0] = masterClerk.Query(-1)
	checkConfig(test, []int64{}, masterClerk)
	if configs[0].Num != 0 {
		test.Fatalf("Configuration 0 has number %v", configs[0].Num)
	}

	// Check after single join
	var gid1 int64 = 1
	masterClerk.Join(gid1, []string{"x", "y", "z"})
	checkConfig(test, []int64{gid1}, masterClerk)
	configs[1] = masterClerk.Query(-1)
	if configs[1].Num != 1 {
		test.Fatalf("Configuration 1 has number %v", configs[1].Num)
	}

	// Check after same group joins twice
	var gid2 int64 = 2
	masterClerk.Join(gid2, []string{"a", "b", "c"})
	checkConfig(test, []int64{gid1, gid2}, masterClerk)
	configs[2] = masterClerk.Query(-1)
	if configs[2].Num != 2 {
		test.Fatalf("Configuration 2 has number %v", configs[2].Num)
	}
	masterClerk.Join(gid2, []string{"a", "b", "c"})
	checkConfig(test, []int64{gid1, gid2}, masterClerk)
	configs[3] = masterClerk.Query(-1)

	// Check that groups are stored correctly
	currentConfig := masterClerk.Query(-1)
	servers1 := currentConfig.Groups[gid1]
	if len(servers1) != 3 || servers1[0] != "x" || servers1[1] != "y" || servers1[2] != "z" {
		test.Fatal("wrong servers for gid %v: %v\n", gid1, servers1)
	}
	servers2 := currentConfig.Groups[gid2]
	if len(servers2) != 3 || servers2[0] != "a" || servers2[1] != "b" || servers2[2] != "c" {
		test.Fatal("wrong servers for gid %v: %v\n", gid2, servers2)
	}

	// Check config after groups leave
	masterClerk.Leave(gid1)
	checkConfig(test, []int64{gid2}, masterClerk)
	configs[4] = masterClerk.Query(-1)

	masterClerk.Leave(gid1)
	checkConfig(test, []int64{gid2}, masterClerk)
	configs[5] = masterClerk.Query(-1)

	fmt.Printf("\n\tPassed")

	fmt.Printf("\nTest: Historical queries ...")

	for i := 0; i < len(configs); i++ {
		oldConfig := masterClerk.Query(configs[i].Num)
		if oldConfig.Num != configs[i].Num {
			test.Fatalf("historical Num wrong for config %v", i)
		}
		if oldConfig.Shards != configs[i].Shards {
			test.Fatalf("historical Shards wrong")
		}
		if len(oldConfig.Groups) != len(configs[i].Groups) {
			test.Fatalf("number of historical Groups is wrong for config %v", i)
		}
		for gid, oldServers := range oldConfig.Groups {
			trueServers, ok := configs[i].Groups[gid]
			if !ok || len(oldServers) != len(trueServers) {
				test.Fatalf("historical len(Groups) wrong for config %v", i)
			}
			if ok && len(oldServers) == len(trueServers) {
				for j := 0; j < len(oldServers); j++ {
					if oldServers[j] != trueServers[j] {
						test.Fatalf("historical Groups wrong for config %v", i)
					}
				}
			}
		}
	}

	fmt.Printf("\n\tPassed")

	fmt.Printf("\nTest: Move ...")
	{
		var gid3 int64 = 503
		masterClerk.Join(gid3, []string{"3a", "3b", "3c"})
		var gid4 int64 = 504
		masterClerk.Join(gid4, []string{"4a", "4b", "4c"})
		for i := 0; i < NShards; i++ {
			currentConfig = masterClerk.Query(-1)
			if i < NShards/2 {
				masterClerk.Move(i, gid3)
				// See if config num increased if shard needed to move
				if currentConfig.Shards[i] != gid3 {
					currentConfig2 := masterClerk.Query(-1)
					if currentConfig2.Num <= currentConfig.Num {
						test.Fatalf("Move should increase Config.Num")
					}
				}
			} else {
				masterClerk.Move(i, gid4)
				// See if config num increased if shard needed to move
				if currentConfig.Shards[i] != gid4 {
					currentConfig2 := masterClerk.Query(-1)
					if currentConfig2.Num <= currentConfig.Num {
						test.Fatalf("Move should increase Config.Num")
					}
				}
			}
		}
		// Check that shards actually moved
		currentConfig2 := masterClerk.Query(-1)
		for i := 0; i < NShards; i++ {
			if i < NShards/2 {
				if currentConfig2.Shards[i] != gid3 {
					test.Fatalf("expected shard %v on gid %v, actually on %v",
						i, gid3, currentConfig2.Shards[i])
				}
			} else {
				if currentConfig2.Shards[i] != gid4 {
					test.Fatalf("expected shard %v on gid %v, actually on %v",
						i, gid4, currentConfig2.Shards[i])
				}
			}
		}
		masterClerk.Leave(gid3)
		masterClerk.Leave(gid4)
	}
	fmt.Printf("\n\tPassed")

	fmt.Printf("\nTest: Concurrent leave/join ...")

	const numConcurrent = 10
	gids := make([]int64, numConcurrent)
	var doneChannels [numConcurrent]chan bool
	for index := 0; index < numConcurrent; index++ {
		gids[index] = int64(index + 1)
		doneChannels[index] = make(chan bool)
		go func(i int) {
			defer func() { doneChannels[i] <- true }()
			var gid int64 = gids[i]
			clerks[(i+0)%numServers].Join(gid+1000, []string{"a", "b", "c"})
			clerks[(i+0)%numServers].Join(gid, []string{"a", "b", "c"})
			clerks[(i+1)%numServers].Leave(gid + 1000)
		}(index)
	}
	for i := 0; i < numConcurrent; i++ {
		<-doneChannels[i]
	}
	checkConfig(test, gids, masterClerk)

	fmt.Printf("\n\tPassed")

	fmt.Printf("\nTest: Min advances after joins ...")

	for i, server := range shardMasterServers {
		if server.px.Min() <= 0 {
			test.Fatalf("Min() for %s did not advance", shardMasterPorts[i])
		}
	}

	fmt.Printf("\n\tPassed")

	fmt.Printf("\nTest: Minimal transfers after joins ...")

	c1 := masterClerk.Query(-1)
	for i := 0; i < 5; i++ {
		masterClerk.Join(int64(numConcurrent+1+i), []string{"a", "b", "c"})
	}
	c2 := masterClerk.Query(-1)
	// numCurrent == numShards, so above 5 extra joins should
	// not cause any shards to move
	// TODO: make a test that actually involves moving shards
	// 		 and counts how many shards transfer
	for i := int64(1); i <= numConcurrent; i++ {
		for j := 0; j < len(c1.Shards); j++ {
			if c2.Shards[j] == i {
				if c1.Shards[j] != i {
					test.Fatalf("non-minimal transfer after Join()s")
				}
			}
		}
	}

	fmt.Printf("\n\tPassed")

	fmt.Printf("\nTest: Minimal transfers after leaves ...")

	for i := 0; i < 5; i++ {
		masterClerk.Leave(int64(numConcurrent + 1 + i))
	}
	c3 := masterClerk.Query(-1)
	// The new groups shouldn't have been assigned any shards
	// So leaving should not cause any assignments to change
	// TODO: make a test that actually involves moving shards
	//       and counts how many shards transfer?
	for i := int64(1); i <= numConcurrent; i++ {
		for j := 0; j < len(c1.Shards); j++ {
			if c2.Shards[j] == i {
				if c3.Shards[j] != i {
					test.Fatalf("non-minimal transfer after Leave()s")
				}
			}
		}
	}

	fmt.Printf("\n\tPassed")
}

func TestFileUnreliable(test *testing.T) {
	if onlyBenchmarks || !runOldTests {
		return
	}
	runtime.GOMAXPROCS(4)

	const numServers = 3
	var shardMasterServers []*ShardMaster = make([]*ShardMaster, numServers)
	var shardMasterPorts []string = make([]string, numServers)
	defer cleanup(shardMasterServers)

	// Make ports for servers
	for i := 0; i < numServers; i++ {
		shardMasterPorts[i] = makePort("unrel", i)
	}
	for i := 0; i < numServers; i++ {
		shardMasterServers[i] = StartServer(shardMasterPorts, i, false)
		// don't turn on unreliable because the assignment
		// doesn't require the shardmaster to detect duplicate
		// client requests.
		// shardMasterServers[i].unreliable = true
	}

	masterClerk := MakeClerk(shardMasterPorts, false)
	var clerks [numServers]*Clerk
	for i := 0; i < numServers; i++ {
		clerks[i] = MakeClerk([]string{shardMasterPorts[i]}, false)
	}

	fmt.Printf("\nTest: Concurrent leave/join, failure ...")

	const numConcurrent = 20
	gids := make([]int64, numConcurrent)
	var doneChannels [numConcurrent]chan bool
	for index := 0; index < numConcurrent; index++ {
		gids[index] = int64(index + 1)
		doneChannels[index] = make(chan bool)
		go func(i int) {
			defer func() { doneChannels[i] <- true }()
			var gid int64 = gids[i]
			clerks[1+(rand.Int()%2)].Join(gid+1000, []string{"a", "b", "c"})
			clerks[1+(rand.Int()%2)].Join(gid, []string{"a", "b", "c"})
			clerks[1+(rand.Int()%2)].Leave(gid + 1000)
			// server 0 won't be able to hear any RPCs.
			os.Remove(shardMasterPorts[0])
		}(index)
	}
	for i := 0; i < numConcurrent; i++ {
		<-doneChannels[i]
	}
	checkConfig(test, gids, masterClerk)

	fmt.Printf("\n\tPassed")
}

func TestFileFreshQuery(test *testing.T) {
	if onlyBenchmarks || !runOldTests {
		return
	}
	runtime.GOMAXPROCS(4)

	const numServers = 3
	var shardMasterServers []*ShardMaster = make([]*ShardMaster, numServers)
	var shardMasterPorts []string = make([]string, numServers)
	defer cleanup(shardMasterServers)

	// Make ports for servers
	for i := 0; i < numServers; i++ {
		shardMasterPorts[i] = makePort("fresh", i)
	}
	for i := 0; i < numServers; i++ {
		shardMasterServers[i] = StartServer(shardMasterPorts, i, false)
	}

	clerk1 := MakeClerk([]string{shardMasterPorts[1]}, false)

	fmt.Printf("\nTest: Query() returns latest configuration ...")

	portx := shardMasterPorts[0] + strconv.Itoa(rand.Int())
	if os.Rename(shardMasterPorts[0], portx) != nil {
		test.Fatalf("os.Rename() failed")
	}
	clerkx := MakeClerk([]string{portx}, false)

	// Use clerk1 to join, and clerkx to query (see if clerkx hears about join)
	clerk1.Join(1001, []string{"a", "b", "c"})
	config := clerkx.Query(-1)
	_, ok := config.Groups[1001]
	if !ok {
		test.Fatalf("Query(-1) produced a stale configuration")
	}

	fmt.Printf("\n\tPassed\n\n")
	os.Remove(portx)
}

func TestFilePersistence(test *testing.T) {
	if onlyBenchmarks || !runNewTests {
		return
	}
	runtime.GOMAXPROCS(4)

	const numServers = 3
	var shardMasterServers []*ShardMaster = make([]*ShardMaster, numServers)
	var shardMasterPorts []string = make([]string, numServers)
	defer cleanup(shardMasterServers)

	// Make ports for servers
	for i := 0; i < numServers; i++ {
		shardMasterPorts[i] = makePort("fresh", i)
	}
	for i := 0; i < numServers; i++ {
		shardMasterServers[i] = StartServer(shardMasterPorts, i, false)
	}

	// Make clerks to communicate with servers
	masterClerk := MakeClerk(shardMasterPorts, false)
	var clerks [numServers]*Clerk
	var gids []int64
	for i := 0; i < numServers; i++ {
		clerks[i] = MakeClerk([]string{shardMasterPorts[i]}, false)
		gids = append(gids, int64(i)+1)
	}
	fmt.Printf("\nTest: Single restarted server (with disk) knows about past and missed configs ...")

	// Make some configs
	for i := 0; i < numServers; i++ {
		clerks[i].Join(gids[i], []string{"a", "b", "c"})
	}
	checkConfig(test, gids, masterClerk)
	// Kill a server
	shardMasterServers[0].KillSaveDisk()

	// Make some more configs
	clerks[1].Leave(gids[1])
	clerks[2].Join(gids[1], []string{"a", "b", "c"})
	checkConfig(test, gids, masterClerk)

	// Bring server back and query configs
	shardMasterServers[0] = StartServer(shardMasterPorts, 0, false)
	if clerks[0].Query(3).Num != 3 {
		test.Fatalf("Restarted server (with disk) does not remember past configs it had seen")
	}
	restartNum := clerks[0].Query(-1).Num
	correctNum := clerks[1].Query(-1).Num
	if restartNum != correctNum {
		test.Fatalf("Restarted server (with disk) did not learn about missed configs (knows up to %v, should know %v)", restartNum, correctNum)
	}
	checkConfig(test, gids, masterClerk)

	fmt.Printf("\n\tPassed")

	fmt.Printf("\nTest: Single restarted server (no disk) knows about past and missed configs ...")

	// Kill a server (erase disk)
	shardMasterServers[0].Kill()

	// Make some more configs
	clerks[1].Leave(gids[1])
	clerks[2].Join(gids[1], []string{"a", "b", "c"})
	checkConfig(test, gids, masterClerk)

	// Bring server back and query configs
	shardMasterServers[0] = StartServer(shardMasterPorts, 0, false)
	if clerks[0].Query(4).Num != 4 {
		test.Fatalf("Restarted server (no disk) does not remember past configs it had seen")
	}
	restartNum = clerks[0].Query(-1).Num
	correctNum = clerks[1].Query(-1).Num
	if restartNum != correctNum {
		test.Fatalf("Restarted server (no disk) did not learn about missed configs (knows up to %v, should know %v)", restartNum, correctNum)
	}
	checkConfig(test, gids, masterClerk)

	fmt.Printf("\n\tPassed")

	fmt.Printf("\nTest: All servers restart (with disk), know about past configs ...")

	// Kill all servers
	for i := 0; i < numServers; i++ {
		shardMasterServers[i].KillSaveDisk()
	}
	time.Sleep(1 * time.Second)

	// Bring servers back
	for i := 0; i < numServers; i++ {
		shardMasterServers[i] = StartServer(shardMasterPorts, i, false)
	}
	// Check configs
	for i := 0; i < numServers; i++ {
		restartNum = clerks[i].Query(-1).Num
		if restartNum != correctNum {
			test.Fatalf("Restarted server %v doesn't have old configs (knows up to %v, should know %v)", i, restartNum, correctNum)
		}
	}
	checkConfig(test, gids, masterClerk)

	fmt.Printf("\n\tPassed\n\n")
}

func BenchmarkJoinSpeed_____(benchmark *testing.B) {
	const numServers = 3
	var shardMasterServers []*ShardMaster = make([]*ShardMaster, numServers)
	var shardMasterPorts []string = make([]string, numServers)
	defer cleanup(shardMasterServers)

	// Make ports for servers
	for i := 0; i < numServers; i++ {
		shardMasterPorts[i] = makePort("fresh", i)
	}
	for i := 0; i < numServers; i++ {
		shardMasterServers[i] = StartServer(shardMasterPorts, i, false)
	}

	// Make clerks to communicate with servers
	var clerks [numServers]*Clerk
	var gids []int64
	for i := 0; i < numServers; i++ {
		clerks[i] = MakeClerk([]string{shardMasterPorts[i]}, false)
		gids = append(gids, int64(i)+1)
	}
	clerks[1].Join(gids[1], []string{"a", "b", "c"})
	clerks[2].Join(gids[2], []string{"a", "b", "c"})

	benchmark.ResetTimer()
	// Join many times
	// Note that if we change shardmaster so that the same group joining
	// twice doesn't cause a new config, this benchmark should be changed
	for i := 0; i < benchmark.N; i++ {
		clerks[0].Join(gids[0], []string{"a", "b", "c"})
	}
}

func BenchmarkLeaveSpeed____(benchmark *testing.B) {
	const numServers = 3
	var shardMasterServers []*ShardMaster = make([]*ShardMaster, numServers)
	var shardMasterPorts []string = make([]string, numServers)
	defer cleanup(shardMasterServers)

	// Make ports for servers
	for i := 0; i < numServers; i++ {
		shardMasterPorts[i] = makePort("fresh", i)
	}
	for i := 0; i < numServers; i++ {
		shardMasterServers[i] = StartServer(shardMasterPorts, i, false)
	}

	// Make clerks to communicate with servers
	var clerks [numServers]*Clerk
	var gids []int64
	for i := 0; i < numServers; i++ {
		clerks[i] = MakeClerk([]string{shardMasterPorts[i]}, false)
		gids = append(gids, int64(i)+1)
	}
	clerks[0].Join(gids[1], []string{"a", "b", "c"})
	clerks[1].Join(gids[1], []string{"a", "b", "c"})
	clerks[2].Join(gids[2], []string{"a", "b", "c"})

	benchmark.ResetTimer()
	// Leave many times
	// Note that if we change shardmaster so that the same group leaving
	// twice doesn't cause a new config, this benchmark should be changed
	for i := 0; i < benchmark.N; i++ {
		clerks[0].Leave(gids[0])
	}
}

func BenchmarkJoinLeaveSpeed(benchmark *testing.B) {
	const numServers = 3
	var shardMasterServers []*ShardMaster = make([]*ShardMaster, numServers)
	var shardMasterPorts []string = make([]string, numServers)
	defer cleanup(shardMasterServers)

	// Make ports for servers
	for i := 0; i < numServers; i++ {
		shardMasterPorts[i] = makePort("fresh", i)
	}
	for i := 0; i < numServers; i++ {
		shardMasterServers[i] = StartServer(shardMasterPorts, i, false)
	}

	// Make clerks to communicate with servers
	var clerks [numServers]*Clerk
	var gids []int64
	for i := 0; i < numServers; i++ {
		clerks[i] = MakeClerk([]string{shardMasterPorts[i]}, false)
		gids = append(gids, int64(i)+1)
	}
	clerks[0].Join(gids[1], []string{"a", "b", "c"})
	clerks[1].Join(gids[1], []string{"a", "b", "c"})
	clerks[2].Join(gids[2], []string{"a", "b", "c"})

	benchmark.ResetTimer()
	// Join/Leave many times
	for i := 0; i < benchmark.N; i++ {
		clerks[0].Join(gids[1], []string{"a", "b", "c"})
		clerks[0].Leave(gids[0])
	}
}
