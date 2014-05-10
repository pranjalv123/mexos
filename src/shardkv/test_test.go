package shardkv

import "testing"
import "shardmaster"
import "runtime"
import "strconv"
import "os"
import "time"
import "fmt"
import "sync"
import "math/rand"

// Note: If a persistence test fails, the next one may fail by panic since kill wasn't called
// If you want, you can find-replace Fatalf with Errorf
//   to allow the tests to finish and then print errors at completion
//   (but tests will erroneously print out "passed")
const runOldTests = true
const runNewTests = true

var rebootChannel chan int

// Make a port using the given tag and host number
func makePort(tag string, host int) string {
	s := "/var/tmp/824-"
	s += strconv.Itoa(os.Getuid()) + "/"
	os.Mkdir(s, 0777)
	s += "skv-"
	s += strconv.Itoa(os.Getpid()) + "-"
	s += tag + "-"
	s += strconv.Itoa(host)
	return s
}

// Use for checking PutHash
func NextValue(hprev string, val string) string {
	h := hash(hprev + val)
	return strconv.Itoa(int(h))
}

// Kill shardmaster servers
func smCleanup(smServers []*shardmaster.ShardMaster) {
	//fmt.Printf("\nsmCleanup")
	for i := 0; i < len(smServers); i++ {
		if smServers[i] != nil {
			//fmt.Printf("\n\tKilling %v", i)
			smServers[i].Kill()
		} else {
			//fmt.Printf("\n\t%v is nil", i)
		}
	}
}

// Kill shardkv servers
func shardkvCleanup(kvServers [][]*ShardKV) {
	//fmt.Printf("\nshardkvCleanup")
	for i := 0; i < len(kvServers); i++ {
		for j := 0; j < len(kvServers[i]); j++ {
			//fmt.Printf("\n\tKilling %v-%v", i, j)
			kvServers[i][j].Kill()
		}
	}
}

// Set up and start servers for shardkv groups and shardmaster
func setup(tag string, unreliable bool, numGroups int, numReplicas int) ([]string, []int64, [][]string, [][]*ShardKV, func()) {
	runtime.GOMAXPROCS(4)

	const numMasters = 3
	var smServers []*shardmaster.ShardMaster = make([]*shardmaster.ShardMaster, numMasters)
	var smPorts []string = make([]string, numMasters)
	// Start shardmaster servers
	for i := 0; i < numMasters; i++ {
		smPorts[i] = makePort(tag+"m", i)
	}
	for i := 0; i < numMasters; i++ {
		smServers[i] = shardmaster.StartServer(smPorts, i, false)
	}

	gids := make([]int64, numGroups)           // each group ID
	kvPorts := make([][]string, numGroups)     // ShardKV ports, [group][replica]
	kvServers := make([][]*ShardKV, numGroups) // ShardKVs
	// Start numReplicas shardkv servers for each of numGroups groups
	for i := 0; i < numGroups; i++ {
		gids[i] = int64(i + 100)
		kvServers[i] = make([]*ShardKV, numReplicas)
		kvPorts[i] = make([]string, numReplicas)
		for j := 0; j < numReplicas; j++ {
			kvPorts[i][j] = makePort(tag+"s", (i*numReplicas)+j)
		}
		for j := 0; j < numReplicas; j++ {
			kvServers[i][j] = StartServer(gids[i], smPorts, kvPorts[i], j, false)
			kvServers[i][j].unreliable = unreliable
		}
	}

	clean := func() { shardkvCleanup(kvServers); smCleanup(smServers) }
	return smPorts, gids, kvPorts, kvServers, clean
}

func TestFileBasic(t *testing.T) {
	if !runOldTests {
		return
	}
	numGroups := 3
	numReplicas := 3
	smPorts, gids, kvPorts, kvServers, clean := setup("basic", false, numGroups, numReplicas)
	defer clean()

	fmt.Printf("\nTest: Basic Join/Leave...")

	smClerk := shardmaster.MakeClerk(smPorts, false)
	smClerk.Join(gids[0], kvPorts[0])

	kvClerk := MakeClerk(smPorts, false)

	// Start listening on reboot channel in case we're testing persistence
	rebootDone := 0
	go rebootListener(&rebootDone, false, smPorts, gids, kvPorts, kvServers, numGroups, numReplicas)

	kvClerk.Put("a", "x")
	v := kvClerk.PutHash("a", "b")
	if v != "x" {
		t.Fatalf("Puthash got wrong value")
	}
	ov := NextValue("x", "b")
	if kvClerk.Get("a") != ov {
		t.Fatalf("Get got wrong value")
	}

	keys := make([]string, 10)
	vals := make([]string, len(keys))
	for i := 0; i < len(keys); i++ {
		keys[i] = strconv.Itoa(rand.Int())
		vals[i] = strconv.Itoa(rand.Int())
		kvClerk.Put(keys[i], vals[i])
	}

	// are keys still there after joins?
	for g := 1; g < len(gids); g++ {
		smClerk.Join(gids[g], kvPorts[g])
		time.Sleep(1 * time.Second)
		for i := 0; i < len(keys); i++ {
			v := kvClerk.Get(keys[i])
			if v != vals[i] {
				t.Fatalf("joining; wrong value; g=%v k=%v wanted=%v got=%v",
					g, keys[i], vals[i], v)
			}
			vals[i] = strconv.Itoa(rand.Int())
			kvClerk.Put(keys[i], vals[i])
		}
	}

	// are keys still there after leaves?
	for g := 0; g < len(gids)-1; g++ {
		smClerk.Leave(gids[g])
		time.Sleep(1 * time.Second)
		for i := 0; i < len(keys); i++ {
			v := kvClerk.Get(keys[i])
			if v != vals[i] {
				t.Fatalf("leaving; wrong value; g=%v k=%v wanted=%v got=%v",
					g, keys[i], vals[i], v)
			}
			vals[i] = strconv.Itoa(rand.Int())
			kvClerk.Put(keys[i], vals[i])
		}
	}

	rebootDone = 1
	fmt.Printf("\n\tMemory usage          : %v", getMemoryUsage())
	fmt.Printf("\n\tPaxos Disk usage      : %v", getPaxosDiskUsage())
	fmt.Printf("\n\tShardmaster Disk usage: %v", getShardMasterDiskUsage())
	fmt.Printf("\n\tShardKV Disk usage    : %v", getShardKVDiskUsage())
	fmt.Printf("\n\tTotal Disk usage      : %v", getDiskUsage())
	fmt.Printf("\n\tPassed\n")
	time.Sleep(2 * time.Second)
}

func TestFileMove(t *testing.T) {
	if !runOldTests {
		return
	}
	numGroups := 3
	numReplicas := 3
	smPorts, gids, kvPorts, kvServers, clean := setup("move", false, numGroups, numReplicas)
	defer clean()

	fmt.Printf("\nTest: Shards really move...")

	smClerk := shardmaster.MakeClerk(smPorts, false)
	smClerk.Join(gids[0], kvPorts[0])

	kvClerk := MakeClerk(smPorts, false)

	// Start listening on reboot channel in case we're testing persistence
	rebootDone := 0
	go rebootListener(&rebootDone, false, smPorts, gids, kvPorts, kvServers, numGroups, numReplicas)

	// insert one key per shard
	for i := 0; i < shardmaster.NShards; i++ {
		kvClerk.Put(string('0'+i), string('0'+i))
	}

	// add group 1.
	smClerk.Join(gids[1], kvPorts[1])
	time.Sleep(5 * time.Second)

	// check that keys are still there.
	for i := 0; i < shardmaster.NShards; i++ {
		if kvClerk.Get(string('0'+i)) != string('0'+i) {
			t.Fatalf("missing key/value")
		}
	}

	// remove sockets from group 0.
	for i := 0; i < len(kvPorts[0]); i++ {
		os.Remove(kvPorts[0][i])
	}

	count := 0
	var mu sync.Mutex
	for i := 0; i < shardmaster.NShards; i++ {
		go func(me int) {
			myck := MakeClerk(smPorts, false)
			v := myck.Get(string('0' + me))
			if v == string('0'+me) {
				mu.Lock()
				count++
				mu.Unlock()
			} else {
				t.Fatalf("Get(%v) yielded %v\n", i, v)
			}
		}(i)
	}

	time.Sleep(10 * time.Second)

	rebootDone = 1
	time.Sleep(2 * time.Second)
	if count > shardmaster.NShards/3 && count < 2*(shardmaster.NShards/3) {
		fmt.Printf("\n\tPassed\n")
	} else {
		t.Fatalf("%v keys worked after killing 1/2 of groups; wanted %v",
			count, shardmaster.NShards/2)
	}
}

func TestFileLimp(t *testing.T) {
	if !runOldTests {
		return
	}
	numGroups := 3
	numReplicas := 3
	smPorts, gids, kvPorts, kvServers, clean := setup("limp", false, numGroups, numReplicas)
	defer clean()

	fmt.Printf("\nTest: Reconfiguration with some dead replicas...")

	smClerk := shardmaster.MakeClerk(smPorts, false)
	smClerk.Join(gids[0], kvPorts[0])

	kvClerk := MakeClerk(smPorts, false)

	// Start listening on reboot channel in case we're testing persistence
	rebootDone := 0
	go rebootListener(&rebootDone, false, smPorts, gids, kvPorts, kvServers, numGroups, numReplicas)

	kvClerk.Put("a", "b")
	if kvClerk.Get("a") != "b" {
		t.Fatalf("got wrong value")
	}

	for g := 0; g < len(kvServers); g++ {
		kvServers[g][rand.Int()%len(kvServers[g])].Kill()
	}

	keys := make([]string, 10)
	vals := make([]string, len(keys))
	for i := 0; i < len(keys); i++ {
		keys[i] = strconv.Itoa(rand.Int())
		vals[i] = strconv.Itoa(rand.Int())
		kvClerk.Put(keys[i], vals[i])
	}

	// are keys still there after joins?
	for g := 1; g < len(gids); g++ {
		smClerk.Join(gids[g], kvPorts[g])
		time.Sleep(1 * time.Second)
		for i := 0; i < len(keys); i++ {
			v := kvClerk.Get(keys[i])
			if v != vals[i] {
				t.Fatalf("joining; wrong value; g=%v k=%v wanted=%v got=%v",
					g, keys[i], vals[i], v)
			}
			vals[i] = strconv.Itoa(rand.Int())
			kvClerk.Put(keys[i], vals[i])
		}
	}

	// are keys still there after leaves?
	for g := 0; g < len(gids)-1; g++ {
		smClerk.Leave(gids[g])
		time.Sleep(2 * time.Second)
		for i := 0; i < len(kvServers[g]); i++ {
			kvServers[g][i].Kill()
		}
		for i := 0; i < len(keys); i++ {
			v := kvClerk.Get(keys[i])
			if v != vals[i] {
				t.Fatalf("leaving; wrong value; g=%v k=%v wanted=%v got=%v",
					g, keys[i], vals[i], v)
			}
			vals[i] = strconv.Itoa(rand.Int())
			kvClerk.Put(keys[i], vals[i])
		}
	}

	rebootDone = 1
	time.Sleep(2 * time.Second)
	fmt.Printf("\n\tPassed\n")
}

func doConcurrent(t *testing.T, unreliable bool) {
	numGroups := 3
	numReplicas := 3
	smPorts, gids, kvPorts, kvServers, clean := setup("conc"+strconv.FormatBool(unreliable), unreliable, numGroups, numReplicas)
	defer clean()

	// Start listening on reboot channel in case we're testing persistence
	rebootDone := 0
	go rebootListener(&rebootDone, unreliable, smPorts, gids, kvPorts, kvServers, numGroups, numReplicas)

	smClerk := shardmaster.MakeClerk(smPorts, false)
	for i := 0; i < len(gids); i++ {
		smClerk.Join(gids[i], kvPorts[i])
	}

	const npara = 11
	var doneChannels [npara]chan bool
	for i := 0; i < npara; i++ {
		doneChannels[i] = make(chan bool)
		go func(me int) {
			ok := true
			defer func() { doneChannels[me] <- ok }()
			kvClerk := MakeClerk(smPorts, false)
			mysmClerk := shardmaster.MakeClerk(smPorts, false)
			key := strconv.Itoa(me)
			last := ""
			for iters := 0; iters < 3; iters++ {
				nv := strconv.Itoa(rand.Int())
				v := kvClerk.PutHash(key, nv)
				if v != last {
					ok = false
					t.Fatalf("PutHash(%v) expected %v got %v\n", key, last, v)
				}
				last = NextValue(last, nv)
				v = kvClerk.Get(key)
				if v != last {
					ok = false
					t.Fatalf("Get(%v) expected %v got %v\n", key, last, v)
				}

				mysmClerk.Move(rand.Int()%shardmaster.NShards,
					gids[rand.Int()%len(gids)])

				time.Sleep(time.Duration(rand.Int()%30) * time.Millisecond)
			}
		}(i)
	}

	for i := 0; i < npara; i++ {
		x := <-doneChannels[i]
		if x == false {
			t.Fatalf("something is wrong")
		}
	}

	rebootDone = 1
	time.Sleep(2 * time.Second)
}

func TestFileConcurrent(t *testing.T) {
	if !runOldTests {
		return
	}
	fmt.Printf("\nTest: Concurrent Put/Get/Move...")
	doConcurrent(t, false)
	fmt.Printf("\n\tPassed\n")
}

func TestFileConcurrentUnreliable(t *testing.T) {
	if !runOldTests {
		return
	}
	fmt.Printf("\nTest: Concurrent Put/Get/Move (unreliable)...")
	doConcurrent(t, true)
	fmt.Printf("\n\tPassed\n")
}

func TestFilePersistenceBasic(t *testing.T) {
	if !runNewTests {
		return
	}

	fmt.Printf("\nTest: One replica per group, all reboot ...")
	// Only use one replicas per group
	smPorts, gids, kvPorts, kvServers, clean := setup("persistBasic", false, 3, 1)

	smClerk := shardmaster.MakeClerk(smPorts, false)
	kvClerk := MakeClerk(smPorts, false)

	// Join all groups with only one server per group
	for i := 0; i < len(gids); i++ {
		smClerk.Join(gids[i], kvPorts[i])
	}

	// Insert one key per shard
	for i := 0; i < shardmaster.NShards; i++ {
		kvClerk.Put(string('0'+i), string('0'+i))
	}

	// Kill and reboot all servers
	for g := 0; g < len(kvServers); g++ {
		kvServers[g][0].KillSaveDisk()
	}
	for g := 0; g < len(kvServers); g++ {
		kvServers[g][0] = StartServer(gids[g], smPorts, kvPorts[g], 0, false)
		kvServers[g][0].unreliable = false
	}

	// Check that the keys are still there.
	for i := 0; i < shardmaster.NShards; i++ {
		if kvClerk.Get(string('0'+i)) != string('0'+i) {
			t.Fatalf("missing key/value")
		}
	}
	clean()
	fmt.Printf("\n\tPassed")

	fmt.Printf("\nTest: Multiple replicas per group, all reboot ...")
	// Only use one replicas per group
	smPorts, gids, kvPorts, kvServers, clean = setup("persistBasic", false, 3, 3)
	defer clean()

	smClerk = shardmaster.MakeClerk(smPorts, false)
	kvClerk = MakeClerk(smPorts, false)

	// Join all groups
	for i := 0; i < len(gids); i++ {
		smClerk.Join(gids[i], kvPorts[i])
	}

	// Insert one key per shard
	for i := 0; i < shardmaster.NShards; i++ {
		kvClerk.Put(string('0'+i), string('0'+i))
	}

	// Kill and reboot all servers
	for g := 0; g < len(kvServers); g++ {
		for s := 0; s < len(kvServers[g]); s++ {
			kvServers[g][s].KillSaveDisk()
		}
	}
	for g := 0; g < len(kvServers); g++ {
		for s := 0; s < len(kvServers[g]); s++ {
			kvServers[g][s] = StartServer(gids[g], smPorts, kvPorts[g], s, false)
			kvServers[g][s].unreliable = false
		}
	}

	// Check that the keys are still there.
	for i := 0; i < shardmaster.NShards; i++ {
		if kvClerk.Get(string('0'+i)) != string('0'+i) {
			t.Fatalf("missing key/value")
		}
	}
	fmt.Printf("\n\tPassed\n")
}

func rebootListener(done *int, unreliable bool, smPorts []string, gids []int64, kvPorts [][]string, kvServers [][]*ShardKV, numGroups int, numReplicas int) {
	for *done == 0 {
		val := <-rebootChannel
		if val == 0 {
			// Reboot a random server
			group := rand.Int() % numGroups
			server := rand.Int() % numReplicas
			if kvServers[group][server].dead {
				continue
			}
			fmt.Printf("\n\tKilling server %v-%v", group, server)
			kvServers[group][server].KillSaveDisk()
			time.Sleep(50 * time.Millisecond)
			//fmt.Printf("\n\tStarting server %v-%v", group, server)
			kvServers[group][server] = StartServer(gids[group], smPorts, kvPorts[group], server, false)
			kvServers[group][server].unreliable = unreliable
			//fmt.Printf("\n\tStarted server %v-%v", group, server)
		} else if val == 1 {
			// Reboot a random group
			group := rand.Int() % numGroups
			fmt.Printf("\n\tKilling group %v ( ", group)
			killed := make(map[int]map[int]bool)
			for g := 0; g < numGroups; g++ {
				killed[g] = make(map[int]bool)
			}
			for server := 0; server < numReplicas; server++ {
				if kvServers[group][server].dead {
					continue
				}
				kvServers[group][server].KillSaveDisk()
				fmt.Printf("%v ", server)
				killed[group][server] = true
			}
			fmt.Printf(")")
			time.Sleep(100 * time.Millisecond)
			for server := 0; server < numReplicas; server++ {
				if !killed[group][server] {
					continue
				}
				kvServers[group][server] = StartServer(gids[group], smPorts, kvPorts[group], server, false)
				kvServers[group][server].unreliable = false
			}
		} else if val == -1 {
			break
		}
	}
	fmt.Printf("\n\tRebooter exiting")
}

func TestFilePersistenceOriginals(t *testing.T) {
	if !runNewTests {
		return
	}
	return
	fmt.Printf("\nTest the original tests but with random reboots of servers or entire groups... ")
	rebootChannel = make(chan int)
	// Start randomly signaling for servers to reboot
	done := false
	go func() {
		for !done {
			// Randomly choose to kill a server or a whole group
			// kill whole groups more often since that's harder to deal with
			if (rand.Int() % 100) < 75 {
				rebootChannel <- 1
			} else {
				rebootChannel <- 0
			}
			time.Sleep(1000 * time.Millisecond)
		}
	}()

	TestFileBasic(t)

	//TestFileMove(t)

	TestFileLimp(t)

	TestFileConcurrent(t)

	TestFileConcurrentUnreliable(t)

	done = true
	fmt.Printf("\n\n\tPassed!!\n")
}

func TestFileMemory(t *testing.T) {
	smPorts, gids, kvPorts, kvServers, clean := setup("persistBasic", false, 3, 3)
	defer clean()
	smClerk := shardmaster.MakeClerk(smPorts, false)
	kvClerk := MakeClerk(smPorts, false)

	targetSize := 100 // Target size in MB
	valueSize := 2000 // Size of each value in KB

	// Periodically check that too much memory isn't being used
	done := false
	go func() {
		for !done {
			runtime.GC()
			time.Sleep(50 * time.Millisecond) // Not sure if this is needed
			usage := getMemoryUsage() / 1000
			if usage > memoryLimit {
				t.Fatalf("Too much memory is being used! Using %v MB, limit is %v MB", usage, memoryLimit)
			}
			time.Sleep(2000 * time.Millisecond)
		}
	}()

	// Make a big value
	big := make([]byte, valueSize*1000)
	for j := 0; j < len(big); j++ {
		big[j] = byte('a' + rand.Int()%26)
	}

	// Join all groups
	for i := 0; i < len(gids); i++ {
		smClerk.Join(gids[i], kvPorts[i])
	}

	time.Sleep(3 * time.Second)

	fmt.Printf("\nPutting %v MB worth of stuff ...\t", targetSize)
	// Many keys with large values
	for i := targetSize * 1000 / valueSize; i >= 0; i-- {
		if (i == 99) && (targetSize*1000/valueSize > 99) {
			fmt.Printf(" ")
		}
		fmt.Printf("%v", i)

		kvClerk.Put(strconv.FormatInt(nrand(), 10), string(big))

		fmt.Printf("\b")
		if i > 9 {
			fmt.Printf("\b")
		}
		if i > 99 {
			fmt.Printf("\b")
		}
	}
	fmt.Printf("\nDone!")

	// Kill a server (destroy disk)
	kvServers[0][0].Kill()

	time.Sleep(3 * time.Second)

	// Reboot server to cause recovery
	kvServers[0][0] = StartServer(gids[0], smPorts, kvPorts[0], 0, false)
	time.Sleep(10 * time.Second)

	fmt.Printf("\n\tMemory usage           (MB): %v", getMemoryUsage()/1000)
	fmt.Printf("\n\tPaxos Disk usage       (MB): %v", getPaxosDiskUsage()/1000)
	fmt.Printf("\n\tShardmaster Disk usage (KB): %v", getShardMasterDiskUsage())
	fmt.Printf("\n\tShardKV Disk usage     (MB): %v", getShardKVDiskUsage()/1000)
	fmt.Printf("\n\tTotal Disk usage       (MB): %v", getDiskUsage()/1000)
	fmt.Printf("\n\tPassed\n")
	done = true
	time.Sleep(5 * time.Second)
}
