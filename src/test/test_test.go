package test

import "testing"
import "shardmaster"
import "shardkv"
import "runtime"
import "strconv"
//import "os"
import "time"
import "fmt"
import "sync"
import "math/rand"
import "log"

// Use for checking PutHash                                                                 
func NextValue(hprev string, val string) string {
        h := hash(hprev + val)
        return strconv.Itoa(int(h))
}

// Set up and start servers for shardkv groups and shardmaster
func setup(tag string, unreliable bool, numGroups int, numReplicas int) ([]string, []int64, [][]string) {
	runtime.GOMAXPROCS(4)

	//number of masters per group
	const numMasters = 1
	kvPorts, smPorts, gids := GetShardkvs(numReplicas, numMasters, numGroups)
	fmt.Printf("kvports = %v, smports = %v\n", kvPorts, smPorts)
	//clean := func() { shardkvCleanup(kvServers); smCleanup(smServers) }
	return smPorts, gids, kvPorts
}

func TestBasic(t *testing.T) {
	numGroups := 3
	numReplicas := 3
	smPorts, gids, kvPorts := setup("basic", false, numGroups, numReplicas)
	//defer clean()

	fmt.Printf("\nTest: Basic Join/Leave...")

	smClerk := shardmaster.MakeClerk(smPorts, true)
	smClerk.Join(gids[0], kvPorts[0])
	kvClerk := shardkv.MakeClerk(smPorts, true)
	kvClerk.Put("a", "x")
	v := kvClerk.PutHash("a", "b")
	if v != "x" {
		t.Fatalf("Puthash got wrong value")
	}
	ov := NextValue("x", "b")
	if kvClerk.Get("a") != ov {
		t.Fatalf("Get got wrong value")
	}
	log.Printf("got here6")
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

	//rebootDone = 1
	fmt.Printf("\n\tPassed\n")
	time.Sleep(2 * time.Second)
}

func TestMove(t *testing.T) {
	numGroups := 3
	numReplicas := 3
	smPorts, gids, kvPorts := setup("move", false, numGroups, numReplicas)

	fmt.Printf("\nTest: Shards really move...")

	smClerk := shardmaster.MakeClerk(smPorts, false)
	smClerk.Join(gids[0], kvPorts[0])

	kvClerk := shardkv.MakeClerk(smPorts, false)

	// Start listening on reboot channel in case we're testing persistence
	//rebootDone := 0
	//go rebootListener(&rebootDone, false, smPorts, gids, kvPorts, kvServers, numGroups, numReplicas)

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

	// remove sockets from group 0.  How do we know shards have
	// been xferred...does join not succeed until they have?
	for i := 0; i < len(kvPorts[0]); i++ {
		//FIXME
		//deafen(kvPorts[0][i])
	}

	count := 0
	var mu sync.Mutex
	for i := 0; i < shardmaster.NShards; i++ {
		go func(me int) {
			myck := shardkv.MakeClerk(smPorts, false)
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

	time.Sleep(30 * time.Second)

	//rebootDone = 1
	time.Sleep(2 * time.Second)
	if count > shardmaster.NShards/3 && count < 2*(shardmaster.NShards/3) {
		fmt.Printf("\n\tPassed\n")
	} else {
		t.Fatalf("%v keys worked after killing 1/2 of groups; wanted %v",
			count, shardmaster.NShards/2)
	}
}


func TestLimp(t *testing.T) {
	numGroups := 3
	numReplicas := 3
	smPorts, gids, kvPorts := setup("limp", false, numGroups, numReplicas)

	fmt.Printf("\nTest: Reconfiguration with some dead replicas...")

	smClerk := shardmaster.MakeClerk(smPorts, false)
	smClerk.Join(gids[0], kvPorts[0])

	kvClerk := shardkv.MakeClerk(smPorts, false)

	// Start listening on reboot channel in case we're testing persistence
	//rebootDone := 0
	//go rebootListener(&rebootDone, false, smPorts, gids, kvPorts, kvServers, numGroups, numReplicas)

	kvClerk.Put("a", "b")
	if kvClerk.Get("a") != "b" {
		t.Fatalf("got wrong value")
	}

	for g := 0; g < len(kvPorts); g++ {
		//FIXME!
		//kvServers[g][rand.Int()%len(kvServers[g])].Kill()
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
		for i := 0; i < len(kvPorts[g]); i++ {
			//FIXME
			//kvServers[g][i].Kill()
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

	//rebootDone = 1
	time.Sleep(2 * time.Second)
	fmt.Printf("\n\tPassed\n")
}

func doConcurrent(t *testing.T, unreliable bool) {
	numGroups := 3
	numReplicas := 3
	smPorts, gids, kvPorts := setup("conc"+strconv.FormatBool(unreliable), unreliable, numGroups, numReplicas)

	// Start listening on reboot channel in case we're testing persistence
	//rebootDone := 0
	//go rebootListener(&rebootDone, unreliable, smPorts, gids, kvPorts, kvServers, numGroups, numReplicas)

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
			kvClerk := shardkv.MakeClerk(smPorts, false)
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

	//rebootDone = 1
	time.Sleep(2 * time.Second)
}

func TestFileConcurrent(t *testing.T) {
	fmt.Printf("\nTest: Concurrent Put/Get/Move...")
	doConcurrent(t, false)
	fmt.Printf("\n\tPassed\n")
}

func BenchmarkClientLatencyOneShard(benchmark *testing.B) {
	numGroups := 3
	numReplicas := 3
	smPorts, gids, kvPorts := setup("basic", false, numGroups, numReplicas)
	//defer clean()

	fmt.Printf("\nBenchmark: client latency, one shard...")

	smClerk := shardmaster.MakeClerk(smPorts, true)
	smClerk.Join(gids[0], kvPorts[0])
	kvClerk := shardkv.MakeClerk(smPorts, true)

	benchmark.ResetTimer()
	//tStart := time.Now()
	for i := 0; i < benchmark.N; i++ {
		kvClerk.PutHash("a", strconv.Itoa(rand.Int()))
	}
}

func BenchmarkClientLatencyManyShard(benchmark *testing.B) {
	numGroups := 3
	numReplicas := 3
	smPorts, gids, kvPorts := setup("basic", false, numGroups, numReplicas)
	//defer clean()

	fmt.Printf("\nBenchmark: client latency, many shards...")

	smClerk := shardmaster.MakeClerk(smPorts, true)
	for i := 0; i < len(gids); i++ {
		smClerk.Join(gids[i], kvPorts[i])
	}
	kvClerk := shardkv.MakeClerk(smPorts, true)

	benchmark.ResetTimer()
	//tStart := time.Now()
	for i := 0; i < benchmark.N; i++ {
		kvClerk.Put(strconv.Itoa(rand.Int()), strconv.Itoa(rand.Int()))
	}
}
