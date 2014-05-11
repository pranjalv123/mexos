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

const numGroups = 1
const numReplicas = 3
const numMasters = 3

// Use for checking PutHash                                                                 
func NextValue(hprev string, val string) string {
        h := hash(hprev + val)
        return strconv.Itoa(int(h))
}

// Set up and start servers for shardkv groups and shardmaster
func setup(tag string, unreliable bool, numGroups int, numReplicas int) ([]string, []int64, [][]string) {
	runtime.GOMAXPROCS(4)

	//number of masters per group
	kvPorts, smPorts, gids := GetShardkvs(numReplicas, numMasters, numGroups)
	fmt.Printf("kvports = %v, smports = %v\n", kvPorts, smPorts)
	//clean := func() { shardkvCleanup(kvServers); smCleanup(smServers) }
	return smPorts, gids, kvPorts
}

func TestBasic(t *testing.T) {
	smPorts, gids, kvPorts := setup("basic", false, numGroups, numReplicas)
	//defer clean()

	fmt.Printf("\nTest: Basic Join/Leave...\n")

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
	log.Printf("got here6\n")
	keys := make([]string, 10)
	vals := make([]string, len(keys))
	for i := 0; i < len(keys); i++ {
		keys[i] = strconv.Itoa(rand.Int())
		vals[i] = strconv.Itoa(rand.Int())
		kvClerk.Put(keys[i], vals[i])
	}
	log.Printf("got here7\n")
	// are keys still there after joins?
	for g := 1; g < len(gids); g++ {
		smClerk.Join(gids[g], kvPorts[g])
		fmt.Printf("gid 10%d joined\n",g)
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
	log.Printf("got here8\n")
	// are keys still there after leaves?
	for g := 0; g < len(gids)-1; g++ {
		fmt.Printf("gid 10%d wants to leave\n",g)
		smClerk.Leave(gids[g])
		fmt.Printf("gid 10%d left\n",g)
		time.Sleep(1 * time.Second)
		for i := 0; i < len(keys); i++ {
			fmt.Printf("getting %v\n",keys[i])
			v := kvClerk.Get(keys[i])
			fmt.Printf("got %v\n",keys[i])
			if v != vals[i] {
				t.Fatalf("leaving; wrong value; g=%v k=%v wanted=%v got=%v",
					g, keys[i], vals[i], v)
			}
			vals[i] = strconv.Itoa(rand.Int())
			fmt.Printf("putting %v\n",keys[i])
			kvClerk.Put(keys[i], vals[i])
			fmt.Printf("put success %v\n",keys[i])
		}
	}

	//rebootDone = 1
	fmt.Printf("\n\tPassed\n")
	time.Sleep(2 * time.Second)
}

func TestMove(t *testing.T) {
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
	smPorts, gids, kvPorts := setup("conc"+strconv.FormatBool(unreliable), 
		unreliable, numGroups, numReplicas)

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
					t.Fatalf("PutHash(%v) expected %v got %v\n",
						key, last, v)
				}
				last = NextValue(last, nv)
				v = kvClerk.Get(key)
				if v != last {
					ok = false
					t.Fatalf("Get(%v) expected %v got %v\n", 
						key, last, v)
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
	smPorts, gids, kvPorts := setup("basic", false, numGroups, numReplicas)
	//defer clean()

	fmt.Printf("\nBenchmark: client latency, one shard...\n")

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
	smPorts, gids, kvPorts := setup("basic", false, numGroups, numReplicas)
	//defer clean()

	fmt.Printf("\nBenchmark: client latency, many shards...\n")

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


func TestManyClientOneShard(t *testing.T) {
	nclients := 35
	nseconds := 10
	smPorts, gids, kvPorts := setup("basic", false, numGroups, numReplicas)
	//defer clean()

	fmt.Printf("\nBenchmark: many clients, one shard...\n")

	
	smClerk := shardmaster.MakeClerk(smPorts, true)
	for i := 0; i < len(gids); i++ {
		smClerk.Join(gids[i], kvPorts[i])
	}
	
	counts := make([]int, nclients)
	for i := 0; i < nclients; i++ {
		go func(i int) {
			ck := shardkv.MakeClerk(smPorts, true)
			tStart := time.Now()
			count := 0
			for time.Since(tStart).Seconds() < float64(nseconds) {
				ck.PutHash("a", strconv.Itoa(rand.Int()))
				count++
			}
			counts[i] = count
		}(i)
	}
	toWait := 12 * time.Second
	time.Sleep(toWait)
	
	tot := 0
	for _,c := range(counts) {
		tot += c
	}
	fmt.Printf("%f operations per second\n", float64(tot)/float64(nseconds))

	fmt.Printf("\nBenchmark: many clients, many shards...\n")
	for i := 0; i < nclients; i++ {
		go func(i int) {
			ck := shardkv.MakeClerk(smPorts, true)
			tStart := time.Now()
			count := 0
			for time.Since(tStart).Seconds() < float64(nseconds) {
				ck.PutHash(strconv.Itoa(rand.Int()), 
					strconv.Itoa(rand.Int()))
				count++
			}
			counts[i] = count
		}(i)
	}

	time.Sleep(toWait)
	
	tot = 0
	for _,c := range(counts) {
		tot += c
	}
	fmt.Printf("%f operations per second\n", float64(tot)/float64(nseconds))
}

func paddedRandIntString(size int) string {
	data := make([]byte, size)
	s := strconv.Itoa(rand.Int())
	for i := range data {
		if i < len(s) {
			data[i] = s[i]
		} else {
			data[i] = byte(rand.Int31() & 0xff)
		}
	}
	return string(data[:])
}

func TestDiskRecovery(t *testing.T) {
	nclients := 1
//	numBytes := 20971520 //20MB
	nItems := 99
	keySize := 32
	valSize := 1024 * 1024
	smPorts, gids, kvPorts := setup("basic", false, numGroups, numReplicas)
	//defer clean()
	
	fmt.Printf("\nDisk recovery benchmark...\n")
	smClerk := shardmaster.MakeClerk(smPorts, true)
	smClerk.Join(gids[0], kvPorts[0])

	counts := make([]int, nclients)
	fmt.Printf("\nBenchmark: many clients, many shards...\n")
	for i := 0; i < nclients; i++ {
		go func(c int) {
			ck := shardkv.MakeClerk(smPorts, true)
			for i := 0; i < nItems; i++ {
				ck.Put(paddedRandIntString(keySize), paddedRandIntString(valSize))
				counts[c]++
			}
		}(i)
	}
	fmt.Println("I am here")
	sum := 0
	toWait := 22*time.Millisecond
	for sum < nItems {
		time.Sleep(toWait)
		sum = 0
		for _,i := range counts {
			sum += i
		}
		if sum%1024 == 0 {
			fmt.Printf("%v/%v\n", sum, nItems)
		}
	}

	fmt.Println("\n", nItems * (keySize + valSize) * nclients/1000000, " MB of data written to database...\n")
}
