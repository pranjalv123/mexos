package test

import "testing"
import "shardmaster"
import "shardkv"
import "runtime"
import "strconv"
//import "os"
import "time"
import "fmt"
//import "sync"
import "math/rand"

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
	kvPorts, smPorts, gids := GetShardkvs(numMasters, numReplicas, numGroups)

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

