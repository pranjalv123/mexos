package test

import "math/big"
import "crypto/rand"
import "hash/fnv"
import "fmt"
import "os"
import "strconv"

const smport = 3333
const kvport = 2222

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


func GetPaxos(npaxos int) []string {
	if npaxos > 12 {
		fmt.Printf("Only 10 servers available, you asked for %i.", npaxos)
		os.Exit(1)
	}
	var pxhosts []string = make([]string, npaxos)
	for i := 1; i <= npaxos; i++ {
		pxhosts[i-1] = "10.0.0."+strconv.Itoa(100+i)+":"+strconv.Itoa(kvport)
	}

	return pxhosts
}

func GetShardmasters(nmasters int, ngroups int) ([]string, []int64) {
	if nmasters*ngroups > 12 {
		fmt.Printf("Invalid configuration parameters!\n" +
			"nmasters*ngroups >12 (= %d)\n", nmasters*ngroups)
		os.Exit(1)
	}
	
	gids := make([]int64, ngroups)    // each group ID                                 
	var smhosts []string = make([]string, nmasters*ngroups)
	
	//make GIDS
	for gid := 0; gid < ngroups; gid++ {
		gids[gid] = int64(gid + 100)
	}
	//make shardmasters
	for i := 1; i <= ngroups*nmasters; i++ {
		smhosts[i-1] = "10.0.0." + strconv.Itoa(100+i) + ":" + strconv.Itoa(smport)
	}
	return smhosts, gids
}

func GetShardkvs(nreplicas int, nmasters int, ngroups int) ([][]string, []string,
	[]int64){
	
	if (nreplicas+nmasters)*ngroups > 12  {
		fmt.Printf("Invalid configuration parameters!\n"+
			"(nmasters+nreplicas)*ngroups >12 (= %d)\n", 
			(nmasters+nreplicas)*ngroups)
		os.Exit(1)
	}
	kvhosts := make([][]string, ngroups)   // ShardKV ports, [group][replica]

	smhosts, gids := GetShardmasters(nmasters, ngroups)

	offset := nmasters*ngroups+1
	for grp := 0; grp < ngroups; grp++ {
		kvhosts[grp] = make([]string, nreplicas)
		for j := 0; j < nreplicas; j++ {
			//fmt.Println(100+(grp*ngroups)+j+offset)
			kvhosts[grp][j] = "10.0.0." + 
				strconv.Itoa(100+(grp*ngroups)+j+offset) +
				":" + strconv.Itoa(kvport)
		}
	}
	
	return kvhosts, smhosts, gids
}

