package main

import "fmt"
import "os"
import "os/exec"
import "strings"
import "strconv"
//import "paxos"
//import "flag"

const smport = 3333
const kvport = 2222
const network = true

func main() {
	args := os.Args
	if len(args) < 1 {
		fmt.Printf("Not enough arguments, must specify program type:\n"+
 			"   paxos|shardmaster|shardkv\n")
		os.Exit(1)
	}

	switch args[1] {
	case "paxos":
		npaxos := 5
		peers := getPaxos(npaxos) 
		fmt.Println(peers)
		me := whoami(peers)
		fmt.Println("me = ", me)
		//paxos.Make(peers, , nil, true) 
		fmt.Printf("Starting paxos server.\n")
	case "shardmaster":
		//peers = getShardmaster(, 3)
		fmt.Printf("Starting shardmaster.\n")
	case "shardkv":
		fmt.Printf("Starting shardkv.\n")
	default: 
		fmt.Printf("Invalid program type, choose one:" +
 			"   paxos|shardmaster|shardkv")
		os.Exit(1)
	}

	done := false
	for (!done) {}
}

func whoami(peers []string) int {
	//ifconfig eth0 | grep 'inet addr:' | cut -d: -f2 | awk '{ print $1}'
	out, err := exec.Command("./getip").Output()
	if err != nil {
		fmt.Printf("Could not execute ifconfig! %s\n", err)
		os.Exit(1)
	}; err = nil

	ip := strings.TrimSpace(string(out))
	//fmt.Printf("myip: %s\n", ip)

	for i, addr := range peers {
		if addr[0:len(addr)-6] == ip {
			return i
		}
	}
	return -1
}

func getPaxos(npaxos int) []string {
	if npaxos > 12 {
		fmt.Printf("Only 10 servers available, you asked for %i.", npaxos)
		os.Exit(1)
	}
	var pxhosts []string = make([]string, npaxos)
	for i := 0; i < npaxos; i++ {
		pxhosts[i] = "10.0.0.10"+strconv.Itoa(i)+":"+strconv.Itoa(kvport)
	}

	return pxhosts
}

func getShardmasters(smPerGroup int, ngroups int) ([]string, []int64) {
	//---------Default Shardmaster Servers------------
	//group 100: 10.0.0.101
	//group 101: 10.0.0.105
	//group 102: 10.0.0.109
	if smPerGroup*ngroups > 12 {
		fmt.Printf("Invalid configuration parameters!")
		os.Exit(1)
	}
	
	gids := make([]int64, ngroups)    // each group ID                                 
	var smhosts []string = make([]string, smPerGroup*ngroups)

	for grp := 0; grp < ngroups; grp++ {
		//make GIDS
		gids[grp] = int64(grp + 100)
		
		//make shardmasters
		for i := 1; i <= smPerGroup; i++ {
			smhosts[i] = "10.0.0." + strconv.Itoa(100+(ngroups*i)+i) + ":" +
				strconv.Itoa(smport)
		}
	}

	return smhosts, gids
}

func getShardkvs(replicasPerGroup int, smPerGroup int, ngroups int) ([][]string, []string,
	[]int64){
	
	if replicasPerGroup*ngroups > 12 || ngroups > replicasPerGroup {
		fmt.Printf("Invalid configuration parameters!")
		os.Exit(1)
	}
	//-----------Default ShardKV Servers--------------
	//group 100: 10.0.0.102, 10.0.0.103, 10.0.0.104
	//group 101: 10.0.0.106, 10.0.0.107, 10.0.0.108
	//group 102: 10.0.0.110, 10.0.0.111, 10.0.0.112
	kvhosts := make([][]string, ngroups)   // ShardKV ports, [group][replica]

	smhosts, gids := getShardmasters(smPerGroup, ngroups)
	
	for grp := 0; grp < ngroups; grp++ {
		kvhosts[grp] = make([]string, replicasPerGroup)
		for j := smPerGroup+1; j <= smPerGroup+replicasPerGroup; j++ {
			kvhosts[grp][j] = "10.0.0." + strconv.Itoa(100+(grp*ngroups)+j) +
				":" + strconv.Itoa(kvport)
		}
	}
	
	return kvhosts, smhosts, gids
}

/*func printList(l []interface) {
	fmt.Println("[")
	comma := ""
	for _, i := range l {
		fmt.Printf("%s%v", comma, i)
		comma = ","
	}
	fmt.Println("]")
	
}*/
