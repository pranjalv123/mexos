package main

import "fmt"
import "os"
import "os/exec"
import "strings"
import "strconv"
import "paxos"
import "shardmaster"
import "shardkv"
import "flag"

const smport = 3333
const kvport = 2222
const network = true

func main() {
	var npaxos = flag.Int("npaxos", 3, "number of paxos instances")
	var ngroups = flag.Int("ngroups", 3, "number of shard groups")
	var nmasters = flag.Int("nmasters", 1, "number of shardmasters per shard group")
	var nreplicas = flag.Int("nreplicas", 3, "number of kvshard replicas per group")
	
	flag.Parse()
	args := flag.Args()
	if len(args) < 1 {
		fmt.Printf("Not enough arguments, must specify program type:\n"+
 			"   paxos|shardmaster|shardkv\n")
		os.Exit(1)
	}

	switch args[0] {
	case "paxos":
		fmt.Println("Attempting to start paxos server...")
		peers := getPaxos(*npaxos) 
		me := whoami(peers)
		if me == -1 {
			fmt.Println("Host didn't find own IP in peer list! Exiting!")
			os.Exit(1)
		}
		paxos.Make(peers, me, nil, network, "somedbtag") 
		fmt.Println("peers: ",peers)
		fmt.Println("me: ", me)
		fmt.Printf("Started paxos server.\n")

	case "shardmaster":
		fmt.Println("Attempting to start shardmaster server...")
		peers, _ := getShardmasters(*nmasters, *ngroups)
		me := whoami(peers)
		if me == -1 {
			fmt.Println("Host didn't find own IP in peer list! Exiting!")
			os.Exit(1)
		}
		shardmaster.StartServer(peers, me, network)
		fmt.Println("peers: ",peers)
		fmt.Println("me: ", me)
		fmt.Printf("Started shardmaster.\n")

	case "shardkv":
		fmt.Println("Attempting to start shardkv server...")
		//---------Default Shardmaster Servers------------
		//group 100: 10.0.0.101
		//group 101: 10.0.0.102
		//group 102: 10.0.0.103
		//-----------Default ShardKV Servers--------------
		//group 100: 10.0.0.104, 10.0.0.105, 10.0.0.106
		//group 101: 10.0.0.107, 10.0.0.108, 10.0.0.109
		//group 102: 10.0.0.110, 10.0.0.111, 10.0.0.112
		metapeers, masters, groups := getShardkvs(*nreplicas, *nmasters, *ngroups)
		me := whoami(masters)
		if me != -1 {
			fmt.Println("Starting shardmaster instead.")
			shardmaster.StartServer(masters, me, network)
			fmt.Printf("peers: %v\n", masters)
			fmt.Printf("me: %d\n", me)
			fmt.Println("Success!")
		}
		var gid int64
		peers := make([]string, *ngroups) 
		for i, v := range metapeers {
			peers = v
			me = whoami(v)
			gid = groups[i]
			if me != -1 {
				break
			}
		}
		if me == -1 {
			fmt.Println("Host didn't find own IP in peer list! Exiting!")
			os.Exit(1)
		}
		fmt.Println("masters:",masters)
		fmt.Printf("peers: %v\n", peers)
		fmt.Printf("me: %d, gid :%d\n", me, gid)
		shardkv.StartServer(gid, masters, peers, me, network)
		fmt.Printf("Successfully started shardkv.\n")

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
	out, err := exec.Command("/home/ubuntu/mexos/src/main/getip").Output()
	//out, err := exec.Command("./getip").Output()
	if err != nil {
		fmt.Printf("Could not execute ifconfig! %s\n", err)
		os.Exit(1)
	}; err = nil

	ip := strings.TrimSpace(string(out))
	//fmt.Printf("myip: %s\n", ip)

	for i, addr := range peers {
		
		if addr[0:len(addr)-5] == ip {
			return i
		} else {
		//	fmt.Println("I am not ",addr[0:len(addr)-5])
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
	for i := 1; i <= npaxos; i++ {
		pxhosts[i-1] = "10.0.0."+strconv.Itoa(100+i)+":"+strconv.Itoa(kvport)
	}

	return pxhosts
}

func getShardmasters(nmasters int, ngroups int) ([]string, []int64) {
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

func getShardkvs(nreplicas int, nmasters int, ngroups int) ([][]string, []string,
	[]int64){
	
	if (nreplicas+nmasters)*ngroups > 12  {
		fmt.Printf("Invalid configuration parameters!\n"+
			"(nmasters+nreplicas)*ngroups >12 (= %d)\n", 
			(nmasters+nreplicas)*ngroups)
		os.Exit(1)
	}
	kvhosts := make([][]string, ngroups)   // ShardKV ports, [group][replica]

	smhosts, gids := getShardmasters(nmasters, ngroups)

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

