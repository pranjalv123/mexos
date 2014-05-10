package main

import "test"
import "fmt"
import "os"
import "os/exec"
import "strings"
//import "strconv"
import "paxos"
import "shardmaster"
import "shardkv"
import "flag"

const network = true

func main() {
	var npaxos = flag.Int("npaxos", 3, "number of paxos instances")
	var ngroups = flag.Int("ngroups", 3, "number of shard groups")
	var nmasters = flag.Int("nmasters", 3, "number of shardmasters per shard group")
	var nreplicas = flag.Int("nreplicas", 3, "number of kvshard replicas per group")
	var clean =  flag.Bool("clean", false, "clean the db")
	
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
		if *clean {
			cleanDB("paxos")
		}
		peers := test.GetPaxos(*npaxos) 
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
		if *clean {
			cleanDB("shardmaster")
		}
		peers, _ := test.GetShardmasters(*nmasters, *ngroups)
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
		fmt.Println("------------------------------------------")
		fmt.Println(getIP(), "attempting to start shardkv server...")
		//---------Default Shardmaster Servers------------
		//group 100: 10.0.0.101
		//group 101: 10.0.0.102
		//group 102: 10.0.0.103
		//-----------Default ShardKV Servers--------------
		//group 100: 10.0.0.104, 10.0.0.105, 10.0.0.106
		//group 101: 10.0.0.107, 10.0.0.108, 10.0.0.109
		//group 102: 10.0.0.110, 10.0.0.111, 10.0.0.112
		metapeers, masters, _ := test.GetShardkvs(*nreplicas, *nmasters, *ngroups)
		
		me := whoami(masters)
		if me != -1 {
			fmt.Println("Starting shardmaster instead.")
			if *clean {
				cleanDB("shardmaster")
			}
			shardmaster.StartServer(masters, me, network)
			fmt.Printf("peers: %v\n", masters)
			fmt.Printf("me: %d\n", me)
			fmt.Println("Success!")
			select {}
		}
		var gid int64
		peers := make([]string, *ngroups) 
		for i, v := range metapeers {
			peers = v
			me = whoami(v)
			gid = int64(100+i)
			if me != -1 {
				break
			}
		}
		if me == -1 {
			fmt.Printf("Exiting! Host didn't find own IP %s in peer list: %v! "+
				"or masters: %v\n",
				getIP(), metapeers, masters)
			os.Exit(1)
		}
		if *clean {
			cleanDB("shardkv")
		}
		fmt.Println("masters:",masters)
		fmt.Printf("peers: %v\n", peers)
		fmt.Printf("me: %d, gid :%d\n", me, gid)
		shardkv.StartServer(gid, masters, peers, me, network)
		
	default: 
		fmt.Printf("Invalid program type, choose one:" +
 			"   paxos|shardmaster|shardkv")
		os.Exit(1)
	}
	select {}
}

func whoami(peers []string) int {
	//fmt.Printf("myip: %s\n", ip)
	ip := getIP()
	for i, addr := range peers {
		
		if addr[0:len(addr)-5] == ip {
			return i
		} else {
		//	fmt.Println("I am not ",addr[0:len(addr)-5])
		}
	}
	return -1
}

func getIP() string {
	//ifconfig eth0 | grep 'inet addr:' | cut -d: -f2 | awk '{ print $1}'
	out, err := exec.Command("/home/ubuntu/mexos/src/main/getip").Output()
	//out, err := exec.Command("./getip").Output()
	if err != nil {
		fmt.Printf("Could not execute ifconfig! %s\n", err)
		os.Exit(1)
	}; err = nil

	ip := strings.TrimSpace(string(out))
	return ip
}


func cleanDB(server string)  {
	//ifconfig eth0 | grep 'inet addr:' | cut -d: -f2 | awk '{ print $1}'
	fmt.Printf("cleaning db....\n")
	cmd := ""
	switch server {
	case "paxos": cmd = "/home/ubuntu/mexos/src/paxos/cleanPersist"
	case "shardmaster": cmd = "/home/ubuntu/mexos/src/shardmaster/cleanPersist"
	case "shardkv": cmd = "/home/ubuntu/mexos/src/shardkv/cleanPersist"
	default:
		fmt.Println("invalid switch in cleanDB()")
		os.Exit(1)
	}
	_, err := exec.Command(cmd).Output()
	//out, err := exec.Command("./getip").Output()
	if err != nil {
		fmt.Printf("Could not clean db: %s\n", err)
		os.Exit(1)
	}
}
