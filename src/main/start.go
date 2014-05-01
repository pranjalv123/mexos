package main

import "fmt"
import "os"
import "paxos"
import "strconv"

func main() {
	args := os.Args
	if len(args) < 3 {
		fmt.Printf("Not enough arguments, must specify program type:\n"+
 			"   localFile|localNet|awsNet paxos\n"+
			"   localFile|localNet|awsNet shardmaster client|server\n"+
			"   localFile|localNet|awsNet shardkv client|server\n")
		os.Exit(1)
	}

	var network bool
	peers := make([]string, 11)

	switch args[1] {
	case "localFile":
		network = false;
	case "localNet":
		network = true;
		for i, _ := range peers {
			peers[i] = "127.0.0.1:200"+strconv.Itoa(i)
		}
	case "awsNet":
		network = true;
		for i, _ := range peers {
			peers[i] = "10.0.0.1"+strconv.Itoa(i)
		}
	default:
		fmt.Printf("Invalid communication system, must specify:\n"+
 			"   localFile|localNet|awsNet\n")
		os.Exit(1)
	} 
	
	switch args[2] {
	case "paxos":
		paxos.Make(peers, 0, nil, network) 
		fmt.Printf("Starting paxos server.\n")
	case "shardmaster":
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
