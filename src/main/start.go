package main

import "fmt"
import "paxos"

func main() {
	peers := []string{"10.0.0.101","10.0.0.102","10.0.0.103","10.0.0.104","10.0.0.105",
		"10.0.0.106", "10.0.0.107", "10.0.0.108", "10.0.0.109", "10.0.0.110"}
	paxos.Make(peers, 0, nil) 
	fmt.Printf("Starting paxos server.\n")
	done := false
	for (!done) {}
}
