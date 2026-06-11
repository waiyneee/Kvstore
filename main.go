package main

import (
	"flag"
	"log"

	"github.com/waiyneee/Kvstore/cluster"
	"github.com/waiyneee/Kvstore/server"
)

func main() {
	portFlag := flag.String("port", "9000", "Port to run the database node on")
	clusterFlag := flag.String("cluster", "127.0.0.1:9000", "Comma-separated list of all nodes in the cluster")
	flag.Parse()

	// Bootstrapping: Build the mental map of the cluster.
	// We pass the string values of the flags into our new Init function.
	cluster.InitTopology(*portFlag, *clusterFlag)
	err := server.HandleConnections(*portFlag)
	if err != nil {
		log.Fatalf("Fatal: Server runtime crashed: %v\n", err)
	}
}
