package cluster

import (
	"log"
	"strings"

	"github.com/waiyneee/Kvstore/chashing"
)

// Global variables
// to hold
var GlobalHashRing *chashing.HashRing
var CurrAdd string

func InitTopology(port string, clusterNodesList string) {

	CurrAdd = "127.0.0.1:" + port
	log.Printf("[CLUSTER] I am node: %s", CurrAdd)
	GlobalHashRing = chashing.New(50)

	nodes := strings.Split(clusterNodesList, ",")

	for _, nodeAddress := range nodes {
		cleanAddress := strings.TrimSpace(nodeAddress)
		if cleanAddress != "" {
			GlobalHashRing.AddNode(cleanAddress)
			log.Printf("[CLUSTER] Added node to HashRing: %s", cleanAddress)
		}
	}
}
