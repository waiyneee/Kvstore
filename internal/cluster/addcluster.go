package cluster

import (
	"hash/crc32"
	"log"
	"strconv"
	"strings"

	"github.com/waiyneee/Kvstore/internal/chashing"

	"github.com/waiyneee/Kvstore/internal/connection"
)

// Global variables
// to hold
var GlobalHashRing *chashing.HashRing
var CurrAdd string

func InitTopology(port string, clusterNodesList string) {

	CurrAdd = "127.0.0.1:" + port
	log.Printf("[CLUSTER] I am node: %s", CurrAdd)
	GlobalHashRing = chashing.New(chashing.WithVirtualNodes(50))

	nodes := strings.Split(clusterNodesList, ",")

	for _, nodeAddress := range nodes {
		cleanAddress := strings.TrimSpace(nodeAddress)
		if cleanAddress != "" {
			GlobalHashRing.AddNode(cleanAddress)
			log.Printf("[CLUSTER] Added node to HashRing: %s", cleanAddress)
		}
	}
}

// checkRedirect returns true if the command was redirected to another node.
// If it returns false, it means this node owns the data and should execute the command.
func CheckRedirect(key string, fd int) bool {
	if fd == -1 {
		return false
	}

	if GlobalHashRing == nil || CurrAdd == "" {
		return false
	}

	targetNode := GlobalHashRing.GetNode(key)
	if targetNode != CurrAdd {

		slot := int(crc32.ChecksumIEEE([]byte(key)) % 16384)

		// Format: -MOVED <slot> <ip:port>\r\n so that we can deiberately
		//move down to other nodes and push our data
		redirectMsg := "-MOVED " + strconv.Itoa(slot) + " " + targetNode + "\r\n"
		connection.QueueWrite(fd, []byte(redirectMsg))

		return true // Yes, we redirected them. Stop execution.
	}

	return false
}
