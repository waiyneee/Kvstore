package cluster

import (
	"hash/crc32"
	"log"
	"strconv"
	"strings"

	"github.com/waiyneee/Kvstore/chashing"

	"github.com/waiyneee/Kvstore/connection"
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
	// 1. BYPASS: If this command is coming down the WAL pipe from our Leader,
	// we MUST execute it locally to keep the replica in sync. No redirects!
	if ServerRole == "FOLLOWER" && fd == LeaderConnectionFD {
		return false
	}

	// 2. Safety check: Ensure the ring actually exists
	if GlobalHashRing == nil || CurrAdd == "" {
		return false
	}

	// 3. Ask the math engine who owns this key
	targetNode := GlobalHashRing.GetNode(key)

	// 4. If the target node is NOT us, tell the client to GO AWAY!
	if targetNode != CurrAdd {
		// Calculate a dummy slot (0-16383) to perfectly mimic standard Redis Cluster protocol
		slot := int(crc32.ChecksumIEEE([]byte(key)) % 16384)

		// Format: -MOVED <slot> <ip:port>\r\n
		redirectMsg := "-MOVED " + strconv.Itoa(slot) + " " + targetNode + "\r\n"
		connection.QueueWrite(fd, []byte(redirectMsg))

		return true // Yes, we redirected them. Stop execution.
	}

	return false
}
