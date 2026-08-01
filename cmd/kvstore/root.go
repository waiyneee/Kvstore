package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
	"github.com/waiyneee/Kvstore/internal/server"
)

var (
	port       int
	nodeId     int32
	raftPort   int
	peerIPs    []string
	maxClients int
)

var rootCmd = &cobra.Command{
	Use:   "kvstore",
	Short: "A distributed Key-Value store",
	Long:  `Kvstore is a custom Epoll-based, Raft-backed distributed key-value database.`,
}

var startCmd = &cobra.Command{
	Use:   "start",
	Short: "Start the KV store server",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("Starting server - Node ID: %d, Epoll Port: %d, Raft Port: %d, Max Clients: %d\n", nodeId, port, raftPort, maxClients)

		srv := server.New(
			server.WithPort(port),
			server.WithNodeId(nodeId),
			server.WithRaftPort(raftPort),
			server.WithPeerIPs(peerIPs),
			server.WithMaxClients(maxClients),
		)

		if err := srv.Start(); err != nil {
			fmt.Printf("Server crashed: %v\n", err)
			os.Exit(1)
		}
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func init() {
	// Define the flags with environment variable fallbacks for Docker / Production compatibility
	startCmd.Flags().IntVarP(&port, "port", "p", getEnvAsInt("PORT", 6379), "Port for the Epoll TCP server")
	startCmd.Flags().Int32VarP(&nodeId, "node-id", "n", int32(getEnvAsInt("NODE_ID", 1)), "Unique Raft Node ID")
	startCmd.Flags().IntVarP(&raftPort, "raft-port", "r", getEnvAsInt("RAFT_PORT", 10000), "Port for Raft gRPC communication")
	startCmd.Flags().StringSliceVar(&peerIPs, "peer-ips", getEnvAsSlice("PEERS", []string{}), "Comma-separated list of peer IPs (e.g., node2:10001,node3:10002)")
	startCmd.Flags().IntVarP(&maxClients, "max-clients", "m", getEnvAsInt("MAX_CLIENTS", 10000), "Maximum concurrent client connections")

	rootCmd.AddCommand(startCmd)
}

// Helper functions for environment variable lookups
func getEnvAsInt(name string, defaultVal int) int {
	if valStr := os.Getenv(name); valStr != "" {
		if v, err := strconv.Atoi(valStr); err == nil {
			return v
		}
	}
	return defaultVal
}

func getEnvAsSlice(name string, defaultVal []string) []string {
	if valStr := os.Getenv(name); valStr != "" {
		return strings.Split(valStr, ",")
	}
	return defaultVal
}
