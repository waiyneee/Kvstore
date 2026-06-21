package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/waiyneee/Kvstore/internal/server"
)

var (
	port     int
	nodeId   int32
	raftPort int
	peerIPs  []string
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
		fmt.Printf("Starting server - Node ID: %d, Epoll Port: %d, Raft Port: %d\n", nodeId, port, raftPort)

		srv := server.New(
			server.WithPort(port),
			server.WithNodeId(nodeId),
			server.WithRaftPort(raftPort),
			server.WithPeerIPs(peerIPs),
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
	// Define the flags and default values
	startCmd.Flags().IntVarP(&port, "port", "p", 6379, "Port for the Epoll TCP server")
	startCmd.Flags().Int32VarP(&nodeId, "node-id", "n", 1, "Unique Raft Node ID")
	startCmd.Flags().IntVarP(&raftPort, "raft-port", "r", 10000, "Port for Raft gRPC communication")
	startCmd.Flags().StringSliceVar(&peerIPs, "peer-ips", []string{}, "Comma-separated list of peer IPs (e.g., 127.0.0.1:10001,127.0.0.1:10002)")

	rootCmd.AddCommand(startCmd)
}
