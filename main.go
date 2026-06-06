package main

import (
	"log"
	// "net/http"
	// _ "net/http/pprof"
	"os"

	"github.com/waiyneee/Kvstore/server"
)

func main() {

	// go func() {
	// 	log.Println("Starting pprof diagnostic server on localhost:6060")
	// 	if err := http.ListenAndServe("localhost:6060", nil); err != nil {
	// 		log.Fatalf("pprof failed: %v", err)
	// 	}
	// }()

	// go run main.go port(6379 preferably)
	arguments := os.Args
	if len(arguments) < 2 {
		// Used log.Fatal instead of fmt.Printf + os.Exit
		log.Fatal("Fatal: we got an unexpected length of args here, define your port number please.")
	}
	port := os.Args[1]

	err := server.HandleConnections(port)
	if err != nil {
		log.Fatalf("Fatal: Server runtime crashed: %v\n", err)
	}
}
