package main

import (
	"fmt"
	"log"
	"os"

	"github.com/waiyneee/Kvstore/server"
)

func main() {

	//go run main.go port(6379 preferably)
	arguments := os.Args
	if len(arguments) < 2 {
		//failure
		//use log instaed of printff later on
		fmt.Printf("we got an unexpected length of args here define your port number please ")
		//telling os to exit
		os.Exit(1)

	}
	port := os.Args[1]

	err := server.HandleConnections(port)
	if err != nil {
		log.Fatalf("Fatal: Server runtime crashed: %v\n", err)
	}

}
