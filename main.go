package main

import (
	"fmt"
	"github.com/waiyneee/Kvstore/server"
	"net"
	"os"
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

	port := fmt.Sprintf(":%s", os.Args[1])

	lstnr, err := net.Listen("tcp", port)
	if err != nil {
		fmt.Println("got listening error", err)
		os.Exit(1)

	}

	defer lstnr.Close()
	fmt.Println("listenign to the server on a specific port ", lstnr.Addr())

	for {
		//blocking call waiting to be accepted
		conn, err := lstnr.Accept()
		if err != nil {
			fmt.Println("failed to get connection ,err:", err)

			continue

		}

		go server.HandleConnections(conn)

		//go automatically gives us concurrency
		//through goroutine no need of system calls
		//we can make this gracefull handling

	}

}
