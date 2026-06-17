package raft

import (
	"fmt"
	"log"
	"net"

	"google.golang.org/grpc"
)
// type Server struct{


// }

const port =10000


func StartrpcServer() error {
	list,err:=net.Listen("tcp", fmt.Sprintf("localhost:%d", port))
	if err!=nil{
		log.Fatalf("filed to load and boot up the server",err)

	}
	var opts []grpc.ServerOption

	grpcServer :=grpc.NewServer(opts...)
	grpcServer.Serve(list)
	return nil

}



