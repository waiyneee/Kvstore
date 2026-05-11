package main

import (
	"fmt"
	"net"
	"os"
)

func main(){


	fmt.Println("Started entry poitn for Kvstores initialization ")



	//go run main.go port(6379 preferably)
	if len(os.Args)<2{
		//failure 
		//use log instaed of printff later on 
		fmt.Printf("we got an unexpected length of args here ")
		//telling os to exit 
		os.Exit(1)


	}

	//LETS STARTS
	port:=fmt.Sprintf(":%s",os.Args[1])

	lstnr,err:=net.Listen("tcp",port)
	if err!=nil{
		fmt.Println("got listening error",err)
		os.Exit(1)


	}

	defer lstnr.Close()
	// fmt.Printf("listenign to :%s\n",lstnr.Addr())

	// for{
	// 	conn,err:=lstnr.Accept()

	// }



}