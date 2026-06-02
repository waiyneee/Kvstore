package cluster


import (
	"github.com/waiyneee/Kvstore/connection"
)

func ResponsewithReplica(args []string,fd int) error{
     if len(args)!=2{
		connection.QueueWrite(fd,[]byte("-ERR wrong number of arguments for 'ping' command\r\n"))
	 }
	 


	 return nil
}