package commands

import (
	"log"
	"net"
	"errors"

	"github.com/waiyneee/Kvstore/resp"
	
)


func ResponsePing(args []string,conn net.Conn) error{

	var buff []byte

	if len(args) >= 2 {
		return errors.New("ERR wrong number of arguments for 'ping' command")
	}

	if len(args) == 0 {
		buff = resp.Encode("PONG", true)
	} else {
		buff = resp.Encode(args[0], false)
	}

	_, err := conn.Write(buff)
	return err

}
func ResponsewithCommand(cmd *Command,conn net.Conn) error{

	log.Println("Command::",cmd)
	switch cmd.Cmd{
	case "PING":
		return ResponsePing(cmd.Args,conn)
	default:
		return ResponsePing(cmd.Args,conn)
	}

	

}