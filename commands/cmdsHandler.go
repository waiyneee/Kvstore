package commands

import (
	"errors"
	"golang.org/x/sys/unix"
	"log"
	"strings"

	"github.com/waiyneee/Kvstore/resp"
)

func ResponsePing(args []string, fd int) error {
	var buff []byte

	if len(args) >= 2 {
		return errors.New("ERR wrong number of arguments for 'ping' command")
	}

	if len(args) == 0 {
		buff = resp.Encode("PONG", true)
	} else {
		buff = resp.Encode(args[0], false)
	}

	_, err := unix.Write(fd, buff)
	return err
}

// func ResponseSetCommand(args []string) error{
// 	//map usage
// 	if len(args)<2 || len(args)==3{
// 		return errors.New("ERR wrong number of arguments for 'set' command")
// 	}
// 	keyValuestore:=make(map[string]string)

//     key:=args[0]
// 	value:=args[1]

// 	var expiry_coinfig string=""
// 	var value_expiry string=""

// 	if len(args)==4{
// 		expiry_coinfig=args[3]
// 		value_expiry=args[4]

// 	}

// 	keyValuestore[key]=value
//     keyValuestore[expiry_coinfig]=value_expiry

// 	return nil

// }

func ResponsewithCommand(cmd *Command, fd int) error {
	log.Println("Command::", cmd)
	switch cmd.Cmd {
	case "PING":
		return ResponsePing(cmd.Args, fd)

	// case "SET":
	// 	return ResponseSetCommand(cmd.Args,fd)
	default:
		return ResponsePing(cmd.Args, fd)
	}
}

func ProcessClientData(fd int) error {
	buffer := make([]byte, 512)

	n, err := unix.Read(fd, buffer)
	if err != nil {
		if err == unix.EAGAIN || err == unix.EWOULDBLOCK {
			return nil
		}
		return err
	}

	if n == 0 {
		return errors.New("client disconnected")
	}

	tokens, err := resp.DecodeArrayString(buffer[:n])
	if err != nil {
		return err
	}

	if len(tokens) == 0 {
		return nil
	}

	cmd := &Command{
		Cmd:  strings.ToUpper(tokens[0]),
		Args: tokens[1:],
	}

	err = ResponsewithCommand(cmd, fd)
	return err
}
