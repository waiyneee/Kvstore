package commands

import (
	"errors"
	"log"
	"strings"
	"golang.org/x/sys/unix"

	"github.com/waiyneee/Kvstore/resp"
)

// Global map 
var clientBuffers = make(map[int][]byte)

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

func ResponsewithCommand(cmd *Command, fd int) error {
	log.Println("Command::", cmd)
	switch cmd.Cmd {
	case "PING":
		return ResponsePing(cmd.Args, fd)
	default:
		return ResponsePing(cmd.Args, fd)
	}
}


func CleanUpClient(fd int) {
	delete(clientBuffers, fd)
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

	// 1. Append the newly arrived stream
	clientBuffers[fd] = append(clientBuffers[fd], buffer[:n]...)

	
	for len(clientBuffers[fd]) > 0 {
		tokens, err := resp.DecodeArrayString(clientBuffers[fd])
		if err != nil {
			// Check if the parser failed simply because the packet is cut off mid-stream
			errStr := err.Error()
			if strings.Contains(errStr, "insufficient data") || strings.Contains(errStr, "no decoded data") {
			
				break
			}
			return err // Actual malformed
		}

		if len(tokens) == 0 {
			break
		}

		cmd := &Command{
			Cmd:  strings.ToUpper(tokens[0]),
			Args: tokens[1:],
		}

		err = ResponsewithCommand(cmd, fd)
		if err != nil {
			return err
		}

	
		clientBuffers[fd] = nil
	}

	return nil
}