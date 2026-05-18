package server

import (
	// "bufio"
	"fmt"
	"io"
	"net"
	"strings"

	"github.com/waiyneee/Kvstore/commands"
	"github.com/waiyneee/Kvstore/resp"

)

// readCommand reads one full line (command) from the connection
func readCommand(conn net.Conn) (*commands.Command, error) {
	var buffer []byte = make([]byte, 512)
	n, err := conn.Read(buffer[:])

	if err != nil {
		return nil, err
	}
	// return strings.TrimSpace(string(bytes)), nil

	tokens,err:= resp.DecodeArrayString(buffer[:n])
	if err!=nil{
		return nil,err
	}


	return &commands.Command{
		Cmd: strings.ToUpper(tokens[0]),
		Args: tokens[1:],
	},nil
}

// respond writes a response back to the connection
func respond(cmd *commands.Command, conn net.Conn) error {
	err := commands.ResponsewithCommand(cmd, conn)
	if err != nil {
		_, writeErr := conn.Write([]byte(fmt.Sprintf("-%s\r\n", err)))
		if writeErr != nil {
			return writeErr
		}
	}
	return err
}

func HandleConnections(conn net.Conn) {
	defer conn.Close()

	// reader := bufio.NewReader(conn)

	for {
		cmd, err := readCommand(conn)
		if err != nil {
			if err != io.EOF {
				fmt.Println("failed to read data:", err)
			}
			return
		}

		if err := respond(cmd, conn); err != nil {
			fmt.Println("failed while writing:", err)
			return
		}
	}
}
