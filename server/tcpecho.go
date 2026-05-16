package server

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strings"
)

// readCommand reads one full line (command) from the connection
func readCommand(r *bufio.Reader) (string, error) {
	bytes, err := r.ReadBytes('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(bytes)), nil
}

// respond writes a response back to the connection
func respond(conn net.Conn, msg string) error {
	_, err := conn.Write([]byte(msg))
	return err
}

func HandleConnections(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)

	for {
		cmd, err := readCommand(reader)
		if err != nil {
			if err != io.EOF {
				fmt.Println("failed to read data:", err)
			}
			return
		}

		fmt.Println("command", cmd)

		// same response style as your original second file
		line := fmt.Sprintf("request: %s", cmd)
		response := fmt.Sprintf("response: %s\n", line)

		if err := respond(conn, response); err != nil {
			fmt.Println("failed while writing:", err)
			return
		}
	}
}