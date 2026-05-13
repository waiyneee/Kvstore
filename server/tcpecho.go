package server

import (
	"fmt"
	"net"

	"bufio"
	"io"
	// "log"
	// "error"
	"strings"
)

func HandleConnections(conn net.Conn) {

	defer conn.Close()

	reader := bufio.NewReader(conn)

	for {

		bytes, err := reader.ReadBytes('\n')

		if err != nil {

			if err != io.EOF {
				fmt.Println("failed to read data:", err)
			}

			return
		}

		line := fmt.Sprintf("request: %s", bytes)

		message := strings.TrimSpace(string(bytes))

		fmt.Println("received:", message)

		response := fmt.Sprintf("response: %s", line)

		_, err = conn.Write([]byte(response))

		if err != nil {
			fmt.Println("failed while writing:", err)
			return
		}
	}
}
