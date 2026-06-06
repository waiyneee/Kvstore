package server

import (
	"errors"
	// "strings"

	"golang.org/x/sys/unix"

	"github.com/waiyneee/Kvstore/commands"
	"github.com/waiyneee/Kvstore/connection"
	"github.com/waiyneee/Kvstore/resp"
)

// Zero-allocation uppercase optimization
func toUpperASCII(s string) string {
	hasLower := false
	for i := 0; i < len(s); i++ {
		if s[i] >= 'a' && s[i] <= 'z' {
			hasLower = true
			break
		}
	}
	if !hasLower {
		return s
	}
	b := make([]byte, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'a' && c <= 'z' {
			c -= 32
		}
		b[i] = c
	}
	return string(b)
}

func ProcessClientData(fd int) error {
	// 4KB tcp standard global buffer
	for {
		n, err := unix.Read(fd, connection.GlobalReadBuffer)
		if err != nil {
			if err == unix.EAGAIN || err == unix.EWOULDBLOCK {
				break
			}
			return err
		}

		if n == 0 {
			return errors.New("client disconnected")
		}

		connection.ClientBuffers[fd] = append(connection.ClientBuffers[fd], connection.GlobalReadBuffer[:n]...)
	}

	for len(connection.ClientBuffers[fd]) > 0 {
		tokens, consumedBytes, err := resp.DecodeArrayString(connection.ClientBuffers[fd])

		if err != nil {
			errStr := err.Error()
			if errStr == "insufficient data" || errStr == "no decoded data" {
				break
			}
			return err
		}

		if len(tokens) == 0 {
			break
		}

		cmd := &commands.Command{
			Cmd:  toUpperASCII(tokens[0]),
			Args: tokens[1:],
		}

		// Hand off to the bll
		err = commands.ResponsewithCommand(cmd, fd)
		if err != nil {
			return err
		}

		connection.ClientBuffers[fd] = connection.ClientBuffers[fd][consumedBytes:]
	}

	return nil
}
