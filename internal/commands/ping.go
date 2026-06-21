package commands

import (
	"github.com/waiyneee/Kvstore/internal/connection"
    "github.com/waiyneee/Kvstore/internal/resp"
)

func ResponsePing(args []string, fd int) error {
	var buff []byte

	if len(args) >= 2 {
		connection.QueueWrite(fd, []byte("-ERR wrong number of arguments for 'ping' command\r\n"))
		return nil
	}

	if len(args) == 0 {
		buff = resp.Encode("PONG", true)
	} else {
		buff = resp.Encode(args[0], false)
	}

	connection.QueueWrite(fd, buff)
	return nil
}