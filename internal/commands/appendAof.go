package commands

import (
	"github.com/waiyneee/Kvstore/internal/connection"
	"github.com/waiyneee/Kvstore/internal/persistence"
)

func AppenAofFile(args []string, fd int) error {
	if len(args) != 0 {
		connection.QueueWrite(fd, []byte("-ERR wrong number of arguments for 'bgrewriteaof' command\r\n"))
		return nil
	}
	return persistence.WriteAheadOfLog()
}
