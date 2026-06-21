package commands


import (
	"github.com/waiyneee/Kvstore/connection"
	"github.com/waiyneee/Kvstore/persistence"
)


func AppenAofFile(args []string, fd int) error {
	if len(args) != 0 {
		connection.QueueWrite(fd, []byte("-ERR wrong number of arguments for 'bgrewriteaof' command\r\n"))
		return nil
	}
	return persistence.WriteAheadOfLog()
}