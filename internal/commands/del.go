package commands

import (
	"github.com/waiyneee/Kvstore/internal/connection"
	"github.com/waiyneee/Kvstore/internal/resp"
	"github.com/waiyneee/Kvstore/internal/store"
)

func ResponseDel(args []string, fd int) error {
	if len(args) < 1 {
		connection.QueueWrite(fd, []byte("-ERR wrong number of arguments for 'del' command\r\n"))
		return nil
	}
	var cnt int64 = 0

	for _, key := range args {
		if ok := store.Del(key); ok {
			cnt++
		}
	}

	if fd != -1 {
		buff := resp.Encode(int64(cnt), false)
		connection.QueueWrite(fd, buff)
	}

	return nil
}
