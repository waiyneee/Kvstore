package commands

import (
	"github.com/waiyneee/Kvstore/internal/connection"
	"github.com/waiyneee/Kvstore/internal/resp"
	"github.com/waiyneee/Kvstore/internal/store"
	"time"
)

func ResponseTTl(args []string, fd int) error {
	if len(args) != 1 {
		connection.QueueWrite(fd, []byte("-ERR wrong number of arguments for 'ttl' command\r\n"))
		return nil
	}

	var key string = args[0]
	obj := store.Get(key)

	if obj == nil {
		connection.QueueWrite(fd, []byte(":-2\r\n"))
		return nil
	}

	if obj.ExpiryAtTimestamps == -1 {
		connection.QueueWrite(fd, []byte(":-1\r\n"))
		return nil
	}

	durationMs := obj.ExpiryAtTimestamps - time.Now().UnixMilli()

	if durationMs < 0 {
		delete(store.Store, key)
		connection.QueueWrite(fd, []byte(":-2\r\n"))
		return nil
	}

	buff := resp.Encode(int64(durationMs/1000), false)
	connection.QueueWrite(fd, buff)

	return nil
}
