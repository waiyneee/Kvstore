package commands

import (
	"github.com/waiyneee/Kvstore/internal/cluster"
	"github.com/waiyneee/Kvstore/internal/connection"
	"github.com/waiyneee/Kvstore/internal/eviction"
	"github.com/waiyneee/Kvstore/internal/resp"
	"github.com/waiyneee/Kvstore/internal/store"

	"strconv"
)

func ResponseSetKv(args []string, fd int) error {
	if len(args) <= 1 {
		connection.QueueWrite(fd, []byte("-ERR wrong number of arguments for 'set' command\r\n"))
		return nil
	}
	var key string = args[0]
	var value string = args[1]
	var durationMs int64 = -1

	for i := 2; i < len(args); i++ {
		switch args[i] {
		case "EX", "ex":
			i++
			if i == len(args) {
				connection.QueueWrite(fd, []byte("-ERR syntax error\r\n"))
				return nil
			}

			expiryDuration, err := strconv.ParseInt(args[i], 10, 64)
			if err != nil {
				connection.QueueWrite(fd, []byte("-ERR value is not an integer or out of range\r\n"))
				return nil
			}
			durationMs = expiryDuration * 1000
		default:
			connection.QueueWrite(fd, []byte("-ERR syntax error\r\n"))
			return nil
		}
	}

	if len(store.Store) >= eviction.LIMIT_KEYS {
		eviction.DoEviction()
	}
	store.Put(key, store.NewObj(value, durationMs))
	if fd != -1 {
		fullCmd := append([]string{"SET"}, args...)
		if fd != cluster.LeaderConnectionFD {
			cluster.BroadcastToReplicas(fullCmd)
			buff := resp.Encode("OK", true)
			connection.QueueWrite(fd, buff)
		}
	}

	return nil
}
