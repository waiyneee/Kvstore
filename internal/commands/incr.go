package commands

import (
	"strconv"

	"github.com/waiyneee/Kvstore/internal/cluster"
	"github.com/waiyneee/Kvstore/internal/connection"
	"github.com/waiyneee/Kvstore/internal/eviction"
	"github.com/waiyneee/Kvstore/internal/persistence"
	"github.com/waiyneee/Kvstore/internal/resp"
	"github.com/waiyneee/Kvstore/internal/store"
)

func ResponseIncr(args []string, fd int) error {
	if len(args) != 1 {
		connection.QueueWrite(fd, []byte("-ERR wrong number of arguments for 'incr' command\r\n"))
		return nil
	}

	var key string = args[0]
	obj := store.Get(key)

	var newCounter int64 = 1

	if obj == nil {
		if len(store.Store) >= eviction.LIMIT_KEYS {
			eviction.DoEviction()
		}
		store.Put(key, store.NewObj("1", -1))
	} else {
		valStr, ok := obj.Value.(string)
		if !ok {
			connection.QueueWrite(fd, []byte("-ERR value is not an integer or out of range\r\n"))
			return nil
		}
		parsedVal, err := strconv.ParseInt(valStr, 10, 64)
		if err != nil {
			connection.QueueWrite(fd, []byte("-ERR value is not an integer or out of range\r\n"))
			return nil
		}

		newCounter = parsedVal + 1
		obj.Value = strconv.FormatInt(newCounter, 10)
	}

	if fd != -1 {
		fullCmd := append([]string{"INCR"}, args...)
		persistence.AppendToAOF(fullCmd)

		cluster.BroadcastToReplicas(fullCmd)

		buff := resp.Encode(newCounter, false)
		connection.QueueWrite(fd, buff)
	}

	return nil
}
