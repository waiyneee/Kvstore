package commands

import (
	"github.com/waiyneee/Kvstore/connection"
	"github.com/waiyneee/Kvstore/resp"
	"github.com/waiyneee/Kvstore/store"
	"github.com/waiyneee/Kvstore/persistence"
	"github.com/waiyneee/Kvstore/cluster"
// 	"time"
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
		if cnt > 0 {
			fullCmd := append([]string{"DEL"}, args...)
			persistence.AppendToAOF(fullCmd)

			if fd != cluster.LeaderConnectionFD {
				cluster.BroadcastToReplicas(fullCmd)
			}
		}

		if fd != cluster.LeaderConnectionFD {
			buff := resp.Encode(int64(cnt), false)
			connection.QueueWrite(fd, buff)
		}
	}

	return nil
}