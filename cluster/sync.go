package cluster

import (
	"strconv"
	"time"

	"github.com/waiyneee/Kvstore/connection"
	"github.com/waiyneee/Kvstore/resp"
	"github.com/waiyneee/Kvstore/store"
)

func ResponseSync(args []string, fd int) error {
	ReplicaFDs = append(ReplicaFDs, fd)
	//added into the list

	//iteate over the hashmap and bget the keys
	//copied dumped to followers
	now := time.Now().UnixMilli()
	for k, obj := range store.Store {

		val, ok := obj.Value.(string)
		if !ok {
			continue
		}

		syncCmd := []string{"SET", k, val}
		if obj.ExpiryAtTimestamps != -1 {
			remSec := (obj.ExpiryAtTimestamps - now) / 1000
			if remSec > 0 {
				syncCmd = append(syncCmd, "EX", strconv.FormatInt(remSec, 10))
			} else {
				continue
				//that is key is dead /expired
			}

		}

		encoded := resp.EncodeArray(syncCmd)
		connection.QueueWrite(fd, encoded)

	}

	return nil
}
