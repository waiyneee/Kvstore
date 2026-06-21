
package commands 

import (

	"strconv"
	"time"

	"github.com/waiyneee/Kvstore/internal/connection"
	"github.com/waiyneee/Kvstore/internal/store"
	"github.com/waiyneee/Kvstore/internal/persistence"

)

func ResponseExpire(args []string, fd int) error {
	if len(args) <= 1 {
		connection.QueueWrite(fd, []byte("-ERR wrong number of arguments for 'expire' command\r\n"))
		return nil
	}

	var key string = args[0]
	expiryDuration, err := strconv.ParseInt(args[1], 10, 64)

	if err != nil {
		connection.QueueWrite(fd, []byte("-ERR value is not an integer or out of range\r\n"))
		return nil
	}

	expiryMs := expiryDuration * 1000

	obj := store.Get(key)
	if obj == nil {
		connection.QueueWrite(fd, []byte(":0\r\n"))
		return nil
	}

	obj.ExpiryAtTimestamps = time.Now().UnixMilli() + expiryMs

	if fd != -1 {
		fullCmd := append([]string{"EXPIRE"}, args...)
		persistence.AppendToAOF(fullCmd)

		connection.QueueWrite(fd, []byte(":1\r\n"))
	}
	return nil
}
