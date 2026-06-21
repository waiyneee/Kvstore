package commands



import (
	"github.com/waiyneee/Kvstore/connection"
	"github.com/waiyneee/Kvstore/resp"
	"github.com/waiyneee/Kvstore/store"
	"time"
)

func ResponseGetk(args []string, fd int) error {
	if len(args) != 1 {
		connection.QueueWrite(fd, []byte("-ERR wrong number of arguments for 'get' command\r\n"))
		return nil
	}

	var key string = args[0]
	obj := store.Get(key)

	if obj == nil {
		var nilbuff []byte = resp.RespNil()
		connection.QueueWrite(fd, nilbuff)
		return nil
	}

	if obj.ExpiryAtTimestamps != -1 && obj.ExpiryAtTimestamps <= time.Now().UnixMilli() {
		delete(store.Store, key)
		var nilbuff []byte = resp.RespNil()
		connection.QueueWrite(fd, nilbuff)
		return nil
	}

	buff := resp.Encode(obj.Value, false)
	connection.QueueWrite(fd, buff)

	return nil
}