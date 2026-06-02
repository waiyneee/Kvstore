package commands

import (
	
	"strconv"
	"strings"
	"time"

	"github.com/waiyneee/Kvstore/eviction"
	"github.com/waiyneee/Kvstore/persistence"
	"github.com/waiyneee/Kvstore/resp"
	"github.com/waiyneee/Kvstore/store"
	"github.com/waiyneee/Kvstore/cluster"
	"github.com/waiyneee/Kvstore/connection"
)


func ResponsePing(args []string, fd int) error {
	var buff []byte

	if len(args) >= 2 {
		connection.QueueWrite(fd, []byte("-ERR wrong number of arguments for 'ping' command\r\n"))
		return nil
	}

	if len(args) == 0 {
		buff = resp.Encode("PONG", true)
	} else {
		buff = resp.Encode(args[0], false)
	}

	connection.QueueWrite(fd, buff)
	return nil
}

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
		persistence.AppendToAOF(fullCmd)

		buff := resp.Encode("OK", true)
		connection.QueueWrite(fd, buff)
	}

	return nil
}

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
		}

		buff := resp.Encode(int64(cnt), false)
		connection.QueueWrite(fd, buff)
	}

	return nil
}

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

func AppenAofFile(args []string, fd int) error {
	if len(args) != 0 {
		connection.QueueWrite(fd, []byte("-ERR wrong number of arguments for 'bgrewriteaof' command\r\n"))
		return nil
	}
	return persistence.WriteAheadOfLog()
}

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
		buff := resp.Encode(newCounter, false)
		connection.QueueWrite(fd, buff)
	}

	return nil
}

func ResponseInfo(args []string, fd int) error {
	eviction.UpdateDBStat(0, "keys", len(store.Store))

	var sb strings.Builder

	sb.WriteString("# Server\r\n")
	sb.WriteString("redis_version:7.0.0\r\n")
	sb.WriteString("os:linux\r\n\r\n")

	sb.WriteString("# Keyspace\r\n")

	currentKeys := eviction.KeyspaceStats[0]["keys"]
	sb.WriteString("db0:keys=" + strconv.Itoa(currentKeys) + ",expires=0,avg_ttl=0\r\n")

	if fd != -1 {
		buff := resp.Encode(sb.String(), false)
		connection.QueueWrite(fd, buff)
	}
	return nil
}

func ResponsewithCommand(cmd *Command, fd int) error {
	switch cmd.Cmd {
	case "PING":
		return ResponsePing(cmd.Args, fd)
	case "SET":
		return ResponseSetKv(cmd.Args, fd)
	case "GET":
		return ResponseGetk(cmd.Args, fd)
	case "TTL":
		return ResponseTTl(cmd.Args, fd)
	case "DEL":
		return ResponseDel(cmd.Args, fd)
	case "EXPIRE":
		return ResponseExpire(cmd.Args, fd)
	case "BGREWRITEAOF":
		return AppenAofFile(cmd.Args, fd)
	case "INCR":
		return ResponseIncr(cmd.Args, fd)
	case "INFO":
		return ResponseInfo(cmd.Args, fd)
	//distribution of systems start from here 
	case "REPLICAOF":
		return cluster.ResponsewithReplica(cmd.Args, fd)
	default:
		if fd != -1 {
			errMessage := "-ERR unknown command '" + cmd.Cmd + "'\r\n"
			connection.QueueWrite(fd, []byte(errMessage))
		}
		return nil
	}
}

