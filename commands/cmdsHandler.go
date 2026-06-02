package commands

import (
	"errors"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sys/unix"

	"github.com/waiyneee/Kvstore/eviction"
	"github.com/waiyneee/Kvstore/persistence"
	"github.com/waiyneee/Kvstore/resp"
	"github.com/waiyneee/Kvstore/store"
)

var clientBuffers = make(map[int][]byte)
var globalReadBuffer = make([]byte, 4096)
var outboundBuffers = make(map[int][]byte)

func toUpperASCII(s string) string {
	hasLower := false
	for i := 0; i < len(s); i++ {
		if s[i] >= 'a' && s[i] <= 'z' {
			hasLower = true
			break
		}
	}
	if !hasLower {
		return s // Return the exact same string pointer (0 allocations!)
	}

	// Only allocate memory if we actually
	// need to change lowercase to uppercase
	b := make([]byte, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'a' && c <= 'z' {
			c -= 32
		}
		b[i] = c
	}
	return string(b)
}

func QueueWrite(fd int, data []byte) {
	if fd == -1 {
		return
	}
	outboundBuffers[fd] = append(outboundBuffers[fd], data...)
}

func FlushWriteBuffer(fd int) (done bool, err error) {
	if len(outboundBuffers[fd]) == 0 {
		return true, nil // Nothing to write
	}

	n, err := unix.Write(fd, outboundBuffers[fd])
	if err != nil {
		if err == unix.EAGAIN || err == unix.EWOULDBLOCK {
			return false, nil // OS buffer full, wait for EPOLLOUT
		}
		return false, err // Fatal network error
	}

	outboundBuffers[fd] = outboundBuffers[fd][n:]

	if len(outboundBuffers[fd]) == 0 {
		return true, nil // We successfully flushed everything
	}

	return false, nil
}

func ResponsePing(args []string, fd int) error {
	var buff []byte

	if len(args) >= 2 {
		QueueWrite(fd, []byte("-ERR wrong number of arguments for 'ping' command\r\n"))
		return nil
	}

	if len(args) == 0 {
		buff = resp.Encode("PONG", true)
	} else {
		buff = resp.Encode(args[0], false)
	}

	QueueWrite(fd, buff)
	return nil
}

func ResponseSetKv(args []string, fd int) error {
	if len(args) <= 1 {
		QueueWrite(fd, []byte("-ERR wrong number of arguments for 'set' command\r\n"))
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
				QueueWrite(fd, []byte("-ERR syntax error\r\n"))
				return nil
			}

			expiryDuration, err := strconv.ParseInt(args[i], 10, 64)
			if err != nil {
				QueueWrite(fd, []byte("-ERR value is not an integer or out of range\r\n"))
				return nil
			}
			durationMs = expiryDuration * 1000
		default:
			QueueWrite(fd, []byte("-ERR syntax error\r\n"))
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
		QueueWrite(fd, buff)
	}

	return nil
}

func ResponseGetk(args []string, fd int) error {
	if len(args) != 1 {
		QueueWrite(fd, []byte("-ERR wrong number of arguments for 'get' command\r\n"))
		return nil
	}

	var key string = args[0]
	obj := store.Get(key)

	if obj == nil {
		var nilbuff []byte = resp.RespNil()
		QueueWrite(fd, nilbuff)
		return nil
	}

	if obj.ExpiryAtTimestamps != -1 && obj.ExpiryAtTimestamps <= time.Now().UnixMilli() {
		delete(store.Store, key)
		var nilbuff []byte = resp.RespNil()
		QueueWrite(fd, nilbuff)
		return nil
	}

	buff := resp.Encode(obj.Value, false)
	QueueWrite(fd, buff)

	return nil
}

func ResponseTTl(args []string, fd int) error {
	if len(args) != 1 {
		QueueWrite(fd, []byte("-ERR wrong number of arguments for 'ttl' command\r\n"))
		return nil
	}

	var key string = args[0]
	obj := store.Get(key)

	if obj == nil {
		QueueWrite(fd, []byte(":-2\r\n"))
		return nil
	}

	if obj.ExpiryAtTimestamps == -1 {
		QueueWrite(fd, []byte(":-1\r\n"))
		return nil
	}

	durationMs := obj.ExpiryAtTimestamps - time.Now().UnixMilli()

	if durationMs < 0 {
		delete(store.Store, key)
		QueueWrite(fd, []byte(":-2\r\n"))
		return nil
	}

	buff := resp.Encode(int64(durationMs/1000), false)
	QueueWrite(fd, buff)

	return nil
}

func ResponseDel(args []string, fd int) error {
	if len(args) < 1 {
		QueueWrite(fd, []byte("-ERR wrong number of arguments for 'del' command\r\n"))
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
		QueueWrite(fd, buff)
	}

	return nil
}

func ResponseExpire(args []string, fd int) error {
	if len(args) <= 1 {
		QueueWrite(fd, []byte("-ERR wrong number of arguments for 'expire' command\r\n"))
		return nil
	}

	var key string = args[0]
	expiryDuration, err := strconv.ParseInt(args[1], 10, 64)

	if err != nil {
		QueueWrite(fd, []byte("-ERR value is not an integer or out of range\r\n"))
		return nil
	}

	expiryMs := expiryDuration * 1000

	obj := store.Get(key)
	if obj == nil {
		QueueWrite(fd, []byte(":0\r\n"))
		return nil
	}

	obj.ExpiryAtTimestamps = time.Now().UnixMilli() + expiryMs

	if fd != -1 {
		fullCmd := append([]string{"EXPIRE"}, args...)
		persistence.AppendToAOF(fullCmd)

		QueueWrite(fd, []byte(":1\r\n"))
	}
	return nil
}

func AppenAofFile(args []string, fd int) error {
	if len(args) != 0 {
		QueueWrite(fd, []byte("-ERR wrong number of arguments for 'bgrewriteaof' command\r\n"))
		return nil
	}
	return persistence.WriteAheadOfLog()
}

func ResponseIncr(args []string, fd int) error {
	if len(args) != 1 {
		QueueWrite(fd, []byte("-ERR wrong number of arguments for 'incr' command\r\n"))
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
			QueueWrite(fd, []byte("-ERR value is not an integer or out of range\r\n"))
			return nil
		}
		parsedVal, err := strconv.ParseInt(valStr, 10, 64)
		if err != nil {
			QueueWrite(fd, []byte("-ERR value is not an integer or out of range\r\n"))
			return nil
		}

		newCounter = parsedVal + 1
		obj.Value = strconv.FormatInt(newCounter, 10)
	}

	if fd != -1 {
		fullCmd := append([]string{"INCR"}, args...)
		persistence.AppendToAOF(fullCmd)
		buff := resp.Encode(newCounter, false)
		QueueWrite(fd, buff)
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
		QueueWrite(fd, buff)
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
	default:
		if fd != -1 {
			errMessage := "-ERR unknown command '" + cmd.Cmd + "'\r\n"
			QueueWrite(fd, []byte(errMessage))
		}
		return nil
	}
}

func CleanUpClient(fd int) {
	delete(clientBuffers, fd)
	delete(outboundBuffers, fd)
}

func ProcessClientData(fd int) error {
	// 4KB tcp standard global buffer
	for {
		n, err := unix.Read(fd, globalReadBuffer)
		if err != nil {
			if err == unix.EAGAIN || err == unix.EWOULDBLOCK {
				break
			}
			return err
		}

		if n == 0 {
			return errors.New("client disconnected")
		}

		clientBuffers[fd] = append(clientBuffers[fd], globalReadBuffer[:n]...)
	}

	for len(clientBuffers[fd]) > 0 {
		tokens, consumedBytes, err := resp.DecodeArrayString(clientBuffers[fd])

		if err != nil {
			errStr := err.Error()
			// OPTIMIZATION: Bypassing the heavy strings.Contains substring match
			if errStr == "insufficient data" || errStr == "no decoded data" {
				break
			}
			return err
		}

		if len(tokens) == 0 {
			break
		}

		cmd := &Command{
			// OPTIMIZATION: Zero-allocation uppercase if already uppercase
			Cmd:  toUpperASCII(tokens[0]),
			Args: tokens[1:],
		}

		err = ResponsewithCommand(cmd, fd)
		if err != nil {
			return err
		}

		clientBuffers[fd] = clientBuffers[fd][consumedBytes:]
	}

	return nil
}
