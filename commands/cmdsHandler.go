package commands

import (
	"errors"
	"log"
	"strconv"
	"strings"
	"time"
	"fmt"

	"golang.org/x/sys/unix"

	"github.com/waiyneee/Kvstore/persistence"
	"github.com/waiyneee/Kvstore/resp"
	"github.com/waiyneee/Kvstore/store"
	"github.com/waiyneee/Kvstore/eviction"
)

var clientBuffers = make(map[int][]byte)

func ResponsePing(args []string, fd int) error {
	var buff []byte

	if len(args) >= 2 {
		
		unix.Write(fd, []byte("-ERR wrong number of arguments for 'ping' command\r\n"))
		return nil
	}

	if len(args) == 0 {
		buff = resp.Encode("PONG", true)
	} else {
		buff = resp.Encode(args[0], false)
	}

	_, err := unix.Write(fd, buff)
	return err
}

func ResponseSetKv(args []string, fd int) error {
	if len(args) <= 1 {
		unix.Write(fd, []byte("-ERR wrong number of arguments for 'set' command\r\n"))
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
				unix.Write(fd, []byte("-ERR syntax error\r\n"))
				return nil
			}

			expiryDuration, err := strconv.ParseInt(args[i], 10, 64)
			if err != nil {
				unix.Write(fd, []byte("-ERR value is not an integer or out of range\r\n"))
				return nil
			}
			durationMs = expiryDuration * 1000
		default:
			unix.Write(fd, []byte("-ERR syntax error\r\n"))
			return nil
		}
	}

	if len(store.Store) >= eviction.LIMIT_KEYS {
		eviction.DoEviction()
	}
	store.Put(key, store.NewObj(value, durationMs))

	// fullCmd := append([]string{"SET"}, args...)
	// persistence.AppendToAOF(fullCmd)
    
	
	//  Only write to disk and network if it's a real client!

	if fd != -1 {
		fullCmd := append([]string{"SET"}, args...)
		persistence.AppendToAOF(fullCmd)

		buff := resp.Encode("OK", true)
		_, err := unix.Write(fd, buff)
		return err
	}

	// buff := resp.Encode("OK", true)
	// _, err := unix.Write(fd, buff)

	return nil
}

func ResponseGetk(args []string, fd int) error {
	if len(args) != 1 {
		unix.Write(fd, []byte("-ERR wrong number of arguments for 'get' command\r\n"))
		return nil
	}

	var key string = args[0]
	obj := store.Get(key)

	if obj == nil {
		var nilbuff []byte = resp.RespNil()
		_, err := unix.Write(fd, nilbuff)
		return err
	}

	if obj.ExpiryAtTimestamps != -1 && obj.ExpiryAtTimestamps <= time.Now().UnixMilli() {
		delete(store.Store, key)
		var nilbuff []byte = resp.RespNil()
		_, err := unix.Write(fd, nilbuff)
		return err
	}

	buff := resp.Encode(obj.Value, false)
	_, err := unix.Write(fd, buff)

	return err
}

func ResponseTTl(args []string, fd int) error {
	if len(args) != 1 {
		unix.Write(fd, []byte("-ERR wrong number of arguments for 'ttl' command\r\n"))
		return nil
	}

	var key string = args[0]
	obj := store.Get(key)

	if obj == nil {
		unix.Write(fd, []byte(":-2\r\n"))
		return nil
	}

	if obj.ExpiryAtTimestamps == -1 {
		unix.Write(fd, []byte(":-1\r\n"))
		return nil
	}

	durationMs := obj.ExpiryAtTimestamps - time.Now().UnixMilli()

	if durationMs < 0 {
		delete(store.Store, key)
		unix.Write(fd, []byte(":-2\r\n"))
		return nil
	}

	buff := resp.Encode(int64(durationMs/1000), false)
	_, err := unix.Write(fd, buff)

	return err
}

func ResponseDel(args []string, fd int) error {
	if len(args) < 1 {
		unix.Write(fd, []byte("-ERR wrong number of arguments for 'del' command\r\n"))
		return nil
	}
	var cnt int64 = 0

	for _, key := range args {
		if ok := store.Del(key); ok {
			cnt++
		}
	}

	// Only write to disk and network if it's a real client!
	if fd != -1 {
		if cnt > 0 {
			fullCmd := append([]string{"DEL"}, args...)
			persistence.AppendToAOF(fullCmd)
		}

		buff := resp.Encode(int64(cnt), false)
		_, err := unix.Write(fd, buff)
		return err
	}

	return nil
}

func ResponseExpire(args []string, fd int) error {
	if len(args) <= 1 {
		unix.Write(fd, []byte("-ERR wrong number of arguments for 'expire' command\r\n"))
		return nil
	}

	var key string = args[0]
	expiryDuration, err := strconv.ParseInt(args[1], 10, 64)

	if err != nil {
		unix.Write(fd, []byte("-ERR value is not an integer or out of range\r\n"))
		return nil
	}

	expiryMs := expiryDuration * 1000

	obj := store.Get(key)
	if obj == nil {
		unix.Write(fd, []byte(":0\r\n"))
		return nil
	}

	obj.ExpiryAtTimestamps = time.Now().UnixMilli() + expiryMs

	if fd != -1 {
		fullCmd := append([]string{"EXPIRE"}, args...)
		persistence.AppendToAOF(fullCmd)

		unix.Write(fd, []byte(":1\r\n"))
	}
	return nil
}

func AppenAofFile(args []string, fd int) error {
	if len(args) != 0 {
		unix.Write(fd, []byte("-ERR wrong number of arguments for 'bgrewriteaof' command\r\n"))
		return nil
	}
	return persistence.WriteAheadOfLog()
}
func ResponseIncr(args []string, fd int) error {
	if len(args) != 1 {
		unix.Write(fd, []byte("-ERR wrong number of arguments for 'incr' command\r\n"))
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
			unix.Write(fd, []byte("-ERR value is not an integer or out of range\r\n"))
			return nil
		}
		parsedVal, err := strconv.ParseInt(valStr, 10, 64)
		if err != nil {
			unix.Write(fd, []byte("-ERR value is not an integer or out of range\r\n"))
			return nil
		}

		newCounter = parsedVal + 1
		obj.Value = strconv.FormatInt(newCounter, 10)
	}

	if fd != -1 {
		fullCmd := append([]string{"INCR"}, args...)
		persistence.AppendToAOF(fullCmd)
		buff := resp.Encode(newCounter, false)
		_, err := unix.Write(fd, buff)
		return err
	}

	return nil
}

func ResponseInfo(args []string,fd int) error {
eviction.UpdateDBStat(0, "keys", len(store.Store))

	var sb strings.Builder

	
	sb.WriteString("# Server\r\n")
	sb.WriteString("redis_version:7.0.0\r\n")
	sb.WriteString("os:linux\r\n\r\n")

	sb.WriteString("# Keyspace\r\n")
	
	// 3. Read from your brand new stats array!
	currentKeys := eviction.KeyspaceStats[0]["keys"]
	sb.WriteString(fmt.Sprintf("db0:keys=%d,expires=0,avg_ttl=0\r\n", currentKeys))

	
	if fd != -1 {
		buff := resp.Encode(sb.String(), false)
		_, err := unix.Write(fd, buff)
		return err
	}
	return nil
}

func ResponsewithCommand(cmd *Command, fd int) error {
	if fd != -1 {
		log.Println("Command::", cmd)
	}

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
		return ResponseInfo(cmd.Args,fd)
	default:
		if fd != -1 {
			errMessage := fmt.Sprintf("-ERR unknown command '%s'\r\n", cmd.Cmd)
			unix.Write(fd, []byte(errMessage))
		}
		return nil
	}
}

func CleanUpClient(fd int) {
	delete(clientBuffers, fd)
}

func ProcessClientData(fd int) error {
	buffer := make([]byte, 512)

	n, err := unix.Read(fd, buffer)
	if err != nil {
		if err == unix.EAGAIN || err == unix.EWOULDBLOCK {
			return nil
		}
		return err
	}

	if n == 0 {
		return errors.New("client disconnected")
	}

	clientBuffers[fd] = append(clientBuffers[fd], buffer[:n]...)

	for len(clientBuffers[fd]) > 0 {
		tokens, consumedBytes, err := resp.DecodeArrayString(clientBuffers[fd])

		if err != nil {
			errStr := err.Error()
			if strings.Contains(errStr, "insufficient data") || strings.Contains(errStr, "no decoded data") {
				break
			}
			return err
		}

		if len(tokens) == 0 {
			break
		}

		cmd := &Command{
			Cmd:  strings.ToUpper(tokens[0]),
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
