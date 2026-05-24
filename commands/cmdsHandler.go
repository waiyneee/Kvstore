package commands

import (
	"errors"
	"log"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sys/unix"

	"github.com/waiyneee/Kvstore/resp"
)

// Global map
var clientBuffers = make(map[int][]byte)

func ResponsePing(args []string, fd int) error {
	var buff []byte

	if len(args) >= 2 {
		return errors.New("ERR wrong number of arguments for 'ping' command")
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

	var durationMs int64 = -1 //return if no expiry set an int value

	//checking expiry
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
	Put(key, NewObj(value, durationMs))
	//rsp encode Ok
	buff := resp.Encode("OK", true)
	_, err := unix.Write(fd, buff)

	return err
}
func ResponseGetk(args []string, fd int) error {
	if len(args) != 1 {
		// ADDED: return nil to stop execution and keep connection alive
		unix.Write(fd, []byte("-ERR wrong number of arguments for 'get' command\r\n"))
		return nil
	}

	var key string = args[0]
	obj := Get(key)

	if obj == nil {
		var nilbuff []byte = resp.RespNil()
		_, err := unix.Write(fd, nilbuff)
		return err
	}

	// If key already expired then return nil AND delete it from memory
	//important no dead key
	if obj.expiryAtTimestamps != -1 && obj.expiryAtTimestamps <= time.Now().UnixMilli() {
		delete(store, key) // PASSIVE EVICTION FIX
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
		// ADDED: return nil
		unix.Write(fd, []byte("-ERR wrong number of arguments for 'ttl' command\r\n"))
		return nil
	}

	var key string = args[0]
	obj := Get(key)

	if obj == nil {
		// ADDED: return nil
		unix.Write(fd, []byte(":-2\r\n"))
		return nil
	}

	if obj.expiryAtTimestamps == -1 {
		// ADDED: return nil
		unix.Write(fd, []byte(":-1\r\n"))
		return nil
	}

	durationMs := obj.expiryAtTimestamps - time.Now().UnixMilli()

	// If key expired
	if durationMs < 0 {
		delete(store, key) // PASSIVE EVICTION FIX
		unix.Write(fd, []byte(":-2\r\n"))
		return nil
	}

	// TTL natively returns seconds, not milliseconds, so we divide by 1000
	buff := resp.Encode(int64(durationMs/1000), false)
	_, err := unix.Write(fd, buff)

	return err
}

func ResponseDel(args []string,fd int) error{
	if len(args)<1{
		unix.Write(fd,[]byte("-ERR wrong number of arguments for 'del' command\r\n"))
		return nil
	}
	var cnt int64=0

	for _,key:=range args{
		if ok:=Del(key);ok{
			cnt++
		}
	}

	buff:=resp.Encode(int64(cnt),false)
	_,err :=unix.Write(fd,buff)

	return err

}

func ResponseExpire(args []string,fd int) error{
	if len(args)<=1{
		unix.Write(fd,[]byte("-ERR wrong number of arguments for 'expire' command\r\n"))
		return nil
	}

	var key string=args[0]
	expiryDuration,err:=strconv.ParseInt(args[1],10,64)
	

	if err!=nil{
		unix.Write(fd,[]byte("-ERR value is not an integer or out of range\r\n"))
		return nil
	}

	expiryMs:=expiryDuration*1000

	obj:=Get(key)
	if obj==nil{
		unix.Write(fd,[]byte(":0\r\n"))
		return nil
	}

	obj.expiryAtTimestamps=time.Now().UnixMilli() + expiryMs
	// 1 if the timeout was set.

	unix.Write(fd,[]byte(":1\r\n"))
	return nil

}

func ResponsewithCommand(cmd *Command, fd int) error {
	log.Println("Command::", cmd)
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
		return ResponseDel(cmd.Args, fd )
	case "EXPIRE":
		return ResponseExpire(cmd.Args, fd )

	default:
		return ResponsePing(cmd.Args, fd)
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

	// 1. Append the newly arrived stream
	clientBuffers[fd] = append(clientBuffers[fd], buffer[:n]...)

	for len(clientBuffers[fd]) > 0 {
		tokens, err := resp.DecodeArrayString(clientBuffers[fd])
		if err != nil {
			// Check if the parser failed simply because the packet is cut off mid-stream
			errStr := err.Error()
			if strings.Contains(errStr, "insufficient data") || strings.Contains(errStr, "no decoded data") {

				break
			}
			return err // Actual malformed
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

		clientBuffers[fd] = nil
	}

	return nil
}
