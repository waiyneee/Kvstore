package commands

import (
	"github.com/waiyneee/Kvstore/internal/cluster"
	"github.com/waiyneee/Kvstore/internal/connection"
)

func ResponsewithCommand(cmd *Command, fd int) error {

	//if Node is a Leader then only
	//it can mutate else not allowed
	if cluster.ServerRole == "FOLLOWER" &&
		(cmd.Cmd == "SET" || cmd.Cmd == "DEL" || cmd.Cmd == "INCR") {

		if fd != cluster.LeaderConnectionFD {
			errMessage := "-READONLY You can't write against a read only replica.\r\n"
			connection.QueueWrite(fd, []byte(errMessage))
			return nil
		}
	}

	isKeyCommand := cmd.Cmd == "SET" || cmd.Cmd == "GET" || cmd.Cmd == "DEL" ||
		cmd.Cmd == "TTL" || cmd.Cmd == "EXPIRE" || cmd.Cmd == "INCR"

	if isKeyCommand && len(cmd.Args) > 0 {
		key := cmd.Args[0] // The key is always the first argument

		// Let the cluster package decide if we own this key
		if cluster.CheckRedirect(key, fd) {
			return nil
		}
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
		return ResponseInfo(cmd.Args, fd)
	//distribution of systems start from here
	case "REPLICAOF":
		return cluster.ResponsewithReplica(cmd.Args, fd)

	case "SYNC":
		return cluster.ResponseSync(cmd.Args, fd)
		//no user must be able to do this apart from raw tcp fds/sockets

	default:
		if fd != -1 {
			errMessage := "-ERR unknown command '" + cmd.Cmd + "'\r\n"
			connection.QueueWrite(fd, []byte(errMessage))
		}
		return nil
	}
}
