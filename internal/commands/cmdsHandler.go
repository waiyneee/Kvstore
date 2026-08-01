package commands

import (
	"github.com/waiyneee/Kvstore/internal/cluster"
	"github.com/waiyneee/Kvstore/internal/connection"
	"github.com/waiyneee/Kvstore/internal/persistence"
	"github.com/waiyneee/Kvstore/internal/statebridge"
)

type CommandHandler func(args []string, fd int) error

var registry = map[string]CommandHandler{
	"PING":         ResponsePing,
	"SET":          ResponseSetKv,
	"GET":          ResponseGetk,
	"TTL":          ResponseTTl,
	"DEL":          ResponseDel,
	"EXPIRE":       ResponseExpire,
	"BGREWRITEAOF": AppenAofFile,
	"INCR":         ResponseIncr,
	"INFO":         ResponseInfo,
}

func ResponsewithCommand(cmd *Command, fd int) error {
	// If Node is a Leader then only it can mutate else not allowed
	if cluster.ServerRole == "FOLLOWER" &&
		(cmd.Cmd == "SET" || cmd.Cmd == "DEL" || cmd.Cmd == "INCR") {

		if fd != -1 {
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

	//the command registry magic happens here
	handler, exists := registry[cmd.Cmd]
	if !exists {
		if fd != -1 {
			errMessage := "-ERR unknown command '" + cmd.Cmd + "'\r\n"
			connection.QueueWrite(fd, []byte(errMessage))
		}
		return nil
	}
	err := handler(cmd.Args, fd)

	if err == nil && fd != -1 {
		isValid := true
		if (cmd.Cmd == "INCR" || cmd.Cmd == "DEL" || cmd.Cmd == "EXPIRE" || cmd.Cmd == "TTL") && len(cmd.Args) != 1 {
			isValid = false
		}
		if cmd.Cmd == "SET" && len(cmd.Args) < 2 {
			isValid = false
		}

		if isValid && (cmd.Cmd == "SET" || cmd.Cmd == "DEL" || cmd.Cmd == "EXPIRE" || cmd.Cmd == "INCR") {
			fullCmd := append([]string{cmd.Cmd}, cmd.Args...)
			persistence.AppendToAOF(fullCmd)
			persistence.SyncAOF()

			if cluster.ServerRole == "LEADER" && statebridge.GlobalRaft != nil {

				raftCmdStr := statebridge.FastRESPEncode(fullCmd)
				statebridge.GlobalRaft.SubmitCommand(raftCmdStr)
			}
		}
	}

	return err
}
