package cluster

import (
	"net"

	"golang.org/x/sys/unix"

	"github.com/waiyneee/Kvstore/connection"
	"github.com/waiyneee/Kvstore/resp"
)

func ResponsewithReplica(args []string, fd int) error {
	if len(args) != 2 {
		connection.QueueWrite(fd, []byte("-ERR wrong number of arguments for 'ping' command\r\n"))
		return nil
	}
	if args[0] == "NO" || args[1] == "ONE" {
		//do something TODOS;;-->become sthe master urself
		ServerRole = "LEADER"
		LeaderAddress = ""
		LeaderConnectionFD = -1
		connection.QueueWrite(fd, resp.Encode("OK", true))

		return nil
	}
	ServerRole = "FOLLOWER"
	LeaderAddress = args[0] + ":" + args[1]
	//Node b conncets to primary A

	conn, err := net.Dial("tcp", LeaderAddress)
	if err != nil {
		connection.QueueWrite(fd, []byte("-ERR unable to connect to leader\r\n"))
		return nil
	}

	//now node B send resp command to node A
	//to announce itself a s a replica
	_, err = conn.Write([]byte("*1\r\n$4\r\nSYNC\r\n"))
	if err != nil {
		connection.QueueWrite(fd, []byte("-Failed to send SYNC command to leader"))

	}

	//raw file descriptor
	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		connection.QueueWrite(fd, []byte("-ERR invalid connection type\r\n"))
		return nil
	}
	rawConn, err := tcpConn.SyscallConn()
	if err != nil {
		connection.QueueWrite(fd, []byte("-ERR failed to get syscall conn\r\n"))
		return nil
	}

	var leaderFD int
	rawConn.Control(func(rawFd uintptr) {
		leaderFD = int(rawFd)
	})

	// 4. Set the new socket to Non-Blocking so it works with our epoll loop
	unix.SetNonblock(leaderFD, true)
	LeaderConnectionFD = leaderFD

	// 5. TODO: Tell the server package to add leaderFD to epoll!
	connection.RegisterSocket(leaderFD)

	if fd != -1 {
		buff := resp.Encode("OK", true)

		connection.QueueWrite(fd, buff)
		return nil
	}

	return nil
}

func BroadcastToReplicas(cmd []string) {
	if len(ReplicaFDs) == 0 {
		return // No followers, do nothing
	}

	encoded := resp.EncodeArray(cmd)
	for _, fd := range ReplicaFDs {
		connection.QueueWrite(fd, encoded)
	}
}

func RemoveReplica(fd int) {
	for i, rfd := range ReplicaFDs {
		if rfd == fd {
			// Fast slice deletion to remove the dead socket
			ReplicaFDs = append(ReplicaFDs[:i], ReplicaFDs[i+1:]...)
			break
		}
	}
}
