package connection

import (
	"golang.org/x/sys/unix"
)

type ClientSession struct {
	Inbound  []byte
	Outbound []byte
}

// Single unified map replacing separate ClientBuffers and OutboundBuffers maps
var Clients = make(map[int]*ClientSession)
var GlobalReadBuffer = make([]byte, 4096)
var GlobalEpollFD int

// GetActiveClientFDs retrieves all active file descriptors for graceful shutdown
func GetActiveClientFDs() []int {
	fds := make([]int, 0, len(Clients))
	for fd := range Clients {
		fds = append(fds, fd)
	}
	return fds
}

func RegisterSocket(fd int) error {
	event := unix.EpollEvent{
		Events: unix.EPOLLIN,
		Fd:     int32(fd),
	}
	return unix.EpollCtl(GlobalEpollFD, unix.EPOLL_CTL_ADD, fd, &event)
}

func QueueWrite(fd int, data []byte) {
	if fd == -1 {
		return
	}
	session, exists := Clients[fd]
	if !exists {
		session = &ClientSession{}
		Clients[fd] = session
	}
	session.Outbound = append(session.Outbound, data...)
}

func FlushWriteBuffer(fd int) (done bool, err error) {
	session, exists := Clients[fd]
	if !exists || len(session.Outbound) == 0 {
		return true, nil // Nothing to write
	}

	n, err := unix.Write(fd, session.Outbound)
	if err != nil {
		if err == unix.EAGAIN || err == unix.EWOULDBLOCK {
			return false, nil // OS buffer full, wait for EPOLLOUT
		}
		return false, err // Fatal network error
	}

	session.Outbound = session.Outbound[n:]

	if len(session.Outbound) == 0 {
		return true, nil // Fully flushed
	}

	return false, nil
}

func CleanUpClient(fd int) {
	delete(Clients, fd)
}
