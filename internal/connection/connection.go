package connection

import (
	"golang.org/x/sys/unix"
)

// Exported buffers so the server can parse incoming bytes
var ClientBuffers = make(map[int][]byte)
var OutboundBuffers = make(map[int][]byte)
var GlobalReadBuffer = make([]byte, 4096)

func QueueWrite(fd int, data []byte) {
	if fd == -1 {
		return
	}
	OutboundBuffers[fd] = append(OutboundBuffers[fd], data...)
}

var GlobalEpollFD int

func RegisterSocket(fd int) error {
	event := unix.EpollEvent{
		Events: unix.EPOLLIN,
		Fd:     int32(fd),
	}
	return unix.EpollCtl(GlobalEpollFD, unix.EPOLL_CTL_ADD, fd, &event)
}

func FlushWriteBuffer(fd int) (done bool, err error) {
	if len(OutboundBuffers[fd]) == 0 {
		return true, nil // Nothing to write
	}

	n, err := unix.Write(fd, OutboundBuffers[fd])
	if err != nil {
		if err == unix.EAGAIN || err == unix.EWOULDBLOCK {
			return false, nil // OS buffer full, wait for EPOLLOUT
		}
		return false, err // Fatal network error
	}

	OutboundBuffers[fd] = OutboundBuffers[fd][n:]

	if len(OutboundBuffers[fd]) == 0 {
		return true, nil // We successfully flushed everything
	}

	return false, nil
}

func CleanUpClient(fd int) {
	delete(ClientBuffers, fd)
	delete(OutboundBuffers, fd)
}
