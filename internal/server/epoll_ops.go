package server

import "golang.org/x/sys/unix"

// createEpoll abstracts the creation of the epoll instance
func createEpoll() (int, error) {
	return unix.EpollCreate1(0)
}

// addEpollRead registers a file descriptor for read events
func addEpollRead(epollFD int, fd int) error {
	event := unix.EpollEvent{Events: unix.EPOLLIN, Fd: int32(fd)}
	return unix.EpollCtl(epollFD, unix.EPOLL_CTL_ADD, fd, &event)
}

// modEpollReadWrite modifies a file descriptor to listen for both read and write
func modEpollReadWrite(epollFD int, fd int) error {
	event := unix.EpollEvent{Events: unix.EPOLLIN | unix.EPOLLOUT, Fd: int32(fd)}
	return unix.EpollCtl(epollFD, unix.EPOLL_CTL_MOD, fd, &event)
}

// modEpollRead reverts a file descriptor to read-only listening
func modEpollRead(epollFD int, fd int) error {
	event := unix.EpollEvent{Events: unix.EPOLLIN, Fd: int32(fd)}
	return unix.EpollCtl(epollFD, unix.EPOLL_CTL_MOD, fd, &event)
}

// delEpoll removes a file descriptor from the epoll instance
func delEpoll(epollFD int, fd int) error {
	return unix.EpollCtl(epollFD, unix.EPOLL_CTL_DEL, fd, nil)
}

// createServerSocket handles the heavy lifting of socket creation and binding
func createServerSocket(port int) (int, error) {
	fd, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM, 0)
	if err != nil {
		return -1, err
	}

	unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_REUSEADDR, 1)
	unix.SetNonblock(fd, true)

	addr := unix.SockaddrInet4{Port: port}
	copy(addr.Addr[:], []byte{0, 0, 0, 0})

	if err := unix.Bind(fd, &addr); err != nil {
		unix.Close(fd)
		return -1, err
	}

	if err := unix.Listen(fd, 10000); err != nil {
		unix.Close(fd)
		return -1, err
	}

	return fd, nil
}
