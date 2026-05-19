package server

import (
	"log"
	"runtime"
	"strconv"
	

	"golang.org/x/sys/unix"

	"github.com/waiyneee/Kvstore/commands"
)

var (
	host string = "0.0.0.0"
)

func HandleConnections(portStr string) error {
	// Take control from go's scheduler
	runtime.GOMAXPROCS(1) // single threaded
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	port, err := strconv.Atoi(portStr)
	if err != nil {
		port = 6379
	}

	log.Printf("Starting pure single-threaded epoll server on %s:%d\n", host, port)
	maxClients := 10000

	// Raw tcp socket
	serverFD, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM, 0)
	if err != nil {
		return err
	}
	defer unix.Close(serverFD)

	if err := unix.SetsockoptInt(serverFD, unix.SOL_SOCKET, unix.SO_REUSEADDR, 1); err != nil {
		return err
	}

	// Making socket non-blocking
	if err := unix.SetNonblock(serverFD, true); err != nil {
		return err
	}

	// Address binding with port
	addr := unix.SockaddrInet4{Port: port}
	copy(addr.Addr[:], []byte{0, 0, 0, 0})
	if err := unix.Bind(serverFD, &addr); err != nil {
		return err
	}

	// Listening
	if err := unix.Listen(serverFD, maxClients); err != nil {
		return err
	}

	// Now epoll_instance descriptor
	epollFD, err := unix.EpollCreate1(0)
	if err != nil {
		return err
	}
	defer unix.Close(epollFD)

	// Event tracking configuration
	serverEvent := unix.EpollEvent{
		Events: unix.EPOLLIN,
		Fd:     int32(serverFD),
	}

	if err := unix.EpollCtl(epollFD, unix.EPOLL_CTL_ADD, serverFD, &serverEvent); err != nil {
		return err
	}

	// An array for kernel collecting cycles
	eventsarr := make([]unix.EpollEvent, maxClients)

	for {
		// Execution blocks here until an event fires
		nEvents, err := unix.EpollWait(epollFD, eventsarr, -1)
		if err != nil {
			if err == unix.EINTR {
				continue // System call was interrupted, retry safely
			}
			log.Println("epoll_wait system error:", err)
			continue
		}

		for i := 0; i < nEvents; i++ {
			currFD := int(eventsarr[i].Fd)

			if currFD == serverFD {
				// New client opening a data connection
				clientFD, _, err := unix.Accept(serverFD)
				if err != nil {
					log.Println("Failed to accept raw client connection:", err)
					continue
				}

				if err := unix.SetNonblock(clientFD, true); err != nil {
					unix.Close(clientFD)
					continue
				}

				// Track this client socket
				clientEvent := unix.EpollEvent{
					Events: unix.EPOLLIN,
					Fd:     int32(clientFD),
				}
				if err := unix.EpollCtl(epollFD, unix.EPOLL_CTL_ADD, clientFD, &clientEvent); err != nil {
					log.Println("Failed adding client socket to epoll:", err)
					unix.Close(clientFD)
				}
			} else {
				// Active client has data
				err := commands.ProcessClientData(currFD)
				if err != nil {
					unix.EpollCtl(epollFD, unix.EPOLL_CTL_DEL, currFD, nil)
					unix.Close(currFD)
				}
			}
		}
	}
}