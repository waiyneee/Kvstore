package server

import (
	"log"
	"runtime"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sys/unix"

	"github.com/waiyneee/Kvstore/connection"
	"github.com/waiyneee/Kvstore/commands"
	"github.com/waiyneee/Kvstore/persistence"
	"github.com/waiyneee/Kvstore/resp"
	"github.com/waiyneee/Kvstore/store"
)

var (
	host string = "0.0.0.0"
)

func HandleConnections(portStr string) error {
	runtime.GOMAXPROCS(1)
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	port, err := strconv.Atoi(portStr)
	if err != nil {
		port = 6379
	}

	log.Printf("Starting pure single-threaded epoll server on %s:%d\n", host, port)
	maxClients := 10000

	serverFD, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM, 0)
	if err != nil {
		return err
	}
	defer unix.Close(serverFD)

	if err := unix.SetsockoptInt(serverFD, unix.SOL_SOCKET, unix.SO_REUSEADDR, 1); err != nil {
		return err
	}

	if err := unix.SetNonblock(serverFD, true); err != nil {
		return err
	}

	addr := unix.SockaddrInet4{Port: port}
	copy(addr.Addr[:], []byte{0, 0, 0, 0})
	if err := unix.Bind(serverFD, &addr); err != nil {
		return err
	}

	if err := unix.Listen(serverFD, maxClients); err != nil {
		return err
	}

	epollFD, err := unix.EpollCreate1(0)
	if err != nil {
		return err
	}
	defer unix.Close(epollFD)

	serverEvent := unix.EpollEvent{
		Events: unix.EPOLLIN,
		Fd:     int32(serverFD),
	}

	if err := unix.EpollCtl(epollFD, unix.EPOLL_CTL_ADD, serverFD, &serverEvent); err != nil {
		return err
	}

	eventsarr := make([]unix.EpollEvent, maxClients)

	log.Println("Checking for AOF persistence file...")
	aofData, _ := persistence.RestoreFromAOF()
	if len(aofData) > 0 {
		log.Println("Restoring database from AOF...")
		for len(aofData) > 0 {
			tokens, consumedBytes, err := resp.DecodeArrayString(aofData)
			if err != nil || len(tokens) == 0 {
				break
			}
			cmd := &commands.Command{
				Cmd:  strings.ToUpper(tokens[0]),
				Args: tokens[1:],
			}

			commands.ResponsewithCommand(cmd, -1)

			aofData = aofData[consumedBytes:]
		}
		log.Println("AOF Restoration complete.")
	}

	cronFrequency := 1 * time.Second
	lastCronExecTime := time.Now()

	for {
		now := time.Now()

		if now.After(lastCronExecTime.Add(cronFrequency)) {
			store.DeleteExpiredKeys()
			lastCronExecTime = time.Now()
		}

		nextCronTime := lastCronExecTime.Add(cronFrequency)
		timeoutMs := int(time.Until(nextCronTime).Milliseconds())

		if timeoutMs <= 0 {
			timeoutMs = 10
		}

		nEvents, err := unix.EpollWait(epollFD, eventsarr, timeoutMs)
		if err != nil {
			if err == unix.EINTR {
				continue
			}
			log.Println("epoll_wait system error:", err)
			continue
		}

		for i := 0; i < nEvents; i++ {
			currFD := int(eventsarr[i].Fd)
			event := eventsarr[i].Events // Grab the specific event flag
			if currFD == serverFD {
				clientFD, _, err := unix.Accept(serverFD)
				if err != nil {
					log.Println("Failed to accept raw client connection:", err)
					continue
				}

				if err := unix.SetNonblock(clientFD, true); err != nil {
					unix.Close(clientFD)
					continue
				}
				clientEvent := unix.EpollEvent{
					Events: unix.EPOLLIN,
					Fd:     int32(clientFD),
				}
				if err := unix.EpollCtl(epollFD, unix.EPOLL_CTL_ADD, clientFD, &clientEvent); err != nil {
					log.Println("Failed adding client socket to epoll:", err)
					unix.Close(clientFD)
				}
				continue
			}
			if event&unix.EPOLLIN != 0 {
				err := ProcessClientData(currFD)
				if err != nil {
					unix.EpollCtl(epollFD, unix.EPOLL_CTL_DEL, currFD, nil)
					unix.Close(currFD)
					connection.CleanUpClient(currFD)
					continue
				}

				// OPTIMISTIC WRITE: Data was processed,
				// let's try to blast the response back immediately
				done, err := connection.FlushWriteBuffer(currFD)
				if err != nil {
					unix.EpollCtl(epollFD, unix.EPOLL_CTL_DEL, currFD, nil)
					unix.Close(currFD)
					connection.CleanUpClient(currFD)
					continue
				}

				// THE MAGIC: If `done` is false, it means we hit EAGAIN.
				// We must tell the kernel to wake us up when the write buffer has free space!
				if !done {
					unix.EpollCtl(epollFD, unix.EPOLL_CTL_MOD, currFD, &unix.EpollEvent{
						Events: unix.EPOLLIN | unix.EPOLLOUT, // Listen for BOTH now
						Fd:     int32(currFD),
					})
				}
			}

			// 3. HANDLE OUTBOUND SPACE AVAILABLE (EPOLLOUT) Now magic happens
			if event&unix.EPOLLOUT != 0 {
				done, err := connection.FlushWriteBuffer(currFD)
				if err != nil {
					unix.EpollCtl(epollFD, unix.EPOLL_CTL_DEL, currFD, nil)
					unix.Close(currFD)
					connection.CleanUpClient(currFD)
					continue
				}

				// If we successfully flushed the queue,
				// turn off EPOLLOUT so the kernel stops spamming us
				if done {
					unix.EpollCtl(epollFD, unix.EPOLL_CTL_MOD, currFD, &unix.EpollEvent{
						Events: unix.EPOLLIN, // Back to read-only mode
						Fd:     int32(currFD),
					})
				}
			}
		}
	}
}
