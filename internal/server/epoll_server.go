package server

import (
	"log"
	"runtime"
	"strings"
	"time"

	"golang.org/x/sys/unix"

	"github.com/waiyneee/Kvstore/internal/cluster"
	"github.com/waiyneee/Kvstore/internal/commands"
	"github.com/waiyneee/Kvstore/internal/connection"
	"github.com/waiyneee/Kvstore/internal/persistence"
	"github.com/waiyneee/Kvstore/internal/raft"
	"github.com/waiyneee/Kvstore/internal/resp"
	"github.com/waiyneee/Kvstore/internal/statebridge"
	"github.com/waiyneee/Kvstore/internal/store"
)

var RaftBrain *raft.RaftNode

// Start boots the server, sets up Epoll, and triggers the main loop
func (s *Server) Start() error {
	runtime.GOMAXPROCS(1)
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	log.Printf("Starting pure single-threaded epoll server on 0.0.0.0:%d\n", s.port)

	RaftBrain = raft.New(s.nodeId, s.peerIPs)

	statebridge.GlobalRaft = RaftBrain
	go func() {
		log.Printf("Booting Raft Control Plane on port %d...", s.raftPort)
		if err := raft.StartrpcServer(s.raftPort, RaftBrain); err != nil {
			log.Fatalf("Fatal: Raft gRPC server crashed: %v", err)
		}
	}()

	go s.startRaftApplierLoop()

	var err error
	s.serverFD, err = createServerSocket(s.port)
	if err != nil {
		return err
	}
	defer unix.Close(s.serverFD)

	s.epollFD, err = createEpoll()
	if err != nil {
		return err
	}
	defer unix.Close(s.epollFD)

	connection.GlobalEpollFD = s.epollFD

	if err := addEpollRead(s.epollFD, s.serverFD); err != nil {
		return err
	}

	s.restoreAOF()

	eventsarr := make([]unix.EpollEvent, s.maxClients)
	cronFrequency := 1 * time.Second
	lastCronExecTime := time.Now()

	for {
		var timeoutMs int
		lastCronExecTime, timeoutMs = s.handleCronJobs(lastCronExecTime, cronFrequency)

		nEvents, err := unix.EpollWait(s.epollFD, eventsarr, timeoutMs)
		if err != nil {
			if err == unix.EINTR {
				continue
			}
			log.Println("epoll_wait system error:", err)
			continue
		}

		for i := 0; i < nEvents; i++ {
			s.handleEvent(int(eventsarr[i].Fd), eventsarr[i].Events)
		}
	}
}

func (s *Server) handleCronJobs(lastCronTime time.Time, freq time.Duration) (time.Time, int) {
	now := time.Now()

	if now.After(lastCronTime.Add(freq)) {
		store.DeleteExpiredKeys()

		lastCronTime = time.Now()
	}

	timeoutMs := int(time.Until(lastCronTime.Add(freq)).Milliseconds())
	if timeoutMs <= 0 {
		timeoutMs = 10
	}

	return lastCronTime, timeoutMs
}

// handleEvent routes
func (s *Server) handleEvent(fd int, event uint32) {
	if fd == s.serverFD {
		s.acceptClient()
		return
	}

	if event&unix.EPOLLIN != 0 {
		s.handleRead(fd)
	}

	if event&unix.EPOLLOUT != 0 {
		s.handleWrite(fd)
	}
}

func (s *Server) acceptClient() {
	clientFD, _, err := unix.Accept(s.serverFD)
	if err != nil {
		log.Println("Failed to accept raw client connection:", err)
		return
	}

	if err := unix.SetNonblock(clientFD, true); err != nil {
		unix.Close(clientFD)
		return
	}

	if err := addEpollRead(s.epollFD, clientFD); err != nil {
		log.Println("Failed adding client socket to epoll:", err)
		unix.Close(clientFD)
	}
}

func (s *Server) handleRead(fd int) {
	if err := ProcessClientData(fd); err != nil {
		s.cleanupClient(fd)
		return
	}

	done, err := connection.FlushWriteBuffer(fd)
	if err != nil {
		s.cleanupClient(fd)
		return
	}

	if !done {
		modEpollReadWrite(s.epollFD, fd)
	}
}

func (s *Server) handleWrite(fd int) {
	done, err := connection.FlushWriteBuffer(fd)
	if err != nil {
		s.cleanupClient(fd)
		return
	}

	if done {
		modEpollRead(s.epollFD, fd)
	}
}

func (s *Server) cleanupClient(fd int) {
	delEpoll(s.epollFD, fd)
	unix.Close(fd)
	connection.CleanUpClient(fd)

}

func (s *Server) restoreAOF() {
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
}

func (s *Server) startRaftApplierLoop() {
	var lastApplied int64 = 0

	for {
		time.Sleep(10 * time.Millisecond)

		commitIndex := RaftBrain.GetCommitIndex()
		if commitIndex <= lastApplied {
			continue
		}

		//	log.Printf("[APPLIER] Pipeline triggered! commitIndex=%d, lastApplied=%d. Processing batch...", commitIndex, lastApplied)

		for i := lastApplied + 1; i <= commitIndex; i++ {
			entry := RaftBrain.GetLogEntry(i)
			if entry == nil {
				log.Printf("[APPLIER ERROR] Found nil log entry at index %d", i)
				continue
			}

			//log.Printf("[APPLIER] Decoding log index %d. Raw string payload: %q", i, entry.Command)

			//if u re a leader u already processed that commands
			// isnide processclientdata

			if cluster.ServerRole == "LEADER" {
				lastApplied = i
				continue
			}
			tokens, _, err := resp.DecodeArrayString([]byte(entry.Command))
			if err != nil || len(tokens) == 0 {

				log.Printf("[APPLIER CRITICAL ERROR] Parsing failed at log index %d: %v (decoded tokens count: %d)", i, err, len(tokens))
				lastApplied = i
				continue
			}

			//log.Printf("[APPLIER SUCCESS] Applying command to Epoll dictionary: %v", tokens)

			cmd := &commands.Command{
				Cmd:  strings.ToUpper(tokens[0]),
				Args: tokens[1:],
			}

			commands.ResponsewithCommand(cmd, -1)
			lastApplied = i
		}
	}
}
