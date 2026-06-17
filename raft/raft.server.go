package raft

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"

	"google.golang.org/grpc"
)

// type Server struct{

// }

type NodeState int

//A node exists in 3 states only
const (
	Follower NodeState = iota
	Candidate
	Leader
)

// const port =10000

type RaftNode struct {
	UnimplementedRaftLeaderElectionServer

	mu sync.Mutex

	id      int32
	peerIps []string

	currentTerm int64
	votedFor    int32 //-1 if no vote is yet done
	log         []*LogEntry

	// Volatile State (Wiped on reboot)
	state       NodeState
	commitIndex int64
	lastApplied int64

	//heartbeats Logic 
	heartbeats chan struct{}
}

//as soon as your server boots or starts up it basically starts up
//as a new state machine role ==follower
func New(nodeId int32, peerIps []string) *RaftNode {
	rn:= &RaftNode{
		id:      nodeId,
		peerIps: peerIps,

		currentTerm: 0,
		votedFor:    -1,
		log:         make([]*LogEntry, 0),
		state:       Follower,
		heartbeats: make(chan struct{},1),
	}

	//boot the watchDog
	go rn.runElectionTimer()
	return rn
}

func (rn *RaftNode) RequestVote(ctx context.Context, req *RequestVoteRequest) (*RequestVoteResponse, error) {

	rn.mu.Lock()
	defer rn.mu.Unlock()

	//Tdod:handling the request vote rpcs here
	if req.Term < rn.currentTerm {
		//stale vote
		return &RequestVoteResponse{
			Term:        rn.currentTerm,
			VoteGranted: false,
		}, nil
	} else if req.Term > rn.currentTerm {
		//step down as candidate to follower
		log.Printf("[RAFT] Node %d stepping down as .Term:%d -->%d", rn.id, rn.currentTerm, req.Term)
		rn.currentTerm = req.Term
		rn.state = Follower
		rn.votedFor = -1
	}

	//already voted or Double-voting request
	if rn.votedFor != -1 && rn.votedFor != req.CandidateId {
		return &RequestVoteResponse{
			Term:        rn.currentTerm,
			VoteGranted: false}, nil
	}

	//LOg REplication ralted rule
	// RULE 4: Log Freshness (Simplified for now, will expand in Log Replication)
	lastLogIndex := int64(len(rn.log))
	var lastLogTerm int64 = 0
	if lastLogIndex > 0 {
		lastLogTerm = rn.log[lastLogIndex-1].Term
	}

	if req.LastLogTerm < lastLogTerm ||
		(req.LastLogTerm == lastLogTerm && req.LastLogIndex < lastLogIndex) {

		log.Printf("[RAFT] Node %d DENIED vote to %d (Stale Log)", rn.id, req.CandidateId)
		return &RequestVoteResponse{
			Term:        rn.currentTerm,
			VoteGranted: false}, nil
	}

	//if everything is fine granting the vote
	rn.votedFor = req.CandidateId
	log.Printf("[RAFT] Node %d GRANTED vote to %d for Term %d", rn.id, req.CandidateId, req.Term)

	return &RequestVoteResponse{
		Term:        rn.currentTerm,
		VoteGranted: true, //default stub
	}, nil

}

func (rn *RaftNode) AppendEntries(ctx context.Context, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {

	rn.mu.Lock()
	defer rn.mu.Unlock()

	//Todo - server sends hearbesta and LOg replication ,also sometimes
	//sometimes it send empty appeend entreues heratbeats
	//handling of Append Entrues
	if req.Term > rn.currentTerm {
		rn.votedFor = -1
		rn.state = Follower
		rn.currentTerm = req.Term

		// rn.heartbeats<-struct{}{}
	} else if req.Term < rn.currentTerm {
		return &AppendEntriesResponse{
			Term:    rn.currentTerm,
			Success: false,
		}, nil
	}
    

	//thi will rest the timer 
	select {
    case 
	   rn.heartbeats <- struct{}{}:
    default:
    }

	rn.state = Follower

	//by default acknowledge the Leader

	return &AppendEntriesResponse{
		Term:    rn.currentTerm,
		Success: true,
	}, nil
}
func StartrpcServer(port int, node *RaftNode) error {
	lstnr, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", port))
	if err != nil {
		return fmt.Errorf("failed to listen on port %d: %v", port, err)
	}

	grpcServer := grpc.NewServer()

	// This binds your generated Protobuf schema to your Go struct
	RegisterRaftLeaderElectionServer(grpcServer, node)

	log.Printf("[RAFT] Control Plane listening on port %d...", port)
	return grpcServer.Serve(lstnr)
}