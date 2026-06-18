
package raft

import (
    "context"
    "fmt"
    "log"
    "net"
    "sync"

    "google.golang.org/grpc"
)

type NodeState int

const (
    Follower NodeState = iota
    Candidate
    Leader
)

type RaftNode struct {
    UnimplementedRaftLeaderElectionServer

    mu sync.Mutex

    id      int32
    peerIps []string

    currentTerm int64
    votedFor    int32
    log         []*LogEntry

    state       NodeState
    commitIndex int64
    lastApplied int64

    heartbeats chan struct{}

    nextIndex  map[string]int64
    matchIndex map[string]int64
}

func New(nodeId int32, peerIps []string) *RaftNode {
    rn := &RaftNode{
        id:          nodeId,
        peerIps:     peerIps,
        currentTerm: 0,
        votedFor:    -1,
        log:         make([]*LogEntry, 0),
        state:       Follower,
        heartbeats:  make(chan struct{}, 1),

        //loog replications handled here 
        nextIndex:   make(map[string]int64),
        matchIndex:  make(map[string]int64),
    }

    go rn.runElectionTimer()
    return rn
}

func (rn *RaftNode) RequestVote(ctx context.Context, req *RequestVoteRequest) (*RequestVoteResponse, error) {
    rn.mu.Lock()
    defer rn.mu.Unlock()

    if req.Term < rn.currentTerm {
        return &RequestVoteResponse{Term: rn.currentTerm, VoteGranted: false}, nil
    } else if req.Term > rn.currentTerm {
        rn.currentTerm = req.Term
        rn.state = Follower
        rn.votedFor = -1
    }

    if rn.votedFor != -1 && rn.votedFor != req.CandidateId {
        return &RequestVoteResponse{Term: rn.currentTerm, VoteGranted: false}, nil
    }

    lastLogIndex := int64(len(rn.log))
    var lastLogTerm int64 = 0
    if lastLogIndex > 0 {
        lastLogTerm = rn.log[lastLogIndex-1].Term
    }

    if req.LastLogTerm < lastLogTerm || (req.LastLogTerm == lastLogTerm && req.LastLogIndex < lastLogIndex) {
        return &RequestVoteResponse{Term: rn.currentTerm, VoteGranted: false}, nil
    }

    rn.votedFor = req.CandidateId
    return &RequestVoteResponse{Term: rn.currentTerm, VoteGranted: true}, nil
}

func (rn *RaftNode) AppendEntries(ctx context.Context, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
    rn.mu.Lock()
    defer rn.mu.Unlock()

    if req.Term > rn.currentTerm {
        rn.votedFor = -1
        rn.state = Follower
        rn.currentTerm = req.Term
    } else if req.Term < rn.currentTerm {
        return &AppendEntriesResponse{Term: rn.currentTerm, Success: false}, nil
    }

    select {
    case rn.heartbeats <- struct{}{}:
    default:
    }
    rn.state = Follower

    if req.PrevLogIndex > 0 {
        if int64(len(rn.log)) < req.PrevLogIndex {
            return &AppendEntriesResponse{Term: rn.currentTerm, Success: false}, nil
        }
        if rn.log[req.PrevLogIndex-1].Term != req.PrevLogTerm {
            return &AppendEntriesResponse{Term: rn.currentTerm, Success: false}, nil
        }
    }

    if req.PrevLogIndex == 0 {
        rn.log = req.Entries
    } else {
        rn.log = append(rn.log[:req.PrevLogIndex], req.Entries...)
    }

    if req.LeaderCommit > rn.commitIndex {
        lastNewEntry := int64(len(rn.log))
        if req.LeaderCommit < lastNewEntry {
            rn.commitIndex = req.LeaderCommit
        } else {
            rn.commitIndex = lastNewEntry
        }
    }

    return &AppendEntriesResponse{Term: rn.currentTerm, Success: true}, nil
}

func StartrpcServer(port int, node *RaftNode) error {
    lstnr, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", port))
    if err != nil {
        return fmt.Errorf("failed to listen on port %d: %v", port, err)
    }

    // Production Setup: Add grpc.Creds() here loaded from server.crt and server.key
    // if I am anyhow doing something on production 
    grpcServer := grpc.NewServer()
    RegisterRaftLeaderElectionServer(grpcServer, node)

    log.Printf("[RAFT] Control Plane listening on port %d...", port)
    return grpcServer.Serve(lstnr)
}