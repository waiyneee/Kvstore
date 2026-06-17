package raft

import (
	"fmt"
	"log"
	"net"
	"sync"
	"context"


	"google.golang.org/grpc"
)
// type Server struct{


// }

type Nodestate int 


//A node exists in 3 states only 
const (
	Follower Nodestate=iota
	Candidate
	Leader
)



const port =10000


type RaftNode struct{
	UnimplementedRaftLeaderElectionServer

	mu sync.Mutex

	id  int32
	peerIps []string

	currentTerm int32
	votedFor int32 //-1 if no vote is yet done 
	log []*LogEntry



	// Volatile State (Wiped on reboot)
	state       NodeState
	commitIndex int64
	lastApplied int64
}


//as soon as your server boots or starts up it basically starts up 
//as a new state machine role ==follower 
func New(nodeId int32,peerIps []string) *RaftNode{
	return &RaftNode{
		id: nodeId,
		peerIps: peerIps,

		currentTerm: 0,
		votedFor: -1,
		log: make([]*LogEntry,0),

	}
}

func (rn *RaftNode) RequestVote(ctx context.Context,req *RequestVoteRequest) 
(*RequestVoteResponse,error){

	rn.mu.Lock()
	defer mu.rn.Unlock()

	//Tdod:handling the request vote rpcs here 


	return &RequestVoteResponse{
		Term: rn.currentTerm,
		voteGranted:false , //default stub 
	},nil

}

func (rn *RaftNode) AppendEntries(ctx context.Context,req *AppendEntriesRequest) 
(*AppendEntriesResponse,error){

	rn.mu.Lock()
	defer rn.mu.Unlock()

	//Todo - server sends hearbesta and LOg replication ,also sometimes 
	//sometimes it send empty appeend entreues heratbeats 
	//handling of Append Entrues 


	return &AppendEntriesResponse{
		Term :rn.currentTerm,
		Success :true,
	},nil
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



