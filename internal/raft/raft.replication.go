package raft

import (
	"context"
	"log"
	"sort"
	"time"

	"github.com/waiyneee/Kvstore/internal/cluster"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func (rn *RaftNode) replicateToPeer(peer string) {
	// >>> BUG are s there using unsecyred
	// //change it to secure
	conn, err := grpc.NewClient(peer, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("[RAFT] Critical: Failed to establish persistent gRPC channel to %s: %v", peer, err)
		return
	}
	defer conn.Close()
	client := NewRaftLeaderElectionClient(conn)

	for {
		rn.mu.Lock()
		if rn.state != Leader {
			rn.mu.Unlock()
			return // Shuts down routine smoothly if node steps down
		}

		term := rn.currentTerm
		leaderId := rn.id
		nextIdx := rn.nextIndex[peer]
		leaderCommit := rn.commitIndex

		var prevLogIndex int64 = 0
		var prevLogTerm int64 = 0
		var entries []*LogEntry

		if nextIdx > 1 {
			prevLogIndex = nextIdx - 1
			if prevLogIndex-1 < int64(len(rn.log)) {
				prevLogTerm = rn.log[prevLogIndex-1].Term
			}
		}

		if int64(len(rn.log)) >= nextIdx {
			entries = rn.log[nextIdx-1:]
		}
		rn.mu.Unlock()

		req := &AppendEntriesRequest{
			Term:         term,
			LeaderId:     leaderId,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: leaderCommit,
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*200)
		res, err := client.AppendEntries(ctx, req)
		cancel()

		if err == nil {
			rn.mu.Lock()
			if res.Term > rn.currentTerm {
				rn.currentTerm = res.Term
				rn.state = Follower
				rn.votedFor = -1

				cluster.ServerRole = "FOLLOWER"
			} else if rn.state == Leader && term == rn.currentTerm {
				if res.Success {
					rn.nextIndex[peer] = nextIdx + int64(len(entries))
					rn.matchIndex[peer] = rn.nextIndex[peer] - 1
					rn.advanceCommitIndex()
				} else {
					rn.nextIndex[peer]--
					if rn.nextIndex[peer] < 1 {
						rn.nextIndex[peer] = 1
					}
				}
			}
			rn.mu.Unlock()
		}

		time.Sleep(time.Millisecond * 50) // Standard 50ms heartbeats
	}
}

func (rn *RaftNode) advanceCommitIndex() {
	matches := make([]int, 0)
	matches = append(matches, len(rn.log))
	for _, peer := range rn.peerIps {
		matches = append(matches, int(rn.matchIndex[peer]))
	}

	sort.Ints(matches)
	majorityIndex := int64(matches[len(matches)/2])

	if majorityIndex > rn.commitIndex && majorityIndex > 0 && rn.log[majorityIndex-1].Term == rn.currentTerm {
		rn.commitIndex = majorityIndex
	}
}
