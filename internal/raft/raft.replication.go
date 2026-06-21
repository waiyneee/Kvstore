package raft

import (
	"context"
	"crypto/tls"
	"log"
	"sort"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func (rn *RaftNode) replicateToPeer(peer string) {
	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	creds := credentials.NewTLS(tlsConfig)

	for {
		rn.mu.Lock()
		if rn.state != Leader {
			rn.mu.Unlock()
			return
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
			prevLogTerm = rn.log[prevLogIndex-1].Term
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

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*50)
		conn, err := grpc.DialContext(ctx, peer, grpc.WithTransportCredentials(creds))
		if err != nil {
			cancel()
			time.Sleep(time.Millisecond * 50)
			continue
		}

		client := NewRaftLeaderElectionClient(conn)
		res, err := client.AppendEntries(ctx, req)
		conn.Close()
		cancel()

		if err == nil {
			rn.mu.Lock()
			if res.Term > rn.currentTerm {
				rn.currentTerm = res.Term
				rn.state = Follower
				rn.votedFor = -1
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

		time.Sleep(time.Millisecond * 50)
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
		log.Printf("[RAFT] LEADER %d COMMITTED LOG INDEX %d", rn.id, rn.commitIndex)
	}
}
