package raft

import (
	"context"
	"log"
	"math/rand"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// runElectionTimer runs continuously in the background
func (rn *RaftNode) runElectionTimer() {
	for {
		rn.mu.Lock()
		if rn.state == Leader {
			rn.mu.Unlock()
			time.Sleep(time.Millisecond * 10) 
			continue
		}
		rn.mu.Unlock()

		// Generate a random timeout between 150ms and 300ms
		timeoutDuration := time.Duration(rand.Intn(151)+150) * time.Millisecond 

		select {
		case <-rn.heartbeats: 
			continue

		case <-time.After(timeoutDuration): 
			rn.mu.Lock()
			if rn.state != Leader {
				rn.state = Candidate
				rn.currentTerm++
				rn.votedFor = rn.id // Vote for yourself

				campaignTerm := rn.currentTerm
				log.Printf("[RAFT] Node %d Election Timer POPPED! Starting campaign for Term %d", rn.id, campaignTerm)

				// Launch network campaign concurrently
				go rn.startCampaign(campaignTerm)
			}
			rn.mu.Unlock()
		}
	}
}

// startCampaign dials all peers concurrently to request votes
func (rn *RaftNode) startCampaign(campaignTerm int64) { 
	rn.mu.Lock()

	lastLogIndex := int64(len(rn.log))

	var lastLogTerm int64 = 0
	
	if lastLogIndex > 0 {
		lastLogTerm = rn.log[lastLogIndex-1].Term
	}

	req := &RequestVoteRequest{
		Term:         campaignTerm,
		CandidateId:  rn.id,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	votesReceived := 1 // We vote for ourselves
	totalNodes := len(rn.peerIps) + 1
	votesNeeded := (totalNodes / 2) + 1
	rn.mu.Unlock()

	// Channel to collect vote results concurrently
	voteCh := make(chan bool, len(rn.peerIps))

	// Step 4: Dials the IP address using grpc.Dial()
	for _, peerIP := range rn.peerIps {
		go func(peer string) {
			// 100ms timeout so a dead peer doesn't freeze our election
			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
			defer cancel()

			conn, err := grpc.DialContext(ctx, peer, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				voteCh <- false
				return
			}
			defer conn.Close()

			client := NewRaftLeaderElectionClient(conn)
			res, err := client.RequestVote(ctx, req)
			if err != nil {
				voteCh <- false
				return
			}

			rn.mu.Lock()
			defer rn.mu.Unlock()

			// Step-Down Rule: Did we encounter a node with a higher term?
			if res.Term > rn.currentTerm {
				rn.currentTerm = res.Term
				rn.state = Follower
				rn.votedFor = -1
				voteCh <- false
				return
			}

			// Did we get the vote, and are we still a valid candidate for this term?
			if res.VoteGranted && rn.state == Candidate && rn.currentTerm == campaignTerm {
				voteCh <- true
				return
			}

			voteCh <- false
		}(peerIP)
	}

	// Tally the votes
	for i := 0; i < len(rn.peerIps); i++ {
		vote := <-voteCh

		rn.mu.Lock()
		// If we stepped down or already won, abort the tally
		if rn.state != Candidate || rn.currentTerm != campaignTerm {
			rn.mu.Unlock()
			return
		}

		if vote {
			votesReceived++
			// If votes > (TotalNodes / 2), the Candidate instantly promotes itself to Leader!
			if votesReceived >= votesNeeded {
				rn.state = Leader
				log.Printf("[RAFT] ---> NODE %d IS THE NEW LEADER FOR TERM %d! <---", rn.id, rn.currentTerm)
				
				// A Leader MUST immediately assert dominance to suppress other timers
				go rn.startHeartbeats()
				rn.mu.Unlock()
				return
			}
		}
		rn.mu.Unlock()
	}
}

// startHeartbeats runs continuously once a node becomes Leader
func (rn *RaftNode) startHeartbeats() {
	for {
		rn.mu.Lock()
		if rn.state != Leader {
			rn.mu.Unlock()
			return // We got deposed. Stop sending heartbeats.
		}
		term := rn.currentTerm
		leaderId := rn.id
		rn.mu.Unlock()

		// Broadcast empty AppendEntries to all followers
		for _, peerIP := range rn.peerIps {
			go func(peer string) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*50)
				defer cancel()

				conn, err := grpc.DialContext(ctx, peer, grpc.WithTransportCredentials(insecure.NewCredentials()))
				if err != nil {
					return
				}
				defer conn.Close()

				client := NewRaftLeaderElectionClient(conn)
				req := &AppendEntriesRequest{
					Term:     term,
					LeaderId: leaderId,
					// Entries array is empty, which makes this a Heartbeat!
				}

				res, err := client.AppendEntries(ctx, req)
				if err == nil {
					rn.mu.Lock()
					// If a follower replies with a higher term, we are a zombie leader. Step down!
					if res.Term > rn.currentTerm {
						rn.currentTerm = res.Term
						rn.state = Follower
						rn.votedFor = -1
					}
					rn.mu.Unlock()
				}
			}(peerIP)
		}

		// Fire heartbeats every 50ms (Must be faster than the 150ms election timeout)
		time.Sleep(time.Millisecond * 50)
	}
}