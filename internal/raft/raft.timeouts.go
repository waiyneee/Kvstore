package raft

import (
	"context"
	//"crypto/tls"
	"log"
	"math/rand"
	"time"

	"github.com/waiyneee/Kvstore/internal/cluster"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func (rn *RaftNode) runElectionTimer() {
	for {
		rn.mu.Lock()
		if rn.state == Leader {
			rn.mu.Unlock()
			time.Sleep(time.Millisecond * 10)
			continue
		}
		rn.mu.Unlock()

		timeoutDuration := time.Duration(rand.Intn(151)+150) * time.Millisecond

		select {
		case <-rn.heartbeats:
			continue
		case <-time.After(timeoutDuration):
			rn.mu.Lock()
			if rn.state != Leader {
				rn.state = Candidate
				rn.currentTerm++
				rn.votedFor = rn.id

				campaignTerm := rn.currentTerm
				log.Printf("[RAFT] Node %d Election Timer POPPED! Campaigning for Term %d", rn.id, campaignTerm)

				go rn.startCampaign(campaignTerm)
			}
			rn.mu.Unlock()
		}
	}
}

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

	votesReceived := 1
	totalNodes := len(rn.peerIps) + 1
	votesNeeded := (totalNodes / 2) + 1
	//what if a single node is there ??
	if len(rn.peerIps) == 0 {
		rn.state = Leader

		cluster.ServerRole="LEADER"
	
		log.Printf("[RAFT]---->Node %d is the new Leader for the term %d standlaone{node}", rn.id, rn.currentTerm)
		rn.mu.Unlock()

		return
	}
	rn.mu.Unlock()

	voteCh := make(chan bool, len(rn.peerIps))

	// Production TLS config - SkipVerify for local dev, configure CAs for production
	// //grpc/creddentials are still there
	// //do not run it on production YETT
	//tlsConfig := &tls.Config{InsecureSkipVerify: true}
	//creds := credentials.NewTLS(tlsConfig)

	for _, peerIP := range rn.peerIps {
		go func(peer string) {
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

			if res.Term > rn.currentTerm {
				rn.currentTerm = res.Term
				rn.state = Follower
				rn.votedFor = -1

				//reset your timer on stepdown
				select {
					case rn.heartbeats <-struct{}{}:
					default:
				}

				cluster.ServerRole="FOLLOWER"
				voteCh <- false
				return
			}

			if res.VoteGranted && rn.state == Candidate && rn.currentTerm == campaignTerm {
				voteCh <- true
				return
			}

			voteCh <- false
		}(peerIP)
	}

	for i := 0; i < len(rn.peerIps); i++ {
		vote := <-voteCh

		rn.mu.Lock()
		if rn.state != Candidate || rn.currentTerm != campaignTerm {
			rn.mu.Unlock()
			return
		}

		if vote {
			votesReceived++
			if votesReceived >= votesNeeded {
				rn.state = Leader

				cluster.ServerRole="LEADER"
				log.Printf("[RAFT] ---> NODE %d IS THE NEW LEADER FOR TERM %d! <---", rn.id, rn.currentTerm)

				for _, p := range rn.peerIps {
					rn.nextIndex[p] = int64(len(rn.log)) + 1
					rn.matchIndex[p] = 0
					go rn.replicateToPeer(p)
				}

				rn.mu.Unlock()
				return
			}
		}
		rn.mu.Unlock()
	}
}
