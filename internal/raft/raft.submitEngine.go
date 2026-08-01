package raft

func (rn *RaftNode) SubmitCommand(command string) bool {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if rn.state != Leader {
		return false
	}

	entry := &LogEntry{
		Term:    rn.currentTerm,
		Command: command,
	}

	rn.log = append(rn.log, entry)

	return true
}
