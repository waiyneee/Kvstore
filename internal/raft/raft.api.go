package raft

func (rn *RaftNode) GetCommitIndex() int64 {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	return rn.commitIndex
}

func (rn *RaftNode) GetLogEntry(index int64) *LogEntry {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	if index > 0 && index <= int64(len(rn.log)) {
		return rn.log[index-1]
	}
	return nil
}