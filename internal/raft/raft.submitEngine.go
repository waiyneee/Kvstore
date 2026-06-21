package raft

import "log"

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

    log.Printf("[RAFT] Leader %d appended new entry: '%s' at index %d", rn.id, command, len(rn.log))

    return true
}