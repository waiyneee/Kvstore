package statebridge

import (
	"strconv"
	"strings"
)

type RaftSubmitter interface {
	SubmitCommand(command string) bool
}

var GlobalRaft RaftSubmitter

func FastRESPEncode(tokens []string) string {
	var sb strings.Builder
	capacity := 1 + len(strconv.Itoa(len(tokens))) + 2
	for _, t := range tokens {
		capacity += 1 + len(strconv.Itoa(len(t))) + 2 + len(t) + 2
	}
	sb.Grow(capacity)

	// Build the string manually
	//in case of raft ::just this specific 
	//one is dealth without my resp
	sb.WriteString("*")
	sb.WriteString(strconv.Itoa(len(tokens)))
	sb.WriteString("\r\n")

	for _, token := range tokens {
		sb.WriteString("$")
		sb.WriteString(strconv.Itoa(len(token)))
		sb.WriteString("\r\n")
		sb.WriteString(token)
		sb.WriteString("\r\n")
	}
	return sb.String()
}