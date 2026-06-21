package commands 

import (
	"strings"
	"strconv"

	"github.com/waiyneee/Kvstore/internal/eviction"
	"github.com/waiyneee/Kvstore/internal/store"
	"github.com/waiyneee/Kvstore/internal/resp"
	"github.com/waiyneee/Kvstore/internal/connection"

)


func ResponseInfo(args []string, fd int) error {
	eviction.UpdateDBStat(0, "keys", len(store.Store))

	var sb strings.Builder

	sb.WriteString("# Server\r\n")
	sb.WriteString("redis_version:7.0.0\r\n")
	sb.WriteString("os:linux\r\n\r\n")

	sb.WriteString("# Keyspace\r\n")

	currentKeys := eviction.KeyspaceStats[0]["keys"]

	sb.WriteString("db0:keys=");
	sb.WriteString(strconv.Itoa(currentKeys));
	sb.WriteString(",expires=0,avg_ttl=0\r\n")

	if fd != -1 {
		buff := resp.Encode(sb.String(), false)
		connection.QueueWrite(fd, buff)
	}
	return nil
}