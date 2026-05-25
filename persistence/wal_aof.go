package persistence

import (
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"github.com/waiyneee/Kvstore/store"
)

var filePath = "./walpersistence.aof"

func WriteAheadOfLog() error {
	fptr, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		log.Println("Error opening AOF file:", err)
		return err
	}
	defer fptr.Close()

	log.Printf("Rewriting the file BY BGREWRITE to %s\n", filePath)

	for key, obj := range store.Store {
		valStr := fmt.Sprintf("%v", obj.Value)
		respCmd := fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(valStr), valStr)
		fptr.Write([]byte(respCmd))
	}
	return nil
}

func AppendToAOF(tokens []string) error {
	fptr, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		return err
	}
	defer fptr.Close()

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("*%d\r\n", len(tokens)))
	for _, token := range tokens {
		sb.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(token), token))
	}

	_, err = fptr.Write([]byte(sb.String()))
	return err
}

func RestoreFromAOF() ([]byte, error) {
	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer file.Close()

	return io.ReadAll(file)
}
