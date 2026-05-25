package persistence

import (
	"fmt"
	"log"
	"os"

	"github.com/waiyneee/Kvstore/store"
)

var filePath = "./walpersistence.aof"

// WriteAheadOfLog acts as a BGREWRITEAOF
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