package resp

import (
	// "fmt"
	"strconv"
	"strings"
)

func Encode(val interface{}, isSimpleString bool) []byte {
	switch v := val.(type) {

	case string:
		if isSimpleString {
			return []byte("+" + v + "\r\n")
		}

		return []byte("$" + strconv.Itoa(len(v)) + "\r\n" + v + "\r\n")
	case int64:
		return []byte(":" + strconv.FormatInt(v, 10) + "\r\n")
	default:
		return []byte("$-1\r\n")
	}
}

// converts a slicee of string into a raw resp
// array for WAL shipping
func EncodeArray(args []string) []byte {
	var sb strings.Builder
	sb.WriteString("*" + strconv.Itoa(len(args)) + "\r\n")
	for _, arg := range args {
		sb.WriteString("$" + strconv.Itoa(len(arg)) + "\r\n" + arg + "\r\n")
	}
	return []byte(sb.String())
}
func RespNil() []byte {
	//(nil) that redis returns to us

	return []byte("$-1\r\n")
}
