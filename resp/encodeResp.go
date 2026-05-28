package resp

import (
	// "fmt"
	"strconv"
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

func RespNil() []byte {
	//(nil) that redis returns to us

	return []byte("$-1\r\n")
}
