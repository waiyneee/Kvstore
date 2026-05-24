package resp

import (
	"fmt"
)

func Encode(val interface{}, isSimpleString bool) []byte {
	switch v := val.(type) {

	case string:
		if isSimpleString {
			return []byte(fmt.Sprintf("+%s\r\n", v))
		}

		return []byte(
			fmt.Sprintf("$%d\r\n%s\r\n", len(v), v),
		)
	case int64:
		return []byte(fmt.Sprintf(":%d\r\n", v))
	default:
		return []byte("$-1\r\n")
	}
}

func RespNil() []byte {
	//(nil) that redis returns to us

	return []byte("$-1\r\n")
}
