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
	}

	return []byte{}
}