package resp

import (
	// "fmt"
	"errors"
	// "fmt"
)

//we will be dealing with large chunk of data mainly strings

func readSimplestring(data []byte) (string, int, error) {

	//we need to have the pos to put it at correct place
	//after successful string extraction
	//skip the first index
	pos := 1
	for ; data[pos] != '\r'; pos++ {
		continue

	}

	return string(data[1:pos]), pos + 2, nil

}
func readrespErrors(data []byte) (interface{}, int, error) {
	//same as simplestrng inly - sign

	return readSimplestring(data)
}
func readBulkString(data []byte) (string, int, error) {
	// Structure: $5\r\nhello\r\n
	// 1. Find the first \r\n to determine the length
	pos := 1
	lenValue := 0

	for ; data[pos] != '\r'; pos++ {
		lenValue = lenValue*10 + int(data[pos]-'0')
	}

	// Skip the \r\n after the length
	start := pos + 2
	end := start + lenValue

	if len(data) < end+2 {
		return "", 0, errors.New("insufficient data for bulk string")
	}

	return string(data[start:end]), end + 2, nil
}

func readArrayResp(data []byte) (interface{}, int, error) {
	// Structure: *<count>\r\n<element1><element2>...
	pos := 1
	count := 0

	// 1. Parse the number of elements in the array
	for ; data[pos] != '\r'; pos++ {
		count = count*10 + int(data[pos]-'0')
	}

	// Move past the first \r\n
	pos += 2

	var result []interface{}
	for i := 0; i < count; i++ {
		val, delta, err := decodeOneResp(data[pos:])
		if err != nil {
			return nil, 0, err
		}

		result = append(result, val)
		// 'delta' tells us how many bytes that specific element consumed
		pos += delta
	}

	return result, pos, nil

	//nesting would also work

}

func readIntegerResp(data []byte) (interface{}, int, error) {
	//int64 basically
	//structre ---> :8090\r\n

	//just string to int conversion
	pos := 1
	var value int64 = 0

	for ; data[pos] != '\r'; pos++ {

		value = value*10 + int64(data[pos]-'0')
	}

	return value, pos + 2, nil
}

func decodeOneResp(data []byte) (interface{}, int, error) {
	//here my core extraction of string ,integer and bulk strings
	//errrors logic lives
	if len(data) == 0 {
		return nil, 0, errors.New("no decoded data to be recieved")
	}

	//just take the first v
	// alue from the slice
	switch data[0] {
	case '+':
		return readSimplestring(data)
	case '-':
		return readrespErrors(data)
	case '*':
		return readArrayResp(data)
	case ':':
		return readIntegerResp(data)
	case '$':
		return readBulkString(data)

	default:
		return readSimplestring(data)

	}

}
func DecodeArrayString(data []byte) ([]string, int, error) {
	// Capture the 'delta' from Decode
	decodedData, delta, err := Decode(data)
	if err != nil {
		return nil, 0, err
	}

	taskInterface, ok := decodedData.([]interface{})
	if !ok {
		return nil, 0, errors.New("resp: data is not a valid array payload")
	}

	tokens := make([]string, len(taskInterface))

	for i := range tokens {
		strVal, ok := taskInterface[i].(string)
		if !ok {
			return nil, 0, errors.New("resp: array element is not a string")
		}
		tokens[i] = strVal
	}

	// Return tokens AND the delta
	return tokens, delta, nil
}

func Decode(data []byte) (interface{}, int, error) {
	if len(data) == 0 {
		return nil, 0, errors.New("there is no data to be recieved please some dtat must be there ")
	}

	// Capture the delta instead of using the blank identifier '_'
	value, delta, err := decodeOneResp(data)

	return value, delta, err
}
