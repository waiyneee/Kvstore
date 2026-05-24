package commands

import "time"

type Obj struct {
	Value              interface{}
	expiryAtTimestamps int64
}

// a hashmap
var store = make(map[string]*Obj)

// func init() {
// 	//constructor
// 	store=make(map[string]*Obj)
// }

func NewObj(value interface{}, durationMs int64) *Obj {
	var expiryat int64 = -1
	if durationMs > 0 {
		expiryat = time.Now().UnixMilli() + durationMs
	}

	return &Obj{
		Value:              value,
		expiryAtTimestamps: expiryat,
	}

}

func Put(key string, obj *Obj) {
	store[key] = obj
}

func Get(key string) *Obj {
	return store[key]

}
