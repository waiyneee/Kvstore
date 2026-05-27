package store

import 	"time"

	


type Obj struct {
	Value              interface{}
	ExpiryAtTimestamps int64
}

// a hashmap
var Store = make(map[string]*Obj)

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
		ExpiryAtTimestamps: expiryat,
	}

}

func Put(key string, obj *Obj) {

	Store[key] = obj
}

func Get(key string) *Obj {
	return Store[key]

}
func Del(key string) bool {
	if _, ok := Store[key]; ok {
		delete(Store, key)
		return true
	}

	return false

}

func expireSample() float32 {
	var limit int = 20
	var expiredCount int = 0

	// Map iteration in Go is naturally randomized
	//so random sampling is easier
	for key, obj := range Store {
		if obj.ExpiryAtTimestamps != -1 {
			limit--
			// If the key is expired, actively delete it
			if obj.ExpiryAtTimestamps <= time.Now().UnixMilli() {
				delete(Store, key)
				expiredCount++
			}
		}

		// Once we've checked 20 keys
		//we are done
		if limit <= 0 {
			break
		}
	}

	return float32(expiredCount) / float32(20.0)
}

// DeleteExpiredKeys runs the probabilistic active expiration cycle
func DeleteExpiredKeys() {
	for {
		frac := expireSample()
		// If less than 25% of the sampled keys were expired,
		//break the cycle
		if frac < 0.25 {
			break
		}
	}
}
