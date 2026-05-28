package eviction

import (

	"github.com/waiyneee/Kvstore/store"

)

func simplefirst(){
	for k:=range store.Store{
		store.Del(k)
		return 
	}
}

func allKeysRandom(){
	cnt:=int64(EVICTION_RATIO * float64(LIMIT_KEYS))

	for k:=range store.Store{
		store.Del(k)
		cnt--
		if cnt<=0{
			break
		}
	}
}

// The FIXED Approximated LRU Eviction function
func allKeysLRU() {
	evictCount := int64(EVICTION_RATIO * float64(LIMIT_KEYS))

	// Loop 250 times to properly clear the required space
	for i := int64(0); i < evictCount; i++ {
		
		// Sample 5 keys PER deletion and toss them in the pool
		sampleSize := 5
		for k, obj := range store.Store {
			ePool.Push(k, obj.LastAccessedTime)
			sampleSize--
			if sampleSize <= 0 {
				break
			}
		}

		// Pop the single oldest key from the pool and delete it
		item := ePool.Pop()
		if item != nil {
			store.Del(item.key)
		}
	}
}
func DoEviction() {
	switch EVICT_STRATEGY {
	case "simple-key":
		simplefirst()
	case "all-keys-random":
		allKeysRandom()
	case "all-keys-lru":
		allKeysLRU()
	default:
		allKeysRandom()
	}
}