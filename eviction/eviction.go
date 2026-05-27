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


func DoEviction(){
	switch string(EVICT_STRATEGY){
	case "simple-key":
		simplefirst()
	case "all-keys-random":
		allKeysRandom()
	}
}