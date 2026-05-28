package eviction

import (
	"sort"
)

// PoolItem tracks the key and its last accessed time
type PoolItem struct {
	key              string
	lastAccessedTime int64
}

type EvictionPool struct {
	pool   []*PoolItem
	keyset map[string]*PoolItem 
}

var ePoolSizeMax int = 16
var ePool *EvictionPool = &EvictionPool{
	pool:   make([]*PoolItem, 0, ePoolSizeMax),
	keyset: make(map[string]*PoolItem),
}

func (pq *EvictionPool) Push(key string, lastAccessedTime int64) {
	if _, ok := pq.keyset[key]; ok {
		return 
	}

	item := &PoolItem{key: key, lastAccessedTime: lastAccessedTime}

	if len(pq.pool) < ePoolSizeMax {
		// Pool is not full, just append and sort
		pq.keyset[key] = item
		pq.pool = append(pq.pool, item)
		sort.Slice(pq.pool, func(i, j int) bool {
			return pq.pool[i].lastAccessedTime < pq.pool[j].lastAccessedTime
		})
	} else if lastAccessedTime < pq.pool[len(pq.pool)-1].lastAccessedTime {
		removedItem := pq.pool[len(pq.pool)-1]
		delete(pq.keyset, removedItem.key)

		pq.pool[len(pq.pool)-1] = item
		pq.keyset[key] = item

		sort.Slice(pq.pool, func(i, j int) bool {
			return pq.pool[i].lastAccessedTime < pq.pool[j].lastAccessedTime
		})
	}
}

// Pop removes and returns the oldest key from the pool
func (pq *EvictionPool) Pop() *PoolItem {
	if len(pq.pool) == 0 {
		return nil
	}

	item := pq.pool[0]
	pq.pool = pq.pool[1:]
	delete(pq.keyset, item.key)
	return item
}