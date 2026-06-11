package chashing

// Mathematical Engine for HashRings
import (
	"hash/crc32"
	"sort"
	"strconv"
)

type HashRing struct {
	ring          []uint32
	virtualToNode map[uint32]string
	vNodeCnt      int
}

func New(vNodeCnt int) *HashRing {
	return &HashRing{
		ring:          make([]uint32, 0),
		virtualToNode: make(map[uint32]string),
		vNodeCnt:      vNodeCnt,
	}
}

func (h *HashRing) sortRing() {
	sort.Slice(h.ring, func(i, j int) bool {
		return h.ring[i] < h.ring[j]
	})
}

func (h *HashRing) AddNode(node string) {
	for i := 0; i < h.vNodeCnt; i++ {
		vnodeKey := node + "#" + strconv.Itoa(i)
		hash := crc32.ChecksumIEEE([]byte(vnodeKey))

		h.ring = append(h.ring, hash)
		h.virtualToNode[hash] = node
	}

	h.sortRing()
}

func (h *HashRing) searchKeyNode(hash uint32) int {
	return sort.Search(len(h.ring), func(i int) bool {
		return h.ring[i] >= hash
	})
}

func (h *HashRing) GetNode(key string) string {
	if len(h.ring) == 0 {
		return ""
	}

	hash := crc32.ChecksumIEEE([]byte(key))
	idx := h.searchKeyNode(hash)

	if idx == len(h.ring) {
		idx = 0
	}

	return h.virtualToNode[h.ring[idx]]
}
