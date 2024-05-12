// Package consistenthash implements consistent hashing.
package consistenthash

import (
	"hash/crc32"
	"sort"
	"strconv"
)

type digestFunc func(data []byte) uint32

// Hash holds consistent hash.
type Hash struct {
	options    Options
	sortedKeys []entry
}

type entry struct {
	sum int
	key string // maps sum back to key
}

// Options configure hash.
type Options struct {
	replicas int
	digest   digestFunc
}

// New creates consistent hash.
func New(options Options) *Hash {
	if options.replicas == 0 {
		options.replicas = 50
	}
	if options.digest == nil {
		options.digest = crc32.ChecksumIEEE
	}
	h := &Hash{
		options: options,
	}
	return h
}

// AddNodes adds keys that can actually be retrieved (nodes).
func (h *Hash) AddNodes(keys []string) {
	for _, key := range keys {
		for i := range h.options.replicas {
			sum := int(h.options.digest([]byte(strconv.Itoa(i) + key)))
			h.sortedKeys = append(h.sortedKeys, entry{sum: sum, key: key})
		}
	}
	sort.Slice(h.sortedKeys, func(i, j int) bool { return h.sortedKeys[i].sum < h.sortedKeys[j].sum })
}

// GetNode retrieves node responsible for key.
func (h *Hash) GetNode(key string) string {
	if len(h.sortedKeys) == 0 {
		return ""
	}

	sum := int(h.options.digest([]byte(key)))

	index := sort.Search(len(h.sortedKeys), func(i int) bool { return h.sortedKeys[i].sum >= sum })

	if index == len(h.sortedKeys) {
		index = 0
	}

	return h.sortedKeys[index].key
}
