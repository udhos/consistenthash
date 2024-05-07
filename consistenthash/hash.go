// Package consistenthash implements consistent hashing.
package consistenthash

import (
	"sort"
	"strconv"

	"github.com/cespare/xxhash"
)

type digestFunc func(data []byte) uint64

// Hash holds consistent hash.
type Hash struct {
	options    Options
	sortedKeys []int
	table      map[int]string // hash => key
}

// Options configure hash.
type Options struct {
	replicas int
	digest   digestFunc
}

// New creates consistent hash.
func New(options Options) *Hash {
	if options.replicas == 0 {
		options.replicas = 10
	}
	if options.digest == nil {
		options.digest = xxhash.Sum64
	}
	h := &Hash{
		options: options,
		table:   map[int]string{},
	}
	return h
}

// AddNodes adds keys that can actually be retrieved (nodes).
func (h *Hash) AddNodes(keys []string) {
	for _, key := range keys {
		for i := 0; i < h.options.replicas; i++ {
			sum := int(h.options.digest([]byte(strconv.Itoa(i) + key)))
			h.sortedKeys = append(h.sortedKeys, sum)
			h.table[sum] = key
		}
	}
	sort.Ints(h.sortedKeys)
}

// GetNode retrieves node responsible for key.
func (h *Hash) GetNode(key string) string {
	if len(h.sortedKeys) == 0 {
		return ""
	}

	sum := int(h.options.digest([]byte(key)))

	index := sort.Search(len(h.sortedKeys), func(i int) bool { return h.sortedKeys[i] >= sum })

	if index == len(h.sortedKeys) {
		index = 0
	}

	return h.table[h.sortedKeys[index]]
}
