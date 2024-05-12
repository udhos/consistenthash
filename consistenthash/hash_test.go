package consistenthash

import (
	"fmt"
	"strconv"
	"testing"
)

func TestHashing(t *testing.T) {

	// Override the hash function to return easier to reason about values. Assumes
	// the keys can be converted to an integer.
	hash := New(Options{
		replicas: 3,
		digest: func(key []byte) uint32 {
			i, err := strconv.Atoi(string(key))
			if err != nil {
				panic(err)
			}
			return uint32(i)
		},
	})

	// Given the above hash function, this will give replicas with "hashes":
	// 2, 4, 6, 12, 14, 16, 22, 24, 26
	hash.AddNodes([]string{"6", "4", "2"})

	testCases := map[string]string{
		"2":  "2",
		"11": "2",
		"23": "4",
		"27": "2",
	}

	for k, v := range testCases {
		if hash.GetNode(k) != v {
			t.Errorf("Asking for %s, should have yielded %s", k, v)
		}
	}

	// Adds 8, 18, 28
	hash.AddNodes([]string{"8"})

	// 27 should now map to 8.
	testCases["27"] = "8"

	for k, v := range testCases {
		if hash.GetNode(k) != v {
			t.Errorf("Asking for %s, should have yielded %s", k, v)
		}
	}

}

func TestConsistency(t *testing.T) {
	hash1 := New(Options{replicas: 1})
	hash2 := New(Options{replicas: 1})

	hash1.AddNodes([]string{"Bill", "Bob", "Bonny"})
	hash2.AddNodes([]string{"Bob", "Bonny", "Bill"})

	if hash1.GetNode("Ben") != hash2.GetNode("Ben") {
		t.Errorf("Fetching 'Ben' from both hashes should be the same")
	}

	hash2.AddNodes([]string{"Becky", "Ben", "Bobby"})

	if s1, s2 := hash1.GetNode("Ben"), hash2.GetNode("Ben"); s1 != s2 {
		t.Errorf("Direct matches should always return the same entry Ben: %s != %s", s1, s2)
	}

	if s1, s2 := hash1.GetNode("Bob"), hash2.GetNode("Bob"); s1 != s2 {
		t.Errorf("Direct matches should always return the same entry Bob: %s != %s", s1, s2)
	}

	if s1, s2 := hash1.GetNode("Bonny"), hash2.GetNode("Bonny"); s1 != s2 {
		t.Errorf("Direct matches should always return the same entry Bonny: %s != %s", s1, s2)
	}

}

/*
func (h *Hash) dump(s string) {
	for i, sum := range h.sortedKeys {
		k := h.table[sum]
		fmt.Printf("%s: %d: %s: %s\n", s, i, k, h.GetNode(k))
	}
}
*/

func BenchmarkGet8(b *testing.B)   { benchmarkGet(b, 8) }
func BenchmarkGet32(b *testing.B)  { benchmarkGet(b, 32) }
func BenchmarkGet128(b *testing.B) { benchmarkGet(b, 128) }
func BenchmarkGet512(b *testing.B) { benchmarkGet(b, 512) }

func benchmarkGet(b *testing.B, shards int) {

	hash := New(Options{replicas: 50})

	var buckets []string
	for i := 0; i < shards; i++ {
		buckets = append(buckets, fmt.Sprintf("shard-%d", i))
	}

	hash.AddNodes(buckets)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		hash.GetNode(buckets[i&(shards-1)])
	}
}
