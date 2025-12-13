package grendezvous

import (
	"fmt"
	"hash/fnv"
	"testing"
)

func fnvHasher(b []byte) uint64 {
	h := fnv.New64a()
	_, _ = h.Write(b)
	return h.Sum64()
}

func TestRendezvousBalance(t *testing.T) {
	nodes := make([]string, 16)
	for i := range nodes {
		nodes[i] = fmt.Sprintf("node-%d", i)
	}
	r := New(nodes, fnvHasher)

	totalKeys := 100000
	counts := make(map[string]int, len(nodes))
	for i := 0; i < totalKeys; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		n := r.Lookup(key)
		counts[n]++
	}

	expected := float64(totalKeys) / float64(len(nodes))
	tolerance := 0.05
	for _, n := range nodes {
		c := float64(counts[n])
		diff := c - expected
		if diff < 0 {
			diff = -diff
		}
		if diff/expected > tolerance {
			t.Fatalf("distribution not within tolerance: node=%s count=%d expected≈%.0f tolerance=%.2f", n, counts[n], expected, tolerance)
		}
	}
}

func BenchmarkRendezvousLookup(b *testing.B) {
	nodes := make([]string, 64)
	for i := range nodes {
		nodes[i] = fmt.Sprintf("node-%d", i)
	}
	r := New(nodes, fnvHasher)

	keys := make([][]byte, 4096)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("key-%d", i))
	}

	b.Run("single", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = r.Lookup(keys[i%len(keys)])
		}
	})

	b.Run("parallel", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				_ = r.Lookup(keys[i%len(keys)])
				i++
			}
		})
	})
}
