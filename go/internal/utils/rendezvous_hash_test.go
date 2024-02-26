package utils

import (
	"fmt"
	"math"
	"testing"
)

func mockHasher(member string, key string) uint64 {
	members := []string{"a", "b", "c"}
	for i, m := range members {
		if m == member {
			return uint64(i)
		}
	}
	return 0
}

func TestRendezvousHash(t *testing.T) {
	members := []string{"a", "b", "c"}
	key := "key"

	// Test that the assign function returns the expected result
	node, error := Assign(key, members, mockHasher)

	if error != nil {
		t.Errorf("Assign() returned an error: %v", error)
	}

	if node != "c" {
		t.Errorf("Assign() = %v, want %v", node, "c")
	}
}

func TestEvenDistribution(t *testing.T) {
	memberCount := 10
	tolerance := 25
	var nodes []string
	for i := 0; i < memberCount; i++ {
		nodes = append(nodes, fmt.Sprint(i+'0')) // Convert int to string
	}

	keyDistribution := make(map[string]int)
	numKeys := 1000

	// Test if keys are evenly distributed across nodes
	for i := 0; i < numKeys; i++ {
		key := "key_" + fmt.Sprint(i)
		node, err := Assign(key, nodes, Murmur3Hasher)
		if err != nil {
			t.Errorf("Assign() returned an error: %v", err)
		}
		keyDistribution[node]++
	}

	// Check if keys are somewhat evenly distributed
	for _, count := range keyDistribution {
		if math.Abs(float64(count-numKeys/memberCount)) > float64(tolerance) {
			t.Errorf("Key distribution is uneven: %v", keyDistribution)
		}
	}
}
