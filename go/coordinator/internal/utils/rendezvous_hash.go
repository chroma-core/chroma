package utils

import (
	"errors"

	"github.com/spaolacci/murmur3"
)

type Hasher = func(member string, key string) uint64
type Member = string
type Members = []Member
type Key = string

// assign assigns a key to a member using the rendezvous hashing algorithm.
func Assign(key Key, members Members, hasher Hasher) (Member, error) {
	if len(members) == 0 {
		return "", errors.New("cannot assign key to empty member list")
	}
	if len(members) == 1 {
		return members[0], nil
	}
	if key == "" {
		return "", errors.New("cannot assign empty key")
	}

	maxScore := uint64(0)
	var maxMember Member

	for _, member := range members {
		score := hasher(string(member), string(key))
		if score > maxScore {
			maxScore = score
			maxMember = member
		}
	}

	return maxMember, nil
}

func mergeHashes(a uint64, b uint64) uint64 {
	acc := a ^ b
	acc ^= acc >> 33
	acc *= 0xff51afd7ed558ccd
	acc ^= acc >> 33
	acc *= 0xc4ceb9fe1a85ec53
	acc ^= acc >> 33
	return acc
}

// NOTE: The python implementation of murmur3 may differ from the golang implementation.
// For now, this is fine since go and python don't need to agree on any hashing schemes
// but if we ever need to agree on a hashing scheme, we should verify that the implementations
// are the same.
func Murmur3Hasher(member string, key string) uint64 {
	hasher := murmur3.New64()
	hasher.Write([]byte(member))
	memberHash := hasher.Sum64()
	hasher.Reset()
	hasher.Write([]byte(key))
	keyHash := hasher.Sum64()
	return mergeHashes(memberHash, keyHash)
}
