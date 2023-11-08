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

func Murmur3Hasher(member string, key string) uint64 {
	hasher := murmur3.New64()
	hasher.Write([]byte(member))
	hasher.Write([]byte(key))
	return hasher.Sum64()
}
