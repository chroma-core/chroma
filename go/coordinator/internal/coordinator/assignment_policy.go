package coordinator

import (
	"fmt"

	"github.com/chroma/chroma-coordinator/internal/types"
)

type CollectionAssignmentPolicy interface {
	AssignCollection(collectionID types.UniqueID) string
}

// SimpleAssignmentPolicy is a simple assignment policy that assigns a 1 collection to 1
// topic based on the id of the collection.
type SimpleAssignmentPolicy struct {
	tenantID string
	topicNS  string
}

func NewSimpleAssignmentPolicy(tenantID string, topicNS string) *SimpleAssignmentPolicy {
	return &SimpleAssignmentPolicy{
		tenantID: tenantID,
		topicNS:  topicNS,
	}
}

func (s *SimpleAssignmentPolicy) AssignCollection(collectionID types.UniqueID) string {
	return createTopicName(s.tenantID, s.topicNS, collectionID.String())
}

func createTopicName(tenantID string, topicNS string, collectionID string) string {
	return fmt.Sprintf("persistent://%s/%s/%s", tenantID, topicNS, collectionID)
}
