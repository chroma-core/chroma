package coordinator

import (
	"fmt"

	"github.com/chroma-core/chroma/go/pkg/types"
	"github.com/chroma-core/chroma/go/pkg/utils"
)

type CollectionAssignmentPolicy interface {
	AssignCollection(collectionID types.UniqueID) (string, error)
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

func (s *SimpleAssignmentPolicy) AssignCollection(collectionID types.UniqueID) (string, error) {
	return createTopicName(s.tenantID, s.topicNS, collectionID.String()), nil
}

func createTopicName(tenantID string, topicNS string, log_name string) string {
	return fmt.Sprintf("persistent://%s/%s/%s", tenantID, topicNS, log_name)
}

// RendezvousAssignmentPolicy is an assignment policy that assigns a collection to a topic
// For now it assumes there are 16 topics and uses the rendezvous hashing algorithm to
// assign a collection to a topic.

var Topics = [16]string{
	"chroma_log_0",
	"chroma_log_1",
	"chroma_log_2",
	"chroma_log_3",
	"chroma_log_4",
	"chroma_log_5",
	"chroma_log_6",
	"chroma_log_7",
	"chroma_log_8",
	"chroma_log_9",
	"chroma_log_10",
	"chroma_log_11",
	"chroma_log_12",
	"chroma_log_13",
	"chroma_log_14",
	"chroma_log_15",
}

type RendezvousAssignmentPolicy struct {
	tenantID string
	topicNS  string
}

func NewRendezvousAssignmentPolicy(tenantID string, topicNS string) *RendezvousAssignmentPolicy {
	return &RendezvousAssignmentPolicy{
		tenantID: tenantID,
		topicNS:  topicNS,
	}
}

func (r *RendezvousAssignmentPolicy) AssignCollection(collectionID types.UniqueID) (string, error) {
	assignment, error := utils.Assign(collectionID.String(), Topics[:], utils.Murmur3Hasher)
	if error != nil {
		return "", error
	}
	return createTopicName(r.tenantID, r.topicNS, assignment), nil
}
