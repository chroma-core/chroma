package memberlist_manager

type INode interface {
	// Get the ip of the node
	GetIP() string
}

// A memberlist represents all ready nodes in the cluster
type Memberlist struct {
	// List of node ips and statuses
	Nodes []INode
}

type IMemberlistStore interface {
	// Get the current memberlist or error
	GetMemberlist() (Memberlist, error)
	// Update the memberlist
	UpdateMemberlist(memberlist Memberlist) error
}

// A mock memberlist store that stores the memberlist in memory
type MockMemberlistStore struct {
	memberlist Memberlist
}

// Get the current memberlist or error
func (s *MockMemberlistStore) GetMemberlist() (Memberlist, error) {
	return s.memberlist, nil
}

// Update the memberlist
func (s *MockMemberlistStore) UpdateMemberlist(memberlist Memberlist) error {
	// passes the memberlist by value so that the memberlist is copied, this is to prevent the memberlist from being modified
	// outside of the memberlist manager. Since it will be small, this should not be a problem
	s.memberlist = memberlist
	return nil
}
