package sysdb

import "context"

type MockSysDB struct {
	collections map[string]bool
}

func NewMockSysDB() *MockSysDB {
	return &MockSysDB{
		collections: make(map[string]bool),
	}
}

func (s *MockSysDB) CheckCollection(ctx context.Context, collectionId string) (bool, error) {
	_, ok := s.collections[collectionId]
	return ok, nil
}

func (s *MockSysDB) AddCollection(ctx context.Context, collectionId string) error {
	s.collections[collectionId] = true
	return nil
}
