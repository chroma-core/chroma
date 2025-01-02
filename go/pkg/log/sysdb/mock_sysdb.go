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

func (s *MockSysDB) CheckCollections(ctx context.Context, collectionIds []string) ([]bool, error) {
	result := make([]bool, len(collectionIds))
	for i, collectionId := range collectionIds {
		_, ok := s.collections[collectionId]
		result[i] = ok
	}
	return result, nil
}

func (s *MockSysDB) AddCollection(ctx context.Context, collectionId string) error {
	s.collections[collectionId] = true
	return nil
}
