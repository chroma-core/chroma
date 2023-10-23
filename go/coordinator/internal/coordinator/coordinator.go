package coordinator

import (
	"context"

	"github.com/chroma/chroma-coordinator/internal/metastore/coordinator"
	"github.com/chroma/chroma-coordinator/internal/types"
	"gorm.io/gorm"
)

type Component interface {
	Start() error
	Stop() error
}

var _ ICoordinator = (*Coordinator)(nil)

type Coordinator struct {
	ctx                        context.Context
	collectionAssignmentPolicy CollectionAssignmentPolicy
	meta                       IMeta
}

func NewCoordinator(ctx context.Context, assignmentPolicy CollectionAssignmentPolicy, db *gorm.DB) (*Coordinator, error) {
	s := &Coordinator{
		ctx:                        ctx,
		collectionAssignmentPolicy: assignmentPolicy,
	}

	catalog := coordinator.NewMemoryCatalog()
	meta, err := NewMetaTable(s.ctx, catalog)
	if err != nil {
		return nil, err
	}
	s.meta = meta

	return s, nil
}

func (s *Coordinator) Start() error {
	return nil
}

func (s *Coordinator) Stop() error {
	return nil
}

func (c *Coordinator) assignCollection(collectionID types.UniqueID) string {
	return c.collectionAssignmentPolicy.AssignCollection(collectionID)
}
