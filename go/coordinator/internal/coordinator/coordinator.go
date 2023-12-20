package coordinator

import (
	"context"
	"log"

	"github.com/chroma/chroma-coordinator/internal/metastore"
	"github.com/chroma/chroma-coordinator/internal/metastore/coordinator"
	"github.com/chroma/chroma-coordinator/internal/metastore/db/dao"
	"github.com/chroma/chroma-coordinator/internal/metastore/db/dbcore"
	"github.com/chroma/chroma-coordinator/internal/notification"
	"github.com/chroma/chroma-coordinator/internal/types"
	"gorm.io/gorm"
)

var _ ICoordinator = (*Coordinator)(nil)

// Coordinator is the implemenation of ICoordinator. It is the top level component.
// Currently, it only has the system catalog related APIs and will be extended to
// support other functionalities such as membership managed and propagation.
type Coordinator struct {
	ctx                        context.Context
	collectionAssignmentPolicy CollectionAssignmentPolicy
	meta                       IMeta
	notificationProcessor      notification.NotificationProcessor
}

func NewCoordinator(ctx context.Context, assignmentPolicy CollectionAssignmentPolicy, db *gorm.DB, notificationStore notification.NotificationStore, notifier notification.Notifier) (*Coordinator, error) {
	s := &Coordinator{
		ctx:                        ctx,
		collectionAssignmentPolicy: assignmentPolicy,
	}

	notificationProcessor := notification.NewSimpleNotificationProcessor(ctx, notificationStore, notifier)

	var catalog metastore.Catalog
	// TODO: move this to server.go
	if db == nil {
		catalog = coordinator.NewMemoryCatalogWithNotification(notificationStore)
	} else {
		txnImpl := dbcore.NewTxImpl()
		metaDomain := dao.NewMetaDomain()
		catalog = coordinator.NewTableCatalogWithNotification(txnImpl, metaDomain, notificationStore)
	}
	meta, err := NewMetaTable(s.ctx, catalog)
	if err != nil {
		return nil, err
	}
	meta.SetNotificationProcessor(notificationProcessor)

	s.meta = meta
	s.notificationProcessor = notificationProcessor

	return s, nil
}

func (s *Coordinator) Start() error {
	err := s.notificationProcessor.Start()
	if err != nil {
		log.Printf("Failed to start notification processor: %v", err)
		return err
	}
	return nil
}

func (s *Coordinator) Stop() error {
	err := s.notificationProcessor.Stop()
	if err != nil {
		log.Printf("Failed to stop notification processor: %v", err)
	}
	return nil
}

func (c *Coordinator) assignCollection(collectionID types.UniqueID) (string, error) {
	return c.collectionAssignmentPolicy.AssignCollection(collectionID)
}
