package coordinator

import (
	"context"
	"log"

	"github.com/chroma-core/chroma/go/pkg/metastore"
	"github.com/chroma-core/chroma/go/pkg/metastore/coordinator"
	"github.com/chroma-core/chroma/go/pkg/metastore/db/dao"
	"github.com/chroma-core/chroma/go/pkg/metastore/db/dbcore"
	"github.com/chroma-core/chroma/go/pkg/notification"
	"gorm.io/gorm"
)

var _ ICoordinator = (*Coordinator)(nil)

// Coordinator is the implemenation of ICoordinator. It is the top level component.
// Currently, it only has the system catalog related APIs and will be extended to
// support other functionalities such as membership managed and propagation.
type Coordinator struct {
	ctx                   context.Context
	notificationProcessor notification.NotificationProcessor
	catalog               metastore.Catalog
}

func NewCoordinator(ctx context.Context, db *gorm.DB, notificationStore notification.NotificationStore, notifier notification.Notifier) (*Coordinator, error) {
	s := &Coordinator{
		ctx: ctx,
	}

	notificationProcessor := notification.NewSimpleNotificationProcessor(ctx, notificationStore, notifier)
	s.notificationProcessor = notificationProcessor

	// catalog
	txnImpl := dbcore.NewTxImpl()
	metaDomain := dao.NewMetaDomain()
	s.catalog = coordinator.NewTableCatalogWithNotification(txnImpl, metaDomain, notificationStore)
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
