package coordinator

import (
	"context"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/chroma/chroma-coordinator/internal/metastore/coordinator"
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
}

func NewCoordinator(ctx context.Context, assignmentPolicy CollectionAssignmentPolicy, db *gorm.DB) (*Coordinator, error) {
	s := &Coordinator{
		ctx:                        ctx,
		collectionAssignmentPolicy: assignmentPolicy,
	}

	notificationStore := notification.NewMemoryNotificationStore()
	notifier, err := createPulsarNotifer()
	if err != nil {
		return nil, err
	}
	notificationProcessor := notification.NewSimpleNotificationProcessor(notificationStore, notifier)

	catalog := coordinator.NewMemoryCatalog()
	meta, err := NewMetaTable(s.ctx, catalog)
	if err != nil {
		return nil, err
	}
	meta.SetNotificationProcessor(notificationProcessor)
	notificationProcessor.Start()
	s.meta = meta

	return s, nil
}

func (s *Coordinator) Start() error {
	return nil
}

func (s *Coordinator) Stop() error {
	return nil
}

func (c *Coordinator) assignCollection(collectionID types.UniqueID) (string, error) {
	return c.collectionAssignmentPolicy.AssignCollection(collectionID)
}

func createPulsarNotifer() (*PulsarNotifier, error) {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: "pulsar://localhost:6650",
	})
	if err != nil {
		return nil, err
	}

	// defer client.Close()

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: "notification-topic",
	})
	if err != nil {
		return nil, err
	}

	// defer producer.Close()
	notifier := NewPulsarNotifier(producer)
	return notifier, nil
}
