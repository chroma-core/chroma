package notification

import (
	"context"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/chroma-core/chroma/go/internal/model"
	"github.com/chroma-core/chroma/go/internal/proto/coordinatorpb"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type Notifier interface {
	Notify(ctx context.Context, notifications []model.Notification) error
}

type PulsarNotifier struct {
	producer pulsar.Producer
}

var _ Notifier = &PulsarNotifier{}

func NewPulsarNotifier(producer pulsar.Producer) *PulsarNotifier {
	return &PulsarNotifier{
		producer: producer,
	}
}

func (p *PulsarNotifier) Notify(ctx context.Context, notifications []model.Notification) error {
	for _, notification := range notifications {
		notificationPb := coordinatorpb.Notification{
			CollectionId: notification.CollectionID,
			Type:         notification.Type,
			Status:       notification.Status,
		}
		payload, err := proto.Marshal(&notificationPb)
		if err != nil {
			log.Error("Failed to marshal notification", zap.Error(err))
			return err
		}
		message := &pulsar.ProducerMessage{
			Key:     notification.CollectionID,
			Payload: payload,
		}
		// Since the number of notifications is small, we can send them synchronously
		// for now. This is easy to reason about hte order of notifications.
		//
		// As follow up optimizations, we can send them asynchronously in batches and
		// track failed messages.
		_, err = p.producer.Send(ctx, message)
		if err != nil {
			log.Error("Failed to send message", zap.Error(err))
			return err
		}
		log.Info("Published message", zap.Any("message", message))

	}
	return nil
}

type MemoryNotifier struct {
	queue []pulsar.ProducerMessage
}

var _ Notifier = &MemoryNotifier{}

func NewMemoryNotifier() *MemoryNotifier {
	return &MemoryNotifier{
		queue: make([]pulsar.ProducerMessage, 0),
	}
}

func (m *MemoryNotifier) Notify(ctx context.Context, notifications []model.Notification) error {
	for _, notification := range notifications {
		notificationPb := coordinatorpb.Notification{
			CollectionId: notification.CollectionID,
			Type:         notification.Type,
			Status:       notification.Status,
		}
		payload, err := proto.Marshal(&notificationPb)
		if err != nil {
			log.Error("Failed to marshal notification", zap.Error(err))
			return err
		}
		message := pulsar.ProducerMessage{
			Key:     notification.CollectionID,
			Payload: payload,
		}
		m.queue = append(m.queue, message)
		log.Info("Published message", zap.Any("message", message))
	}
	return nil
}
