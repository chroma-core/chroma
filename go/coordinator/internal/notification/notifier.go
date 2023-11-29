package notification

import (
	"context"
	"encoding/json"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/chroma/chroma-coordinator/internal/model"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type Notifier interface {
	Notify(ctx context.Context, msg model.Notification) error
}

type PulsarNotifier struct {
	producer pulsar.Producer
}

func NewPulsarNotifier(producer pulsar.Producer) *PulsarNotifier {
	return &PulsarNotifier{
		producer: producer,
	}
}

func (p *PulsarNotifier) Notify(ctx context.Context, msg model.Notification) error {
	payload, err := json.Marshal(msg)
	if err != nil {
		log.Error("Failed to marshal notification", zap.Error(err))
		return err
	}
	message := &pulsar.ProducerMessage{
		Key:     msg.CollectionID,
		Payload: payload,
	}
	p.producer.SendAsync(ctx, message, func(msgID pulsar.MessageID, producerMessage *pulsar.ProducerMessage, err error) {
		if err != nil {
			log.Error("Failed to send message", zap.Error(err))
		} else {
			log.Info("Published message", zap.String("messageID", msgID.String()))
		}
	})
	p.producer.Flush()
	return nil
}
