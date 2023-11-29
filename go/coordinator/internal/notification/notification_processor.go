package notification

import (
	"context"
	"time"

	"github.com/chroma/chroma-coordinator/internal/common"
	"github.com/chroma/chroma-coordinator/internal/model"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type NotificationProcessor interface {
	common.Component
	Process(ctx context.Context)
	Trigger(ctx context.Context, triggerMsg TriggerMessage)
}

type SimpleNotificationProcessor struct {
	store   NotificationStore
	notifer Notifier
	channel chan TriggerMessage
}

type TriggerMessage struct {
	Msg        model.Notification
	ResultChan chan error
}

var _ NotificationProcessor = &SimpleNotificationProcessor{}

func NewSimpleNotificationProcessor(store NotificationStore, notifier Notifier) *SimpleNotificationProcessor {
	return &SimpleNotificationProcessor{
		store:   store,
		notifer: notifier,
		channel: make(chan TriggerMessage),
	}
}

func (n *SimpleNotificationProcessor) Start() error {
	go n.Process(context.Background())
	return nil
}

func (n *SimpleNotificationProcessor) Stop() error {
	return nil
}

func (n *SimpleNotificationProcessor) Process(ctx context.Context) {
	// Needs to put the logic in the notification processor class
	log.Info("Starting notification processor")
	ticker := time.NewTicker(1000 * time.Millisecond)
	for {
		select {
		case triggerMsg := <-n.channel:
			msg := triggerMsg.Msg
			log.Info("Received notification", zap.Any("msg", msg))
			err := n.notifer.Notify(ctx, msg)
			if err != nil {
				log.Error("Failed to notify", zap.Error(err))
			} else {
				log.Info("Notified", zap.Any("msg", msg))
			}
			triggerMsg.ResultChan <- err
		case <-ticker.C:
			log.Info("Checking pending notifications")
			msgs, err := n.store.GetAllPendingNotifications(ctx)
			if err != nil {
				log.Error("Failed to get all pending notifications", zap.Error(err))
			} else {
				for _, value := range msgs {
					for _, msg := range value {
						err := n.notifer.Notify(ctx, msg)
						if err != nil {
							log.Error("Failed to notify", zap.Error(err))
						} else {
							log.Info("Notified", zap.Any("msg", msg))
						}
					}
				}
			}
		}
	}
}

func (n *SimpleNotificationProcessor) Trigger(ctx context.Context, triggerMsg TriggerMessage) {
	log.Info("Triggering notification", zap.Any("msg", triggerMsg.Msg))
	n.channel <- triggerMsg
}
