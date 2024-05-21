package notification

import (
	"context"
	"sync/atomic"

	"github.com/chroma-core/chroma/go/pkg/common"
	"github.com/chroma-core/chroma/go/pkg/model"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type NotificationProcessor interface {
	common.Component
	Process(ctx context.Context) error
	Trigger(ctx context.Context, triggerMsg TriggerMessage)
}

type SimpleNotificationProcessor struct {
	ctx         context.Context
	store       NotificationStore
	notifer     Notifier
	channel     chan TriggerMessage
	doneChannel chan bool
	running     atomic.Bool
}

type TriggerMessage struct {
	Msg        model.Notification
	ResultChan chan error
}

const triggerChannelSize = 1000

var _ NotificationProcessor = &SimpleNotificationProcessor{}

func NewSimpleNotificationProcessor(ctx context.Context, store NotificationStore, notifier Notifier) *SimpleNotificationProcessor {
	return &SimpleNotificationProcessor{
		ctx:         ctx,
		store:       store,
		notifer:     notifier,
		channel:     make(chan TriggerMessage, triggerChannelSize),
		doneChannel: make(chan bool),
	}
}

func (n *SimpleNotificationProcessor) Start() error {
	// During startup, first sending all pending notifications in the store to the notification topic
	log.Info("Starting notification processor")
	err := n.sendPendingNotifications(n.ctx)
	if err != nil {
		log.Error("Failed to send pending notifications", zap.Error(err))
		return err
	}
	n.running.Store(true)
	go n.Process(n.ctx)
	return nil
}

func (n *SimpleNotificationProcessor) Stop() error {
	n.running.Store(false)
	n.doneChannel <- true
	return nil
}

func (n *SimpleNotificationProcessor) Process(ctx context.Context) error {
	log.Info("Waiting for new notifications")
	for {
		select {
		case triggerMsg := <-n.channel:
			msg := triggerMsg.Msg
			log.Info("Received notification", zap.Any("msg", msg))
			running := n.running.Load()
			log.Info("Notification processor is running", zap.Bool("running", running))
			// We need to block here until the notifications are sent successfully
			for running {
				// Check the notification store if this notification is already processed
				// If it is already processed, just return
				// If it is not processed, send notifications and remove from the store
				notifications, err := n.store.GetNotifications(ctx, msg.CollectionID)
				if err != nil {
					log.Error("Failed to get notifications", zap.Error(err))
					triggerMsg.ResultChan <- err
					continue
				}
				if len(notifications) == 0 {
					log.Info("No pending notifications found")
					triggerMsg.ResultChan <- nil
					break
				}
				log.Info("Got notifications from notification store", zap.Any("notifications", notifications))
				err = n.notifer.Notify(ctx, notifications)
				if err != nil {
					log.Error("Failed to send pending notifications", zap.Error(err))
				} else {
					n.store.RemoveNotifications(ctx, notifications)
					log.Info("Rmove notifications from notification store", zap.Any("notifications", notifications))
					triggerMsg.ResultChan <- nil
					break
				}
			}
		case <-n.doneChannel:
			log.Info("Stopping notification processor")
			return nil
		}
	}
}

func (n *SimpleNotificationProcessor) Trigger(ctx context.Context, triggerMsg TriggerMessage) {
	log.Info("Triggering notification", zap.Any("msg", triggerMsg.Msg))
	if len(n.channel) == triggerChannelSize {
		log.Error("Notification channel is full, dropping notification", zap.Any("msg", triggerMsg.Msg))
		triggerMsg.ResultChan <- nil
		return
	}
	n.channel <- triggerMsg
}

func (n *SimpleNotificationProcessor) sendPendingNotifications(ctx context.Context) error {
	notificationMap, err := n.store.GetAllPendingNotifications(ctx)
	if err != nil {
		log.Error("Failed to get all pending notifications", zap.Error(err))
		return err
	}
	for collectionID, notifications := range notificationMap {
		log.Info("Sending pending notifications", zap.Any("collectionID", collectionID), zap.Any("notifications", notifications))
		for {
			err = n.notifer.Notify(ctx, notifications)
			if err != nil {
				log.Error("Failed to send pending notifications", zap.Error(err))
			} else {
				n.store.RemoveNotifications(ctx, notifications)
				break
			}
		}
	}
	return nil
}
