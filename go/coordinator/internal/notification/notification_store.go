package notification

import (
	"context"

	"github.com/chroma/chroma-coordinator/internal/metastore/db/dbmodel"
	"github.com/chroma/chroma-coordinator/internal/model"
)

type NotificationStore interface {
	GetAllPendingNotifications(ctx context.Context) (map[string][]model.Notification, error)
	GetNotifications(ctx context.Context, collecitonID string) ([]model.Notification, error)
	AddNotification(ctx context.Context, notification model.Notification) error
	RemoveNotification(ctx context.Context, notification model.Notification) error
}

type MemoryNotificationStore struct {
	notifications map[string][]model.Notification
}

var _ NotificationStore = &MemoryNotificationStore{}

func NewMemoryNotificationStore() *MemoryNotificationStore {
	return &MemoryNotificationStore{
		notifications: make(map[string][]model.Notification),
	}
}

func (m *MemoryNotificationStore) GetAllPendingNotifications(ctx context.Context) (map[string][]model.Notification, error) {
	return m.notifications, nil
}

func (m *MemoryNotificationStore) GetNotifications(ctx context.Context, collectionID string) ([]model.Notification, error) {
	return m.notifications[collectionID], nil
}

func (m *MemoryNotificationStore) AddNotification(ctx context.Context, notification model.Notification) error {
	m.notifications[notification.CollectionID] = append(m.notifications[notification.CollectionID], notification)
	return nil
}

func (m *MemoryNotificationStore) RemoveNotification(ctx context.Context, notification model.Notification) error {
	notifications := m.notifications[notification.CollectionID]
	for i, n := range notifications {
		if n.ID == notification.ID {
			m.notifications[notification.CollectionID] = append(notifications[:i], notifications[i+1:]...)
			break
		}
	}
	return nil
}

type DatabaseNotificationStore struct {
	metaDomain dbmodel.IMetaDomain
	txImpl     dbmodel.ITransaction
}

var _ NotificationStore = &DatabaseNotificationStore{}

func NewDatabaseNotificationStore(metaDomain dbmodel.IMetaDomain, txImpl dbmodel.ITransaction) *DatabaseNotificationStore {
	return &DatabaseNotificationStore{
		metaDomain: metaDomain,
		txImpl:     txImpl,
	}
}

func (d *DatabaseNotificationStore) GetAllPendingNotifications(ctx context.Context) (map[string][]model.Notification, error) {
	notifications, err := d.metaDomain.NotificationDb(ctx).GetAllPendingNotifications()
	if err != nil {
		return nil, err
	}

	notificationMap := make(map[string][]model.Notification)
	for _, notification := range notifications {
		notificationMap[notification.CollectionID] = append(notificationMap[notification.CollectionID], model.Notification{
			ID:           notification.ID,
			CollectionID: notification.CollectionID,
			Type:         notification.Type,
			Status:       notification.Status,
		})
	}
	return notificationMap, nil
}

func (d *DatabaseNotificationStore) GetNotifications(ctx context.Context, collectionID string) ([]model.Notification, error) {
	notifications, err := d.metaDomain.NotificationDb(ctx).GetNotificationByCollectionID(collectionID)
	if err != nil {
		return nil, err
	}

	var result []model.Notification
	for _, notification := range notifications {
		result = append(result, model.Notification{
			ID:           notification.ID,
			CollectionID: notification.CollectionID,
			Type:         notification.Type,
			Status:       notification.Status,
		})
	}
	return result, nil
}

func (d *DatabaseNotificationStore) AddNotification(ctx context.Context, notification model.Notification) error {
	return d.txImpl.Transaction(ctx, func(ctx context.Context) error {
		err := d.metaDomain.NotificationDb(ctx).Insert(&dbmodel.Notification{
			CollectionID: notification.CollectionID,
			Type:         notification.Type,
			Status:       notification.Status,
		})
		if err != nil {
			return err
		}
		return nil
	})
}

func (d *DatabaseNotificationStore) RemoveNotification(ctx context.Context, notification model.Notification) error {
	return d.txImpl.Transaction(ctx, func(ctx context.Context) error {
		err := d.metaDomain.NotificationDb(ctx).Delete(notification.ID)
		if err != nil {
			return err
		}
		return nil
	})
}
