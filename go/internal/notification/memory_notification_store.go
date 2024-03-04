package notification

import (
	"context"
	"sort"

	"github.com/chroma-core/chroma/go/internal/model"
)

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
	result := make(map[string][]model.Notification)
	for collectionID, notifications := range m.notifications {
		for _, notification := range notifications {
			if notification.Status == model.NotificationStatusPending {
				result[collectionID] = append(result[collectionID], notification)
			}
		}
		// sort notifications by ID
		sort.Slice(result[collectionID], func(i, j int) bool {
			return result[collectionID][i].ID < result[collectionID][j].ID
		})
	}
	return result, nil
}

func (m *MemoryNotificationStore) GetNotifications(ctx context.Context, collectionID string) ([]model.Notification, error) {
	notifications, ok := m.notifications[collectionID]
	if !ok {
		return nil, nil
	}
	// sort notifications by ID
	sort.Slice(notifications, func(i, j int) bool {
		return notifications[i].ID < notifications[j].ID
	})
	return notifications, nil
}

func (m *MemoryNotificationStore) AddNotification(ctx context.Context, notification model.Notification) error {
	m.notifications[notification.CollectionID] = append(m.notifications[notification.CollectionID], notification)
	return nil
}

func (m *MemoryNotificationStore) RemoveNotifications(ctx context.Context, notifications []model.Notification) error {
	for _, notification := range notifications {
		for i, n := range m.notifications[notification.CollectionID] {
			if n.ID == notification.ID {
				m.notifications[notification.CollectionID] = append(m.notifications[notification.CollectionID][:i], m.notifications[notification.CollectionID][i+1:]...)
				break
			}
		}
	}
	return nil
}
