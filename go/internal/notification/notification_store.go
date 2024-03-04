package notification

import (
	"context"

	"github.com/chroma-core/chroma/go/internal/model"
)

type NotificationStore interface {
	GetAllPendingNotifications(ctx context.Context) (map[string][]model.Notification, error)
	GetNotifications(ctx context.Context, collecitonID string) ([]model.Notification, error)
	AddNotification(ctx context.Context, notification model.Notification) error
	RemoveNotifications(ctx context.Context, notifications []model.Notification) error
}
