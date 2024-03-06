package notification

import (
	"context"
	"sort"

	"github.com/chroma-core/chroma/go/pkg/metastore/db/dbmodel"
	"github.com/chroma-core/chroma/go/pkg/model"
)

type DatabaseNotificationStore struct {
	metaDomain dbmodel.IMetaDomain
	txImpl     dbmodel.ITransaction
}

var _ NotificationStore = &DatabaseNotificationStore{}

func NewDatabaseNotificationStore(txImpl dbmodel.ITransaction, metaDomain dbmodel.IMetaDomain) *DatabaseNotificationStore {
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
		// sort notifications by ID, this is ok because of the small number of notifications
		sort.Slice(notificationMap[notification.CollectionID], func(i, j int) bool {
			return notificationMap[notification.CollectionID][i].ID < notificationMap[notification.CollectionID][j].ID
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
	// sort notifications by ID, this is ok because of the small number of notifications
	sort.Slice(result, func(i, j int) bool {
		return result[i].ID < result[j].ID
	})
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

func (d *DatabaseNotificationStore) RemoveNotifications(ctx context.Context, notification []model.Notification) error {
	return d.txImpl.Transaction(ctx, func(ctx context.Context) error {
		ids := make([]int64, 0, len(notification))
		for _, n := range notification {
			ids = append(ids, n.ID)
		}
		err := d.metaDomain.NotificationDb(ctx).Delete(ids)
		if err != nil {
			return err
		}
		return nil
	})
}
