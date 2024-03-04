package dao

import (
	"github.com/chroma-core/chroma/go/internal/metastore/db/dbmodel"
	"gorm.io/gorm"
)

type notificationDb struct {
	db *gorm.DB
}

var _ dbmodel.INotificationDb = &notificationDb{}

func (s *notificationDb) DeleteAll() error {
	return s.db.Where("1 = 1").Delete(&dbmodel.Notification{}).Error
}

func (s *notificationDb) Delete(id []int64) error {
	return s.db.Where("id IN ?", id).Delete(&dbmodel.Notification{}).Error
}

func (s *notificationDb) Insert(in *dbmodel.Notification) error {
	return s.db.Create(in).Error
}

func (s *notificationDb) GetNotificationByCollectionID(collectionID string) ([]*dbmodel.Notification, error) {
	var notifications []*dbmodel.Notification
	err := s.db.Where("collection_id = ? AND status = ?", collectionID, dbmodel.NotificationStatusPending).Find(&notifications).Error
	if err != nil {
		return nil, err
	}
	return notifications, nil
}

func (s *notificationDb) GetAllPendingNotifications() ([]*dbmodel.Notification, error) {
	var notifications []*dbmodel.Notification
	err := s.db.Where("status = ?", dbmodel.NotificationStatusPending).Find(&notifications).Error
	if err != nil {
		return nil, err
	}
	return notifications, nil
}
