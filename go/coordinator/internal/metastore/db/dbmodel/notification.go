package dbmodel

type Notification struct {
	ID           int64  `gorm:"id;primaryKey"`
	CollectionID string `gorm:"collection_id"`
	Type         string `gorm:"type"`
	Status       string `gorm:"status"`
}

const (
	NotificationTypeCreateCollection = "create_collection"
	NotificationTypeDeleteCollection = "delete_collection"
)

const (
	NotificationStatusPending = "pending"
)

//go:generate mockery --name=IOutBoxDb
type INotificationDb interface {
	DeleteAll() error
	Delete(id int64) error
	Insert(in *Notification) error
	GetAllPendingNotifications() ([]*Notification, error)
	GetNotificationByCollectionID(collectionID string) ([]*Notification, error)
}
