package model

const (
	NotificationTypeCreateCollection = "create_collection"
	NotificationTypeDeleteCollection = "delete_collection"
)

const (
	NotificationStatusPending = "pending"
)

type Notification struct {
	ID           int64
	CollectionID string
	Type         string
	Status       string
}
