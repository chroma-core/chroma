package model

const (
	NotificationTypeCreateCollection = "create_collection"
	NotificationTypeDeleteCollection = "delete_collection"
)

const (
	NotificationStatusPending = "pending"
)

type Notification struct {
	ID           int64  `json:"id"`
	CollectionID string `json:"collection_id"`
	Type         string `json:"type"`
	Status       string `json:"status"`
}

type Acknowledgement struct {
	ID           int64  `json:"id"`
	CollectionID string `json:"collection_id"`
	From         string `json:"from"`
}
