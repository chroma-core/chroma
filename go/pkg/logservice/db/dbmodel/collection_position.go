package dbmodel

import "time"

type CollectionPosition struct {
	ID          string    `gorm:"id;primaryKey"`
	CreatedAt   time.Time `gorm:"created_at;type:timestamp;not null;default:current_timestamp"`
	UpdatedAt   time.Time `gorm:"updated_at;type:timestamp;not null;default:current_timestamp"`
	LogPosition int64     `gorm:"log_position;default:0"`
}

func (v CollectionPosition) TableName() string {
	return "collection_position"
}
