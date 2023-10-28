package dbmodel

import (
	"time"

	"github.com/chroma/chroma-coordinator/internal/types"
)

type Database struct {
	ID        string          `db:"id;primaryKey;unique"`
	Name      string          `db:"name;not null"`
	TenantID  string          `db:"tenant_id"`
	Ts        types.Timestamp `gorm:"ts"`
	IsDeleted bool            `gorm:"default:false"`
	CreatedAt time.Time       `gorm:"created_at;default:CURRENT_TIMESTAMP"`
	UpdatedAt time.Time       `gorm:"created_at;default:CURRENT_TIMESTAMP"`
}

func (v Database) TableName() string {
	return "databases"
}

//go:generate mockery --name=IDatabaseDb
type IDatabaseDb interface {
	GetAllDatabases() ([]*Database, error)
	GetDatabases(tenantID string, databaseName string) ([]*Database, error)
	Insert(in *Database) error
	DeleteAll() error
}
