package dbmodel

import (
	"time"

	"github.com/chroma-core/chroma/go/pkg/types"
)

type Database struct {
	ID        string          `gorm:"id;primaryKey;unique;index:idx_databases_list,priority:4"`
	Name      string          `gorm:"name;type:varchar(128);not null;uniqueIndex:idx_tenantid_name"`
	TenantID  string          `gorm:"tenant_id;type:varchar(128);not null;uniqueIndex:idx_tenantid_name;index:idx_databases_list,priority:1"`
	Ts        types.Timestamp `gorm:"ts;type:bigint;default:0"`
	IsDeleted bool            `gorm:"is_deleted;type:bool;default:false;index:idx_databases_list,priority:2"`
	CreatedAt time.Time       `gorm:"created_at;type:timestamp;not null;default:current_timestamp;index:idx_databases_list,priority:3"`
	UpdatedAt time.Time       `gorm:"updated_at;type:timestamp;not null;default:current_timestamp"`
}

func (v Database) TableName() string {
	return "databases"
}

//go:generate mockery --name=IDatabaseDb
type IDatabaseDb interface {
	GetDatabases(tenantID string, databaseName string) ([]*Database, error)
	GetByID(databaseID string) (*Database, error)
	ListDatabases(limit *int32, offset *int32, tenantID string) ([]*Database, error)
	Insert(in *Database) error
	DeleteAll() error
	SoftDelete(databaseID string) error
	FinishDatabaseDeletion(cutoffTime time.Time) (uint64, error)
}
