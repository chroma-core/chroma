package dbmodel

import (
	"time"

	"github.com/chroma-core/chroma/go/pkg/types"
)

type Tenant struct {
	ID                 string          `gorm:"id;primaryKey;unique"`
	Ts                 types.Timestamp `gorm:"ts;type:bigint;default:0"`
	IsDeleted          bool            `gorm:"is_deleted;type:bool;default:false"`
	CreatedAt          time.Time       `gorm:"created_at;type:timestamp;not null;default:current_timestamp"`
	UpdatedAt          time.Time       `gorm:"updated_at;type:timestamp;not null;default:current_timestamp"`
	LastCompactionTime time.Time       `gorm:"last_compaction_time;type:timestamp;not null;default:current_timestamp"`
}

func (v Tenant) TableName() string {
	return "tenants"
}

//go:generate mockery --name=ITenantDb
type ITenantDb interface {
	GetAllTenants() ([]*Tenant, error)
	GetTenants(tenantID string) ([]*Tenant, error)
	Insert(in *Tenant) error
	DeleteAll() error
}
