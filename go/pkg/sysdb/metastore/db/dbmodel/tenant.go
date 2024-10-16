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
	LastCompactionTime int64           `gorm:"last_compaction_time;not null"`
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
	UpdateTenantLastCompactionTime(tenantID string, lastCompactionTime int64) error
	GetTenantsLastCompactionTime(tenantIDs []string) ([]*Tenant, error)
}
