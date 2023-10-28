package dbmodel

import (
	"time"

	"github.com/chroma/chroma-coordinator/internal/types"
)

type Tenant struct {
	ID        string          `db:"id;primaryKey"`
	Ts        types.Timestamp `gorm:"ts"`
	IsDeleted bool            `gorm:"default:false"`
	CreatedAt time.Time       `gorm:"created_at;default:CURRENT_TIMESTAMP"`
	UpdatedAt time.Time       `gorm:"created_at;default:CURRENT_TIMESTAMP"`
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
