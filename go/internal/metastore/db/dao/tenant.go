package dao

import (
	"github.com/chroma-core/chroma/go/internal/metastore/db/dbmodel"
	"gorm.io/gorm"
)

type tenantDb struct {
	db *gorm.DB
}

var _ dbmodel.ITenantDb = &tenantDb{}

func (s *tenantDb) DeleteAll() error {
	return s.db.Where("1 = 1").Delete(&dbmodel.Tenant{}).Error
}

func (s *tenantDb) GetAllTenants() ([]*dbmodel.Tenant, error) {
	var tenants []*dbmodel.Tenant

	if err := s.db.Find(&tenants).Error; err != nil {
		return nil, err
	}
	return tenants, nil
}

func (s *tenantDb) GetTenants(tenantID string) ([]*dbmodel.Tenant, error) {
	var tenants []*dbmodel.Tenant

	if err := s.db.Where("id = ?", tenantID).Find(&tenants).Error; err != nil {
		return nil, err
	}
	return tenants, nil
}

func (s *tenantDb) Insert(tenant *dbmodel.Tenant) error {
	return s.db.Create(tenant).Error
}
