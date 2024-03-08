package dao

import (
	"errors"
	"github.com/chroma-core/chroma/go/pkg/common"
	"github.com/chroma-core/chroma/go/pkg/metastore/db/dbmodel"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pingcap/log"
	"go.uber.org/zap"
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
	err := s.db.Create(tenant).Error
	if err != nil {
		log.Error("create tenant failed", zap.Error(err))
		var pgErr *pgconn.PgError
		ok := errors.As(err, &pgErr)
		if ok {
			log.Error("Postgres Error")
			switch pgErr.Code {
			case "23505":
				log.Error("tenant already exists")
				return common.ErrTenantUniqueConstraintViolation
			default:
				return err
			}
		}
		return err
	}
	return nil
}

func (s *tenantDb) GetTenantsLastCompactionTime(tenantIDs []string) ([]*dbmodel.Tenant, error) {
	var tenants []*dbmodel.Tenant

	// TODO: implement this
	return tenants, nil
}
