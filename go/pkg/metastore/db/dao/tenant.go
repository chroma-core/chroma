package dao

import (
	"errors"
	"github.com/chroma-core/chroma/go/pkg/common"
	"github.com/chroma-core/chroma/go/pkg/metastore/db/dbmodel"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type tenantDb struct {
	db *gorm.DB
}

var _ dbmodel.ITenantDb = &tenantDb{}

func (s *tenantDb) DeleteAll() error {
	return s.db.Where("1 = 1").Delete(&dbmodel.Tenant{}).Error
}

func (s *tenantDb) DeleteByID(tenantID string) (int, error) {
	var tenants []dbmodel.Tenant
	err := s.db.Clauses(clause.Returning{}).Where("id = ?", tenantID).Delete(&tenants).Error
	return len(tenants), err
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

func (s *tenantDb) UpdateTenantLastCompactionTime(tenantID string, lastCompactionTime int64) error {
	log.Info("UpdateTenantLastCompactionTime", zap.String("tenantID", tenantID), zap.Int64("lastCompactionTime", lastCompactionTime))
	var tenants []dbmodel.Tenant
	result := s.db.Model(&tenants).
		Clauses(clause.Returning{Columns: []clause.Column{{Name: "id"}}}).
		Where("id = ?", tenantID).
		Update("last_compaction_time", lastCompactionTime)

	if result.Error != nil {
		log.Error("UpdateTenantLastCompactionTime error", zap.Error(result.Error))
		return result.Error
	}
	if result.RowsAffected == 0 {
		return common.ErrTenantNotFound
	}
	return nil
}

func (s *tenantDb) GetTenantsLastCompactionTime(tenantIDs []string) ([]*dbmodel.Tenant, error) {
	log.Info("GetTenantsLastCompactionTime", zap.Any("tenantIDs", tenantIDs))
	var tenants []*dbmodel.Tenant

	result := s.db.Select("id", "last_compaction_time").Find(&tenants, "id IN ?", tenantIDs)
	if result.Error != nil {
		log.Error("GetTenantsLastCompactionTime error", zap.Error(result.Error))
		return nil, result.Error
	}

	return tenants, nil
}
