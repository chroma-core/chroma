package dao

import (
	"errors"
	"time"

	"github.com/chroma-core/chroma/go/pkg/common"
	"github.com/chroma-core/chroma/go/pkg/sysdb/metastore/db/dbmodel"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type databaseDb struct {
	db *gorm.DB
}

var _ dbmodel.IDatabaseDb = &databaseDb{}

func (s *databaseDb) DeleteAll() error {
	return s.db.Where("1 = 1").Delete(&dbmodel.Database{}).Error
}

func (s *databaseDb) DeleteByTenantIdAndName(tenantId string, databaseName string) (int, error) {
	var databases []dbmodel.Database
	err := s.db.Clauses(clause.Returning{}).Where("tenant_id = ?", tenantId).Where("name = ?", databaseName).Delete(&databases).Error
	return len(databases), err
}

func (s *databaseDb) ListDatabases(limit *int32, offset *int32, tenantID string) ([]*dbmodel.Database, error) {
	var databases []*dbmodel.Database
	query := s.db.Table("databases").
		Select("databases.id, databases.name, databases.tenant_id").
		Where("databases.tenant_id = ?", tenantID).
		Where("databases.is_deleted = ?", false).
		Order("databases.created_at ASC")

	if limit != nil {
		query = query.Limit(int(*limit))
	}

	if offset != nil {
		query = query.Offset(int(*offset))
	}

	if err := query.Find(&databases).Error; err != nil {
		log.Error("ListDatabases", zap.Error(err))
		return nil, err
	}
	return databases, nil
}

func (s *databaseDb) GetDatabases(tenantID string, databaseName string) ([]*dbmodel.Database, error) {
	var databases []*dbmodel.Database
	query := s.db.Table("databases").
		Select("databases.id, databases.name, databases.tenant_id").
		Where("databases.name = ?", databaseName).
		Where("databases.tenant_id = ?", tenantID).
		Where("databases.is_deleted = ?", false)

	if err := query.Find(&databases).Error; err != nil {
		log.Error("GetDatabases", zap.Error(err))
		return nil, err
	}
	return databases, nil
}

func (s *databaseDb) GetByID(databaseID string) (*dbmodel.Database, error) {
	var database dbmodel.Database
	query := s.db.Table("databases").
		Select("databases.id, databases.name, databases.tenant_id").
		Where("databases.id = ?", databaseID).
		Where("databases.is_deleted = ?", false)

	if err := query.First(&database).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		log.Error("GetByID", zap.Error(err))
		return nil, err
	}
	return &database, nil
}

func (s *databaseDb) Insert(database *dbmodel.Database) error {
	err := s.db.Create(database).Error
	if err != nil {
		log.Error("insert database failed", zap.Error(err))
		var pgErr *pgconn.PgError
		ok := errors.As(err, &pgErr)
		if ok {
			log.Error("Postgres Error")
			switch pgErr.Code {
			case "23505":
				log.Error("database already exists")
				return common.ErrDatabaseUniqueConstraintViolation
			default:
				return err
			}
		}
		return err
	}
	return err
}

func (s *databaseDb) SoftDelete(databaseID string) error {
	return s.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Table("databases").
			Where("id = ?", databaseID).
			Update("is_deleted", true).
			Update("updated_at", time.Now()).
			Error; err != nil {
			return err
		}

		return nil
	})
}

func (s *databaseDb) GetDatabasesByTenantID(tenantID string) ([]*dbmodel.Database, error) {
	var databases []*dbmodel.Database
	query := s.db.Table("databases").
		Select("databases.id, databases.name, databases.tenant_id").
		Where("databases.tenant_id = ?", tenantID).
		Where("databases.is_deleted = ?", false)

	if err := query.Find(&databases).Error; err != nil {
		log.Error("GetDatabasesByTenantID", zap.Error(err))
		return nil, err
	}
	return databases, nil
}

func (s *databaseDb) FinishDatabaseDeletion(cutoffTime time.Time) (uint64, error) {
	numDeleted := uint64(0)

	for {
		// Only hard delete databases that were soft deleted prior to the cutoff time and have no collections
		databasesSubQuery := s.db.
			Table("databases d").
			Select("d.id").
			Joins("LEFT JOIN collections c ON c.database_id = d.id").
			Where("d.is_deleted = ?", true).
			Where("d.updated_at < ?", cutoffTime).
			Group("d.id").
			Having("COUNT(c.id) = 0").
			Limit(1000)

		res := s.db.Table("databases").
			Where("id IN (?)", databasesSubQuery).
			Delete(&dbmodel.Database{})
		if res.Error != nil {
			return numDeleted, res.Error
		}

		numDeleted += uint64(res.RowsAffected)

		if res.RowsAffected == 0 {
			break
		}
	}

	return numDeleted, nil
}
