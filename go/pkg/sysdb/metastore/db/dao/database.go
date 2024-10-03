package dao

import (
	"errors"

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

func (s *databaseDb) GetAllDatabases() ([]*dbmodel.Database, error) {
	var databases []*dbmodel.Database
	query := s.db.Table("databases")

	if err := query.Find(&databases).Error; err != nil {
		return nil, err
	}
	return databases, nil
}

func (s *databaseDb) GetDatabases(tenantID string, databaseName string) ([]*dbmodel.Database, error) {
	var databases []*dbmodel.Database
	query := s.db.Table("databases").
		Select("databases.id, databases.name, databases.tenant_id").
		Where("databases.name = ?", databaseName).
		Where("databases.tenant_id = ?", tenantID)

	if err := query.Find(&databases).Error; err != nil {
		log.Error("GetDatabases", zap.Error(err))
		return nil, err
	}
	return databases, nil
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

func (s *databaseDb) GetDatabasesByTenantID(tenantID string) ([]*dbmodel.Database, error) {
	var databases []*dbmodel.Database
	query := s.db.Table("databases").
		Select("databases.id, databases.name, databases.tenant_id").
		Where("databases.tenant_id = ?", tenantID)

	if err := query.Find(&databases).Error; err != nil {
		log.Error("GetDatabasesByTenantID", zap.Error(err))
		return nil, err
	}
	return databases, nil
}
