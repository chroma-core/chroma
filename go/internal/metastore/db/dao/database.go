package dao

import (
	"github.com/chroma-core/chroma/go/internal/metastore/db/dbmodel"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type databaseDb struct {
	db *gorm.DB
}

var _ dbmodel.IDatabaseDb = &databaseDb{}

func (s *databaseDb) DeleteAll() error {
	return s.db.Where("1 = 1").Delete(&dbmodel.Database{}).Error
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
	return s.db.Create(database).Error
}
