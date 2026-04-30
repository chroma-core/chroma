package dao

import (
	"github.com/chroma-core/chroma/go/pkg/sysdb/metastore/db/dbmodel"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type databaseMetadataDb struct {
	db *gorm.DB
}

func (s *databaseMetadataDb) GetByDatabaseID(databaseID string) ([]*dbmodel.DatabaseMetadata, error) {
	var metadata []*dbmodel.DatabaseMetadata
	err := s.db.Where("database_id = ?", databaseID).Find(&metadata).Error
	return metadata, err
}

func (s *databaseMetadataDb) GetByDatabaseIDs(databaseIDs []string) ([]*dbmodel.DatabaseMetadata, error) {
	if len(databaseIDs) == 0 {
		return nil, nil
	}
	var metadata []*dbmodel.DatabaseMetadata
	err := s.db.Where("database_id IN ?", databaseIDs).Find(&metadata).Error
	return metadata, err
}

func (s *databaseMetadataDb) DeleteAll() error {
	return s.db.Where("1 = 1").Delete(&dbmodel.DatabaseMetadata{}).Error
}

func (s *databaseMetadataDb) DeleteByDatabaseID(databaseID string) (int, error) {
	var metadata []dbmodel.DatabaseMetadata
	err := s.db.Clauses(clause.Returning{}).Where("database_id = ?", databaseID).Delete(&metadata).Error
	return len(metadata), err
}

func (s *databaseMetadataDb) Insert(in []*dbmodel.DatabaseMetadata) error {
	return s.db.Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "database_id"}, {Name: "key"}},
		DoUpdates: clause.Assignments(map[string]interface{}{
			"str_value":   gorm.Expr("excluded.str_value"),
			"int_value":   gorm.Expr("excluded.int_value"),
			"float_value": gorm.Expr("excluded.float_value"),
			"bool_value":  gorm.Expr("excluded.bool_value"),
			"updated_at":  gorm.Expr("CURRENT_TIMESTAMP"),
		}),
	}).Create(in).Error
}
