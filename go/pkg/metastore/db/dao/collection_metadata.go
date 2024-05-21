package dao

import (
	"github.com/chroma-core/chroma/go/pkg/metastore/db/dbmodel"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type collectionMetadataDb struct {
	db *gorm.DB
}

func (s *collectionMetadataDb) DeleteAll() error {
	return s.db.Where("1 = 1").Delete(&dbmodel.CollectionMetadata{}).Error
}

func (s *collectionMetadataDb) GetForCollection(collectionID string) (metadata []dbmodel.CollectionMetadata, err error) {
	err = s.db.Where("collection_id = ?", collectionID).Find(&metadata).Error
	return
}

func (s *collectionMetadataDb) DeleteByCollectionID(collectionID string) (int, error) {
	var metadata []dbmodel.CollectionMetadata
	err := s.db.Clauses(clause.Returning{}).Where("collection_id = ?", collectionID).Delete(&metadata).Error
	return len(metadata), err
}

func (s *collectionMetadataDb) Insert(in []*dbmodel.CollectionMetadata) error {
	return s.db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "collection_id"}, {Name: "key"}},
		DoUpdates: clause.AssignmentColumns([]string{"str_value", "int_value", "float_value"}),
	}).Create(in).Error
}
