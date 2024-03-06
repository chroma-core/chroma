package dao

import (
	"github.com/chroma-core/chroma/go/pkg/metastore/db/dbmodel"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type segmentMetadataDb struct {
	db *gorm.DB
}

func (s *segmentMetadataDb) DeleteAll() error {
	return s.db.Where("1 = 1").Delete(&dbmodel.SegmentMetadata{}).Error
}

func (s *segmentMetadataDb) DeleteBySegmentID(segmentID string) error {
	return s.db.Where("segment_id = ?", segmentID).Delete(&dbmodel.SegmentMetadata{}).Error
}

func (s *segmentMetadataDb) DeleteBySegmentIDAndKeys(segmentID string, keys []string) error {
	return s.db.
		Where("segment_id = ?", segmentID).
		Where("key IN ?", keys).
		Delete(&dbmodel.SegmentMetadata{}).Error
}

func (s *segmentMetadataDb) Insert(in []*dbmodel.SegmentMetadata) error {
	return s.db.Clauses(
		clause.OnConflict{
			Columns:   []clause.Column{{Name: "segment_id"}, {Name: "key"}},
			DoUpdates: clause.AssignmentColumns([]string{"str_value", "int_value", "float_value", "ts"}),
		},
	).Create(in).Error
}
