package dao

import (
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/chroma-core/chroma/go/pkg/common"
	"github.com/chroma-core/chroma/go/pkg/sysdb/coordinator/model"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/chroma-core/chroma/go/pkg/sysdb/metastore/db/dbmodel"
	"github.com/chroma-core/chroma/go/pkg/types"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type segmentDb struct {
	db *gorm.DB
}

func (s *segmentDb) DeleteAll() error {
	return s.db.Where("1=1").Delete(&dbmodel.Segment{}).Error
}

func (s *segmentDb) DeleteSegmentByID(id string) error {
	return s.db.Where("id = ?", id).Delete(&dbmodel.Segment{}).Error
}

func (s *segmentDb) Insert(in *dbmodel.Segment) error {
	err := s.db.Create(&in).Error

	if err != nil {
		log.Error("create segment failed", zap.Error(err))
		var pgErr *pgconn.PgError
		ok := errors.As(err, &pgErr)
		if ok {
			log.Error("Postgres Error")
			switch pgErr.Code {
			case "23505":
				log.Error("segment already exists")
				return common.ErrSegmentUniqueConstraintViolation
			default:
				return err
			}
		}
		return err
	}
	return nil
}

func (s *segmentDb) GetSegments(id types.UniqueID, segmentType *string, scope *string, collectionID types.UniqueID) ([]*dbmodel.SegmentAndMetadata, error) {
	if collectionID == types.NilUniqueID() {
		return nil, common.ErrMissingCollectionID
	}

	var segments []*dbmodel.SegmentAndMetadata

	query := s.db.Table("segments").
		Select("segments.id, segments.collection_id, segments.type, segments.scope, segments.file_paths, segment_metadata.key, segment_metadata.str_value, segment_metadata.int_value, segment_metadata.float_value, segment_metadata.bool_value").
		Joins("LEFT JOIN segment_metadata ON segments.id = segment_metadata.segment_id").
		Where("segments.collection_id = ?", collectionID.String()).
		Order("segments.id")

	if id != types.NilUniqueID() {
		query = query.Where("id = ?", id.String())
	}
	if segmentType != nil {
		query = query.Where("type = ?", segmentType)
	}
	if scope != nil {
		query = query.Where("scope = ?", scope)
	}

	if query.Error != nil {
		log.Error("get segments failed", zap.Error(query.Error))
		return nil, query.Error
	}

	rows, err := query.Rows()
	if err != nil {
		segmentTypeStr := "nil"
		scopeStr := "nil"

		if segmentType != nil {
			segmentTypeStr = *segmentType
		}
		if scope != nil {
			scopeStr = *scope
		}

		log.Error("get segments failed", zap.String("segmentID", id.String()), zap.String("segmentType", segmentTypeStr), zap.String("scope", scopeStr), zap.Error(err))
		return nil, err
	}
	defer rows.Close()

	var currentSegmentID string = ""
	var metadata []*dbmodel.SegmentMetadata
	var currentSegment *dbmodel.SegmentAndMetadata

	for rows.Next() {
		var (
			segmentID     string
			collectionID  sql.NullString
			segmentType   string
			scope         string
			filePathsJson string
			key           sql.NullString
			strValue      sql.NullString
			intValue      sql.NullInt64
			floatValue    sql.NullFloat64
			boolValue     sql.NullBool
		)

		err := rows.Scan(&segmentID, &collectionID, &segmentType, &scope, &filePathsJson, &key, &strValue, &intValue, &floatValue, &boolValue)
		if err != nil {
			log.Error("scan segment failed", zap.Error(err))
		}
		if segmentID != currentSegmentID {
			currentSegmentID = segmentID
			metadata = nil

			var filePaths map[string][]string
			err := json.Unmarshal([]byte(filePathsJson), &filePaths)
			if err != nil {
				return nil, err
			}
			currentSegment = &dbmodel.SegmentAndMetadata{
				Segment: &dbmodel.Segment{
					ID:        segmentID,
					Type:      segmentType,
					Scope:     scope,
					FilePaths: filePaths,
				},
				SegmentMetadata: metadata,
			}
			if collectionID.Valid {
				currentSegment.Segment.CollectionID = &collectionID.String
			} else {
				currentSegment.Segment.CollectionID = nil
			}

			if currentSegmentID != "" {
				segments = append(segments, currentSegment)
			}

		}
		segmentMetadata := &dbmodel.SegmentMetadata{
			SegmentID: segmentID,
		}
		if key.Valid {
			segmentMetadata.Key = &key.String
		} else {
			segmentMetadata.Key = nil
		}

		if strValue.Valid {
			segmentMetadata.StrValue = &strValue.String
		} else {
			segmentMetadata.StrValue = nil
		}

		if intValue.Valid {
			segmentMetadata.IntValue = &intValue.Int64
		} else {
			segmentMetadata.IntValue = nil
		}

		if floatValue.Valid {
			segmentMetadata.FloatValue = &floatValue.Float64
		} else {
			segmentMetadata.FloatValue = nil
		}

		if boolValue.Valid {
			segmentMetadata.BoolValue = &boolValue.Bool
		} else {
			segmentMetadata.BoolValue = nil
		}

		metadata = append(metadata, segmentMetadata)
		currentSegment.SegmentMetadata = metadata
	}
	log.Info("get segments success", zap.Any("segments", segments))
	return segments, nil
}

func generateSegmentUpdatesWithoutID(in *dbmodel.UpdateSegment) map[string]interface{} {
	log.Info("generate segment updates without id", zap.Any("in", in))
	ret := map[string]interface{}{}

	// TODO: check this
	//if in.ResetCollection {
	//	if in.Collection == nil {
	//		ret["collection_id"] = nil
	//	}
	//} else {
	//	if in.Collection != nil {
	//		ret["collection_id"] = *in.Collection
	//	}
	//}
	//log.Info("generate segment updates without id", zap.Any("updates", ret))
	return ret
}

func (s *segmentDb) Update(in *dbmodel.UpdateSegment) error {
	updates := generateSegmentUpdatesWithoutID(in)
	return s.db.Model(&dbmodel.Segment{}).
		Where("collection_id = ?", &in.Collection).
		Where("id = ?", in.ID).Updates(updates).Error
}

func (s *segmentDb) RegisterFilePaths(flushSegmentCompactions []*model.FlushSegmentCompaction) error {
	log.Info("register file paths", zap.Any("flushSegmentCompactions", flushSegmentCompactions))
	for _, flushSegmentCompaction := range flushSegmentCompactions {
		filePaths, err := json.Marshal(flushSegmentCompaction.FilePaths)
		if err != nil {
			log.Error("marshal file paths failed", zap.Error(err))
			return err
		}
		err = s.db.Model(&dbmodel.Segment{}).
			Where("id = ?", flushSegmentCompaction.ID).
			Update("file_paths", filePaths).Error
		if err != nil {
			log.Error("register file path failed", zap.Error(err))
			return err
		}
	}
	return nil
}

func (s *segmentDb) GetSegmentsByCollectionID(collectionID string) ([]*dbmodel.Segment, error) {
	var segments []*dbmodel.Segment
	err := s.db.Where("collection_id = ?", collectionID).Find(&segments).Error
	if err != nil {
		return nil, err
	}
	return segments, nil
}
