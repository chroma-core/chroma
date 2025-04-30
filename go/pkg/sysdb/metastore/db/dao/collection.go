package dao

import (
	"errors"
	"sort"
	"time"

	"github.com/chroma-core/chroma/go/pkg/common"
	"github.com/jackc/pgx/v5/pgconn"
	"gorm.io/gorm/clause"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/chroma-core/chroma/go/pkg/sysdb/metastore/db/dbmodel"
	"github.com/pingcap/log"
)

type collectionDb struct {
	db      *gorm.DB
	read_db *gorm.DB
}

var _ dbmodel.ICollectionDb = &collectionDb{}

func (s *collectionDb) DeleteAll() error {
	return s.db.Where("1 = 1").Delete(&dbmodel.Collection{}).Error
}

func (s *collectionDb) GetCollectionEntry(collectionID *string, databaseName *string) (*dbmodel.Collection, error) {
	var collections []*dbmodel.Collection
	query := s.db.Table("collections").
		Select("collections.id, collections.name, collections.database_id, collections.is_deleted, collections.tenant, collections.version, collections.version_file_name, collections.root_collection_id").
		Joins("INNER JOIN databases ON collections.database_id = databases.id").
		Where("collections.id = ?", collectionID)

	if databaseName != nil && *databaseName != "" {
		query = query.Where("databases.name = ?", databaseName)
	}

	err := query.Find(&collections).Error
	if err != nil {
		return nil, err
	}
	if len(collections) == 0 {
		return nil, nil
	}
	return collections[0], nil
}

func (s *collectionDb) GetCollectionEntries(id *string, name *string, tenantID string, databaseName string, limit *int32, offset *int32) ([]*dbmodel.CollectionAndMetadata, error) {
	return s.getCollections(id, name, tenantID, databaseName, limit, offset, true)
}

func (s *collectionDb) GetCollections(id *string, name *string, tenantID string, databaseName string, limit *int32, offset *int32) ([]*dbmodel.CollectionAndMetadata, error) {
	return s.getCollections(id, name, tenantID, databaseName, limit, offset, false)
}

func (s *collectionDb) ListCollectionsToGc(cutoffTimeSecs *uint64, limit *uint64) ([]*dbmodel.CollectionToGc, error) {
	var collections []*dbmodel.CollectionToGc
	// Use the read replica for this so as to not overwhelm the writer.
	query := s.read_db.Table("collections").
		Select("collections.id, collections.name, collections.version, collections.version_file_name, collections.oldest_version_ts, collections.num_versions, databases.tenant_id").
		Joins("INNER JOIN databases ON collections.database_id = databases.id").
		Where("version > 0").
		Where("version_file_name IS NOT NULL").
		Where("version_file_name != ''").
		Where("root_collection_id IS NULL OR root_collection_id = ''").
		Where("lineage_file_name IS NULL OR lineage_file_name = ''")
	// Apply cutoff time filter only if provided
	if cutoffTimeSecs != nil {
		cutoffTime := time.Unix(int64(*cutoffTimeSecs), 0)
		query = query.Where("oldest_version_ts < ?", cutoffTime)
	}

	query = query.Order("num_versions DESC")

	// Apply limit only if provided
	if limit != nil {
		query = query.Limit(int(*limit))
	}

	err := query.Find(&collections).Error
	if err != nil {
		return nil, err
	}
	log.Debug("collections to gc", zap.Any("collections", collections))
	return collections, nil
}

func (s *collectionDb) getCollections(id *string, name *string, tenantID string, databaseName string, limit *int32, offset *int32, is_deleted bool) (collectionWithMetdata []*dbmodel.CollectionAndMetadata, err error) {
	type Result struct {
		// Collection fields
		CollectionId               string     `gorm:"column:collection_id"`
		CollectionName             *string    `gorm:"column:collection_name"`
		ConfigurationJsonStr       *string    `gorm:"column:configuration_json_str"`
		Dimension                  *int32     `gorm:"column:dimension"`
		DatabaseID                 string     `gorm:"column:database_id"`
		CollectionTs               *int64     `gorm:"column:collection_ts"`
		IsDeleted                  bool       `gorm:"column:is_deleted"`
		CollectionCreatedAt        *time.Time `gorm:"column:collection_created_at"`
		CollectionUpdatedAt        *time.Time `gorm:"column:collection_updated_at"`
		LogPosition                int64      `gorm:"column:log_position"`
		Version                    int32      `gorm:"column:version"`
		VersionFileName            string     `gorm:"column:version_file_name"`
		RootCollectionId           string     `gorm:"column:root_collection_id"`
		LineageFileName            string     `gorm:"column:lineage_file_name"`
		TotalRecordsPostCompaction uint64     `gorm:"column:total_records_post_compaction"`
		SizeBytesPostCompaction    uint64     `gorm:"column:size_bytes_post_compaction"`
		LastCompactionTimeSecs     uint64     `gorm:"column:last_compaction_time_secs"`
		DatabaseName               string     `gorm:"column:database_name"`
		TenantID                   string     `gorm:"column:db_tenant_id"`
		Tenant                     string     `gorm:"column:tenant"`
		// Metadata fields
		Key               *string    `gorm:"column:key"`
		StrValue          *string    `gorm:"column:str_value"`
		IntValue          *int64     `gorm:"column:int_value"`
		FloatValue        *float64   `gorm:"column:float_value"`
		BoolValue         *bool      `gorm:"column:bool_value"`
		MetadataTs        *int64     `gorm:"column:metadata_ts"`
		MetadataCreatedAt *time.Time `gorm:"column:metadata_created_at"`
		MetadataUpdatedAt *time.Time `gorm:"column:metadata_updated_at"`
	}

	query := s.db.Table("collections").
		Select("collections.id as collection_id, collections.name as collection_name, collections.configuration_json_str, collections.dimension, collections.database_id, collections.ts as collection_ts, collections.is_deleted, collections.created_at as collection_created_at, collections.updated_at as collection_updated_at, collections.log_position, collections.version, collections.version_file_name, collections.root_collection_id, collections.lineage_file_name, collections.total_records_post_compaction, collections.size_bytes_post_compaction, collections.last_compaction_time_secs, databases.name as database_name, databases.tenant_id as db_tenant_id, collections.tenant as tenant").
		Joins("INNER JOIN databases ON collections.database_id = databases.id").
		Order("collections.created_at ASC")

	if databaseName != "" {
		query = query.Where("databases.name = ?", databaseName)
	}
	if tenantID != "" {
		query = query.Where("databases.tenant_id = ?", tenantID)
	}
	if id != nil {
		query = query.Where("collections.id = ?", *id)
	}
	if name != nil {
		query = query.Where("collections.name = ?", *name)
	}
	query = query.Where("collections.is_deleted = ?", is_deleted)

	if limit != nil {
		query = query.Limit(int(*limit))
	}
	if offset != nil {
		query = query.Offset(int(*offset))

	}
	var results []Result
	err = s.db.Table("(?) as ci", query).
		Select(`
            ci.*,
            cm.key,
            cm.str_value,
            cm.int_value,
            cm.float_value,
            cm.bool_value,
            cm.ts as metadata_ts,
            cm.created_at as metadata_created_at,
            cm.updated_at as metadata_updated_at
        `).
		Joins("LEFT JOIN collection_metadata cm ON cm.collection_id = ci.collection_id").
		Scan(&results).Error

	if err != nil {
		return nil, err
	}

	var collectionsMap = make(map[string]*dbmodel.CollectionAndMetadata)

	for _, r := range results {
		collection, exists := collectionsMap[r.CollectionId]
		if !exists {
			// Create new collection
			var col = &dbmodel.Collection{
				ID:                         r.CollectionId,
				Name:                       r.CollectionName,
				ConfigurationJsonStr:       r.ConfigurationJsonStr,
				Dimension:                  r.Dimension,
				DatabaseID:                 r.DatabaseID,
				IsDeleted:                  r.IsDeleted,
				LogPosition:                r.LogPosition,
				Version:                    r.Version,
				VersionFileName:            r.VersionFileName,
				RootCollectionId:           r.RootCollectionId,
				LineageFileName:            r.LineageFileName,
				TotalRecordsPostCompaction: r.TotalRecordsPostCompaction,
				SizeBytesPostCompaction:    r.SizeBytesPostCompaction,
				LastCompactionTimeSecs:     r.LastCompactionTimeSecs,
				Tenant:                     r.Tenant,
			}
			if r.CollectionTs != nil {
				col.Ts = *r.CollectionTs
			} else {
				col.Ts = 0
			}

			if r.CollectionCreatedAt != nil {
				col.CreatedAt = *r.CollectionCreatedAt
			} else {
				// Current time as default.
				col.CreatedAt = time.Now()
			}

			if r.CollectionUpdatedAt != nil {
				col.UpdatedAt = *r.CollectionUpdatedAt
			} else {
				// Current time as default.
				col.UpdatedAt = time.Now()
			}

			collection = &dbmodel.CollectionAndMetadata{
				Collection:         col,
				TenantID:           r.TenantID,
				DatabaseName:       r.DatabaseName,
				CollectionMetadata: make([]*dbmodel.CollectionMetadata, 0),
			}
			collectionsMap[r.CollectionId] = collection
		}

		// Populate metadata if it exists.
		var metadata = &dbmodel.CollectionMetadata{}
		if r.Key != nil {
			metadata.Key = r.Key
			metadata.StrValue = r.StrValue
			metadata.IntValue = r.IntValue
			metadata.FloatValue = r.FloatValue
			metadata.BoolValue = r.BoolValue
			if r.MetadataTs != nil {
				metadata.Ts = *r.MetadataTs
			} else {
				metadata.Ts = 0
			}
			if r.MetadataCreatedAt != nil {
				metadata.CreatedAt = *r.MetadataCreatedAt
			} else {
				// current time
				metadata.CreatedAt = time.Now()
			}

			if r.MetadataUpdatedAt != nil {
				metadata.UpdatedAt = *r.MetadataUpdatedAt
			} else {
				// current time
				metadata.UpdatedAt = time.Now()
			}
			collection.CollectionMetadata = append(collection.CollectionMetadata, metadata)
		}
	}

	var collections = make([]*dbmodel.CollectionAndMetadata, 0, len(collectionsMap))
	for _, c := range collectionsMap {
		collections = append(collections, c)
	}

	// Sort the result by created time.
	sort.Slice(collections, func(i, j int) bool {
		return collections[i].Collection.CreatedAt.Before(collections[j].Collection.CreatedAt)
	})

	return collections, nil
}

func (s *collectionDb) CountCollections(tenantID string, databaseName *string) (uint64, error) {
	var count int64
	query := s.db.Table("collections").
		Joins("INNER JOIN databases ON collections.database_id = databases.id").
		Where("databases.tenant_id = ? AND collections.is_deleted = ?", tenantID, false)

	if databaseName != nil {
		query = query.Where("databases.name = ?", databaseName)
	}

	result := query.Count(&count)

	if result.Error != nil {
		return 0, result.Error
	}

	return uint64(count), nil
}

func (s *collectionDb) GetCollectionSize(id string) (uint64, error) {
	query := s.read_db.Table("collections").
		Select("collections.total_records_post_compaction").
		Where("collections.id = ?", id)

	rows, err := query.Rows()
	if err != nil {
		return 0, err
	}

	var totalRecordsPostCompaction uint64

	for rows.Next() {
		err := rows.Scan(&totalRecordsPostCompaction)
		if err != nil {
			log.Error("scan collection failed", zap.Error(err))
			return 0, err
		}
	}
	rows.Close()
	return totalRecordsPostCompaction, nil
}

func (s *collectionDb) GetSoftDeletedCollections(collectionID *string, tenantID string, databaseName string, limit int32) ([]*dbmodel.CollectionAndMetadata, error) {
	return s.getCollections(collectionID, nil, tenantID, databaseName, &limit, nil, true)
}

// NOTE: This is the only method to do a hard delete of a single collection.
func (s *collectionDb) DeleteCollectionByID(collectionID string) (int, error) {
	var collections []dbmodel.Collection
	err := s.db.Clauses(clause.Returning{}).Where("id = ?", collectionID).Delete(&collections).Error
	return len(collections), err
}

func (s *collectionDb) Insert(in *dbmodel.Collection) error {
	err := s.db.Create(&in).Error
	if err != nil {
		log.Error("create collection failed", zap.Error(err))
		var pgErr *pgconn.PgError
		ok := errors.As(err, &pgErr)
		if ok {
			log.Error("Postgres Error")
			switch pgErr.Code {
			case "23505":
				log.Error("collection already exists")
				return common.ErrCollectionUniqueConstraintViolation
			default:
				return err
			}
		}
		return err
	}
	return nil
}

func generateCollectionUpdatesWithoutID(in *dbmodel.Collection) map[string]interface{} {
	ret := map[string]interface{}{}
	if in.Name != nil {
		ret["name"] = *in.Name
	}
	if in.Dimension != nil {
		ret["dimension"] = *in.Dimension
	}
	if in.IsDeleted {
		ret["is_deleted"] = true
	}
	return ret
}

func (s *collectionDb) Update(in *dbmodel.Collection) error {
	log.Info("update collection", zap.Any("collection", in))
	updates := generateCollectionUpdatesWithoutID(in)
	err := s.db.Model(&dbmodel.Collection{}).Where("id = ?", in.ID).Updates(updates).Error
	if err != nil {
		log.Error("update collection failed", zap.Error(err))
		var pgErr *pgconn.PgError
		ok := errors.As(err, &pgErr)
		if ok {
			log.Error("Postgres Error")
			switch pgErr.Code {
			case "23505":
				log.Error("collection already exists")
				return common.ErrCollectionUniqueConstraintViolation
			default:
				return err
			}
		}
		return err
	}
	return nil
}

func (s *collectionDb) UpdateLogPositionAndVersionInfo(
	collectionID string,
	logPosition int64,
	currentCollectionVersion int32,
	currentVersionFileName string,
	newCollectionVersion int32,
	newVersionFileName string,
	totalRecordsPostCompaction uint64,
	sizeBytesPostCompaction uint64,
	lastCompactionTimeSecs uint64,
) (int64, error) {
	// TODO(rohitcp): Investigate if we need to hold the lock using "UPDATE"
	// strength, or if we can use "SELECT FOR UPDATE" or some other less
	// expensive locking mechanism. Taking the lock as a caution for now.
	result := s.db.Model(&dbmodel.Collection{}).
		Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("id = ? AND version = ? AND (version_file_name IS NULL OR version_file_name = ?)",
			collectionID,
			currentCollectionVersion,
			currentVersionFileName).
		Updates(map[string]interface{}{
			"log_position":                  logPosition,
			"version":                       newCollectionVersion,
			"version_file_name":             newVersionFileName,
			"total_records_post_compaction": totalRecordsPostCompaction,
			"size_bytes_post_compaction":    sizeBytesPostCompaction,
			"last_compaction_time_secs":     lastCompactionTimeSecs,
		})
	if result.Error != nil {
		return 0, result.Error
	}
	return result.RowsAffected, nil
}

func (s *collectionDb) UpdateLogPositionVersionTotalRecordsAndLogicalSize(collectionID string, logPosition int64, currentCollectionVersion int32, totalRecordsPostCompaction uint64, sizeBytesPostCompaction uint64, lastCompactionTimeSecs uint64, tenant string) (int32, error) {
	log.Info("update log position, version, and total records post compaction", zap.String("collectionID", collectionID), zap.Int64("logPosition", logPosition), zap.Int32("currentCollectionVersion", currentCollectionVersion), zap.Uint64("totalRecords", totalRecordsPostCompaction))
	var collection dbmodel.Collection
	// We use select for update to ensure no lost update happens even for isolation level read committed or below
	// https://patrick.engineering/posts/postgres-internals/
	err := s.db.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", collectionID).First(&collection).Error
	if err != nil {
		return 0, err
	}
	if collection.LogPosition > logPosition {
		return 0, common.ErrCollectionLogPositionStale
	}
	if collection.Version > currentCollectionVersion {
		return 0, common.ErrCollectionVersionStale
	}
	if collection.Version < currentCollectionVersion {
		// this should not happen, potentially a bug
		return 0, common.ErrCollectionVersionInvalid
	}

	version := currentCollectionVersion + 1
	err = s.db.Model(&dbmodel.Collection{}).Where("id = ?", collectionID).Updates(map[string]interface{}{"log_position": logPosition, "version": version, "total_records_post_compaction": totalRecordsPostCompaction, "size_bytes_post_compaction": sizeBytesPostCompaction, "last_compaction_time_secs": lastCompactionTimeSecs, "tenant": tenant}).Error
	if err != nil {
		return 0, err
	}
	return version, nil
}

func (s *collectionDb) UpdateVersionRelatedFields(collectionID, existingVersionFileName, newVersionFileName string, oldestVersionTs *time.Time, numActiveVersions *int) (int64, error) {
	// Create updates map with required version_file_name
	updates := map[string]interface{}{
		"version_file_name": newVersionFileName,
	}

	// Only add optional fields if they are not nil
	if oldestVersionTs != nil {
		updates["oldest_version_ts"] = oldestVersionTs
	}
	if numActiveVersions != nil {
		updates["num_versions"] = numActiveVersions
	}

	result := s.db.Model(&dbmodel.Collection{}).
		Where("id = ? AND (version_file_name IS NULL OR version_file_name = ?)",
			collectionID, existingVersionFileName).
		Updates(updates)
	if result.Error != nil {
		return 0, result.Error
	}
	return result.RowsAffected, nil
}

func (s *collectionDb) LockCollection(collectionID string) error {
	var collections []dbmodel.Collection
	err := s.db.Model(&dbmodel.Collection{}).
		Where("collections.id = ?", collectionID).Clauses(clause.Locking{
		Strength: "UPDATE",
	}).Find(&collections).Error
	if err != nil {
		return err
	}
	if len(collections) == 0 {
		return common.ErrCollectionNotFound
	}

	err = s.db.Model(&dbmodel.CollectionMetadata{}).
		Where("collection_metadata.collection_id = ?", collectionID).Clauses(clause.Locking{
		Strength: "UPDATE",
	}).Find(nil).Error
	if err != nil {
		return err
	}

	var segments []*dbmodel.Segment
	err = s.db.Model(&dbmodel.Segment{}).
		Where("segments.collection_id = ?", collectionID).Clauses(clause.Locking{
		Strength: "UPDATE",
	}).Find(&segments).Error
	if err != nil {
		return err
	}

	var segmentIDs []*string
	for _, segment := range segments {
		segmentIDs = append(segmentIDs, &segment.ID)
	}

	err = s.db.Model(&dbmodel.SegmentMetadata{}).
		Where("segment_metadata.segment_id IN ?", segmentIDs).Clauses(clause.Locking{
		Strength: "UPDATE",
	}).Find(nil).Error
	if err != nil {
		return err
	}

	return nil
}

func (s *collectionDb) UpdateCollectionLineageFilePath(collectionID string, currentLineageFileName string, newLineageFileName string) error {
	return s.db.Model(&dbmodel.Collection{}).
		Where("id = ? AND (lineage_file_name IS NULL OR lineage_file_name = ?)", collectionID, currentLineageFileName).
		Updates(map[string]interface{}{
			"lineage_file_name": newLineageFileName,
		}).Error

}
