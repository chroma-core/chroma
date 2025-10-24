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

	"github.com/chroma-core/chroma/go/pkg/sysdb/metastore/db/dbcore"
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

func (s *collectionDb) GetCollectionWithoutMetadata(collectionID *string, databaseName *string, softDeletedFlag *bool) (*dbmodel.Collection, error) {
	var collections []*dbmodel.Collection
	query := s.db.Table("collections").
		Select("collections.id, collections.name, collections.database_id, collections.is_deleted, collections.tenant, collections.version, collections.version_file_name, collections.log_position, NULLIF(collections.root_collection_id, '') AS root_collection_id, NULLIF(collections.lineage_file_name, '') AS lineage_file_name").
		Joins("INNER JOIN databases ON collections.database_id = databases.id").
		Where("collections.id = ?", collectionID)

	if databaseName != nil && *databaseName != "" {
		query = query.Where("databases.name = ?", databaseName)
	}

	if softDeletedFlag != nil {
		query = query.Where("collections.is_deleted = ?", *softDeletedFlag)
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
	ids := []string{}
	if id != nil {
		ids = append(ids, *id)
	}
	return s.getCollections(ids, name, tenantID, databaseName, limit, offset, nil)
}

func (s *collectionDb) GetCollections(ids []string, name *string, tenantID string, databaseName string, limit *int32, offset *int32, includeSoftDeleted bool) ([]*dbmodel.CollectionAndMetadata, error) {
	isDeleted := false
	isDeletedPtr := &isDeleted
	if includeSoftDeleted {
		isDeletedPtr = nil
	}

	return s.getCollections(ids, name, tenantID, databaseName, limit, offset, isDeletedPtr)
}

func (s *collectionDb) GetCollectionByResourceName(tenantResourceName string, databaseName string, collectionName string) (*dbmodel.CollectionAndMetadata, error) {
	var tenant dbmodel.Tenant
	err := s.db.Table("tenants").Where("resource_name = ?", tenantResourceName).First(&tenant).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, common.ErrCollectionNotFound
		}
		return nil, err
	}

	isDeleted := false
	isDeletedPtr := &isDeleted

	collections, err := s.getCollections(nil, &collectionName, tenant.ID, databaseName, nil, nil, isDeletedPtr)
	if err != nil {
		return nil, err
	}
	if len(collections) == 0 {
		return nil, common.ErrCollectionNotFound
	}
	return collections[0], nil
}

func (s *collectionDb) ListCollectionsToGc(cutoffTimeSecs *uint64, limit *uint64, tenantID *string, minVersionsIfAlive *uint64) ([]*dbmodel.CollectionToGc, error) {
	// There are three types of collections:
	// 1. Regular: a collection created by a normal call to create_collection(). Does not have a root_collection_id or a lineage_file_name.
	// 2. Root of fork tree: a collection created by a call to create_collection() which was later the source of a fork with fork(). Has a lineage_file_name.
	// 3. Fork of a root: a collection created by a call to fork(). Has a root_collection_id.
	//
	// For the purposes of this method, we group by fork "trees". A fork tree is a root collection and all its forks (or, in the case of regular collections, a single collection). For every fork tree, we check if at least one collection in the tree meets the GC requirements. If so, we return the root collection of the tree. We ignore forks in the response as the garbage collector will GC forks when run on the root collection.

	sub := s.read_db.Table("collections").
		Select("COALESCE(NULLIF(root_collection_id, ''), id) AS id, MIN(oldest_version_ts) AS min_oldest_version_ts, MAX(num_versions) AS max_num_versions, BOOL_OR(is_deleted) AS any_deleted").
		Group("COALESCE(NULLIF(root_collection_id, ''), id)").
		Where("version_file_name IS NOT NULL").
		Where("version_file_name != ''")

	if tenantID != nil {
		sub = sub.Where("tenant = ?", *tenantID)
	}

	query := s.read_db.Table("collections").
		Select("collections.id, collections.name, collections.version_file_name, sub.min_oldest_version_ts AS oldest_version_ts, databases.tenant_id, NULLIF(collections.lineage_file_name, '') AS lineage_file_name").
		Joins("INNER JOIN databases ON collections.database_id = databases.id").
		Joins("INNER JOIN (?) AS sub ON collections.id = sub.id", sub)

	// Apply cutoff time filter only if provided
	if cutoffTimeSecs != nil {
		cutoffTime := time.Unix(int64(*cutoffTimeSecs), 0)
		query = query.Where("oldest_version_ts < ?", cutoffTime)
	}

	if minVersionsIfAlive != nil {
		query = query.Where("sub.max_num_versions >= ? OR sub.any_deleted = true", minVersionsIfAlive)
	}

	query = query.Order("sub.max_num_versions DESC")

	// Apply limit only if provided
	if limit != nil {
		query = query.Limit(int(*limit))
	}

	var collections []*dbmodel.CollectionToGc
	err := query.Find(&collections).Error
	if err != nil {
		return nil, err
	}
	log.Debug("collections to gc", zap.Any("collections", collections))
	return collections, nil
}

func (s *collectionDb) getCollections(ids []string, name *string, tenantID string, databaseName string, limit *int32, offset *int32, is_deleted *bool) (collectionWithMetdata []*dbmodel.CollectionAndMetadata, err error) {
	type Result struct {
		// Collection fields
		CollectionId               string     `gorm:"column:collection_id"`
		CollectionName             *string    `gorm:"column:collection_name"`
		ConfigurationJsonStr       *string    `gorm:"column:configuration_json_str"`
		SchemaStr                  *string    `gorm:"column:schema_str"`
		Dimension                  *int32     `gorm:"column:dimension"`
		DatabaseID                 string     `gorm:"column:database_id"`
		CollectionTs               *int64     `gorm:"column:collection_ts"`
		IsDeleted                  bool       `gorm:"column:is_deleted"`
		CollectionCreatedAt        *time.Time `gorm:"column:collection_created_at"`
		CollectionUpdatedAt        *time.Time `gorm:"column:collection_updated_at"`
		LogPosition                int64      `gorm:"column:log_position"`
		Version                    int32      `gorm:"column:version"`
		VersionFileName            string     `gorm:"column:version_file_name"`
		RootCollectionId           *string    `gorm:"column:root_collection_id"`
		LineageFileName            *string    `gorm:"column:lineage_file_name"`
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

	isQueryOptimized := dbcore.IsOptimizedCollectionQueriesEnabled() && databaseName != "" && tenantID != ""

	query := s.db.Table("collections")
	collection_targets := "collections.id as collection_id, " +
		"collections.name as collection_name, " +
		"collections.configuration_json_str, " +
		"collections.schema_str, " +
		"collections.dimension, " +
		"collections.database_id AS database_id, " +
		"collections.ts as collection_ts, " +
		"collections.is_deleted, " +
		"collections.created_at as collection_created_at, " +
		"collections.updated_at as collection_updated_at, " +
		"collections.log_position, " +
		"collections.version, " +
		"collections.version_file_name, " +
		"collections.root_collection_id, " +
		"NULLIF(collections.lineage_file_name, '') AS lineage_file_name, " +
		"collections.total_records_post_compaction, " +
		"collections.size_bytes_post_compaction, " +
		"collections.last_compaction_time_secs, "
	db_targets := "databases.name as database_name, databases.tenant_id as db_tenant_id, "
	collection_tenant := "collections.tenant as tenant"

	if isQueryOptimized {
		db_id_query := s.db.Model(&dbmodel.Database{}).
			Select("id").
			Where("tenant_id = ?", tenantID).
			Where("name = ?", databaseName).
			Limit(1)

		// We rewrite the query to get the one database_id with what is hopefully an initplan
		// that first gets the database_id and then uses it to do an ordered scan over
		// the matching collections.
		query = query.Select(collection_targets+"? as database_name, ? as db_tenant_id, "+collection_tenant, databaseName, tenantID).
			Where("collections.database_id = (?)", db_id_query)
	} else {
		query = query.Select(collection_targets + db_targets + collection_tenant).
			Joins("INNER JOIN databases ON collections.database_id = databases.id")
	}

	query = query.Order("collections.created_at ASC")

	if databaseName != "" && !isQueryOptimized {
		query = query.Where("databases.name = ?", databaseName)
	}
	if tenantID != "" && !isQueryOptimized {
		query = query.Where("databases.tenant_id = ?", tenantID)
	}
	if ids != nil {
		query = query.Where("collections.id IN ?", ids)
	}
	if name != nil {
		query = query.Where("collections.name = ?", *name)
	}
	if is_deleted != nil {
		query = query.Where("collections.is_deleted = ?", *is_deleted)
	}

	if limit != nil {
		query = query.Limit(int(*limit))
	}
	if offset != nil {
		query = query.Offset(int(*offset))
	}

	var results []Result
	query = s.db.Table("(?) as ci", query).
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
		Joins("LEFT JOIN collection_metadata cm ON cm.collection_id = ci.collection_id")

	if isQueryOptimized {
		// Setting random_page_cost to 1.1 because that's usually the recommended value
		// for SSD based databases. This encourages index usage. The default used
		// to be 4.0 which was more for HDD based databases where random seeking
		// was way more expensive than sequential access.
		var dummy []Result
		stmt := query.Session(&gorm.Session{DryRun: true}).Find(&dummy).Statement
		sqlString := stmt.SQL.String()

		// Use a transaction to execute both commands in a single round trip
		err = s.db.Transaction(func(tx *gorm.DB) error {
			if err := tx.Exec("SET LOCAL random_page_cost = 1.1").Error; err != nil {
				return err
			}
			return tx.Raw(sqlString, stmt.Vars...).Scan(&results).Error
		})
	} else {
		err = query.Scan(&results).Error
	}

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
				SchemaStr:                  r.SchemaStr,
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
				UpdatedAt:                  *r.CollectionUpdatedAt,
				CreatedAt:                  *r.CollectionCreatedAt,
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
	isDeleted := true
	ids := ([]string)(nil)
	if collectionID != nil {
		ids = []string{*collectionID}
	}
	return s.getCollections(ids, nil, tenantID, databaseName, &limit, nil, &isDeleted)
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
		log.Error("Insert collection failed", zap.Error(err))
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

// InsertOnConflictDoNothing inserts a collection into the database, ignoring any conflicts.
// It returns true if the collection was inserted, false if it already existed.
// It returns an error if there was a problem with the insert.
// This is used for upstream get_or_create
func (s *collectionDb) InsertOnConflictDoNothing(in *dbmodel.Collection) (didInsert bool, err error) {
	// Ignore conflict on (name, database_id) since we have "idx_name" unique index on it in migration 20240411201006
	tx := s.db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "name"}, {Name: "database_id"}},
		DoNothing: true,
	}).Create(&in)
	if tx.Error != nil {
		log.Error("InsertOnConflictDoNothing collection failed", zap.Error(err))
		return false, err
	}
	if tx.RowsAffected == 0 {
		log.Debug("InsertOnConflictDoNothing collection already exists")
		return false, nil
	} else {
		log.Debug("InsertOnConflictDoNothing collection inserted")
		return true, nil
	}
}

func generateCollectionUpdatesWithoutID(in *dbmodel.Collection) map[string]interface{} {
	ret := map[string]interface{}{}
	if in.Name != nil {
		ret["name"] = *in.Name
	}
	if in.ConfigurationJsonStr != nil {
		ret["configuration_json_str"] = *in.ConfigurationJsonStr
	}
	if in.SchemaStr != nil {
		ret["schema_str"] = *in.SchemaStr
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
	numVersions uint64,
	schemaStr *string,
) (int64, error) {
	// TODO(rohitcp): Investigate if we need to hold the lock using "UPDATE"
	// strength, or if we can use "SELECT FOR UPDATE" or some other less
	// expensive locking mechanism. Taking the lock as a caution for now.
	updates := map[string]interface{}{
		"log_position":                  logPosition,
		"version":                       newCollectionVersion,
		"version_file_name":             newVersionFileName,
		"total_records_post_compaction": totalRecordsPostCompaction,
		"size_bytes_post_compaction":    sizeBytesPostCompaction,
		"last_compaction_time_secs":     lastCompactionTimeSecs,
		"num_versions":                  numVersions,
	}

	if schemaStr != nil {
		updates["schema_str"] = schemaStr
	}

	result := s.db.Model(&dbmodel.Collection{}).
		Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("id = ? AND version = ? AND (version_file_name IS NULL OR version_file_name = ?)",
			collectionID,
			currentCollectionVersion,
			currentVersionFileName).
		Updates(updates)
	if result.Error != nil {
		return 0, result.Error
	}
	return result.RowsAffected, nil
}

func (s *collectionDb) UpdateLogPositionVersionTotalRecordsAndLogicalSize(collectionID string, logPosition int64, currentCollectionVersion int32, totalRecordsPostCompaction uint64, sizeBytesPostCompaction uint64, lastCompactionTimeSecs uint64, tenant string, schemaStr *string) (int32, error) {
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
	// only writing if schemaStr is not nil to avoid overwriting the schemaStr
	if schemaStr != nil {
		err = s.db.Model(&dbmodel.Collection{}).Where("id = ?", collectionID).Updates(map[string]interface{}{"log_position": logPosition, "version": version, "total_records_post_compaction": totalRecordsPostCompaction, "size_bytes_post_compaction": sizeBytesPostCompaction, "last_compaction_time_secs": lastCompactionTimeSecs, "tenant": tenant, "schema_str": schemaStr}).Error
	} else {
		err = s.db.Model(&dbmodel.Collection{}).Where("id = ?", collectionID).Updates(map[string]interface{}{"log_position": logPosition, "version": version, "total_records_post_compaction": totalRecordsPostCompaction, "size_bytes_post_compaction": sizeBytesPostCompaction, "last_compaction_time_secs": lastCompactionTimeSecs, "tenant": tenant}).Error
	}
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

func (s *collectionDb) LockCollection(collectionID string) (*bool, error) {
	var collections []dbmodel.Collection
	err := s.db.Model(&dbmodel.Collection{}).
		Where("collections.id = ?", collectionID).Clauses(clause.Locking{
		Strength: "UPDATE",
	}).Find(&collections).Error
	if err != nil {
		return nil, err
	}
	if len(collections) == 0 {
		return nil, common.ErrCollectionNotFound
	}

	err = s.db.Model(&dbmodel.CollectionMetadata{}).
		Where("collection_metadata.collection_id = ?", collectionID).Clauses(clause.Locking{
		Strength: "UPDATE",
	}).Find(nil).Error
	if err != nil {
		return nil, err
	}

	var segments []*dbmodel.Segment
	err = s.db.Model(&dbmodel.Segment{}).
		Where("segments.collection_id = ?", collectionID).Clauses(clause.Locking{
		Strength: "UPDATE",
	}).Find(&segments).Error
	if err != nil {
		return nil, err
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
		return nil, err
	}

	return &collections[0].IsDeleted, nil
}

func (s *collectionDb) UpdateCollectionLineageFilePath(collectionID string, currentLineageFileName *string, newLineageFileName string) error {
	return s.db.Model(&dbmodel.Collection{}).
		Where("id = ? AND (lineage_file_name IS NULL OR lineage_file_name = ?)", collectionID, currentLineageFileName).
		Updates(map[string]interface{}{
			"lineage_file_name": newLineageFileName,
		}).Error

}

func (s *collectionDb) BatchGetCollectionVersionFilePaths(collectionIDs []string) (map[string]string, error) {
	var collections []dbmodel.Collection
	err := s.read_db.Model(&dbmodel.Collection{}).
		Select("id, version_file_name").
		Where("id IN ?", collectionIDs).
		Find(&collections).Error
	if err != nil {
		return nil, err
	}

	result := make(map[string]string)
	for _, collection := range collections {
		result[collection.ID] = collection.VersionFileName
	}
	return result, nil
}

func (s *collectionDb) BatchGetCollectionSoftDeleteStatus(collectionIDs []string) (map[string]bool, error) {
	var collections []dbmodel.Collection
	err := s.read_db.Model(&dbmodel.Collection{}).
		Select("id, is_deleted").
		Where("id IN ?", collectionIDs).
		Find(&collections).Error
	if err != nil {
		return nil, err
	}

	result := make(map[string]bool)
	for _, collection := range collections {
		result[collection.ID] = collection.IsDeleted
	}
	return result, nil
}
