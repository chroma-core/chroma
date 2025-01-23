package dao

import (
	"database/sql"
	"errors"

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
		Select("collections.id, collections.name, collections.database_id, collections.is_deleted, databases.name, databases.tenant_id").
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

func (s *collectionDb) GetCollections(id *string, name *string, tenantID string, databaseName string, limit *int32, offset *int32) ([]*dbmodel.CollectionAndMetadata, error) {
	return s.getCollections(id, name, tenantID, databaseName, limit, offset, false)
}

func (s *collectionDb) ListCollectionsToGc() ([]*dbmodel.CollectionToGc, error) {
	// TODO(Sanket): Read version file path.
	var collections []*dbmodel.CollectionToGc
	// Use the read replica for this so as to not overwhelm the writer.
	// Skip collections that have not been compacted even once.
	err := s.read_db.Table("collections").Select("id, name, version").Find(&collections).Where("version > 0").Error
	if err != nil {
		return nil, err
	}
	return collections, nil
}

func (s *collectionDb) getCollections(id *string, name *string, tenantID string, databaseName string, limit *int32, offset *int32, is_deleted bool) (collectionWithMetdata []*dbmodel.CollectionAndMetadata, err error) {
	var collections []*dbmodel.Collection
	query := s.db.Table("collections").
		Select("collections.id, collections.log_position, collections.version, collections.name, collections.configuration_json_str, collections.dimension, collections.database_id, collections.is_deleted, collections.total_records_post_compaction, databases.name, databases.tenant_id").
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
	rows, err := query.Rows()
	if err != nil {
		return nil, err
	}
	collectionWithMetdata = make([]*dbmodel.CollectionAndMetadata, 0, len(collections))
	for rows.Next() {
		var (
			collectionID                   string
			logPosition                    int64
			version                        int32
			collectionName                 string
			collectionConfigurationJsonStr string
			collectionDimension            sql.NullInt32
			collectionDatabaseID           string
			collectionIsDeleted            bool
			collectionCreatedAt            sql.NullTime
			databaseName                   string
			databaseTenantID               string
			totalRecordsPostCompaction     uint64
		)

		err := rows.Scan(&collectionID, &logPosition, &version, &collectionName, &collectionConfigurationJsonStr, &collectionDimension, &collectionDatabaseID, &collectionIsDeleted, &totalRecordsPostCompaction, &databaseName, &databaseTenantID)
		if err != nil {
			log.Error("scan collection failed", zap.Error(err))
			return nil, err
		}

		collection := &dbmodel.Collection{
			ID:                         collectionID,
			Name:                       &collectionName,
			ConfigurationJsonStr:       &collectionConfigurationJsonStr,
			DatabaseID:                 collectionDatabaseID,
			LogPosition:                logPosition,
			Version:                    version,
			IsDeleted:                  collectionIsDeleted,
			TotalRecordsPostCompaction: totalRecordsPostCompaction,
		}
		if collectionDimension.Valid {
			collection.Dimension = &collectionDimension.Int32
		}
		if collectionCreatedAt.Valid {
			collection.CreatedAt = collectionCreatedAt.Time
		}

		collectionWithMetdata = append(collectionWithMetdata, &dbmodel.CollectionAndMetadata{
			Collection:   collection,
			TenantID:     databaseTenantID,
			DatabaseName: databaseName,
		})
	}
	rows.Close()
	for _, collection := range collectionWithMetdata {
		var metadata []*dbmodel.CollectionMetadata
		err = s.db.Where("collection_id = ?", collection.Collection.ID).Find(&metadata).Error
		if err != nil {
			log.Error("get collection metadata failed", zap.Error(err))
			return nil, err
		}
		collection.CollectionMetadata = metadata
	}

	return
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
) (int64, error) {
	// TODO(rohitcp): Investigate if we need to hold the lock using "UPDATE"
	// strength, or if we can use "SELECT FOR UPDATE" or some other less
	// expensive locking mechanism. Taking the lock as a caution for now.
	result := s.db.Model(&dbmodel.Collection{}).
		Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("id = ? AND version = ? AND version_file_name = ?",
			collectionID,
			currentCollectionVersion,
			currentVersionFileName).
		Updates(map[string]interface{}{
			"log_position":      logPosition,
			"version":           newCollectionVersion,
			"version_file_name": newVersionFileName,
		})
	if result.Error != nil {
		return 0, result.Error
	}
	return result.RowsAffected, nil
}

func (s *collectionDb) UpdateLogPositionVersionAndTotalRecords(collectionID string, logPosition int64, currentCollectionVersion int32, totalRecordsPostCompaction uint64) (int32, error) {
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
	err = s.db.Model(&dbmodel.Collection{}).Where("id = ?", collectionID).Updates(map[string]interface{}{"log_position": logPosition, "version": version, "total_records_post_compaction": totalRecordsPostCompaction}).Error
	if err != nil {
		return 0, err
	}
	return version, nil
}
