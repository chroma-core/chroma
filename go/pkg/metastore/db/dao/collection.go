package dao

import (
	"database/sql"
	"errors"
	"strings"

	"github.com/chroma-core/chroma/go/pkg/common"
	"github.com/jackc/pgx/v5/pgconn"
	"gorm.io/gorm/clause"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/chroma-core/chroma/go/pkg/metastore/db/dbmodel"
	"github.com/pingcap/log"
)

type collectionDb struct {
	db *gorm.DB
}

var _ dbmodel.ICollectionDb = &collectionDb{}

func (s *collectionDb) DeleteAll() error {
	return s.db.Where("1 = 1").Delete(&dbmodel.Collection{}).Error
}

func (s *collectionDb) GetCollections(id *string, name *string, tenantID string, databaseName string, limit *int32, offset *int32) ([]*dbmodel.CollectionAndMetadata, error) {
	var getCollectionInput strings.Builder
	getCollectionInput.WriteString("GetCollections input: ")

	var collections []*dbmodel.CollectionAndMetadata

	query := s.db.Table("collections").
		Select("collections.id, collections.log_position, collections.version, collections.name, collections.dimension, collections.database_id, collections.created_at, databases.name, databases.tenant_id, collection_metadata.key, collection_metadata.str_value, collection_metadata.int_value, collection_metadata.float_value").
		Joins("LEFT JOIN collection_metadata ON collections.id = collection_metadata.collection_id").
		Joins("INNER JOIN databases ON collections.database_id = databases.id").
		Order("collections.id")
	if limit != nil {
		query = query.Limit(int(*limit))
		getCollectionInput.WriteString("limit: " + string(*limit) + ", ")
	}

	if offset != nil {
		query = query.Offset(int(*offset))
		getCollectionInput.WriteString("offset: " + string(*offset) + ", ")
	}

	if databaseName != "" {
		query = query.Where("databases.name = ?", databaseName)
		getCollectionInput.WriteString("databases.name: " + databaseName + ", ")
	}

	if tenantID != "" {
		query = query.Where("databases.tenant_id = ?", tenantID)
		getCollectionInput.WriteString("databases.tenant_id: " + tenantID + ", ")
	}

	if id != nil {
		query = query.Where("collections.id = ?", *id)
		getCollectionInput.WriteString("collections.id: " + *id + ", ")
	}
	if name != nil {
		query = query.Where("collections.name = ?", *name)
		getCollectionInput.WriteString("collections.name: " + *name + ", ")
	}
	log.Info(getCollectionInput.String())

	rows, err := query.Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var currentCollectionID string = ""
	var metadata []*dbmodel.CollectionMetadata
	var currentCollection *dbmodel.CollectionAndMetadata

	for rows.Next() {
		var (
			collectionID         string
			logPosition          int64
			version              int32
			collectionName       string
			collectionDimension  sql.NullInt32
			collectionDatabaseID string
			collectionCreatedAt  sql.NullTime
			databaseName         string
			databaseTenantID     string
			key                  sql.NullString
			strValue             sql.NullString
			intValue             sql.NullInt64
			floatValue           sql.NullFloat64
		)

		err := rows.Scan(&collectionID, &logPosition, &version, &collectionName, &collectionDimension, &collectionDatabaseID, &collectionCreatedAt, &databaseName, &databaseTenantID, &key, &strValue, &intValue, &floatValue)
		if err != nil {
			log.Error("scan collection failed", zap.Error(err))
			return nil, err
		}
		if collectionID != currentCollectionID {
			currentCollectionID = collectionID
			metadata = nil

			currentCollection = &dbmodel.CollectionAndMetadata{
				Collection: &dbmodel.Collection{
					ID:          collectionID,
					Name:        &collectionName,
					DatabaseID:  collectionDatabaseID,
					LogPosition: logPosition,
					Version:     version,
				},
				CollectionMetadata: metadata,
				TenantID:           databaseTenantID,
				DatabaseName:       databaseName,
			}
			if collectionDimension.Valid {
				currentCollection.Collection.Dimension = &collectionDimension.Int32
			} else {
				currentCollection.Collection.Dimension = nil
			}
			if collectionCreatedAt.Valid {
				currentCollection.Collection.CreatedAt = collectionCreatedAt.Time
			}

			if currentCollectionID != "" {
				collections = append(collections, currentCollection)
			}
		}

		collectionMetadata := &dbmodel.CollectionMetadata{
			CollectionID: collectionID,
		}

		if key.Valid {
			collectionMetadata.Key = &key.String
		} else {
			collectionMetadata.Key = nil
		}

		if strValue.Valid {
			collectionMetadata.StrValue = &strValue.String
		} else {
			collectionMetadata.StrValue = nil
		}
		if intValue.Valid {
			collectionMetadata.IntValue = &intValue.Int64
		} else {
			collectionMetadata.IntValue = nil
		}
		if floatValue.Valid {
			collectionMetadata.FloatValue = &floatValue.Float64
		} else {
			collectionMetadata.FloatValue = nil
		}

		metadata = append(metadata, collectionMetadata)
		currentCollection.CollectionMetadata = metadata
	}
	log.Info("collections", zap.Any("collections", collections))
	return collections, nil
}

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
	return ret
}

func (s *collectionDb) Update(in *dbmodel.Collection) error {
	log.Info("update collection", zap.Any("collection", in))
	updates := generateCollectionUpdatesWithoutID(in)
	return s.db.Model(&dbmodel.Collection{}).Where("id = ?", in.ID).Updates(updates).Error
}

func (s *collectionDb) UpdateLogPositionAndVersion(collectionID string, logPosition int64, currentCollectionVersion int32) (int32, error) {
	log.Info("update log position and version", zap.String("collectionID", collectionID), zap.Int64("logPosition", logPosition), zap.Int32("currentCollectionVersion", currentCollectionVersion))
	var collection dbmodel.Collection
	err := s.db.Where("id = ?", collectionID).First(&collection).Error
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
	err = s.db.Model(&dbmodel.Collection{}).Where("id = ?", collectionID).Updates(map[string]interface{}{"log_position": logPosition, "version": version}).Error
	if err != nil {
		return 0, err
	}
	return version, nil
}
