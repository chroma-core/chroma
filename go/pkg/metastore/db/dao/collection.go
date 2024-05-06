package dao

import (
	"database/sql"
	"errors"

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

func (s *collectionDb) GetCollections(id *string, name *string, tenantID string, databaseName string, limit *int32, offset *int32) (collectionWithMetdata []*dbmodel.CollectionAndMetadata, err error) {
	var collections []*dbmodel.Collection
	query := s.db.Table("collections").
		Select("collections.id, collections.log_position, collections.version, collections.name, collections.dimension, collections.database_id, databases.name, databases.tenant_id").
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
			collectionID         string
			logPosition          int64
			version              int32
			collectionName       string
			collectionDimension  sql.NullInt32
			collectionDatabaseID string
			collectionCreatedAt  sql.NullTime
			databaseName         string
			databaseTenantID     string
		)

		err := rows.Scan(&collectionID, &logPosition, &version, &collectionName, &collectionDimension, &collectionDatabaseID, &databaseName, &databaseTenantID)
		if err != nil {
			log.Error("scan collection failed", zap.Error(err))
			return nil, err
		}

		collection := &dbmodel.Collection{
			ID:          collectionID,
			Name:        &collectionName,
			DatabaseID:  collectionDatabaseID,
			LogPosition: logPosition,
			Version:     version,
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
	err := s.db.Model(&dbmodel.Collection{}).Where("id = ?", in.ID).Updates(updates).Error
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

func (s *collectionDb) UpdateLogPositionAndVersion(collectionID string, logPosition int64, currentCollectionVersion int32) (int32, error) {
	log.Info("update log position and version", zap.String("collectionID", collectionID), zap.Int64("logPosition", logPosition), zap.Int32("currentCollectionVersion", currentCollectionVersion))
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
	err = s.db.Model(&dbmodel.Collection{}).Where("id = ?", collectionID).Updates(map[string]interface{}{"log_position": logPosition, "version": version}).Error
	if err != nil {
		return 0, err
	}
	return version, nil
}
