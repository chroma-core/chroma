package dao

import (
	"database/sql"
	"github.com/chroma-core/chroma/go/pkg/logservice/db/dbmodel"
	"github.com/chroma-core/chroma/go/pkg/types"
	"gorm.io/gorm"
)

//go:generate mockery --name=ICollectionPositionDb
type ICollectionPositionDb interface {
	SetCollectionPosition(collectionID types.UniqueID, position int64) error
	GetCollectionPosition(collectionID types.UniqueID) (position int64, err error)
}

type collectionPositionDb struct {
	db *gorm.DB
}

func (s *collectionPositionDb) SetCollectionPosition(collectionID types.UniqueID, position int64) error {
	id := types.FromUniqueID(collectionID)
	// Update only if the new position is greater than the current position
	var rawSql = `
			INSERT INTO collection_position (id, log_position)
			VALUES
				(@id, @log_position) 
			ON CONFLICT (id) 
			DO UPDATE 
			SET log_position = @log_position
			WHERE collection_position.log_position < EXCLUDED.log_position;
	`
	return s.db.Exec(rawSql, sql.Named("id", id), sql.Named("log_position", position)).Error
}

func (s *collectionPositionDb) GetCollectionPosition(collectionID types.UniqueID) (position int64, err error) {
	var collectionPosition dbmodel.CollectionPosition
	s.db.Where("id = ?", types.FromUniqueID(collectionID)).Find(&collectionPosition)
	return collectionPosition.LogPosition, nil
}
