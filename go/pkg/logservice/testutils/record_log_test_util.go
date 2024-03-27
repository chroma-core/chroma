package testutils

import (
	"strconv"

	"github.com/chroma-core/chroma/go/pkg/metastore/db/dbmodel"
	"github.com/chroma-core/chroma/go/pkg/types"
	"gorm.io/gorm"
)

func CreateCollections(db *gorm.DB, collectionIds ...types.UniqueID) error {
	// create test collections
	for index, collectionId := range collectionIds {
		collectionName := "collection" + strconv.Itoa(index+1)
		var collectionDimension int32 = 6
		collection := &dbmodel.Collection{
			ID:         collectionId.String(),
			Name:       &collectionName,
			Dimension:  &collectionDimension,
			DatabaseID: types.NewUniqueID().String(),
		}
		err := db.Create(collection).Error
		if err != nil {
			return err
		}
	}
	return nil
}

func CleanupCollections(db *gorm.DB, collectionIds ...types.UniqueID) error {
	// delete test collections
	for _, collectionId := range collectionIds {
		err := db.Where("id = ?", collectionId.String()).Delete(&dbmodel.Collection{}).Error
		if err != nil {
			return err
		}
	}

	// cleanup logs
	err := db.Where("collection_id in ?", collectionIds).Delete(&dbmodel.RecordLog{}).Error
	if err != nil {
		return err
	}
	return nil
}

func MoveLogPosition(db *gorm.DB, collectionId types.UniqueID, position int64) {
	db.Model(&dbmodel.Collection{}).Where("id = ?", collectionId.String()).Update("log_position", position)
}
