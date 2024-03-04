package testutils

import (
	"github.com/chroma-core/chroma/go/internal/metastore/db/dbmodel"
	"github.com/chroma-core/chroma/go/internal/types"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"strconv"
)

func SetupTest(db *gorm.DB, collectionIds ...types.UniqueID) {
	db.Migrator().DropTable(&dbmodel.Segment{})
	db.Migrator().CreateTable(&dbmodel.Segment{})
	db.Migrator().DropTable(&dbmodel.Collection{})
	db.Migrator().CreateTable(&dbmodel.Collection{})
	db.Migrator().DropTable(&dbmodel.RecordLog{})
	db.Migrator().CreateTable(&dbmodel.RecordLog{})

	// create test collections
	for index, collectionId := range collectionIds {
		collectionName := "collection" + strconv.Itoa(index+1)
		collectionTopic := "topic" + strconv.Itoa(index+1)
		var collectionDimension int32 = 6
		collection := &dbmodel.Collection{
			ID:         collectionId.String(),
			Name:       &collectionName,
			Topic:      &collectionTopic,
			Dimension:  &collectionDimension,
			DatabaseID: types.NewUniqueID().String(),
		}
		err := db.Create(collection).Error
		if err != nil {
			log.Error("create collection error", zap.Error(err))
		}
	}
}

func TearDownTest(db *gorm.DB) {
	db.Migrator().DropTable(&dbmodel.Segment{})
	db.Migrator().CreateTable(&dbmodel.Segment{})
	db.Migrator().DropTable(&dbmodel.Collection{})
	db.Migrator().CreateTable(&dbmodel.Collection{})
	db.Migrator().DropTable(&dbmodel.RecordLog{})
	db.Migrator().CreateTable(&dbmodel.RecordLog{})
}

func MoveLogPosition(db *gorm.DB, collectionId types.UniqueID, position int64) {
	db.Model(&dbmodel.Collection{}).Where("id = ?", collectionId.String()).Update("log_position", position)
}
