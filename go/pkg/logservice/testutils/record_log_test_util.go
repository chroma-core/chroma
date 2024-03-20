package testutils

import (
	"github.com/chroma-core/chroma/go/pkg/types"
	"gorm.io/gorm"
)

func SetupTest(db *gorm.DB, collectionIds ...types.UniqueID) {
	//dbcore.ResetTestTables(db)
	//
	//// create test collections
	//for index, collectionId := range collectionIds {
	//	collectionName := "collection" + strconv.Itoa(index+1)
	//	collectionTopic := "topic" + strconv.Itoa(index+1)
	//	var collectionDimension int32 = 6
	//	collection := &dbmodel.Collection{
	//		ID:         collectionId.String(),
	//		Name:       &collectionName,
	//		Topic:      &collectionTopic,
	//		Dimension:  &collectionDimension,
	//		DatabaseID: types.NewUniqueID().String(),
	//	}
	//	err := db.Create(collection).Error
	//	if err != nil {
	//		log.Error("create collection error", zap.Error(err))
	//	}
	//}
}

func TearDownTest(db *gorm.DB) {
	//db.Migrator().DropTable(&dbmodel.RecordLog{})
	//db.Migrator().CreateTable(&dbmodel.RecordLog{})
}

func MoveLogPosition(db *gorm.DB, collectionId types.UniqueID, position int64) {
	//db.Model(&dbmodel.Collection{}).Where("id = ?", collectionId.String()).Update("log_position", position)
}
