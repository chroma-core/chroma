package migrate

import (
	"github.com/chroma/chroma-coordinator/internal/metastore/db/dbmodel"
	"github.com/pingcap/log"
	"gorm.io/gorm"
)

var models = []interface{}{
	&dbmodel.Tenant{},
	&dbmodel.Database{},
	&dbmodel.Collection{},
	&dbmodel.CollectionMetadata{},
}

var _ = AddMigrateRecord(
	"202311091400",
	func(db *gorm.DB) error {
		log.Info("migration 202311091400 started")
		if err := db.AutoMigrate(models...); err != nil {
			return err
		}
		log.Info("migration 202311091400 finished")
		return nil
	},
)
