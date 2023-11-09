package migrate

import (
	"github.com/chroma/chroma-coordinator/internal/metastore/db/dbmodel"
	"github.com/pingcap/log"
	"gorm.io/gorm"
)

var _ = AddMigrateRecord(
	"202311091500",
	func(db *gorm.DB) error {
		log.Info("migration 202311091500 started")
		if err := db.AutoMigrate(&dbmodel.Segment{}); err != nil {
			return err
		}
		if err := db.AutoMigrate(dbmodel.SegmentMetadata{}); err != nil {
			return err
		}
		log.Info("migration 202311091500 finished")
		return nil
	},
)
