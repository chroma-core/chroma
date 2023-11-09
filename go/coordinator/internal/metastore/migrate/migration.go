package migrate

import (
	"fmt"
	"sort"

	"github.com/go-gormigrate/gormigrate/v2"
	"gorm.io/gorm"
)

var migrationRecords []*migration

type migration struct {
	migrate func(*gorm.DB) error
	version string
}

func RunMigration(db *gorm.DB) error {
	// we don't depend on foreign key constraint, so just disable it.
	db.DisableForeignKeyConstraintWhenMigrating = true
	return doMigrate(db, migrationRecords)
}

func AddMigrateRecord(version string, m func(*gorm.DB) error) bool {
	checkDuplicateMigrationVersion(version, migrationRecords)
	migrationRecords = append(migrationRecords, &migration{
		version: version,
		migrate: m,
	})
	return true
}

// byVersion implements sort.Interface based on the version field.
type byVersion []*migration

func (a byVersion) Len() int           { return len(a) }
func (a byVersion) Less(i, j int) bool { return a[i].version < a[j].version }
func (a byVersion) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

func doMigrate(db *gorm.DB, migrations []*migration) error {
	sort.Sort(byVersion(migrations))
	var gormMigrations []*gormigrate.Migration
	for _, item := range migrations {
		gormMigrations = append(gormMigrations, &gormigrate.Migration{
			ID:      item.version,
			Migrate: item.migrate,
		})
	}
	options := &gormigrate.Options{
		TableName:                 "system_catalog_migrations",
		IDColumnName:              "id",
		IDColumnSize:              255,
		UseTransaction:            false,
		ValidateUnknownMigrations: false,
	}
	goMigration := gormigrate.New(db, options, gormMigrations)
	return goMigration.Migrate()
}

func checkDuplicateMigrationVersion(version string, records []*migration) {
	for _, item := range records {
		if item.version == version {
			panic(fmt.Sprintf("migration version is already added: %s", version))
		}
	}
}
