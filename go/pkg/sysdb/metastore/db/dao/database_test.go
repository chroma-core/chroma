package dao

import (
	"fmt"
	"testing"

	"github.com/chroma-core/chroma/go/pkg/sysdb/metastore/db/dbcore"
	"github.com/chroma-core/chroma/go/pkg/sysdb/metastore/db/dbmodel"
	"github.com/chroma-core/chroma/go/pkg/types"
	"github.com/pingcap/log"
	"github.com/stretchr/testify/suite"
	"gorm.io/gorm"
)

type DatabaseDbTestSuite struct {
	suite.Suite
	db       *gorm.DB
	Db       *databaseDb
	TenantDb *tenantDb
	t        *testing.T
}

func (suite *DatabaseDbTestSuite) SetupSuite() {
	log.Info("setup suite")
	suite.db, _ = dbcore.ConfigDatabaseForTesting()
	suite.Db = &databaseDb{db: suite.db}
	suite.TenantDb = &tenantDb{db: suite.db}
}

// TestDatabaseDb_SoftDeleteRenamesRow verifies that SoftDelete renames the
// database row to "_deleted_<name>_<id>" and flips is_deleted, mirroring the
// collection soft-delete pattern. This frees the original name for reuse.
func (suite *DatabaseDbTestSuite) TestDatabaseDb_SoftDeleteRenamesRow() {
	tenantID := "testSoftDeleteRenamesRow_tenant"
	suite.Require().NoError(suite.TenantDb.Insert(&dbmodel.Tenant{ID: tenantID}))
	defer suite.db.Delete(&dbmodel.Tenant{}, "id = ?", tenantID)

	dbID := types.NewUniqueID().String()
	originalName := "testSoftDeleteRenamesRow_db"
	suite.Require().NoError(suite.Db.Insert(&dbmodel.Database{
		ID:       dbID,
		Name:     originalName,
		TenantID: tenantID,
	}))
	defer suite.db.Unscoped().Delete(&dbmodel.Database{}, "id = ?", dbID)

	// Sanity check: active lookups find the database by its original name.
	active, err := suite.Db.GetDatabases(tenantID, originalName)
	suite.Require().NoError(err)
	suite.Require().Len(active, 1)
	suite.Require().Equal(dbID, active[0].ID)

	// Soft delete the database.
	suite.Require().NoError(suite.Db.SoftDelete(dbID))

	// Row should no longer be returned by the active-name lookup.
	active, err = suite.Db.GetDatabases(tenantID, originalName)
	suite.Require().NoError(err)
	suite.Require().Empty(active)

	// Fetch the raw row (bypassing the is_deleted=false filter) and assert the
	// rename + is_deleted flag.
	var raw dbmodel.Database
	suite.Require().NoError(
		suite.db.Table("databases").Where("id = ?", dbID).First(&raw).Error,
	)
	expectedName := fmt.Sprintf("_deleted_%s_%s", originalName, dbID)
	suite.Require().Equal(expectedName, raw.Name)
	suite.Require().True(raw.IsDeleted)

	// The original name is now free: inserting a new database with the same
	// (tenant_id, name) must succeed despite the uniqueIndex on that pair.
	newID := types.NewUniqueID().String()
	suite.Require().NoError(suite.Db.Insert(&dbmodel.Database{
		ID:       newID,
		Name:     originalName,
		TenantID: tenantID,
	}))
	defer suite.db.Unscoped().Delete(&dbmodel.Database{}, "id = ?", newID)

	active, err = suite.Db.GetDatabases(tenantID, originalName)
	suite.Require().NoError(err)
	suite.Require().Len(active, 1)
	suite.Require().Equal(newID, active[0].ID)

	// Re-soft-deleting an already soft-deleted database must be a no-op: the
	// name must NOT gain another "_deleted_" prefix.
	suite.Require().NoError(suite.Db.SoftDelete(dbID))
	suite.Require().NoError(
		suite.db.Table("databases").Where("id = ?", dbID).First(&raw).Error,
	)
	suite.Require().Equal(expectedName, raw.Name)
}

func TestDatabaseDbTestSuite(t *testing.T) {
	testSuite := new(DatabaseDbTestSuite)
	testSuite.t = t
	suite.Run(t, testSuite)
}
