package grpc

import (
	"context"
	"testing"
	"time"

	"github.com/chroma-core/chroma/go/pkg/grpcutils"
	"github.com/chroma-core/chroma/go/pkg/sysdb/coordinator/model"
	"github.com/chroma-core/chroma/go/pkg/sysdb/metastore/db/dao"
	"github.com/chroma-core/chroma/go/pkg/sysdb/metastore/db/dbcore"
	"github.com/chroma-core/chroma/go/pkg/types"
	"github.com/pingcap/log"
	"github.com/stretchr/testify/suite"
	"gorm.io/gorm"
)

type CleanupTestSuite struct {
	suite.Suite
	db           *gorm.DB
	s            *Server
	tenantName   string
	databaseName string
	databaseId   string
}

func (suite *CleanupTestSuite) SetupSuite() {
	log.Info("setup suite")
	suite.db = dbcore.ConfigDatabaseForTesting()
	s, err := NewWithGrpcProvider(Config{
		SystemCatalogProvider: "database",
		Testing:               true}, grpcutils.Default, suite.db)
	if err != nil {
		suite.T().Fatalf("error creating server: %v", err)
	}
	suite.s = s
	suite.tenantName = "tenant_" + suite.T().Name()
	suite.databaseName = "database_" + suite.T().Name()
	DbId, err := dao.CreateTestTenantAndDatabase(suite.db, suite.tenantName, suite.databaseName)
	suite.NoError(err)
	suite.databaseId = DbId
}

func (suite *CleanupTestSuite) TearDownSuite() {
	log.Info("teardown suite")
	err := dao.CleanUpTestDatabase(suite.db, suite.tenantName, suite.databaseName)
	suite.NoError(err)
	err = dao.CleanUpTestTenant(suite.db, suite.tenantName)
	suite.NoError(err)
}

func (suite *CleanupTestSuite) TestSoftDeleteCleanup() {
	// Create 2 test collections
	collections := make([]string, 2)
	for i := 0; i < 2; i++ {
		collectionName := "cleanup_test_collection_" + string(i)
		collectionID, err := dao.CreateTestCollection(suite.db, collectionName, 128, suite.databaseId)
		suite.NoError(err)
		collections[i] = collectionID
	}

	// Soft delete both collections
	for _, collectionID := range collections {
		err := suite.s.coordinator.SoftDeleteCollection(context.Background(), &model.DeleteCollection{
			ID: types.UniqueID(collectionID),
		})
		suite.NoError(err)
	}

	// Verify collections are soft deleted
	softDeletedCollections, timestamps, err := suite.s.coordinator.GetSoftDeletedCollections(context.Background(), "", "", 10)
	suite.NoError(err)
	suite.Equal(2, len(softDeletedCollections))
	suite.Equal(2, len(timestamps))

	// Create cleaner with short grace period for testing
	cleaner := NewSoftDeleteCleaner(suite.s.coordinator, 1, 0)
	cleaner.Start()

	// Wait for cleanup cycle
	time.Sleep(2 * time.Second)

	// Verify collections are permanently deleted
	softDeletedCollections, timestamps, err = suite.s.coordinator.GetSoftDeletedCollections(context.Background(), "", "", 10)
	suite.NoError(err)
	suite.Equal(0, len(softDeletedCollections))
	suite.Equal(0, len(timestamps))

	cleaner.Stop()
}

func TestCleanupTestSuite(t *testing.T) {
	testSuite := new(CleanupTestSuite)
	suite.Run(t, testSuite)
}
