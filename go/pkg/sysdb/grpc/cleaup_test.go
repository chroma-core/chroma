package grpc

import (
	"context"
	"strconv"
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
		SystemCatalogProvider:      "database",
		SoftDeleteEnabled:          true,
		SoftDeleteCleanupInterval:  1 * time.Second,
		SoftDeleteMaxAge:           0,
		SoftDeleteCleanupBatchSize: 10,
		Testing:                    true}, grpcutils.Default, suite.db)
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
		collectionName := "cleanup_test_collection_" + strconv.Itoa(i)
		collectionID, err := dao.CreateTestCollection(suite.db, collectionName, 128, suite.databaseId)
		suite.NoError(err)
		collections[i] = collectionID
	}

	// Soft delete both collections
	for _, collectionID := range collections {
		err := suite.s.coordinator.DeleteCollection(context.Background(), &model.DeleteCollection{
			ID: types.MustParse(collectionID),
		})
		suite.NoError(err)
	}

	// Verify collections are soft deleted
	softDeletedCollections, err := suite.s.coordinator.GetSoftDeletedCollections(context.Background(), nil, "", "", 10)
	suite.NoError(err)
	suite.Equal(2, len(softDeletedCollections))

	// Start the cleaner.
	suite.s.softDeleteCleaner.Start()

	// Wait for cleanup cycle
	time.Sleep(2 * time.Second)

	// Verify collections are permanently deleted
	softDeletedCollections, err = suite.s.coordinator.GetSoftDeletedCollections(context.Background(), nil, "", "", 10)
	suite.NoError(err)
	suite.Equal(0, len(softDeletedCollections))

	// Stop the cleaner.
	suite.s.softDeleteCleaner.Stop()
}

func TestCleanupTestSuite(t *testing.T) {
	testSuite := new(CleanupTestSuite)
	suite.Run(t, testSuite)
}
