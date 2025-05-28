package grpc

import (
	"context"
	"testing"
	"time"

	"github.com/chroma-core/chroma/go/pkg/common"
	"github.com/chroma-core/chroma/go/pkg/grpcutils"
	"github.com/chroma-core/chroma/go/pkg/proto/coordinatorpb"
	"github.com/chroma-core/chroma/go/pkg/sysdb/coordinator"
	"github.com/chroma-core/chroma/go/pkg/sysdb/coordinator/model"
	"github.com/chroma-core/chroma/go/pkg/sysdb/metastore/db/dao"
	"github.com/chroma-core/chroma/go/pkg/sysdb/metastore/db/dbcore"
	"github.com/chroma-core/chroma/go/pkg/sysdb/metastore/db/dbmodel"
	s3metastore "github.com/chroma-core/chroma/go/pkg/sysdb/metastore/s3"
	"github.com/google/uuid"
	"github.com/pingcap/log"
	"github.com/stretchr/testify/suite"
	"google.golang.org/genproto/googleapis/rpc/code"
	codes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gorm.io/gorm"
)

type TenantDatabaseServiceTestSuite struct {
	suite.Suite
	catalog *coordinator.Catalog
	db      *gorm.DB
	s       *Server
}

func (suite *TenantDatabaseServiceTestSuite) SetupSuite() {
	log.Info("setup suite")
	suite.db, _ = dbcore.ConfigDatabaseForTesting()
	s, err := NewWithGrpcProvider(Config{
		SystemCatalogProvider: "database",
		Testing:               true,
		MetaStoreConfig: s3metastore.S3MetaStoreConfig{
			BucketName:              "test-bucket",
			Region:                  "us-east-1",
			Endpoint:                "http://localhost:9000",
			AccessKeyID:             "minio",
			SecretAccessKey:         "minio123",
			ForcePathStyle:          true,
			CreateBucketIfNotExists: true,
		},
	}, grpcutils.Default)
	if err != nil {
		suite.T().Fatalf("error creating server: %v", err)
	}
	suite.s = s
	txnImpl := dbcore.NewTxImpl()
	metaDomain := dao.NewMetaDomain()
	suite.catalog = coordinator.NewTableCatalog(txnImpl, metaDomain, nil, false)
}

func (suite *TenantDatabaseServiceTestSuite) SetupTest() {
	log.Info("setup test")
}

func (suite *TenantDatabaseServiceTestSuite) TearDownTest() {
	log.Info("teardown test")
}

func (suite *TenantDatabaseServiceTestSuite) TestServer_TenantLastCompactionTime() {
	log.Info("TestServer_TenantLastCompactionTime")
	tenantId := "TestTenantLastCompactionTime"
	request := &coordinatorpb.SetLastCompactionTimeForTenantRequest{
		TenantLastCompactionTime: &coordinatorpb.TenantLastCompactionTime{
			TenantId:           tenantId,
			LastCompactionTime: 0,
		},
	}
	_, err := suite.s.SetLastCompactionTimeForTenant(context.Background(), request)
	suite.Equal(status.Error(codes.Code(code.Code_INTERNAL), common.ErrTenantNotFound.Error()), err)

	// create tenant
	_, err = suite.catalog.CreateTenant(context.Background(), &model.CreateTenant{
		Name: tenantId,
		Ts:   time.Now().Unix(),
	}, time.Now().Unix())
	if err != nil {
		return
	}
	suite.NoError(err)

	_, err = suite.s.SetLastCompactionTimeForTenant(context.Background(), request)
	suite.NoError(err)
	tenants, err := suite.s.GetLastCompactionTimeForTenant(context.Background(), &coordinatorpb.GetLastCompactionTimeForTenantRequest{
		TenantId: []string{tenantId},
	})
	suite.NoError(err)
	suite.Equal(1, len(tenants.TenantLastCompactionTime))
	suite.Equal(tenantId, tenants.TenantLastCompactionTime[0].TenantId)
	suite.Equal(int64(0), tenants.TenantLastCompactionTime[0].LastCompactionTime)

	// update last compaction time
	request.TenantLastCompactionTime.LastCompactionTime = 1
	_, err = suite.s.SetLastCompactionTimeForTenant(context.Background(), request)
	suite.NoError(err)
	tenants, err = suite.s.GetLastCompactionTimeForTenant(context.Background(), &coordinatorpb.GetLastCompactionTimeForTenantRequest{
		TenantId: []string{tenantId},
	})
	suite.NoError(err)
	suite.Equal(1, len(tenants.TenantLastCompactionTime))
	suite.Equal(tenantId, tenants.TenantLastCompactionTime[0].TenantId)
	suite.Equal(int64(1), tenants.TenantLastCompactionTime[0].LastCompactionTime)

	// clean up
	err = dao.CleanUpTestTenant(suite.db, tenantId)
	suite.NoError(err)
}

func (suite *TenantDatabaseServiceTestSuite) TestServer_DeleteDatabase() {
	tenantName := "TestDeleteDatabase"
	databaseName := "TestDeleteDatabase"
	// Generate random uuid for db id
	databaseeId := uuid.New().String()

	_, err := suite.catalog.CreateTenant(context.Background(), &model.CreateTenant{
		Name: tenantName,
		Ts:   time.Now().Unix(),
	}, time.Now().Unix())
	suite.NoError(err)

	_, err = suite.catalog.CreateDatabase(context.Background(), &model.CreateDatabase{
		Tenant: tenantName,
		Name:   databaseName,
		ID:     databaseeId,
		Ts:     time.Now().Unix(),
	}, time.Now().Unix())
	suite.NoError(err)

	_, _, err = suite.catalog.CreateCollection(context.Background(), &model.CreateCollection{
		TenantID:     tenantName,
		DatabaseName: databaseName,
		Name:         "TestCollection",
	}, time.Now().Unix())
	suite.NoError(err)

	err = suite.catalog.DeleteDatabase(context.Background(), &model.DeleteDatabase{
		Tenant: tenantName,
		Name:   databaseName,
	})
	suite.NoError(err)

	// Check that associated collection was deleted
	var count int64
	var collections []*dbmodel.Collection
	suite.NoError(suite.db.Find(&collections).Count(&count).Error)
	suite.Equal(int64(0), count)
}

func TestTenantDatabaseServiceTestSuite(t *testing.T) {
	testSuite := new(TenantDatabaseServiceTestSuite)
	suite.Run(t, testSuite)
}
