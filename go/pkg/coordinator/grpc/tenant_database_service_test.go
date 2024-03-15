package grpc

import (
	"context"
	"github.com/chroma-core/chroma/go/pkg/grpcutils"
	"github.com/chroma-core/chroma/go/pkg/metastore/coordinator"
	"github.com/chroma-core/chroma/go/pkg/metastore/db/dao"
	"github.com/chroma-core/chroma/go/pkg/metastore/db/dbcore"
	"github.com/chroma-core/chroma/go/pkg/model"
	"github.com/chroma-core/chroma/go/pkg/proto/coordinatorpb"
	"github.com/chroma-core/chroma/go/pkg/types"
	"github.com/pingcap/log"
	"github.com/stretchr/testify/suite"
	"google.golang.org/genproto/googleapis/rpc/code"
	codes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gorm.io/gorm"
	"testing"
	"time"
)

type TenantDatabaseServiceTestSuite struct {
	suite.Suite
	catalog      *coordinator.Catalog
	db           *gorm.DB
	s            *Server
	t            *testing.T
	collectionId types.UniqueID
}

func (suite *TenantDatabaseServiceTestSuite) SetupSuite() {
	log.Info("setup suite")
	suite.db = dbcore.ConfigDatabaseForTesting()
	s, err := NewWithGrpcProvider(Config{
		AssignmentPolicy:          "simple",
		SystemCatalogProvider:     "memory",
		NotificationStoreProvider: "memory",
		NotifierProvider:          "memory",
		Testing:                   true}, grpcutils.Default, suite.db)
	if err != nil {
		suite.t.Fatalf("error creating server: %v", err)
	}
	suite.s = s
	txnImpl := dbcore.NewTxImpl()
	metaDomain := dao.NewMetaDomain()
	suite.catalog = coordinator.NewTableCatalogWithNotification(txnImpl, metaDomain, nil)
}

func (suite *TenantDatabaseServiceTestSuite) SetupTest() {
	log.Info("setup test")
}

func (suite *TenantDatabaseServiceTestSuite) TearDownTest() {
	log.Info("teardown test")
	// TODO: clean up per test when delete is implemented for tenant
	dbcore.ResetTestTables(suite.db)
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
	suite.Equal(status.Error(codes.Code(code.Code_INTERNAL), "error SetTenantLastCompactionTime"), err)

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
}

func TestTenantDatabaseServiceTestSuite(t *testing.T) {
	testSuite := new(TenantDatabaseServiceTestSuite)
	testSuite.t = t
	suite.Run(t, testSuite)
}
