package dao

import (
	"strconv"
	"testing"
	"time"

	"github.com/chroma-core/chroma/go/pkg/common"
	"github.com/chroma-core/chroma/go/pkg/sysdb/metastore/db/dbcore"
	"github.com/chroma-core/chroma/go/pkg/sysdb/metastore/db/dbmodel"
	"github.com/pingcap/log"
	"github.com/stretchr/testify/suite"
	"gorm.io/gorm"
)

type TenantDbTestSuite struct {
	suite.Suite
	db *gorm.DB
	Db *tenantDb
	t  *testing.T
}

func (suite *TenantDbTestSuite) SetupSuite() {
	log.Info("setup suite")
	suite.db, _ = dbcore.ConfigDatabaseForTesting()
	suite.Db = &tenantDb{
		db: suite.db,
	}
}

func (suite *TenantDbTestSuite) SetupTest() {
	log.Info("setup test")
}

func (suite *TenantDbTestSuite) TearDownTest() {
	log.Info("teardown test")
}

func (suite *TenantDbTestSuite) TestTenantDb_UpdateTenantLastCompactionTime() {
	tenantId := "testUpdateTenantLastCompactionTime"
	var tenant dbmodel.Tenant
	err := suite.Db.Insert(&dbmodel.Tenant{
		ID:                 tenantId,
		LastCompactionTime: 0,
	})
	suite.Require().NoError(err)
	suite.db.First(&tenant, "id = ?", tenantId)
	suite.Require().Equal(int64(0), tenant.LastCompactionTime)

	err = suite.Db.UpdateTenantLastCompactionTime(tenantId, 1)
	suite.Require().NoError(err)
	suite.db.First(&tenant, "id = ?", tenantId)
	suite.Require().Equal(int64(1), tenant.LastCompactionTime)

	currentTime := time.Now().Unix()
	err = suite.Db.UpdateTenantLastCompactionTime(tenantId, currentTime)
	suite.Require().NoError(err)
	suite.db.First(&tenant, "id = ?", tenantId)
	suite.Require().Equal(currentTime, tenant.LastCompactionTime)

	suite.db.Delete(&tenant, "id = ?", tenantId)
}

func (suite *TenantDbTestSuite) TestTenantDb_GetTenantsLastCompactionTime() {
	tenantIds := make([]string, 0)
	for i := 0; i < 10; i++ {
		tenantId := "testGetTenantsLastCompactionTime" + strconv.Itoa(i)
		err := suite.Db.Insert(&dbmodel.Tenant{
			ID:                 tenantId,
			LastCompactionTime: int64(i),
		})
		suite.Require().NoError(err)
		tenantIds = append(tenantIds, tenantId)
	}

	tenants, err := suite.Db.GetTenantsLastCompactionTime(tenantIds)
	suite.Require().NoError(err)
	suite.Require().Len(tenants, 10)
	for i, tenant := range tenants {
		suite.Require().Equal(int64(i), tenant.LastCompactionTime)
	}

	currentTime := time.Now().Unix()
	for _, tenantId := range tenantIds {
		err := suite.Db.UpdateTenantLastCompactionTime(tenantId, currentTime)
		suite.Require().NoError(err)
	}
	tenants, err = suite.Db.GetTenantsLastCompactionTime(tenantIds)
	suite.Require().NoError(err)
	suite.Require().Len(tenants, 10)
	for _, tenant := range tenants {
		suite.Require().Equal(currentTime, tenant.LastCompactionTime)
	}

	for _, tenantId := range tenantIds {
		suite.db.Delete(&dbmodel.Tenant{}, "id = ?", tenantId)
	}
}

func (suite *TenantDbTestSuite) TestTenantDb_SetTenantResourceName() {
	tenantId := "testSetTenantResourceName"
	err := suite.Db.Insert(&dbmodel.Tenant{
		ID: tenantId,
	})
	suite.Require().NoError(err)
	tenant, err := suite.Db.GetTenants(tenantId)
	suite.Require().NoError(err)
	suite.Require().Len(tenant, 1)
	suite.Assert().Nil(tenant[0].ResourceName)

	err = suite.Db.SetTenantResourceName(tenantId, "resourceName")
	suite.Require().NoError(err)

	tenant, err = suite.Db.GetTenants(tenantId)
	suite.Require().NoError(err)
	suite.Require().Len(tenant, 1)
	suite.Require().Equal("resourceName", *tenant[0].ResourceName)

	err = suite.Db.SetTenantResourceName(tenantId, "resourceName")
	suite.Require().Equal(common.ErrTenantResourceNameAlreadySet, err)

	err = suite.Db.SetTenantResourceName("fake-tenant", "resourceName")
	suite.Require().Error(err)
	suite.Require().Equal(common.ErrTenantNotFound, err)

	suite.db.Delete(&dbmodel.Tenant{}, "id = ?", tenantId)
}

func TestTenantDbTestSuite(t *testing.T) {
	testSuite := new(TenantDbTestSuite)
	testSuite.t = t
	suite.Run(t, testSuite)
}
