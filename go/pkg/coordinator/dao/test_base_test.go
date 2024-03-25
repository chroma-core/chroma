package dao

import (
	"context"
	"github.com/chroma-core/chroma/go/pkg/coordinator/ent"
	"github.com/chroma-core/chroma/go/pkg/coordinator/ent/testbase"
	"github.com/chroma-core/chroma/go/pkg/metastore/db/dbcore"
	"github.com/google/uuid"
	"github.com/pingcap/log"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"testing"
)

type BaseTestSuite struct {
	suite.Suite
	client *ent.Client
}

func (suite *BaseTestSuite) SetupSuite() {
	client, err := dbcore.ConfigEntClientForTesting()
	suite.Require().NoError(err)
	suite.client = client
}

func (suite *BaseTestSuite) TestBase_TestCreate() {
	ctx := context.Background()
	client := suite.client
	parentId, err := uuid.NewUUID()
	name := "TestBase_TestCreate"
	text := "TestBase_TestCreate_text"
	suite.Require().NoError(err)
	suite.T().Log("TestCreate")

	dpo := &ent.TestBase{
		ParentID: parentId,
		Name:     &name,
		Text:     &text,
	}

	var generatedId uuid.UUID
	err = WithTx(ctx, client, func(tx *ent.Tx) error {
		base, err := Create(ctx, tx, dpo)
		suite.Require().NoError(err)
		suite.Equal(parentId, base.Dpo.ParentID)
		suite.Equal("TestBase_TestCreate", *base.Dpo.Name)
		suite.Equal("TestBase_TestCreate_text", *base.Dpo.Text)
		suite.NotNil(base.Dpo.ID)
		suite.NotEqual(int64(0), base.Dpo.UpdatedAt)
		suite.NotEqual(int64(0), base.Dpo.CreatedAt)
		suite.Equal(int64(0), base.Dpo.DeletedAt)
		suite.Equal(0, base.Dpo.Version)
		generatedId = base.Dpo.ID
		return nil
	})
	suite.Require().NoError(err)
	baseInDB, err := client.TestBase.Query().Where(testbase.Name(*dpo.Name)).Only(ctx)
	suite.Require().NoError(err)
	log.Info("TestBase_TestCreate", zap.Any("baseInDB", baseInDB))
	suite.Equal(parentId, baseInDB.ParentID)
	suite.Equal("TestBase_TestCreate", *baseInDB.Name)
	suite.Equal("TestBase_TestCreate_text", *baseInDB.Text)
	suite.NotNil(baseInDB.ID)
	suite.NotEqual(int64(0), baseInDB.UpdatedAt)
	suite.NotEqual(int64(0), baseInDB.CreatedAt)
	suite.Equal(int64(0), baseInDB.DeletedAt)
	suite.Equal(0, baseInDB.Version)

	baseInDB, err = client.TestBase.Get(ctx, generatedId)
	suite.Require().NoError(err)
	suite.Equal(parentId, baseInDB.ParentID)
	suite.Equal("TestBase_TestCreate", *baseInDB.Name)
	suite.Equal("TestBase_TestCreate_text", *baseInDB.Text)
	suite.NotNil(baseInDB.ID)
	suite.NotEqual(int64(0), baseInDB.UpdatedAt)
	suite.NotEqual(int64(0), baseInDB.CreatedAt)
	suite.Equal(int64(0), baseInDB.DeletedAt)
	suite.Equal(0, baseInDB.Version)

	// clean up
	client.TestBase.DeleteOneID(baseInDB.ID).ExecX(ctx)
}

func TestBaseTestSuite(t *testing.T) {
	testSuite := new(BaseTestSuite)
	suite.Run(t, testSuite)
}
