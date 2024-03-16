package dao

import (
	"context"
	"github.com/chroma-core/chroma/go/pkg/coordinator/ent"
	"github.com/chroma-core/chroma/go/pkg/metastore/db/dbcore"
	"github.com/stretchr/testify/suite"
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
	suite.T().Log("TestCreate")
	tx, err := suite.client.Tx(context.Background())
	suite.Require().NoError(err)

}

func TestBaseTestSuite(t *testing.T) {
	testSuite := new(BaseTestSuite)
	suite.Run(t, testSuite)
}
