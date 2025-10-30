package coordinator

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/chroma-core/chroma/go/pkg/proto/coordinatorpb"
	"github.com/chroma-core/chroma/go/pkg/sysdb/metastore/db/dbmodel"
	dbmodel_mocks "github.com/chroma-core/chroma/go/pkg/sysdb/metastore/db/dbmodel/mocks"
	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

type ListAttachedFunctionsTestSuite struct {
	suite.Suite
	mockMetaDomain         *dbmodel_mocks.IMetaDomain
	mockAttachedFunctionDb *dbmodel_mocks.IAttachedFunctionDb
	mockFunctionDb         *dbmodel_mocks.IFunctionDb
	coordinator            *Coordinator
}

func (suite *ListAttachedFunctionsTestSuite) SetupTest() {
	suite.mockMetaDomain = &dbmodel_mocks.IMetaDomain{}
	suite.mockMetaDomain.Test(suite.T())

	suite.mockAttachedFunctionDb = &dbmodel_mocks.IAttachedFunctionDb{}
	suite.mockAttachedFunctionDb.Test(suite.T())

	suite.mockFunctionDb = &dbmodel_mocks.IFunctionDb{}
	suite.mockFunctionDb.Test(suite.T())

	suite.coordinator = &Coordinator{
		catalog: Catalog{
			metaDomain: suite.mockMetaDomain,
		},
	}
}

func (suite *ListAttachedFunctionsTestSuite) TearDownTest() {
	suite.mockMetaDomain.AssertExpectations(suite.T())
	suite.mockAttachedFunctionDb.AssertExpectations(suite.T())
	suite.mockFunctionDb.AssertExpectations(suite.T())
}

func (suite *ListAttachedFunctionsTestSuite) TestListAttachedFunctions_Success() {
	ctx := context.Background()
	collectionID := "test-collection"
	functionID1 := uuid.New()
	functionID2 := uuid.New()

	now := time.Now()
	attachedFunctions := []*dbmodel.AttachedFunction{
		{
			ID:                      uuid.New(),
			Name:                    "af-1",
			FunctionID:              functionID1,
			InputCollectionID:       collectionID,
			OutputCollectionName:    "output-1",
			FunctionParams:          `{"foo":"bar"}`,
			TenantID:                "tenant",
			DatabaseID:              "db",
			CompletionOffset:        10,
			MinRecordsForInvocation: 5,
			NextNonce:               uuid.Must(uuid.NewV7()),
			LowestLiveNonce:         uuidPtr(uuid.Must(uuid.NewV7())),
			NextRun:                 now,
			CreatedAt:               now,
			UpdatedAt:               now,
		},
		{
			ID:                      uuid.New(),
			Name:                    "af-2",
			FunctionID:              functionID2,
			InputCollectionID:       collectionID,
			OutputCollectionName:    "output-2",
			FunctionParams:          `{}`,
			TenantID:                "tenant",
			DatabaseID:              "db",
			CompletionOffset:        20,
			MinRecordsForInvocation: 15,
			NextNonce:               uuid.Must(uuid.NewV7()),
			NextRun:                 now,
			CreatedAt:               now,
			UpdatedAt:               now,
		},
	}

	suite.mockMetaDomain.On("AttachedFunctionDb", ctx).Return(suite.mockAttachedFunctionDb).Once()
	suite.mockAttachedFunctionDb.On("GetByCollectionID", collectionID).Return(attachedFunctions, nil).Once()

	functionOne := &dbmodel.Function{ID: functionID1, Name: "function-one"}
	functionTwo := &dbmodel.Function{ID: functionID2, Name: "function-two"}

	suite.mockMetaDomain.On("FunctionDb", ctx).Return(suite.mockFunctionDb).Once()
	suite.mockFunctionDb.
		On("GetByIDs", mock.MatchedBy(func(ids []uuid.UUID) bool {
			if len(ids) != 2 {
				return false
			}
			found := map[uuid.UUID]struct{}{
				functionID1: {},
				functionID2: {},
			}
			for _, id := range ids {
				delete(found, id)
			}
			return len(found) == 0
		})).
		Return([]*dbmodel.Function{functionOne, functionTwo}, nil).Once()

	req := &coordinatorpb.ListAttachedFunctionsRequest{InputCollectionId: collectionID}
	resp, err := suite.coordinator.ListAttachedFunctions(ctx, req)

	suite.Require().NoError(err)
	suite.Require().NotNil(resp)
	suite.Len(resp.AttachedFunctions, 2)

	suite.Equal("function-one", resp.AttachedFunctions[0].FunctionName)
	suite.Equal(uint64(10), resp.AttachedFunctions[0].CompletionOffset)
	suite.NotNil(resp.AttachedFunctions[0].Params)

	suite.Equal("function-two", resp.AttachedFunctions[1].FunctionName)
	suite.Equal(uint64(20), resp.AttachedFunctions[1].CompletionOffset)
	suite.NotNil(resp.AttachedFunctions[1].Params)
	suite.Len(resp.AttachedFunctions[1].Params.Fields, 0)
}

func (suite *ListAttachedFunctionsTestSuite) TestListAttachedFunctions_EmptyResult() {
	ctx := context.Background()
	collectionID := "test-collection"

	suite.mockMetaDomain.On("AttachedFunctionDb", ctx).Return(suite.mockAttachedFunctionDb).Once()
	suite.mockAttachedFunctionDb.On("GetByCollectionID", collectionID).Return([]*dbmodel.AttachedFunction{}, nil).Once()

	req := &coordinatorpb.ListAttachedFunctionsRequest{InputCollectionId: collectionID}
	resp, err := suite.coordinator.ListAttachedFunctions(ctx, req)

	suite.Require().NoError(err)
	suite.Require().NotNil(resp)
	suite.Len(resp.AttachedFunctions, 0)
}

func (suite *ListAttachedFunctionsTestSuite) TestListAttachedFunctions_FunctionDbError() {
	ctx := context.Background()
	collectionID := "test-collection"
	functionID := uuid.New()
	now := time.Now()

	attachedFunction := &dbmodel.AttachedFunction{
		ID:                      uuid.New(),
		Name:                    "af",
		FunctionID:              functionID,
		InputCollectionID:       collectionID,
		OutputCollectionName:    "output",
		FunctionParams:          `{}`,
		TenantID:                "tenant",
		DatabaseID:              "db",
		CompletionOffset:        0,
		MinRecordsForInvocation: 1,
		NextNonce:               uuid.Must(uuid.NewV7()),
		NextRun:                 now,
		CreatedAt:               now,
		UpdatedAt:               now,
	}

	suite.mockMetaDomain.On("AttachedFunctionDb", ctx).Return(suite.mockAttachedFunctionDb).Once()
	suite.mockAttachedFunctionDb.On("GetByCollectionID", collectionID).Return([]*dbmodel.AttachedFunction{attachedFunction}, nil).Once()

	suite.mockMetaDomain.On("FunctionDb", ctx).Return(suite.mockFunctionDb).Once()
	suite.mockFunctionDb.On("GetByIDs", []uuid.UUID{functionID}).Return(nil, errors.New("db error")).Once()

	req := &coordinatorpb.ListAttachedFunctionsRequest{InputCollectionId: collectionID}
	resp, err := suite.coordinator.ListAttachedFunctions(ctx, req)

	suite.Require().Error(err)
	suite.Nil(resp)
}

func (suite *ListAttachedFunctionsTestSuite) TestListAttachedFunctions_InvalidParams() {
	ctx := context.Background()
	collectionID := "test-collection"
	functionID := uuid.New()
	now := time.Now()

	attachedFunction := &dbmodel.AttachedFunction{
		ID:                      uuid.New(),
		Name:                    "af",
		FunctionID:              functionID,
		InputCollectionID:       collectionID,
		OutputCollectionName:    "output",
		FunctionParams:          `{invalid json}`,
		TenantID:                "tenant",
		DatabaseID:              "db",
		CompletionOffset:        0,
		MinRecordsForInvocation: 1,
		NextNonce:               uuid.Must(uuid.NewV7()),
		NextRun:                 now,
		CreatedAt:               now,
		UpdatedAt:               now,
	}

	suite.mockMetaDomain.On("AttachedFunctionDb", ctx).Return(suite.mockAttachedFunctionDb).Once()
	suite.mockAttachedFunctionDb.On("GetByCollectionID", collectionID).Return([]*dbmodel.AttachedFunction{attachedFunction}, nil).Once()

	functionModel := &dbmodel.Function{ID: functionID, Name: "function"}

	suite.mockMetaDomain.On("FunctionDb", ctx).Return(suite.mockFunctionDb).Once()
	suite.mockFunctionDb.On("GetByIDs", []uuid.UUID{functionID}).Return([]*dbmodel.Function{functionModel}, nil).Once()

	req := &coordinatorpb.ListAttachedFunctionsRequest{InputCollectionId: collectionID}
	resp, err := suite.coordinator.ListAttachedFunctions(ctx, req)

	suite.Require().Error(err)
	suite.Nil(resp)
}

func uuidPtr(id uuid.UUID) *uuid.UUID {
	return &id
}

func TestListAttachedFunctionsTestSuite(t *testing.T) {
	suite.Run(t, new(ListAttachedFunctionsTestSuite))
}
