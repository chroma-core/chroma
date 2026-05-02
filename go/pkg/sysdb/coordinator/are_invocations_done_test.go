package coordinator

import (
	"context"
	"errors"
	"testing"

	"github.com/chroma-core/chroma/go/pkg/proto/coordinatorpb"
	"github.com/chroma-core/chroma/go/pkg/sysdb/metastore/db/dbmodel"
	dbmodel_mocks "github.com/chroma-core/chroma/go/pkg/sysdb/metastore/db/dbmodel/mocks"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type AreInvocationsDoneTestSuite struct {
	suite.Suite
	mockMetaDomain         *dbmodel_mocks.IMetaDomain
	mockAttachedFunctionDb *dbmodel_mocks.IAttachedFunctionDb
	coordinator            *Coordinator
}

func (suite *AreInvocationsDoneTestSuite) SetupTest() {
	suite.mockMetaDomain = &dbmodel_mocks.IMetaDomain{}
	suite.mockMetaDomain.Test(suite.T())

	suite.mockAttachedFunctionDb = &dbmodel_mocks.IAttachedFunctionDb{}
	suite.mockAttachedFunctionDb.Test(suite.T())

	suite.coordinator = &Coordinator{
		catalog: Catalog{
			metaDomain: suite.mockMetaDomain,
		},
	}
}

func (suite *AreInvocationsDoneTestSuite) TearDownTest() {
	suite.mockMetaDomain.AssertExpectations(suite.T())
	suite.mockAttachedFunctionDb.AssertExpectations(suite.T())
}

func (suite *AreInvocationsDoneTestSuite) TestAreInvocationsDone_Success() {
	ctx := context.Background()

	// Test data
	fnID1 := uuid.New()
	fnID2 := uuid.New()
	fnID3 := uuid.New()
	collectionID1 := uuid.New().String()
	collectionID2 := uuid.New().String()

	// Setup mocks
	suite.mockMetaDomain.On("AttachedFunctionDb", ctx).Return(suite.mockAttachedFunctionDb, nil)

	expectedItems := []dbmodel.InvocationCheckItem{
		{
			FunctionID:        fnID1,
			InputCollectionID: collectionID1,
			CompletionOffset:  100,
		},
		{
			FunctionID:        fnID2,
			InputCollectionID: collectionID1,
			CompletionOffset:  200,
		},
		{
			FunctionID:        fnID3,
			InputCollectionID: collectionID2,
			CompletionOffset:  50,
		},
	}

	suite.mockAttachedFunctionDb.On("AreInvocationsDone", expectedItems).
		Return([]bool{true, false, true}, nil).Once()

	// Create request
	req := &coordinatorpb.AreInvocationsDoneRequest{
		Items: []*coordinatorpb.InvocationCheckItem{
			{
				FunctionId:        fnID1.String(),
				InputCollectionId: collectionID1,
				CompletionOffset:  100,
			},
			{
				FunctionId:        fnID2.String(),
				InputCollectionId: collectionID1,
				CompletionOffset:  200,
			},
			{
				FunctionId:        fnID3.String(),
				InputCollectionId: collectionID2,
				CompletionOffset:  50,
			},
		},
	}

	// Execute
	resp, err := suite.coordinator.AreInvocationsDone(ctx, req)

	// Assert
	suite.NoError(err)
	suite.NotNil(resp)
	suite.Equal([]bool{true, false, true}, resp.Done)
}

func (suite *AreInvocationsDoneTestSuite) TestAreInvocationsDone_EmptyRequest() {
	ctx := context.Background()

	// No mocks needed for empty request

	// Create request
	req := &coordinatorpb.AreInvocationsDoneRequest{
		Items: []*coordinatorpb.InvocationCheckItem{},
	}

	// Execute
	resp, err := suite.coordinator.AreInvocationsDone(ctx, req)

	// Assert
	suite.NoError(err)
	suite.NotNil(resp)
	suite.Equal([]bool{}, resp.Done)
}

func (suite *AreInvocationsDoneTestSuite) TestAreInvocationsDone_InvalidFunctionID() {
	ctx := context.Background()
	collectionID := uuid.New().String()

	// Create request with invalid UUID
	req := &coordinatorpb.AreInvocationsDoneRequest{
		Items: []*coordinatorpb.InvocationCheckItem{
			{
				FunctionId:        "invalid-uuid",
				InputCollectionId: collectionID,
				CompletionOffset:  100,
			},
		},
	}

	// Execute
	_, err := suite.coordinator.AreInvocationsDone(ctx, req)

	// Assert
	suite.Error(err)
	suite.Contains(err.Error(), "invalid function_id at index 0")
}

func (suite *AreInvocationsDoneTestSuite) TestAreInvocationsDone_InvalidCollectionID() {
	ctx := context.Background()
	fnID := uuid.New()

	// Create request with invalid collection UUID
	req := &coordinatorpb.AreInvocationsDoneRequest{
		Items: []*coordinatorpb.InvocationCheckItem{
			{
				FunctionId:        fnID.String(),
				InputCollectionId: "invalid-collection-uuid",
				CompletionOffset:  100,
			},
		},
	}

	// Execute
	_, err := suite.coordinator.AreInvocationsDone(ctx, req)

	// Assert
	suite.Error(err)
	suite.Contains(err.Error(), "invalid input_collection_id at index 0")
}

func (suite *AreInvocationsDoneTestSuite) TestAreInvocationsDone_DatabaseError() {
	ctx := context.Background()
	fnID := uuid.New()
	collectionID := uuid.New().String()

	// Setup mocks
	suite.mockMetaDomain.On("AttachedFunctionDb", ctx).Return(suite.mockAttachedFunctionDb, nil)

	expectedItems := []dbmodel.InvocationCheckItem{
		{
			FunctionID:        fnID,
			InputCollectionID: collectionID,
			CompletionOffset:  100,
		},
	}

	suite.mockAttachedFunctionDb.On("AreInvocationsDone", expectedItems).
		Return(nil, errors.New("database error")).Once()

	// Create request
	req := &coordinatorpb.AreInvocationsDoneRequest{
		Items: []*coordinatorpb.InvocationCheckItem{
			{
				FunctionId:        fnID.String(),
				InputCollectionId: collectionID,
				CompletionOffset:  100,
			},
		},
	}

	// Execute
	_, err := suite.coordinator.AreInvocationsDone(ctx, req)

	// Assert
	suite.Error(err)
	suite.Contains(err.Error(), "database error")
}

func TestAreInvocationsDoneSuite(t *testing.T) {
	suite.Run(t, new(AreInvocationsDoneTestSuite))
}

// TestAreInvocationsDone_BasicFunctionality tests that the function correctly identifies done/not-done invocations
func TestAreInvocationsDone_BasicFunctionality(t *testing.T) {
	ctx := context.Background()

	// Setup mocks
	mockMetaDomain := &dbmodel_mocks.IMetaDomain{}
	mockMetaDomain.Test(t)
	mockAttachedFunctionDb := &dbmodel_mocks.IAttachedFunctionDb{}
	mockAttachedFunctionDb.Test(t)

	coordinator := &Coordinator{
		catalog: Catalog{
			metaDomain: mockMetaDomain,
		},
	}

	// Test data
	fnID1 := uuid.New() // Not done: current_completion_offset <= provided_completion_offset
	fnID2 := uuid.New() // Done: soft deleted
	fnID3 := uuid.New() // Done: hard deleted (not in DB)
	fnID4 := uuid.New() // Done: current_completion_offset > provided_completion_offset AND heap_entry_pending=false
	fnID5 := uuid.New() // Not done: current_completion_offset > provided_completion_offset BUT heap_entry_pending=true
	collectionID := uuid.New().String()

	// Setup mock
	mockMetaDomain.On("AttachedFunctionDb", ctx).Return(mockAttachedFunctionDb, nil)

	expectedItems := []dbmodel.InvocationCheckItem{
		{FunctionID: fnID1, InputCollectionID: collectionID, CompletionOffset: 100}, // Not done
		{FunctionID: fnID2, InputCollectionID: collectionID, CompletionOffset: 50},  // Done (soft deleted)
		{FunctionID: fnID3, InputCollectionID: collectionID, CompletionOffset: 50},  // Done (hard deleted)
		{FunctionID: fnID4, InputCollectionID: collectionID, CompletionOffset: 50},  // Done (completed)
		{FunctionID: fnID5, InputCollectionID: collectionID, CompletionOffset: 50},  // Not done (heap_entry_pending=true)
	}

	// Mock the DAO response based on our SQL logic:
	// - fnID1: not done (current_completion_offset <= provided_completion_offset)
	// - fnID2: done (soft deleted)
	// - fnID3: done (hard deleted/not in DB)
	// - fnID4: done (current_completion_offset > provided_completion_offset AND heap_entry_pending=false)
	// - fnID5: not done (heap_entry_pending=true)
	mockAttachedFunctionDb.On("AreInvocationsDone", expectedItems).
		Return([]bool{false, true, true, true, false}, nil).Once()

	// Create request
	req := &coordinatorpb.AreInvocationsDoneRequest{
		Items: []*coordinatorpb.InvocationCheckItem{
			{FunctionId: fnID1.String(), InputCollectionId: collectionID, CompletionOffset: 100},
			{FunctionId: fnID2.String(), InputCollectionId: collectionID, CompletionOffset: 50},
			{FunctionId: fnID3.String(), InputCollectionId: collectionID, CompletionOffset: 50},
			{FunctionId: fnID4.String(), InputCollectionId: collectionID, CompletionOffset: 50},
			{FunctionId: fnID5.String(), InputCollectionId: collectionID, CompletionOffset: 50},
		},
	}

	// Execute
	resp, err := coordinator.AreInvocationsDone(ctx, req)

	// Assert
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, []bool{false, true, true, true, false}, resp.Done)

	// Verify the results match our expectations:
	assert.False(t, resp.Done[0], "fnID1: current_completion_offset <= provided_completion_offset should be NOT done")
	assert.True(t, resp.Done[1], "fnID2: soft deleted should be done")
	assert.True(t, resp.Done[2], "fnID3: hard deleted should be done")
	assert.True(t, resp.Done[3], "fnID4: current_completion_offset > provided_completion_offset AND heap_entry_pending=false should be done")
	assert.False(t, resp.Done[4], "fnID5: heap_entry_pending=true should be NOT done")

	mockMetaDomain.AssertExpectations(t)
	mockAttachedFunctionDb.AssertExpectations(t)
}
