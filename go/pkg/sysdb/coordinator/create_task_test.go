package coordinator

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/chroma-core/chroma/go/pkg/common"
	"github.com/chroma-core/chroma/go/pkg/memberlist_manager"
	"github.com/chroma-core/chroma/go/pkg/proto/coordinatorpb"
	"github.com/chroma-core/chroma/go/pkg/sysdb/metastore/db/dbmodel"
	dbmodel_mocks "github.com/chroma-core/chroma/go/pkg/sysdb/metastore/db/dbmodel/mocks"
	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/types/known/structpb"
)

// MockHeapClient is a mock implementation of HeapClient for testing
type MockHeapClient struct {
	mock.Mock
}

func (m *MockHeapClient) Push(ctx context.Context, collectionID string, schedules []*coordinatorpb.Schedule) error {
	args := m.Called(ctx, collectionID, schedules)
	return args.Error(0)
}

func (m *MockHeapClient) Summary(ctx context.Context) (*coordinatorpb.HeapSummaryResponse, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*coordinatorpb.HeapSummaryResponse), args.Error(1)
}

func (m *MockHeapClient) Close() error {
	args := m.Called()
	return args.Error(0)
}

// MockMemberlistStore is a mock implementation of memberlist_manager.IMemberlistStore for testing
type MockMemberlistStore struct {
	mock.Mock
}

func (m *MockMemberlistStore) GetMemberlist(ctx context.Context) (memberlist memberlist_manager.Memberlist, resourceVersion string, err error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.String(1), args.Error(2)
	}
	return args.Get(0).(memberlist_manager.Memberlist), args.String(1), args.Error(2)
}

func (m *MockMemberlistStore) UpdateMemberlist(ctx context.Context, memberlist memberlist_manager.Memberlist, resourceVersion string) error {
	args := m.Called(ctx, memberlist, resourceVersion)
	return args.Error(0)
}

// AttachFunctionTestSuite is a test suite for testing AttachFunction two-phase commit logic
type AttachFunctionTestSuite struct {
	suite.Suite
	mockMetaDomain         *dbmodel_mocks.IMetaDomain
	mockTxImpl             *dbmodel_mocks.ITransaction
	mockAttachedFunctionDb *dbmodel_mocks.IAttachedFunctionDb
	mockFunctionDb         *dbmodel_mocks.IFunctionDb
	mockDatabaseDb         *dbmodel_mocks.IDatabaseDb
	mockCollectionDb       *dbmodel_mocks.ICollectionDb
	mockHeapClient         *MockHeapClient
	coordinator            *Coordinator
}

// setupAttachFunctionMocks sets up all the mocks for an AttachFunction call (Phases 0 and 1)
// Returns a function that can be called to capture the created attached function ID
func (suite *AttachFunctionTestSuite) setupAttachFunctionMocks(ctx context.Context, request *coordinatorpb.AttachFunctionRequest, databaseID string, functionID uuid.UUID) func(*dbmodel.AttachedFunction) bool {
	inputCollectionID := request.InputCollectionId
	attachedFunctionName := request.Name
	outputCollectionName := request.OutputCollectionName
	tenantID := request.TenantId
	databaseName := request.Database
	functionName := request.FunctionName

	// Phase 0: No existing attached function
	suite.mockMetaDomain.On("AttachedFunctionDb", ctx).Return(suite.mockAttachedFunctionDb).Once()
	suite.mockAttachedFunctionDb.On("GetByName", inputCollectionID, attachedFunctionName).
		Return(nil, nil).Once()

	// Phase 1: Create attached function in transaction
	suite.mockMetaDomain.On("AttachedFunctionDb", mock.Anything).Return(suite.mockAttachedFunctionDb).Once()
	suite.mockAttachedFunctionDb.On("GetByName", inputCollectionID, attachedFunctionName).
		Return(nil, nil).Once()

	suite.mockMetaDomain.On("DatabaseDb", mock.Anything).Return(suite.mockDatabaseDb).Once()
	suite.mockDatabaseDb.On("GetDatabases", tenantID, databaseName).
		Return([]*dbmodel.Database{{ID: databaseID, Name: databaseName}}, nil).Once()

	suite.mockMetaDomain.On("FunctionDb", mock.Anything).Return(suite.mockFunctionDb).Once()
	suite.mockFunctionDb.On("GetByName", functionName).
		Return(&dbmodel.Function{ID: functionID, Name: functionName}, nil).Once()

	suite.mockMetaDomain.On("CollectionDb", mock.Anything).Return(suite.mockCollectionDb).Once()
	suite.mockCollectionDb.On("GetCollections",
		[]string{inputCollectionID}, (*string)(nil), tenantID, databaseName, (*int32)(nil), (*int32)(nil), false).
		Return([]*dbmodel.CollectionAndMetadata{{Collection: &dbmodel.Collection{ID: inputCollectionID}}}, nil).Once()

	suite.mockMetaDomain.On("CollectionDb", mock.Anything).Return(suite.mockCollectionDb).Once()
	suite.mockCollectionDb.On("GetCollections",
		[]string(nil), &outputCollectionName, tenantID, databaseName, (*int32)(nil), (*int32)(nil), false).
		Return([]*dbmodel.CollectionAndMetadata{}, nil).Once()

	// Return a matcher function that can be used to capture attached function data
	return func(attachedFunction *dbmodel.AttachedFunction) bool {
		return attachedFunction.LowestLiveNonce == nil
	}
}

func (suite *AttachFunctionTestSuite) SetupTest() {
	// Create all mocks - note: we manually control AssertExpectations
	// to avoid conflicts with automatic cleanup
	suite.mockMetaDomain = &dbmodel_mocks.IMetaDomain{}
	suite.mockMetaDomain.Test(suite.T())

	suite.mockTxImpl = &dbmodel_mocks.ITransaction{}
	suite.mockTxImpl.Test(suite.T())

	suite.mockAttachedFunctionDb = &dbmodel_mocks.IAttachedFunctionDb{}
	suite.mockAttachedFunctionDb.Test(suite.T())

	suite.mockFunctionDb = &dbmodel_mocks.IFunctionDb{}
	suite.mockFunctionDb.Test(suite.T())

	suite.mockDatabaseDb = &dbmodel_mocks.IDatabaseDb{}
	suite.mockDatabaseDb.Test(suite.T())

	suite.mockCollectionDb = &dbmodel_mocks.ICollectionDb{}
	suite.mockCollectionDb.Test(suite.T())

	suite.mockHeapClient = new(MockHeapClient)
	suite.mockHeapClient.Test(suite.T())

	// Setup coordinator with mocks
	suite.coordinator = &Coordinator{
		ctx: context.Background(),
		catalog: Catalog{
			metaDomain: suite.mockMetaDomain,
			txImpl:     suite.mockTxImpl,
		},
		heapClient: suite.mockHeapClient,
	}
}

// TestAttachFunction_SuccessfulCreation_WithHeapService tests the happy path:
// - No existing attached function (Phase 0)
// - Create attached function with NULL lowest_live_nonce (Phase 1)
// - Push to heap service (Phase 2)
// - Update lowest_live_nonce to complete initialization (Phase 3)
func (suite *AttachFunctionTestSuite) TestAttachFunction_SuccessfulCreation_WithHeapService() {
	ctx := context.Background()

	// Test data
	attachedFunctionName := "test-attachedFunction"
	inputCollectionID := "input-collection-id"
	outputCollectionName := "output-collection"
	functionName := "record_counter"
	tenantID := "test-tenant"
	databaseName := "test-database"
	databaseID := "database-uuid"
	functionID := uuid.New()
	minRecordsForRun := uint64(100)

	params := &structpb.Struct{
		Fields: map[string]*structpb.Value{
			"param1": structpb.NewStringValue("value1"),
		},
	}

	request := &coordinatorpb.AttachFunctionRequest{
		Name:                 attachedFunctionName,
		InputCollectionId:    inputCollectionID,
		OutputCollectionName: outputCollectionName,
		FunctionName:         functionName,
		TenantId:             tenantID,
		Database:             databaseName,
		MinRecordsForRun:     minRecordsForRun,
		Params:               params,
	}

	// ===== Phase 0: Check if attached function exists =====
	// Mock GetByName - attached function doesn't exist
	suite.mockMetaDomain.On("AttachedFunctionDb", ctx).Return(suite.mockAttachedFunctionDb).Once()
	suite.mockAttachedFunctionDb.On("GetByName", inputCollectionID, attachedFunctionName).
		Return(nil, nil).Once()

	// ===== Phase 1: Create attached function in transaction =====
	// Setup mocks that will be called within the transaction (using mock.Anything for context)
	// Double-check attached function doesn't exist
	suite.mockMetaDomain.On("AttachedFunctionDb", mock.Anything).Return(suite.mockAttachedFunctionDb).Once()
	suite.mockAttachedFunctionDb.On("GetByName", inputCollectionID, attachedFunctionName).
		Return(nil, nil).Once()

	// Look up database
	suite.mockMetaDomain.On("DatabaseDb", mock.Anything).Return(suite.mockDatabaseDb).Once()
	suite.mockDatabaseDb.On("GetDatabases", tenantID, databaseName).
		Return([]*dbmodel.Database{{ID: databaseID, Name: databaseName}}, nil).Once()

	// Look up function
	suite.mockMetaDomain.On("FunctionDb", mock.Anything).Return(suite.mockFunctionDb).Once()
	suite.mockFunctionDb.On("GetByName", functionName).
		Return(&dbmodel.Function{ID: functionID, Name: functionName}, nil).Once()

	// Check input collection exists
	suite.mockMetaDomain.On("CollectionDb", mock.Anything).Return(suite.mockCollectionDb).Once()
	suite.mockCollectionDb.On("GetCollections",
		[]string{inputCollectionID}, (*string)(nil), tenantID, databaseName, (*int32)(nil), (*int32)(nil), false).
		Return([]*dbmodel.CollectionAndMetadata{{Collection: &dbmodel.Collection{ID: inputCollectionID}}}, nil).Once()

	// Check output collection doesn't exist
	suite.mockMetaDomain.On("CollectionDb", mock.Anything).Return(suite.mockCollectionDb).Once()
	suite.mockCollectionDb.On("GetCollections",
		[]string(nil), &outputCollectionName, tenantID, databaseName, (*int32)(nil), (*int32)(nil), false).
		Return([]*dbmodel.CollectionAndMetadata{}, nil).Once()

	// Insert attached function with lowest_live_nonce = NULL
	suite.mockMetaDomain.On("AttachedFunctionDb", mock.Anything).Return(suite.mockAttachedFunctionDb).Once()
	suite.mockAttachedFunctionDb.On("Insert", mock.MatchedBy(func(attachedFunction *dbmodel.AttachedFunction) bool {
		// Verify attached function structure
		return attachedFunction.Name == attachedFunctionName &&
			attachedFunction.InputCollectionID == inputCollectionID &&
			attachedFunction.OutputCollectionName == outputCollectionName &&
			attachedFunction.FunctionID == functionID &&
			attachedFunction.TenantID == tenantID &&
			attachedFunction.DatabaseID == databaseID &&
			attachedFunction.MinRecordsForRun == int64(minRecordsForRun) &&
			attachedFunction.LowestLiveNonce == nil && // KEY: Must be NULL for 2PC
			attachedFunction.NextNonce != uuid.Nil
	})).Return(nil).Once()

	// Mock the Transaction call itself - it will execute the function
	suite.mockTxImpl.On("Transaction", ctx, mock.AnythingOfType("func(context.Context) error")).
		Run(func(args mock.Arguments) {
			txFunc := args.Get(1).(func(context.Context) error)
			txCtx := context.Background() // Simulated transaction context
			// Execute the transaction function
			err := txFunc(txCtx)
			suite.NoError(err)
		}).Return(nil).Once()

	// ===== Phase 2: Push to heap service =====
	suite.mockHeapClient.On("Push", ctx, inputCollectionID, mock.MatchedBy(func(schedules []*coordinatorpb.Schedule) bool {
		// Verify schedule structure
		if len(schedules) != 1 {
			return false
		}
		schedule := schedules[0]
		return schedule.Triggerable.PartitioningUuid == inputCollectionID &&
			schedule.Triggerable.SchedulingUuid != "" &&
			schedule.Nonce == minimalUUIDv7.String() && // Should use minimal UUID
			schedule.NextScheduled != nil
	})).Return(nil).Once()

	// ===== Phase 3: Update lowest_live_nonce =====
	suite.mockMetaDomain.On("AttachedFunctionDb", ctx).Return(suite.mockAttachedFunctionDb).Once()
	suite.mockAttachedFunctionDb.On("UpdateLowestLiveNonce", mock.AnythingOfType("uuid.UUID"), minimalUUIDv7).
		Return(nil).Once()

	// Execute AttachFunction
	response, err := suite.coordinator.AttachFunction(ctx, request)

	// Assertions
	suite.NoError(err)
	suite.NotNil(response)
	suite.NotEmpty(response.AttachedFunctionId)

	// Verify attached function ID is valid UUID
	attachedFunctionID, err := uuid.Parse(response.AttachedFunctionId)
	suite.NoError(err)
	suite.NotEqual(uuid.Nil, attachedFunctionID)

	// Verify all mocks were called as expected
	suite.mockMetaDomain.AssertExpectations(suite.T())
	suite.mockAttachedFunctionDb.AssertExpectations(suite.T())
	suite.mockFunctionDb.AssertExpectations(suite.T())
	suite.mockDatabaseDb.AssertExpectations(suite.T())
	suite.mockCollectionDb.AssertExpectations(suite.T())
	suite.mockHeapClient.AssertExpectations(suite.T())
	suite.mockTxImpl.AssertExpectations(suite.T())
}

// TestAttachFunction_IdempotentRequest_AlreadyInitialized tests idempotency:
// - Attached function already exists with lowest_live_nonce set (fully initialized)
// - Should return existing attached function immediately without any writes
// - Should validate that all parameters match
func (suite *AttachFunctionTestSuite) TestAttachFunction_IdempotentRequest_AlreadyInitialized() {
	ctx := context.Background()

	// Test data
	existingAttachedFunctionID := uuid.New()
	attachedFunctionName := "existing-attachedFunction"
	inputCollectionID := "input-collection-id"
	outputCollectionName := "output-collection"
	functionName := "record_counter"
	tenantID := "test-tenant"
	databaseName := "test-database"
	databaseID := "database-uuid"
	functionID := uuid.New()
	minRecordsForRun := uint64(100)
	nextNonce := uuid.Must(uuid.NewV7())
	lowestLiveNonce := uuid.Must(uuid.NewV7())

	params := &structpb.Struct{
		Fields: map[string]*structpb.Value{
			"param1": structpb.NewStringValue("value1"),
		},
	}

	request := &coordinatorpb.AttachFunctionRequest{
		Name:                 attachedFunctionName,
		InputCollectionId:    inputCollectionID,
		OutputCollectionName: outputCollectionName,
		FunctionName:         functionName,
		TenantId:             tenantID,
		Database:             databaseName,
		MinRecordsForRun:     minRecordsForRun,
		Params:               params,
	}

	// Existing attached function in database (fully initialized)
	now := time.Now()
	existingAttachedFunction := &dbmodel.AttachedFunction{
		ID:                   existingAttachedFunctionID,
		Name:                 attachedFunctionName,
		TenantID:             tenantID,
		DatabaseID:           databaseID,
		InputCollectionID:    inputCollectionID,
		OutputCollectionName: outputCollectionName,
		FunctionID:           functionID,
		MinRecordsForRun:     int64(minRecordsForRun),
		NextNonce:            nextNonce,
		LowestLiveNonce:      &lowestLiveNonce, // KEY: Already initialized
		NextRun:              now,
		CreatedAt:            now,
		UpdatedAt:            now,
	}

	// ===== Phase 0: Check if attached function exists =====
	suite.mockMetaDomain.On("AttachedFunctionDb", ctx).Return(suite.mockAttachedFunctionDb).Once()
	suite.mockAttachedFunctionDb.On("GetByName", inputCollectionID, attachedFunctionName).
		Return(existingAttachedFunction, nil).Once()

	// Validate function matches
	suite.mockMetaDomain.On("FunctionDb", ctx).Return(suite.mockFunctionDb).Once()
	suite.mockFunctionDb.On("GetByName", functionName).
		Return(&dbmodel.Function{ID: functionID, Name: functionName}, nil).Once()

	// Validate database matches
	suite.mockMetaDomain.On("DatabaseDb", ctx).Return(suite.mockDatabaseDb).Once()
	suite.mockDatabaseDb.On("GetDatabases", tenantID, databaseName).
		Return([]*dbmodel.Database{{ID: databaseID, Name: databaseName}}, nil).Once()

	// Execute AttachFunction
	response, err := suite.coordinator.AttachFunction(ctx, request)

	// Assertions
	suite.NoError(err)
	suite.NotNil(response)
	suite.Equal(existingAttachedFunctionID.String(), response.AttachedFunctionId)

	// Verify no writes occurred (no Transaction, no Insert, no UpdateLowestLiveNonce, no heap Push)
	suite.mockTxImpl.AssertNotCalled(suite.T(), "Transaction")
	suite.mockAttachedFunctionDb.AssertNotCalled(suite.T(), "Insert")
	suite.mockAttachedFunctionDb.AssertNotCalled(suite.T(), "UpdateLowestLiveNonce")
	suite.mockHeapClient.AssertNotCalled(suite.T(), "Push")

	// Verify all read mocks were called
	suite.mockMetaDomain.AssertExpectations(suite.T())
	suite.mockAttachedFunctionDb.AssertExpectations(suite.T())
	suite.mockFunctionDb.AssertExpectations(suite.T())
	suite.mockDatabaseDb.AssertExpectations(suite.T())
}

// TestAttachFunction_RecoveryFlow_HeapFailureThenSuccess tests the realistic recovery scenario:
// - First AttachFunction: Phase 1 succeeds (attached function created), Phase 2 fails (heap error)
// - Attached function left in incomplete state (lowest_live_nonce = NULL)
// - GetAttachedFunctionByName: Returns ErrAttachedFunctionNotReady because attached function is incomplete
// - Second AttachFunction: Detects incomplete attached function, completes Phase 2 & 3, succeeds
// - GetAttachedFunctionByName: Now succeeds and returns the ready attached function
func (suite *AttachFunctionTestSuite) TestAttachFunction_RecoveryFlow_HeapFailureThenSuccess() {
	ctx := context.Background()

	// Test data
	incompleteAttachedFunctionID := uuid.New()
	attachedFunctionName := "attachedFunction-with-heap-failure"
	inputCollectionID := "input-collection-id"
	outputCollectionName := "output-collection"
	functionName := "record_counter"
	tenantID := "test-tenant"
	databaseName := "test-database"
	databaseID := "database-uuid"
	functionID := uuid.New()
	minRecordsForRun := uint64(100)
	nextNonce := uuid.Must(uuid.NewV7())
	now := time.Now()

	params := &structpb.Struct{
		Fields: map[string]*structpb.Value{
			"param1": structpb.NewStringValue("value1"),
		},
	}

	request := &coordinatorpb.AttachFunctionRequest{
		Name:                 attachedFunctionName,
		InputCollectionId:    inputCollectionID,
		OutputCollectionName: outputCollectionName,
		FunctionName:         functionName,
		TenantId:             tenantID,
		Database:             databaseName,
		MinRecordsForRun:     minRecordsForRun,
		Params:               params,
	}

	// ========== FIRST ATTEMPT: Heap Push Fails ==========

	// Phase 0: No existing attached function
	suite.mockMetaDomain.On("AttachedFunctionDb", ctx).Return(suite.mockAttachedFunctionDb).Once()
	suite.mockAttachedFunctionDb.On("GetByName", inputCollectionID, attachedFunctionName).
		Return(nil, nil).Once()

	// Phase 1: Create attached function in transaction (all the same mocks as successful creation)
	suite.mockMetaDomain.On("AttachedFunctionDb", mock.Anything).Return(suite.mockAttachedFunctionDb).Once()
	suite.mockAttachedFunctionDb.On("GetByName", inputCollectionID, attachedFunctionName).
		Return(nil, nil).Once()

	suite.mockMetaDomain.On("DatabaseDb", mock.Anything).Return(suite.mockDatabaseDb).Once()
	suite.mockDatabaseDb.On("GetDatabases", tenantID, databaseName).
		Return([]*dbmodel.Database{{ID: databaseID, Name: databaseName}}, nil).Once()

	suite.mockMetaDomain.On("FunctionDb", mock.Anything).Return(suite.mockFunctionDb).Once()
	suite.mockFunctionDb.On("GetByName", functionName).
		Return(&dbmodel.Function{ID: functionID, Name: functionName}, nil).Once()

	suite.mockMetaDomain.On("CollectionDb", mock.Anything).Return(suite.mockCollectionDb).Once()
	suite.mockCollectionDb.On("GetCollections",
		[]string{inputCollectionID}, (*string)(nil), tenantID, databaseName, (*int32)(nil), (*int32)(nil), false).
		Return([]*dbmodel.CollectionAndMetadata{{Collection: &dbmodel.Collection{ID: inputCollectionID}}}, nil).Once()

	suite.mockMetaDomain.On("CollectionDb", mock.Anything).Return(suite.mockCollectionDb).Once()
	suite.mockCollectionDb.On("GetCollections",
		[]string(nil), &outputCollectionName, tenantID, databaseName, (*int32)(nil), (*int32)(nil), false).
		Return([]*dbmodel.CollectionAndMetadata{}, nil).Once()

	suite.mockMetaDomain.On("AttachedFunctionDb", mock.Anything).Return(suite.mockAttachedFunctionDb).Once()
	suite.mockAttachedFunctionDb.On("Insert", mock.MatchedBy(func(attachedFunction *dbmodel.AttachedFunction) bool {
		return attachedFunction.LowestLiveNonce == nil
	})).Return(nil).Once()

	suite.mockTxImpl.On("Transaction", ctx, mock.AnythingOfType("func(context.Context) error")).
		Run(func(args mock.Arguments) {
			txFunc := args.Get(1).(func(context.Context) error)
			_ = txFunc(context.Background())
		}).Return(nil).Once()

	// Phase 2: HEAP PUSH FAILS
	suite.mockHeapClient.On("Push", ctx, inputCollectionID, mock.Anything).
		Return(errors.New("heap service temporarily unavailable")).Once()

	// Phase 3: NOT REACHED (because Phase 2 failed)

	// First AttachFunction call - should fail at heap push
	response1, err1 := suite.coordinator.AttachFunction(ctx, request)
	suite.Error(err1)
	suite.Nil(response1)
	suite.Contains(err1.Error(), "heap service")

	// ========== GetAttachedFunctionByName: Should Return ErrAttachedFunctionNotReady ==========

	incompleteAttachedFunction := &dbmodel.AttachedFunction{
		ID:                   incompleteAttachedFunctionID,
		Name:                 attachedFunctionName,
		TenantID:             tenantID,
		DatabaseID:           databaseID,
		InputCollectionID:    inputCollectionID,
		OutputCollectionName: outputCollectionName,
		FunctionID:           functionID,
		MinRecordsForRun:     int64(minRecordsForRun),
		NextNonce:            nextNonce,
		LowestLiveNonce:      nil,
		NextRun:              now,
		CreatedAt:            now,
		UpdatedAt:            now,
	}

	// ========== SECOND ATTEMPT: Recovery Succeeds ==========

	// Phase 0: GetByName returns incomplete attached function (with ErrAttachedFunctionNotReady, which AttachFunction handles)
	suite.mockMetaDomain.On("AttachedFunctionDb", ctx).Return(suite.mockAttachedFunctionDb).Once()
	suite.mockAttachedFunctionDb.On("GetByName", inputCollectionID, attachedFunctionName).
		Return(incompleteAttachedFunction, common.ErrAttachedFunctionNotReady).Once()

	// Validate function matches
	suite.mockMetaDomain.On("FunctionDb", ctx).Return(suite.mockFunctionDb).Once()
	suite.mockFunctionDb.On("GetByID", functionID).
		Return(&dbmodel.Function{ID: functionID, Name: functionName}, nil).Once()

	// Validate database matches
	suite.mockMetaDomain.On("DatabaseDb", ctx).Return(suite.mockDatabaseDb).Once()
	suite.mockDatabaseDb.On("GetDatabases", tenantID, databaseName).
		Return([]*dbmodel.Database{{ID: databaseID, Name: databaseName}}, nil).Once()

	// Phase 1: SKIPPED (attached function exists)

	// Phase 2: Heap push succeeds this time
	suite.mockHeapClient.On("Push", ctx, inputCollectionID, mock.MatchedBy(func(schedules []*coordinatorpb.Schedule) bool {
		if len(schedules) != 1 {
			return false
		}
		schedule := schedules[0]
		return schedule.Triggerable.PartitioningUuid == inputCollectionID &&
			schedule.Triggerable.SchedulingUuid == incompleteAttachedFunctionID.String() &&
			schedule.Nonce == minimalUUIDv7.String() &&
			schedule.NextScheduled != nil
	})).Return(nil).Once()

	// Phase 3: Update lowest_live_nonce to complete initialization
	suite.mockMetaDomain.On("AttachedFunctionDb", ctx).Return(suite.mockAttachedFunctionDb).Once()
	suite.mockAttachedFunctionDb.On("UpdateLowestLiveNonce", incompleteAttachedFunctionID, minimalUUIDv7).
		Return(nil).Once()

	// Second AttachFunction call - should succeed
	response2, err2 := suite.coordinator.AttachFunction(ctx, request)
	suite.NoError(err2)
	suite.NotNil(response2)
	suite.Equal(incompleteAttachedFunctionID.String(), response2.AttachedFunctionId)

	// Verify Phase 1 was skipped in recovery
	suite.mockTxImpl.AssertNumberOfCalls(suite.T(), "Transaction", 1) // Only from first attempt

	// Verify Phase 2 and 3 were executed in recovery
	suite.mockHeapClient.AssertExpectations(suite.T())
	suite.mockAttachedFunctionDb.AssertExpectations(suite.T())
	suite.mockMetaDomain.AssertExpectations(suite.T())
}

// TestAttachFunction_IdempotentRequest_ParameterMismatch tests when attached function exists but with different parameters:
// - Attached function already exists with different function_name
// - Should return AlreadyExists error with descriptive message
// - Should not proceed with any initialization
func (suite *AttachFunctionTestSuite) TestAttachFunction_IdempotentRequest_ParameterMismatch() {
	ctx := context.Background()

	// Test data
	existingAttachedFunctionID := uuid.New()
	attachedFunctionName := "existing-attachedFunction"
	inputCollectionID := "input-collection-id"
	outputCollectionName := "output-collection"
	existingOperatorName := "record_counter"
	requestedOperatorName := "different_function" // DIFFERENT
	tenantID := "test-tenant"
	databaseName := "test-database"
	databaseID := "database-uuid"
	existingOperatorID := uuid.New()
	minRecordsForRun := uint64(100)
	nextNonce := uuid.Must(uuid.NewV7())
	lowestLiveNonce := uuid.Must(uuid.NewV7())
	now := time.Now()

	params := &structpb.Struct{
		Fields: map[string]*structpb.Value{
			"param1": structpb.NewStringValue("value1"),
		},
	}

	request := &coordinatorpb.AttachFunctionRequest{
		Name:                 attachedFunctionName,
		InputCollectionId:    inputCollectionID,
		OutputCollectionName: outputCollectionName,
		FunctionName:         requestedOperatorName, // Different from existing
		TenantId:             tenantID,
		Database:             databaseName,
		MinRecordsForRun:     minRecordsForRun,
		Params:               params,
	}

	// Existing attached function in database with DIFFERENT function
	existingAttachedFunction := &dbmodel.AttachedFunction{
		ID:                   existingAttachedFunctionID,
		Name:                 attachedFunctionName,
		TenantID:             tenantID,
		DatabaseID:           databaseID,
		InputCollectionID:    inputCollectionID,
		OutputCollectionName: outputCollectionName,
		FunctionID:           existingOperatorID,
		MinRecordsForRun:     int64(minRecordsForRun),
		NextNonce:            nextNonce,
		LowestLiveNonce:      &lowestLiveNonce, // Already initialized
		NextRun:              now,
		CreatedAt:            now,
		UpdatedAt:            now,
	}

	// ===== Phase 0: Check if attached function exists - finds attached function with different params =====
	suite.mockMetaDomain.On("AttachedFunctionDb", ctx).Return(suite.mockAttachedFunctionDb).Once()
	suite.mockAttachedFunctionDb.On("GetByName", inputCollectionID, attachedFunctionName).
		Return(existingAttachedFunction, nil).Once()

	// Validate function - returns DIFFERENT function name
	suite.mockMetaDomain.On("FunctionDb", ctx).Return(suite.mockFunctionDb).Once()
	suite.mockFunctionDb.On("GetByID", existingOperatorID).
		Return(&dbmodel.Function{
			ID:   existingOperatorID,
			Name: existingOperatorName, // Different from request
		}, nil).Once()

	// Database lookup happens before the error is returned
	suite.mockMetaDomain.On("DatabaseDb", ctx).Return(suite.mockDatabaseDb).Once()
	suite.mockDatabaseDb.On("GetDatabases", tenantID, databaseName).
		Return([]*dbmodel.Database{{ID: databaseID, Name: databaseName}}, nil).Once()

	// Execute AttachFunction
	response, err := suite.coordinator.AttachFunction(ctx, request)

	// Assertions - should fail with AlreadyExists error
	suite.Error(err)
	suite.Nil(response)
	suite.Contains(err.Error(), "attached function already exists with different function")
	suite.Contains(err.Error(), existingOperatorName)
	suite.Contains(err.Error(), requestedOperatorName)

	// Verify no writes occurred
	suite.mockTxImpl.AssertNotCalled(suite.T(), "Transaction")
	suite.mockAttachedFunctionDb.AssertNotCalled(suite.T(), "Insert")
	suite.mockAttachedFunctionDb.AssertNotCalled(suite.T(), "UpdateLowestLiveNonce")
	suite.mockHeapClient.AssertNotCalled(suite.T(), "Push")

	// Verify read mocks were called
	suite.mockMetaDomain.AssertExpectations(suite.T())
	suite.mockAttachedFunctionDb.AssertExpectations(suite.T())
	suite.mockFunctionDb.AssertExpectations(suite.T())
}

// TestAttachFunction_CleanupExpiredPartialAttachedFunctions tests the full stuck attached function cleanup flow:
// 1. First AttachFunction: Phase 1 succeeds (attached function created), Phase 2 fails (heap error)
// 2. Attached function is left stuck with lowest_live_nonce = NULL
// 3. CleanupExpiredPartialAttachedFunctions soft deletes the stuck attached function
// 4. Second AttachFunction: Now succeeds without conflict since stuck attached function was cleaned up
func (suite *AttachFunctionTestSuite) TestAttachFunction_CleanupExpiredPartialAttachedFunctions() {
	ctx := context.Background()

	// Test data
	attachedFunctionName := "attachedFunction-to-cleanup"
	inputCollectionID := "input-collection-id"
	outputCollectionName := "output-collection"
	functionName := "record_counter"
	tenantID := "test-tenant"
	databaseName := "test-database"
	databaseID := "database-uuid"
	functionID := uuid.New()
	minRecordsForRun := uint64(100)

	params := &structpb.Struct{
		Fields: map[string]*structpb.Value{
			"param1": structpb.NewStringValue("value1"),
		},
	}

	request := &coordinatorpb.AttachFunctionRequest{
		Name:                 attachedFunctionName,
		InputCollectionId:    inputCollectionID,
		OutputCollectionName: outputCollectionName,
		FunctionName:         functionName,
		TenantId:             tenantID,
		Database:             databaseName,
		MinRecordsForRun:     minRecordsForRun,
		Params:               params,
	}

	// ========== STEP 1: Create attached function that gets stuck (heap push fails) ==========

	// Setup mocks for Phase 0 & 1
	_ = suite.setupAttachFunctionMocks(ctx, request, databaseID, functionID)

	var stuckAttachedFunctionID uuid.UUID
	suite.mockMetaDomain.On("AttachedFunctionDb", mock.Anything).Return(suite.mockAttachedFunctionDb).Once()
	suite.mockAttachedFunctionDb.On("Insert", mock.MatchedBy(func(attachedFunction *dbmodel.AttachedFunction) bool {
		if attachedFunction.LowestLiveNonce == nil {
			stuckAttachedFunctionID = attachedFunction.ID // Capture the attached function ID
			return true
		}
		return false
	})).Return(nil).Once()

	suite.mockTxImpl.On("Transaction", ctx, mock.AnythingOfType("func(context.Context) error")).
		Run(func(args mock.Arguments) {
			txFunc := args.Get(1).(func(context.Context) error)
			_ = txFunc(context.Background())
		}).Return(nil).Once()

	// Phase 2: HEAP PUSH FAILS - attached function gets stuck
	suite.mockHeapClient.On("Push", ctx, inputCollectionID, mock.Anything).
		Return(errors.New("heap service unavailable")).Once()

	// Execute - should fail
	response1, err1 := suite.coordinator.AttachFunction(ctx, request)
	suite.Error(err1)
	suite.Nil(response1)
	suite.Contains(err1.Error(), "heap service")

	// ========== STEP 1.5: Verify stuck attached function returns ErrAttachedFunctionNotReady ==========

	// When GetByName is called on a stuck attached function (lowest_live_nonce = NULL),
	// the DAO returns ErrAttachedFunctionNotReady to indicate the attached function is incomplete
	incompleteAttachedFunction := &dbmodel.AttachedFunction{
		ID:                stuckAttachedFunctionID,
		Name:              attachedFunctionName,
		InputCollectionID: inputCollectionID,
		FunctionID:        functionID,
		LowestLiveNonce:   nil, // NULL = not ready
		IsDeleted:         false,
	}

	suite.mockMetaDomain.On("AttachedFunctionDb", ctx).Return(suite.mockAttachedFunctionDb).Once()
	suite.mockAttachedFunctionDb.On("GetByName", inputCollectionID, attachedFunctionName).
		Return(incompleteAttachedFunction, common.ErrAttachedFunctionNotReady).Once()

	// GetAttachedFunctionByName should return NotFound error when attached function is not ready
	getReq := &coordinatorpb.GetAttachedFunctionByNameRequest{
		InputCollectionId: inputCollectionID,
		Name:              attachedFunctionName,
	}
	getResp, getErr := suite.coordinator.GetAttachedFunctionByName(ctx, getReq)
	suite.Error(getErr)
	suite.Nil(getResp)
	suite.Equal(common.ErrAttachedFunctionNotFound, getErr)

	// ========== STEP 2: Cleanup the stuck attached function ==========

	// Mock CleanupExpiredPartialAttachedFunctions - finds and soft deletes the stuck attached function
	suite.mockMetaDomain.On("AttachedFunctionDb", ctx).Return(suite.mockAttachedFunctionDb).Once()
	suite.mockAttachedFunctionDb.On("CleanupExpiredPartialAttachedFunctions", uint64(300)). // 5 minutes
												Return([]uuid.UUID{stuckAttachedFunctionID}, nil).Once()

	// Execute cleanup
	cleanupReq := &coordinatorpb.CleanupExpiredPartialAttachedFunctionsRequest{
		MaxAgeSeconds: 300, // 5 minutes
	}
	cleanupResp, err := suite.coordinator.CleanupExpiredPartialAttachedFunctions(ctx, cleanupReq)

	// Assertions
	suite.NoError(err)
	suite.NotNil(cleanupResp)
	suite.Equal(uint64(1), cleanupResp.CleanedUpCount)
	suite.Len(cleanupResp.CleanedUpAttachedFunctionIds, 1)
	suite.Equal(stuckAttachedFunctionID.String(), cleanupResp.CleanedUpAttachedFunctionIds[0])

	// ========== STEP 3: Verify new AttachFunction succeeds after cleanup ==========

	// Setup mocks for Phase 0 & 1 (retry after cleanup)
	attachedFunctionMatcher := suite.setupAttachFunctionMocks(ctx, request, databaseID, functionID)

	suite.mockMetaDomain.On("AttachedFunctionDb", mock.Anything).Return(suite.mockAttachedFunctionDb).Once()
	suite.mockAttachedFunctionDb.On("Insert", mock.MatchedBy(attachedFunctionMatcher)).Return(nil).Once()

	suite.mockTxImpl.On("Transaction", ctx, mock.AnythingOfType("func(context.Context) error")).
		Run(func(args mock.Arguments) {
			txFunc := args.Get(1).(func(context.Context) error)
			_ = txFunc(context.Background())
		}).Return(nil).Once()

	// Phase 2: Heap push succeeds this time
	suite.mockHeapClient.On("Push", ctx, inputCollectionID, mock.Anything).
		Return(nil).Once()

	// Phase 3: Update lowest_live_nonce
	suite.mockMetaDomain.On("AttachedFunctionDb", ctx).Return(suite.mockAttachedFunctionDb).Once()
	suite.mockAttachedFunctionDb.On("UpdateLowestLiveNonce", mock.AnythingOfType("uuid.UUID"), minimalUUIDv7).
		Return(nil).Once()

	// Execute - should succeed now
	response2, err2 := suite.coordinator.AttachFunction(ctx, request)
	suite.NoError(err2)
	suite.NotNil(response2)
	suite.NotEmpty(response2.AttachedFunctionId)

	// Verify all mocks
	suite.mockMetaDomain.AssertExpectations(suite.T())
	suite.mockAttachedFunctionDb.AssertExpectations(suite.T())
	suite.mockFunctionDb.AssertExpectations(suite.T())
	suite.mockDatabaseDb.AssertExpectations(suite.T())
	suite.mockCollectionDb.AssertExpectations(suite.T())
	suite.mockHeapClient.AssertExpectations(suite.T())
	suite.mockTxImpl.AssertExpectations(suite.T())
}

func TestAttachFunctionTestSuite(t *testing.T) {
	suite.Run(t, new(AttachFunctionTestSuite))
}
