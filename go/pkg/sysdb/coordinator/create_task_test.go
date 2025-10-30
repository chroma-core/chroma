package coordinator

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/chroma-core/chroma/go/pkg/memberlist_manager"
	"github.com/chroma-core/chroma/go/pkg/proto/coordinatorpb"
	"github.com/chroma-core/chroma/go/pkg/sysdb/metastore/db/dbmodel"
	dbmodel_mocks "github.com/chroma-core/chroma/go/pkg/sysdb/metastore/db/dbmodel/mocks"
	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// testMinimalUUIDv7 is the test's copy of minimalUUIDv7 from task.go
// UUIDv7 format: [timestamp (48 bits)][version (4 bits)][random (12 bits)][variant (2 bits)][random (62 bits)]
// This UUID has all zeros for timestamp and random bits, making it the minimal valid UUIDv7.
var testMinimalUUIDv7 = uuid.UUID{
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // timestamp = 0 (bytes 0-5)
	0x70, 0x00, // version 7 (0x7) in high nibble, low nibble = 0 (bytes 6-7)
	0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // variant bits + rest = 0 (bytes 8-15)
}

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
	MinRecordsForInvocation := uint64(100)

	params := &structpb.Struct{
		Fields: map[string]*structpb.Value{
			"param1": structpb.NewStringValue("value1"),
		},
	}

	request := &coordinatorpb.AttachFunctionRequest{
		Name:                    attachedFunctionName,
		InputCollectionId:       inputCollectionID,
		OutputCollectionName:    outputCollectionName,
		FunctionName:            functionName,
		TenantId:                tenantID,
		Database:                databaseName,
		MinRecordsForInvocation: MinRecordsForInvocation,
		Params:                  params,
	}

	// ===== Phase 1: Attach function in transaction =====
	// Setup mocks that will be called within the transaction (using mock.Anything for context)
	// Check if attached function exists (idempotency check inside transaction)
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
			attachedFunction.MinRecordsForInvocation == int64(MinRecordsForInvocation) &&
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
			schedule.Nonce == testMinimalUUIDv7.String() && // Should use minimal UUID
			schedule.NextScheduled != nil
	})).Return(nil).Once()

	// ===== Phase 3: Update lowest_live_nonce =====
	suite.mockMetaDomain.On("AttachedFunctionDb", ctx).Return(suite.mockAttachedFunctionDb).Once()
	suite.mockAttachedFunctionDb.On("UpdateLowestLiveNonce", mock.AnythingOfType("uuid.UUID"), testMinimalUUIDv7).
		Return(nil).Once()

	// Execute AttachFunction
	response, err := suite.coordinator.AttachFunction(ctx, request)

	// Assertions
	suite.NoError(err)
	suite.NotNil(response)
	suite.NotEmpty(response.Id)

	// Verify attached function ID is valid UUID
	attachedFunctionID, err := uuid.Parse(response.Id)
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
	MinRecordsForInvocation := uint64(100)
	nextNonce := uuid.Must(uuid.NewV7())
	lowestLiveNonce := uuid.Must(uuid.NewV7())

	params := &structpb.Struct{
		Fields: map[string]*structpb.Value{
			"param1": structpb.NewStringValue("value1"),
		},
	}

	request := &coordinatorpb.AttachFunctionRequest{
		Name:                    attachedFunctionName,
		InputCollectionId:       inputCollectionID,
		OutputCollectionName:    outputCollectionName,
		FunctionName:            functionName,
		TenantId:                tenantID,
		Database:                databaseName,
		MinRecordsForInvocation: MinRecordsForInvocation,
		Params:                  params,
	}

	// Existing attached function in database (fully initialized)
	now := time.Now()
	existingAttachedFunction := &dbmodel.AttachedFunction{
		ID:                      existingAttachedFunctionID,
		Name:                    attachedFunctionName,
		TenantID:                tenantID,
		DatabaseID:              databaseID,
		InputCollectionID:       inputCollectionID,
		OutputCollectionName:    outputCollectionName,
		FunctionID:              functionID,
		MinRecordsForInvocation: int64(MinRecordsForInvocation),
		NextNonce:               nextNonce,
		LowestLiveNonce:         &lowestLiveNonce, // KEY: Already initialized
		NextRun:                 now,
		CreatedAt:               now,
		UpdatedAt:               now,
	}

	// ===== Phase 1: Transaction checks if attached function exists =====
	suite.mockMetaDomain.On("AttachedFunctionDb", ctx).Return(suite.mockAttachedFunctionDb).Once()
	suite.mockAttachedFunctionDb.On("GetByName", inputCollectionID, attachedFunctionName).
		Return(existingAttachedFunction, nil).Once()

	// Mock transaction call
	suite.mockTxImpl.On("Transaction", ctx, mock.AnythingOfType("func(context.Context) error")).
		Run(func(args mock.Arguments) {
			txFunc := args.Get(1).(func(context.Context) error)
			txCtx := context.Background()

			// Inside transaction: validate function by ID
			suite.mockMetaDomain.On("FunctionDb", txCtx).Return(suite.mockFunctionDb).Once()
			suite.mockFunctionDb.On("GetByID", functionID).
				Return(&dbmodel.Function{ID: functionID, Name: functionName}, nil).Once()

			// Validate database matches
			suite.mockMetaDomain.On("DatabaseDb", txCtx).Return(suite.mockDatabaseDb).Once()
			suite.mockDatabaseDb.On("GetDatabases", tenantID, databaseName).
				Return([]*dbmodel.Database{{ID: databaseID, Name: databaseName}}, nil).Once()

			_ = txFunc(txCtx)
		}).Return(nil).Once()

	// Execute AttachFunction
	response, err := suite.coordinator.AttachFunction(ctx, request)

	// Assertions
	suite.NoError(err)
	suite.NotNil(response)
	suite.Equal(existingAttachedFunctionID.String(), response.Id)

	// Verify no writes occurred (no Insert, no UpdateLowestLiveNonce, no heap Push)
	// Note: Transaction IS called for idempotency check, but no writes happen inside it
	suite.mockTxImpl.AssertNumberOfCalls(suite.T(), "Transaction", 1)
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
	MinRecordsForInvocation := uint64(100)
	nextNonce := uuid.Must(uuid.NewV7())
	now := time.Now()

	params := &structpb.Struct{
		Fields: map[string]*structpb.Value{
			"param1": structpb.NewStringValue("value1"),
		},
	}

	request := &coordinatorpb.AttachFunctionRequest{
		Name:                    attachedFunctionName,
		InputCollectionId:       inputCollectionID,
		OutputCollectionName:    outputCollectionName,
		FunctionName:            functionName,
		TenantId:                tenantID,
		Database:                databaseName,
		MinRecordsForInvocation: MinRecordsForInvocation,
		Params:                  params,
	}

	// ========== FIRST ATTEMPT: Heap Push Fails ==========

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
		ID:                      incompleteAttachedFunctionID,
		Name:                    attachedFunctionName,
		TenantID:                tenantID,
		DatabaseID:              databaseID,
		InputCollectionID:       inputCollectionID,
		OutputCollectionName:    outputCollectionName,
		FunctionID:              functionID,
		MinRecordsForInvocation: int64(MinRecordsForInvocation),
		NextNonce:               nextNonce,
		LowestLiveNonce:         nil,
		NextRun:                 now,
		CreatedAt:               now,
		UpdatedAt:               now,
	}

	// ========== SECOND ATTEMPT: Recovery Succeeds ==========

	// Phase 0: GetByName returns incomplete attached function (with ErrAttachedFunctionNotReady, which AttachFunction handles)
	suite.mockMetaDomain.On("AttachedFunctionDb", ctx).Return(suite.mockAttachedFunctionDb).Once()
	suite.mockAttachedFunctionDb.On("GetByName", inputCollectionID, attachedFunctionName).
		Return(incompleteAttachedFunction, nil).Once()

	// Validate function matches
	suite.mockMetaDomain.On("FunctionDb", ctx).Return(suite.mockFunctionDb).Once()
	suite.mockFunctionDb.On("GetByID", functionID).
		Return(&dbmodel.Function{ID: functionID, Name: functionName}, nil).Once()

	// Validate database matches (inside validateTaskMatchesRequest, called within transaction)
	suite.mockMetaDomain.On("DatabaseDb", mock.Anything).Return(suite.mockDatabaseDb).Once()
	suite.mockDatabaseDb.On("GetDatabases", tenantID, databaseName).
		Return([]*dbmodel.Database{{ID: databaseID, Name: databaseName}}, nil).Once()

	// Mock the Transaction call
	suite.mockTxImpl.On("Transaction", ctx, mock.AnythingOfType("func(context.Context) error")).
		Run(func(args mock.Arguments) {
			txFunc := args.Get(1).(func(context.Context) error)
			_ = txFunc(context.Background())
		}).Return(nil).Once()

	// Phase 2: Heap push succeeds this time
	suite.mockHeapClient.On("Push", ctx, inputCollectionID, mock.MatchedBy(func(schedules []*coordinatorpb.Schedule) bool {
		if len(schedules) != 1 {
			return false
		}
		schedule := schedules[0]
		return schedule.Triggerable.PartitioningUuid == inputCollectionID &&
			schedule.Triggerable.SchedulingUuid == incompleteAttachedFunctionID.String() &&
			schedule.Nonce == testMinimalUUIDv7.String() &&
			schedule.NextScheduled != nil
	})).Return(nil).Once()

	// Phase 3: Update lowest_live_nonce to complete initialization
	suite.mockMetaDomain.On("AttachedFunctionDb", ctx).Return(suite.mockAttachedFunctionDb).Once()
	suite.mockAttachedFunctionDb.On("UpdateLowestLiveNonce", incompleteAttachedFunctionID, testMinimalUUIDv7).
		Return(nil).Once()

	// Second AttachFunction call - should succeed
	response2, err2 := suite.coordinator.AttachFunction(ctx, request)
	suite.NoError(err2)
	suite.NotNil(response2)
	suite.Equal(incompleteAttachedFunctionID.String(), response2.Id)

	// Verify transaction was called in both attempts (idempotency check happens in transaction)
	suite.mockTxImpl.AssertNumberOfCalls(suite.T(), "Transaction", 2) // First attempt + recovery attempt

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
	MinRecordsForInvocation := uint64(100)
	nextNonce := uuid.Must(uuid.NewV7())
	lowestLiveNonce := uuid.Must(uuid.NewV7())
	now := time.Now()

	params := &structpb.Struct{
		Fields: map[string]*structpb.Value{
			"param1": structpb.NewStringValue("value1"),
		},
	}

	request := &coordinatorpb.AttachFunctionRequest{
		Name:                    attachedFunctionName,
		InputCollectionId:       inputCollectionID,
		OutputCollectionName:    outputCollectionName,
		FunctionName:            requestedOperatorName, // Different from existing
		TenantId:                tenantID,
		Database:                databaseName,
		MinRecordsForInvocation: MinRecordsForInvocation,
		Params:                  params,
	}

	// Existing attached function in database with DIFFERENT function
	existingAttachedFunction := &dbmodel.AttachedFunction{
		ID:                      existingAttachedFunctionID,
		Name:                    attachedFunctionName,
		TenantID:                tenantID,
		DatabaseID:              databaseID,
		InputCollectionID:       inputCollectionID,
		OutputCollectionName:    outputCollectionName,
		FunctionID:              existingOperatorID,
		MinRecordsForInvocation: int64(MinRecordsForInvocation),
		NextNonce:               nextNonce,
		LowestLiveNonce:         &lowestLiveNonce, // Already initialized
		NextRun:                 now,
		CreatedAt:               now,
		UpdatedAt:               now,
	}

	// ===== Phase 1: Transaction checks if task exists - finds task with different params =====
	suite.mockMetaDomain.On("AttachedFunctionDb", mock.Anything).Return(suite.mockAttachedFunctionDb).Once()
	suite.mockAttachedFunctionDb.On("GetByName", inputCollectionID, attachedFunctionName).
		Return(existingAttachedFunction, nil).Once()

	// Validate function - returns DIFFERENT function name
	suite.mockMetaDomain.On("FunctionDb", mock.Anything).Return(suite.mockFunctionDb).Once()
	suite.mockFunctionDb.On("GetByID", existingOperatorID).
		Return(&dbmodel.Function{
			ID:   existingOperatorID,
			Name: existingOperatorName, // Different from request
		}, nil).Once()

	// Database lookup happens before the error is returned (inside transaction)
	suite.mockMetaDomain.On("DatabaseDb", mock.Anything).Return(suite.mockDatabaseDb).Once()
	suite.mockDatabaseDb.On("GetDatabases", tenantID, databaseName).
		Return([]*dbmodel.Database{{ID: databaseID, Name: databaseName}}, nil).Once()

	// Mock transaction call - it will fail with validation error
	suite.mockTxImpl.On("Transaction", ctx, mock.AnythingOfType("func(context.Context) error")).
		Run(func(args mock.Arguments) {
			txFunc := args.Get(1).(func(context.Context) error)
			_ = txFunc(context.Background())
		}).Return(status.Errorf(codes.AlreadyExists, "different function is attached with this name: existing=%s, requested=%s", existingOperatorName, requestedOperatorName)).Once()

	// Execute AttachFunction
	response, err := suite.coordinator.AttachFunction(ctx, request)

	// Assertions - should fail with AlreadyExists error
	suite.Error(err)
	suite.Nil(response)
	suite.Contains(err.Error(), "different function is attached with this name")
	suite.Contains(err.Error(), existingOperatorName)
	suite.Contains(err.Error(), requestedOperatorName)

	// Verify no writes occurred (Transaction IS called but Insert/Update/Push are not)
	suite.mockTxImpl.AssertNumberOfCalls(suite.T(), "Transaction", 1)
	suite.mockAttachedFunctionDb.AssertNotCalled(suite.T(), "Insert")
	suite.mockAttachedFunctionDb.AssertNotCalled(suite.T(), "UpdateLowestLiveNonce")
	suite.mockHeapClient.AssertNotCalled(suite.T(), "Push")

	// Verify read mocks were called
	suite.mockMetaDomain.AssertExpectations(suite.T())
	suite.mockAttachedFunctionDb.AssertExpectations(suite.T())
	suite.mockFunctionDb.AssertExpectations(suite.T())
}

func TestAttachFunctionTestSuite(t *testing.T) {
	suite.Run(t, new(AttachFunctionTestSuite))
}

// TestGetSoftDeletedAttachedFunctions_TimestampConsistency verifies that timestamps
// are returned in microseconds (UnixMicro) to match other API methods
func TestGetSoftDeletedAttachedFunctions_TimestampConsistency(t *testing.T) {
	ctx := context.Background()

	// Create test timestamps with known values
	testTime := time.Date(2025, 10, 30, 12, 0, 0, 123456000, time.UTC) // 123.456 milliseconds
	expectedMicros := uint64(testTime.UnixMicro())

	// Create mock coordinator with minimal setup
	mockMetaDomain := &dbmodel_mocks.IMetaDomain{}
	mockAttachedFunctionDb := &dbmodel_mocks.IAttachedFunctionDb{}
	mockMetaDomain.On("AttachedFunctionDb", mock.Anything).Return(mockAttachedFunctionDb)

	// Mock the database response with our test timestamps
	attachedFunctions := []*dbmodel.AttachedFunction{
		{
			ID:                      uuid.New(),
			Name:                    "test_function",
			InputCollectionID:       "collection_123",
			OutputCollectionName:    "output_collection",
			CompletionOffset:        100,
			MinRecordsForInvocation: 10,
			CreatedAt:               testTime,
			UpdatedAt:               testTime,
			NextRun:                 testTime,
		},
	}

	mockAttachedFunctionDb.On("GetSoftDeletedAttachedFunctions", mock.Anything, mock.Anything).
		Return(attachedFunctions, nil)

	coordinator := &Coordinator{
		catalog: Catalog{
			metaDomain: mockMetaDomain,
		},
	}

	// Call GetSoftDeletedAttachedFunctions
	cutoffTime := timestamppb.New(testTime.Add(-24 * time.Hour))
	resp, err := coordinator.GetSoftDeletedAttachedFunctions(ctx, &coordinatorpb.GetSoftDeletedAttachedFunctionsRequest{
		CutoffTime: cutoffTime,
		Limit:      100,
	})

	// Verify response
	if err != nil {
		t.Fatalf("GetSoftDeletedAttachedFunctions failed: %v", err)
	}
	if len(resp.AttachedFunctions) != 1 {
		t.Fatalf("Expected 1 attached function, got %d", len(resp.AttachedFunctions))
	}

	af := resp.AttachedFunctions[0]

	// Verify timestamps are in microseconds (not seconds)
	if af.CreatedAt != expectedMicros {
		t.Errorf("CreatedAt timestamp mismatch: expected %d microseconds, got %d", expectedMicros, af.CreatedAt)
	}
	if af.UpdatedAt != expectedMicros {
		t.Errorf("UpdatedAt timestamp mismatch: expected %d microseconds, got %d", expectedMicros, af.UpdatedAt)
	}
	if af.NextRunAt != expectedMicros {
		t.Errorf("NextRunAt timestamp mismatch: expected %d microseconds, got %d", expectedMicros, af.NextRunAt)
	}

	// Verify these are NOT in seconds (would be ~1000x smaller)
	expectedSeconds := uint64(testTime.Unix())
	if af.CreatedAt == expectedSeconds {
		t.Error("CreatedAt appears to be in seconds instead of microseconds")
	}
	if af.UpdatedAt == expectedSeconds {
		t.Error("UpdatedAt appears to be in seconds instead of microseconds")
	}
	if af.NextRunAt == expectedSeconds {
		t.Error("NextRunAt appears to be in seconds instead of microseconds")
	}

	mockMetaDomain.AssertExpectations(t)
	mockAttachedFunctionDb.AssertExpectations(t)
}
