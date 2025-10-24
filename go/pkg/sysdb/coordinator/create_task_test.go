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

// CreateTaskTestSuite is a test suite for testing CreateTask two-phase commit logic
type CreateTaskTestSuite struct {
	suite.Suite
	mockMetaDomain   *dbmodel_mocks.IMetaDomain
	mockTxImpl       *dbmodel_mocks.ITransaction
	mockTaskDb       *dbmodel_mocks.ITaskDb
	mockOperatorDb   *dbmodel_mocks.IOperatorDb
	mockDatabaseDb   *dbmodel_mocks.IDatabaseDb
	mockCollectionDb *dbmodel_mocks.ICollectionDb
	mockHeapClient   *MockHeapClient
	coordinator      *Coordinator
}

// setupCreateTaskMocks sets up all the mocks for a CreateTask call (Phases 0 and 1)
// Returns a function that can be called to capture the created task ID
func (suite *CreateTaskTestSuite) setupCreateTaskMocks(ctx context.Context, request *coordinatorpb.CreateTaskRequest, databaseID string, operatorID uuid.UUID) func(*dbmodel.Task) bool {
	inputCollectionID := request.InputCollectionId
	taskName := request.Name
	outputCollectionName := request.OutputCollectionName
	tenantID := request.TenantId
	databaseName := request.Database
	operatorName := request.OperatorName

	// Phase 0: No existing task
	suite.mockMetaDomain.On("TaskDb", ctx).Return(suite.mockTaskDb).Once()
	suite.mockTaskDb.On("GetByName", inputCollectionID, taskName).
		Return(nil, nil).Once()

	// Phase 1: Create task in transaction
	suite.mockMetaDomain.On("TaskDb", mock.Anything).Return(suite.mockTaskDb).Once()
	suite.mockTaskDb.On("GetByName", inputCollectionID, taskName).
		Return(nil, nil).Once()

	suite.mockMetaDomain.On("DatabaseDb", mock.Anything).Return(suite.mockDatabaseDb).Once()
	suite.mockDatabaseDb.On("GetDatabases", tenantID, databaseName).
		Return([]*dbmodel.Database{{ID: databaseID, Name: databaseName}}, nil).Once()

	suite.mockMetaDomain.On("OperatorDb", mock.Anything).Return(suite.mockOperatorDb).Once()
	suite.mockOperatorDb.On("GetByName", operatorName).
		Return(&dbmodel.Operator{OperatorID: operatorID, OperatorName: operatorName}, nil).Once()

	suite.mockMetaDomain.On("CollectionDb", mock.Anything).Return(suite.mockCollectionDb).Once()
	suite.mockCollectionDb.On("GetCollections",
		[]string{inputCollectionID}, (*string)(nil), tenantID, databaseName, (*int32)(nil), (*int32)(nil), false).
		Return([]*dbmodel.CollectionAndMetadata{{Collection: &dbmodel.Collection{ID: inputCollectionID}}}, nil).Once()

	suite.mockMetaDomain.On("CollectionDb", mock.Anything).Return(suite.mockCollectionDb).Once()
	suite.mockCollectionDb.On("GetCollections",
		[]string(nil), &outputCollectionName, tenantID, databaseName, (*int32)(nil), (*int32)(nil), false).
		Return([]*dbmodel.CollectionAndMetadata{}, nil).Once()

	// Return a matcher function that can be used to capture task data
	return func(task *dbmodel.Task) bool {
		return task.LowestLiveNonce == nil
	}
}

func (suite *CreateTaskTestSuite) SetupTest() {
	// Create all mocks - note: we manually control AssertExpectations
	// to avoid conflicts with automatic cleanup
	suite.mockMetaDomain = &dbmodel_mocks.IMetaDomain{}
	suite.mockMetaDomain.Test(suite.T())

	suite.mockTxImpl = &dbmodel_mocks.ITransaction{}
	suite.mockTxImpl.Test(suite.T())

	suite.mockTaskDb = &dbmodel_mocks.ITaskDb{}
	suite.mockTaskDb.Test(suite.T())

	suite.mockOperatorDb = &dbmodel_mocks.IOperatorDb{}
	suite.mockOperatorDb.Test(suite.T())

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

// TestCreateTask_SuccessfulCreation_WithHeapService tests the happy path:
// - No existing task (Phase 0)
// - Create task with NULL lowest_live_nonce (Phase 1)
// - Push to heap service (Phase 2)
// - Update lowest_live_nonce to complete initialization (Phase 3)
func (suite *CreateTaskTestSuite) TestCreateTask_SuccessfulCreation_WithHeapService() {
	ctx := context.Background()

	// Test data
	taskName := "test-task"
	inputCollectionID := "input-collection-id"
	outputCollectionName := "output-collection"
	operatorName := "record_counter"
	tenantID := "test-tenant"
	databaseName := "test-database"
	databaseID := "database-uuid"
	operatorID := uuid.New()
	minRecordsForTask := uint64(100)

	params := &structpb.Struct{
		Fields: map[string]*structpb.Value{
			"param1": structpb.NewStringValue("value1"),
		},
	}

	request := &coordinatorpb.CreateTaskRequest{
		Name:                 taskName,
		InputCollectionId:    inputCollectionID,
		OutputCollectionName: outputCollectionName,
		OperatorName:         operatorName,
		TenantId:             tenantID,
		Database:             databaseName,
		MinRecordsForTask:    minRecordsForTask,
		Params:               params,
	}

	// ===== Phase 0: Check if task exists =====
	// Mock GetByName - task doesn't exist
	suite.mockMetaDomain.On("TaskDb", ctx).Return(suite.mockTaskDb).Once()
	suite.mockTaskDb.On("GetByName", inputCollectionID, taskName).
		Return(nil, nil).Once()

	// ===== Phase 1: Create task in transaction =====
	// Setup mocks that will be called within the transaction (using mock.Anything for context)
	// Double-check task doesn't exist
	suite.mockMetaDomain.On("TaskDb", mock.Anything).Return(suite.mockTaskDb).Once()
	suite.mockTaskDb.On("GetByName", inputCollectionID, taskName).
		Return(nil, nil).Once()

	// Look up database
	suite.mockMetaDomain.On("DatabaseDb", mock.Anything).Return(suite.mockDatabaseDb).Once()
	suite.mockDatabaseDb.On("GetDatabases", tenantID, databaseName).
		Return([]*dbmodel.Database{{ID: databaseID, Name: databaseName}}, nil).Once()

	// Look up operator
	suite.mockMetaDomain.On("OperatorDb", mock.Anything).Return(suite.mockOperatorDb).Once()
	suite.mockOperatorDb.On("GetByName", operatorName).
		Return(&dbmodel.Operator{OperatorID: operatorID, OperatorName: operatorName}, nil).Once()

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

	// Insert task with lowest_live_nonce = NULL
	suite.mockMetaDomain.On("TaskDb", mock.Anything).Return(suite.mockTaskDb).Once()
	suite.mockTaskDb.On("Insert", mock.MatchedBy(func(task *dbmodel.Task) bool {
		// Verify task structure
		return task.Name == taskName &&
			task.InputCollectionID == inputCollectionID &&
			task.OutputCollectionName == outputCollectionName &&
			task.OperatorID == operatorID &&
			task.TenantID == tenantID &&
			task.DatabaseID == databaseID &&
			task.MinRecordsForTask == int64(minRecordsForTask) &&
			task.LowestLiveNonce == nil && // KEY: Must be NULL for 2PC
			task.NextNonce != uuid.Nil
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
	suite.mockMetaDomain.On("TaskDb", ctx).Return(suite.mockTaskDb).Once()
	suite.mockTaskDb.On("UpdateLowestLiveNonce", mock.AnythingOfType("uuid.UUID"), minimalUUIDv7).
		Return(nil).Once()

	// Execute CreateTask
	response, err := suite.coordinator.CreateTask(ctx, request)

	// Assertions
	suite.NoError(err)
	suite.NotNil(response)
	suite.NotEmpty(response.TaskId)

	// Verify task ID is valid UUID
	taskID, err := uuid.Parse(response.TaskId)
	suite.NoError(err)
	suite.NotEqual(uuid.Nil, taskID)

	// Verify all mocks were called as expected
	suite.mockMetaDomain.AssertExpectations(suite.T())
	suite.mockTaskDb.AssertExpectations(suite.T())
	suite.mockOperatorDb.AssertExpectations(suite.T())
	suite.mockDatabaseDb.AssertExpectations(suite.T())
	suite.mockCollectionDb.AssertExpectations(suite.T())
	suite.mockHeapClient.AssertExpectations(suite.T())
	suite.mockTxImpl.AssertExpectations(suite.T())
}

// TestCreateTask_IdempotentRequest_AlreadyInitialized tests idempotency:
// - Task already exists with lowest_live_nonce set (fully initialized)
// - Should return existing task immediately without any writes
// - Should validate that all parameters match
func (suite *CreateTaskTestSuite) TestCreateTask_IdempotentRequest_AlreadyInitialized() {
	ctx := context.Background()

	// Test data
	existingTaskID := uuid.New()
	taskName := "existing-task"
	inputCollectionID := "input-collection-id"
	outputCollectionName := "output-collection"
	operatorName := "record_counter"
	tenantID := "test-tenant"
	databaseName := "test-database"
	databaseID := "database-uuid"
	operatorID := uuid.New()
	minRecordsForTask := uint64(100)
	nextNonce := uuid.Must(uuid.NewV7())
	lowestLiveNonce := uuid.Must(uuid.NewV7())

	params := &structpb.Struct{
		Fields: map[string]*structpb.Value{
			"param1": structpb.NewStringValue("value1"),
		},
	}

	request := &coordinatorpb.CreateTaskRequest{
		Name:                 taskName,
		InputCollectionId:    inputCollectionID,
		OutputCollectionName: outputCollectionName,
		OperatorName:         operatorName,
		TenantId:             tenantID,
		Database:             databaseName,
		MinRecordsForTask:    minRecordsForTask,
		Params:               params,
	}

	// Existing task in database (fully initialized)
	now := time.Now()
	existingTask := &dbmodel.Task{
		ID:                   existingTaskID,
		Name:                 taskName,
		TenantID:             tenantID,
		DatabaseID:           databaseID,
		InputCollectionID:    inputCollectionID,
		OutputCollectionName: outputCollectionName,
		OperatorID:           operatorID,
		MinRecordsForTask:    int64(minRecordsForTask),
		NextNonce:            nextNonce,
		LowestLiveNonce:      &lowestLiveNonce, // KEY: Already initialized
		NextRun:              now,
		CreatedAt:            now,
		UpdatedAt:            now,
	}

	// ===== Phase 0: Check if task exists =====
	suite.mockMetaDomain.On("TaskDb", ctx).Return(suite.mockTaskDb).Once()
	suite.mockTaskDb.On("GetByName", inputCollectionID, taskName).
		Return(existingTask, nil).Once()

	// Validate operator matches
	suite.mockMetaDomain.On("OperatorDb", ctx).Return(suite.mockOperatorDb).Once()
	suite.mockOperatorDb.On("GetByID", operatorID).
		Return(&dbmodel.Operator{OperatorID: operatorID, OperatorName: operatorName}, nil).Once()

	// Validate database matches
	suite.mockMetaDomain.On("DatabaseDb", ctx).Return(suite.mockDatabaseDb).Once()
	suite.mockDatabaseDb.On("GetDatabases", tenantID, databaseName).
		Return([]*dbmodel.Database{{ID: databaseID, Name: databaseName}}, nil).Once()

	// Execute CreateTask
	response, err := suite.coordinator.CreateTask(ctx, request)

	// Assertions
	suite.NoError(err)
	suite.NotNil(response)
	suite.Equal(existingTaskID.String(), response.TaskId)

	// Verify no writes occurred (no Transaction, no Insert, no UpdateLowestLiveNonce, no heap Push)
	suite.mockTxImpl.AssertNotCalled(suite.T(), "Transaction")
	suite.mockTaskDb.AssertNotCalled(suite.T(), "Insert")
	suite.mockTaskDb.AssertNotCalled(suite.T(), "UpdateLowestLiveNonce")
	suite.mockHeapClient.AssertNotCalled(suite.T(), "Push")

	// Verify all read mocks were called
	suite.mockMetaDomain.AssertExpectations(suite.T())
	suite.mockTaskDb.AssertExpectations(suite.T())
	suite.mockOperatorDb.AssertExpectations(suite.T())
	suite.mockDatabaseDb.AssertExpectations(suite.T())
}

// TestCreateTask_RecoveryFlow_HeapFailureThenSuccess tests the realistic recovery scenario:
// - First CreateTask: Phase 1 succeeds (task created), Phase 2 fails (heap error)
// - Task left in incomplete state (lowest_live_nonce = NULL)
// - GetTaskByName: Returns ErrTaskNotReady because task is incomplete
// - Second CreateTask: Detects incomplete task, completes Phase 2 & 3, succeeds
// - GetTaskByName: Now succeeds and returns the ready task
func (suite *CreateTaskTestSuite) TestCreateTask_RecoveryFlow_HeapFailureThenSuccess() {
	ctx := context.Background()

	// Test data
	incompleteTaskID := uuid.New()
	taskName := "task-with-heap-failure"
	inputCollectionID := "input-collection-id"
	outputCollectionName := "output-collection"
	operatorName := "record_counter"
	tenantID := "test-tenant"
	databaseName := "test-database"
	databaseID := "database-uuid"
	operatorID := uuid.New()
	minRecordsForTask := uint64(100)
	nextNonce := uuid.Must(uuid.NewV7())
	now := time.Now()

	params := &structpb.Struct{
		Fields: map[string]*structpb.Value{
			"param1": structpb.NewStringValue("value1"),
		},
	}

	request := &coordinatorpb.CreateTaskRequest{
		Name:                 taskName,
		InputCollectionId:    inputCollectionID,
		OutputCollectionName: outputCollectionName,
		OperatorName:         operatorName,
		TenantId:             tenantID,
		Database:             databaseName,
		MinRecordsForTask:    minRecordsForTask,
		Params:               params,
	}

	// ========== FIRST ATTEMPT: Heap Push Fails ==========

	// Phase 0: No existing task
	suite.mockMetaDomain.On("TaskDb", ctx).Return(suite.mockTaskDb).Once()
	suite.mockTaskDb.On("GetByName", inputCollectionID, taskName).
		Return(nil, nil).Once()

	// Phase 1: Create task in transaction (all the same mocks as successful creation)
	suite.mockMetaDomain.On("TaskDb", mock.Anything).Return(suite.mockTaskDb).Once()
	suite.mockTaskDb.On("GetByName", inputCollectionID, taskName).
		Return(nil, nil).Once()

	suite.mockMetaDomain.On("DatabaseDb", mock.Anything).Return(suite.mockDatabaseDb).Once()
	suite.mockDatabaseDb.On("GetDatabases", tenantID, databaseName).
		Return([]*dbmodel.Database{{ID: databaseID, Name: databaseName}}, nil).Once()

	suite.mockMetaDomain.On("OperatorDb", mock.Anything).Return(suite.mockOperatorDb).Once()
	suite.mockOperatorDb.On("GetByName", operatorName).
		Return(&dbmodel.Operator{OperatorID: operatorID, OperatorName: operatorName}, nil).Once()

	suite.mockMetaDomain.On("CollectionDb", mock.Anything).Return(suite.mockCollectionDb).Once()
	suite.mockCollectionDb.On("GetCollections",
		[]string{inputCollectionID}, (*string)(nil), tenantID, databaseName, (*int32)(nil), (*int32)(nil), false).
		Return([]*dbmodel.CollectionAndMetadata{{Collection: &dbmodel.Collection{ID: inputCollectionID}}}, nil).Once()

	suite.mockMetaDomain.On("CollectionDb", mock.Anything).Return(suite.mockCollectionDb).Once()
	suite.mockCollectionDb.On("GetCollections",
		[]string(nil), &outputCollectionName, tenantID, databaseName, (*int32)(nil), (*int32)(nil), false).
		Return([]*dbmodel.CollectionAndMetadata{}, nil).Once()

	suite.mockMetaDomain.On("TaskDb", mock.Anything).Return(suite.mockTaskDb).Once()
	suite.mockTaskDb.On("Insert", mock.MatchedBy(func(task *dbmodel.Task) bool {
		return task.LowestLiveNonce == nil
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

	// First CreateTask call - should fail at heap push
	response1, err1 := suite.coordinator.CreateTask(ctx, request)
	suite.Error(err1)
	suite.Nil(response1)
	suite.Contains(err1.Error(), "heap service")

	// ========== GetTaskByName: Should Return ErrTaskNotReady ==========

	incompleteTask := &dbmodel.Task{
		ID:                   incompleteTaskID,
		Name:                 taskName,
		TenantID:             tenantID,
		DatabaseID:           databaseID,
		InputCollectionID:    inputCollectionID,
		OutputCollectionName: outputCollectionName,
		OperatorID:           operatorID,
		MinRecordsForTask:    int64(minRecordsForTask),
		NextNonce:            nextNonce,
		LowestLiveNonce:      nil,
		NextRun:              now,
		CreatedAt:            now,
		UpdatedAt:            now,
	}

	// ========== SECOND ATTEMPT: Recovery Succeeds ==========

	// Phase 0: GetByName returns incomplete task (with ErrTaskNotReady, which CreateTask handles)
	suite.mockMetaDomain.On("TaskDb", ctx).Return(suite.mockTaskDb).Once()
	suite.mockTaskDb.On("GetByName", inputCollectionID, taskName).
		Return(incompleteTask, common.ErrTaskNotReady).Once()

	// Validate operator matches
	suite.mockMetaDomain.On("OperatorDb", ctx).Return(suite.mockOperatorDb).Once()
	suite.mockOperatorDb.On("GetByID", operatorID).
		Return(&dbmodel.Operator{OperatorID: operatorID, OperatorName: operatorName}, nil).Once()

	// Validate database matches
	suite.mockMetaDomain.On("DatabaseDb", ctx).Return(suite.mockDatabaseDb).Once()
	suite.mockDatabaseDb.On("GetDatabases", tenantID, databaseName).
		Return([]*dbmodel.Database{{ID: databaseID, Name: databaseName}}, nil).Once()

	// Phase 1: SKIPPED (task exists)

	// Phase 2: Heap push succeeds this time
	suite.mockHeapClient.On("Push", ctx, inputCollectionID, mock.MatchedBy(func(schedules []*coordinatorpb.Schedule) bool {
		if len(schedules) != 1 {
			return false
		}
		schedule := schedules[0]
		return schedule.Triggerable.PartitioningUuid == inputCollectionID &&
			schedule.Triggerable.SchedulingUuid == incompleteTaskID.String() &&
			schedule.Nonce == minimalUUIDv7.String() &&
			schedule.NextScheduled != nil
	})).Return(nil).Once()

	// Phase 3: Update lowest_live_nonce to complete initialization
	suite.mockMetaDomain.On("TaskDb", ctx).Return(suite.mockTaskDb).Once()
	suite.mockTaskDb.On("UpdateLowestLiveNonce", incompleteTaskID, minimalUUIDv7).
		Return(nil).Once()

	// Second CreateTask call - should succeed
	response2, err2 := suite.coordinator.CreateTask(ctx, request)
	suite.NoError(err2)
	suite.NotNil(response2)
	suite.Equal(incompleteTaskID.String(), response2.TaskId)

	// Verify Phase 1 was skipped in recovery
	suite.mockTxImpl.AssertNumberOfCalls(suite.T(), "Transaction", 1) // Only from first attempt

	// Verify Phase 2 and 3 were executed in recovery
	suite.mockHeapClient.AssertExpectations(suite.T())
	suite.mockTaskDb.AssertExpectations(suite.T())
	suite.mockMetaDomain.AssertExpectations(suite.T())
}

// TestCreateTask_IdempotentRequest_ParameterMismatch tests when task exists but with different parameters:
// - Task already exists with different operator_name
// - Should return AlreadyExists error with descriptive message
// - Should not proceed with any initialization
func (suite *CreateTaskTestSuite) TestCreateTask_IdempotentRequest_ParameterMismatch() {
	ctx := context.Background()

	// Test data
	existingTaskID := uuid.New()
	taskName := "existing-task"
	inputCollectionID := "input-collection-id"
	outputCollectionName := "output-collection"
	existingOperatorName := "record_counter"
	requestedOperatorName := "different_operator" // DIFFERENT
	tenantID := "test-tenant"
	databaseName := "test-database"
	databaseID := "database-uuid"
	existingOperatorID := uuid.New()
	minRecordsForTask := uint64(100)
	nextNonce := uuid.Must(uuid.NewV7())
	lowestLiveNonce := uuid.Must(uuid.NewV7())
	now := time.Now()

	params := &structpb.Struct{
		Fields: map[string]*structpb.Value{
			"param1": structpb.NewStringValue("value1"),
		},
	}

	request := &coordinatorpb.CreateTaskRequest{
		Name:                 taskName,
		InputCollectionId:    inputCollectionID,
		OutputCollectionName: outputCollectionName,
		OperatorName:         requestedOperatorName, // Different from existing
		TenantId:             tenantID,
		Database:             databaseName,
		MinRecordsForTask:    minRecordsForTask,
		Params:               params,
	}

	// Existing task in database with DIFFERENT operator
	existingTask := &dbmodel.Task{
		ID:                   existingTaskID,
		Name:                 taskName,
		TenantID:             tenantID,
		DatabaseID:           databaseID,
		InputCollectionID:    inputCollectionID,
		OutputCollectionName: outputCollectionName,
		OperatorID:           existingOperatorID,
		MinRecordsForTask:    int64(minRecordsForTask),
		NextNonce:            nextNonce,
		LowestLiveNonce:      &lowestLiveNonce, // Already initialized
		NextRun:              now,
		CreatedAt:            now,
		UpdatedAt:            now,
	}

	// ===== Phase 0: Check if task exists - finds task with different params =====
	suite.mockMetaDomain.On("TaskDb", ctx).Return(suite.mockTaskDb).Once()
	suite.mockTaskDb.On("GetByName", inputCollectionID, taskName).
		Return(existingTask, nil).Once()

	// Validate operator - returns DIFFERENT operator name
	suite.mockMetaDomain.On("OperatorDb", ctx).Return(suite.mockOperatorDb).Once()
	suite.mockOperatorDb.On("GetByID", existingOperatorID).
		Return(&dbmodel.Operator{
			OperatorID:   existingOperatorID,
			OperatorName: existingOperatorName, // Different from request
		}, nil).Once()

	// Database lookup happens before the error is returned
	suite.mockMetaDomain.On("DatabaseDb", ctx).Return(suite.mockDatabaseDb).Once()
	suite.mockDatabaseDb.On("GetDatabases", tenantID, databaseName).
		Return([]*dbmodel.Database{{ID: databaseID, Name: databaseName}}, nil).Once()

	// Execute CreateTask
	response, err := suite.coordinator.CreateTask(ctx, request)

	// Assertions - should fail with AlreadyExists error
	suite.Error(err)
	suite.Nil(response)
	suite.Contains(err.Error(), "task already exists with different operator")
	suite.Contains(err.Error(), existingOperatorName)
	suite.Contains(err.Error(), requestedOperatorName)

	// Verify no writes occurred
	suite.mockTxImpl.AssertNotCalled(suite.T(), "Transaction")
	suite.mockTaskDb.AssertNotCalled(suite.T(), "Insert")
	suite.mockTaskDb.AssertNotCalled(suite.T(), "UpdateLowestLiveNonce")
	suite.mockHeapClient.AssertNotCalled(suite.T(), "Push")

	// Verify read mocks were called
	suite.mockMetaDomain.AssertExpectations(suite.T())
	suite.mockTaskDb.AssertExpectations(suite.T())
	suite.mockOperatorDb.AssertExpectations(suite.T())
}

// TestCreateTask_CleanupExpiredPartialTasks tests the full stuck task cleanup flow:
// 1. First CreateTask: Phase 1 succeeds (task created), Phase 2 fails (heap error)
// 2. Task is left stuck with lowest_live_nonce = NULL
// 3. CleanupExpiredPartialTasks soft deletes the stuck task
// 4. Second CreateTask: Now succeeds without conflict since stuck task was cleaned up
func (suite *CreateTaskTestSuite) TestCreateTask_CleanupExpiredPartialTasks() {
	ctx := context.Background()

	// Test data
	taskName := "task-to-cleanup"
	inputCollectionID := "input-collection-id"
	outputCollectionName := "output-collection"
	operatorName := "record_counter"
	tenantID := "test-tenant"
	databaseName := "test-database"
	databaseID := "database-uuid"
	operatorID := uuid.New()
	minRecordsForTask := uint64(100)

	params := &structpb.Struct{
		Fields: map[string]*structpb.Value{
			"param1": structpb.NewStringValue("value1"),
		},
	}

	request := &coordinatorpb.CreateTaskRequest{
		Name:                 taskName,
		InputCollectionId:    inputCollectionID,
		OutputCollectionName: outputCollectionName,
		OperatorName:         operatorName,
		TenantId:             tenantID,
		Database:             databaseName,
		MinRecordsForTask:    minRecordsForTask,
		Params:               params,
	}

	// ========== STEP 1: Create task that gets stuck (heap push fails) ==========

	// Setup mocks for Phase 0 & 1
	_ = suite.setupCreateTaskMocks(ctx, request, databaseID, operatorID)

	var stuckTaskID uuid.UUID
	suite.mockMetaDomain.On("TaskDb", mock.Anything).Return(suite.mockTaskDb).Once()
	suite.mockTaskDb.On("Insert", mock.MatchedBy(func(task *dbmodel.Task) bool {
		if task.LowestLiveNonce == nil {
			stuckTaskID = task.ID // Capture the task ID
			return true
		}
		return false
	})).Return(nil).Once()

	suite.mockTxImpl.On("Transaction", ctx, mock.AnythingOfType("func(context.Context) error")).
		Run(func(args mock.Arguments) {
			txFunc := args.Get(1).(func(context.Context) error)
			_ = txFunc(context.Background())
		}).Return(nil).Once()

	// Phase 2: HEAP PUSH FAILS - task gets stuck
	suite.mockHeapClient.On("Push", ctx, inputCollectionID, mock.Anything).
		Return(errors.New("heap service unavailable")).Once()

	// Execute - should fail
	response1, err1 := suite.coordinator.CreateTask(ctx, request)
	suite.Error(err1)
	suite.Nil(response1)
	suite.Contains(err1.Error(), "heap service")

	// ========== STEP 1.5: Verify stuck task returns ErrTaskNotReady ==========

	// When GetByName is called on a stuck task (lowest_live_nonce = NULL),
	// the DAO returns ErrTaskNotReady to indicate the task is incomplete
	incompleteTask := &dbmodel.Task{
		ID:                stuckTaskID,
		Name:              taskName,
		InputCollectionID: inputCollectionID,
		OperatorID:        operatorID,
		LowestLiveNonce:   nil, // NULL = not ready
		IsDeleted:         false,
	}

	suite.mockMetaDomain.On("TaskDb", ctx).Return(suite.mockTaskDb).Once()
	suite.mockTaskDb.On("GetByName", inputCollectionID, taskName).
		Return(incompleteTask, common.ErrTaskNotReady).Once()

	// GetTaskByName should return NotFound error when task is not ready
	getReq := &coordinatorpb.GetTaskByNameRequest{
		InputCollectionId: inputCollectionID,
		TaskName:          taskName,
	}
	getResp, getErr := suite.coordinator.GetTaskByName(ctx, getReq)
	suite.Error(getErr)
	suite.Nil(getResp)
	suite.Equal(common.ErrTaskNotFound, getErr)

	// ========== STEP 2: Cleanup the stuck task ==========

	// Mock CleanupExpiredPartialTasks - finds and soft deletes the stuck task
	suite.mockMetaDomain.On("TaskDb", ctx).Return(suite.mockTaskDb).Once()
	suite.mockTaskDb.On("CleanupExpiredPartialTasks", uint64(300)). // 5 minutes
									Return([]uuid.UUID{stuckTaskID}, nil).Once()

	// Execute cleanup
	cleanupReq := &coordinatorpb.CleanupExpiredPartialTasksRequest{
		MaxAgeSeconds: 300, // 5 minutes
	}
	cleanupResp, err := suite.coordinator.CleanupExpiredPartialTasks(ctx, cleanupReq)

	// Assertions
	suite.NoError(err)
	suite.NotNil(cleanupResp)
	suite.Equal(uint64(1), cleanupResp.CleanedUpCount)
	suite.Len(cleanupResp.CleanedUpTaskIds, 1)
	suite.Equal(stuckTaskID.String(), cleanupResp.CleanedUpTaskIds[0])

	// ========== STEP 3: Verify new CreateTask succeeds after cleanup ==========

	// Setup mocks for Phase 0 & 1 (retry after cleanup)
	taskMatcher := suite.setupCreateTaskMocks(ctx, request, databaseID, operatorID)

	suite.mockMetaDomain.On("TaskDb", mock.Anything).Return(suite.mockTaskDb).Once()
	suite.mockTaskDb.On("Insert", mock.MatchedBy(taskMatcher)).Return(nil).Once()

	suite.mockTxImpl.On("Transaction", ctx, mock.AnythingOfType("func(context.Context) error")).
		Run(func(args mock.Arguments) {
			txFunc := args.Get(1).(func(context.Context) error)
			_ = txFunc(context.Background())
		}).Return(nil).Once()

	// Phase 2: Heap push succeeds this time
	suite.mockHeapClient.On("Push", ctx, inputCollectionID, mock.Anything).
		Return(nil).Once()

	// Phase 3: Update lowest_live_nonce
	suite.mockMetaDomain.On("TaskDb", ctx).Return(suite.mockTaskDb).Once()
	suite.mockTaskDb.On("UpdateLowestLiveNonce", mock.AnythingOfType("uuid.UUID"), minimalUUIDv7).
		Return(nil).Once()

	// Execute - should succeed now
	response2, err2 := suite.coordinator.CreateTask(ctx, request)
	suite.NoError(err2)
	suite.NotNil(response2)
	suite.NotEmpty(response2.TaskId)

	// Verify all mocks
	suite.mockMetaDomain.AssertExpectations(suite.T())
	suite.mockTaskDb.AssertExpectations(suite.T())
	suite.mockOperatorDb.AssertExpectations(suite.T())
	suite.mockDatabaseDb.AssertExpectations(suite.T())
	suite.mockCollectionDb.AssertExpectations(suite.T())
	suite.mockHeapClient.AssertExpectations(suite.T())
	suite.mockTxImpl.AssertExpectations(suite.T())
}

func TestCreateTaskTestSuite(t *testing.T) {
	suite.Run(t, new(CreateTaskTestSuite))
}
