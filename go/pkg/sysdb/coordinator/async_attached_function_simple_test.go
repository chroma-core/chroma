package coordinator

import (
	"context"
	"testing"

	"github.com/chroma-core/chroma/go/pkg/common"
	"github.com/chroma-core/chroma/go/pkg/sysdb/metastore/db/dbmodel"
	dbmodel_mocks "github.com/chroma-core/chroma/go/pkg/sysdb/metastore/db/dbmodel/mocks"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"

	coordinatorpb "github.com/chroma-core/chroma/go/pkg/proto/coordinatorpb"
)

// Test the flow where repair is needed, then finalize, then success
func TestAsyncFunctionRepairFlowSimple(t *testing.T) {
	zap.ReplaceGlobals(zap.Must(zap.NewDevelopment()))

	ctx := context.Background()
	attachedFunctionID := uuid.New()
	functionID := uuid.New()
	collectionID := uuid.New().String()
	newCompletionOffset := uint64(50)
	witnessedLogOffset := int64(100) // Higher than new offset - needs repair

	attachedFunction := &dbmodel.AttachedFunction{
		ID:                attachedFunctionID,
		FunctionID:        functionID,
		InputCollectionID: collectionID,
		CompletionOffset:  40,
	}

	function := &dbmodel.Function{
		ID:      functionID,
		IsAsync: true,
	}

	collection := &dbmodel.CollectionAndMetadata{
		Collection: &dbmodel.Collection{
			ID:          collectionID,
			LogPosition: witnessedLogOffset,
		},
	}

	// Setup mocks
	mockTxImpl := &dbmodel_mocks.ITransaction{}
	mockMetaDomain := &dbmodel_mocks.IMetaDomain{}
	mockAttachedFunctionDb := &dbmodel_mocks.IAttachedFunctionDb{}
	mockCollectionDb := &dbmodel_mocks.ICollectionDb{}
	mockFunctionDb := &dbmodel_mocks.IFunctionDb{}

	coordinator := &Coordinator{
		ctx: ctx,
		catalog: Catalog{
			txImpl:     mockTxImpl,
			metaDomain: mockMetaDomain,
		},
	}

	// First call - should return needs repair
	// Transaction executes the function immediately
	mockTxImpl.On("Transaction", mock.Anything, mock.AnythingOfType("func(context.Context) error")).
		Return(func(ctx context.Context, fn func(context.Context) error) error {
			return fn(ctx)
		})

	// Mock all the DB calls
	mockMetaDomain.On("AttachedFunctionDb", mock.Anything).Return(mockAttachedFunctionDb)
	mockAttachedFunctionDb.On("GetAttachedFunctions", &attachedFunctionID, (*string)(nil), (*string)(nil), true).
		Return([]*dbmodel.AttachedFunction{attachedFunction}, nil)

	mockMetaDomain.On("FunctionDb", mock.Anything).Return(mockFunctionDb)
	mockFunctionDb.On("GetByID", functionID).Return(function, nil)

	mockMetaDomain.On("CollectionDb", mock.Anything).Return(mockCollectionDb)
	mockCollectionDb.On("GetCollections", []string{collectionID}, (*string)(nil), "", "", (*int32)(nil), (*int32)(nil), false).
		Return([]*dbmodel.CollectionAndMetadata{collection}, nil)

	// Should update - heap_entry_pending is computed atomically within the update
	mockAttachedFunctionDb.On("UpdateCompletionOffsetAndHeapEntry", attachedFunctionID, collectionID, int64(newCompletionOffset)).
		Return(nil)

	req := &coordinatorpb.TryFinishAsyncAttachedFunctionInvocationRequest{
		AttachedFunctionId:  attachedFunctionID.String(),
		CollectionId:        collectionID,
		NewCompletionOffset: newCompletionOffset,
	}

	resp, err := coordinator.TryFinishAsyncAttachedFunctionInvocation(ctx, req)

	// Verify needs repair response
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.NotNil(t, resp.Result)

	repairResp, ok := resp.Result.(*coordinatorpb.TryFinishAsyncAttachedFunctionInvocationResponse_NeedsRepair)
	assert.True(t, ok, "Expected NeedsRepair response, got %T", resp.Result)
	if ok {
		assert.Equal(t, uint64(witnessedLogOffset), repairResp.NeedsRepair.CurrentCollectionLogOffset)
	}

	// Step 2: Finalize repair
	mockAttachedFunctionDb.On("UpdateHeapEntryPending", attachedFunctionID, false).Return(nil)

	finishReq := &coordinatorpb.FinalizeAsyncAttachedFunctionRepairRequest{
		AttachedFunctionId: attachedFunctionID.String(),
	}

	finishResp, err := coordinator.FinalizeAsyncAttachedFunctionRepair(ctx, finishReq)
	assert.NoError(t, err)
	assert.NotNil(t, finishResp)

	// Step 3: Try again with updated log position - should succeed
	collection.Collection.LogPosition = int64(newCompletionOffset)

	// Now should update - heap_entry_pending is computed atomically within the update
	mockAttachedFunctionDb.On("UpdateCompletionOffsetAndHeapEntry", attachedFunctionID, collectionID, int64(newCompletionOffset)).
		Return(nil)

	resp2, err := coordinator.TryFinishAsyncAttachedFunctionInvocation(ctx, req)

	// Verify success response
	assert.NoError(t, err)
	assert.NotNil(t, resp2)
	assert.NotNil(t, resp2.Result)

	successResp, ok := resp2.Result.(*coordinatorpb.TryFinishAsyncAttachedFunctionInvocationResponse_Success)
	assert.True(t, ok, "Expected Success response, got %T", resp2.Result)
	if ok {
		assert.Equal(t, newCompletionOffset, successResp.Success.UpdatedCompletionOffset)
	}
}

// Test no repair needed case
func TestAsyncFunctionNoRepairSimple(t *testing.T) {
	zap.ReplaceGlobals(zap.Must(zap.NewDevelopment()))

	ctx := context.Background()
	attachedFunctionID := uuid.New()
	functionID := uuid.New()
	collectionID := uuid.New().String()
	newCompletionOffset := uint64(100)
	witnessedLogOffset := int64(80) // Lower than new offset - no repair needed

	attachedFunction := &dbmodel.AttachedFunction{
		ID:                attachedFunctionID,
		FunctionID:        functionID,
		InputCollectionID: collectionID,
		CompletionOffset:  50,
	}

	function := &dbmodel.Function{
		ID:      functionID,
		IsAsync: true,
	}

	collection := &dbmodel.CollectionAndMetadata{
		Collection: &dbmodel.Collection{
			ID:          collectionID,
			LogPosition: witnessedLogOffset,
		},
	}

	// Setup mocks
	mockTxImpl := &dbmodel_mocks.ITransaction{}
	mockMetaDomain := &dbmodel_mocks.IMetaDomain{}
	mockAttachedFunctionDb := &dbmodel_mocks.IAttachedFunctionDb{}
	mockCollectionDb := &dbmodel_mocks.ICollectionDb{}
	mockFunctionDb := &dbmodel_mocks.IFunctionDb{}

	coordinator := &Coordinator{
		ctx: ctx,
		catalog: Catalog{
			txImpl:     mockTxImpl,
			metaDomain: mockMetaDomain,
		},
	}

	// Transaction executes the function immediately
	mockTxImpl.On("Transaction", mock.Anything, mock.AnythingOfType("func(context.Context) error")).
		Return(func(ctx context.Context, fn func(context.Context) error) error {
			return fn(ctx)
		})

	// Mock all the DB calls
	mockMetaDomain.On("AttachedFunctionDb", mock.Anything).Return(mockAttachedFunctionDb)
	mockAttachedFunctionDb.On("GetAttachedFunctions", &attachedFunctionID, (*string)(nil), (*string)(nil), true).
		Return([]*dbmodel.AttachedFunction{attachedFunction}, nil)

	mockMetaDomain.On("FunctionDb", mock.Anything).Return(mockFunctionDb)
	mockFunctionDb.On("GetByID", functionID).Return(function, nil)

	mockMetaDomain.On("CollectionDb", mock.Anything).Return(mockCollectionDb)
	mockCollectionDb.On("GetCollections", []string{collectionID}, (*string)(nil), "", "", (*int32)(nil), (*int32)(nil), false).
		Return([]*dbmodel.CollectionAndMetadata{collection}, nil)

	// Should update - heap_entry_pending is computed atomically within the update
	mockAttachedFunctionDb.On("UpdateCompletionOffsetAndHeapEntry", attachedFunctionID, collectionID, int64(newCompletionOffset)).
		Return(nil)

	req := &coordinatorpb.TryFinishAsyncAttachedFunctionInvocationRequest{
		AttachedFunctionId:  attachedFunctionID.String(),
		CollectionId:        collectionID,
		NewCompletionOffset: newCompletionOffset,
	}

	resp, err := coordinator.TryFinishAsyncAttachedFunctionInvocation(ctx, req)

	// Verify success response
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.NotNil(t, resp.Result)

	successResp, ok := resp.Result.(*coordinatorpb.TryFinishAsyncAttachedFunctionInvocationResponse_Success)
	assert.True(t, ok, "Expected Success response, got %T", resp.Result)
	if ok {
		assert.Equal(t, newCompletionOffset, successResp.Success.UpdatedCompletionOffset)
	}
}

// Test idempotency of TryFinishAsyncAttachedFunctionInvocation
func TestAsyncFunctionTryFinishIdempotent(t *testing.T) {
	zap.ReplaceGlobals(zap.Must(zap.NewDevelopment()))

	ctx := context.Background()
	attachedFunctionID := uuid.New()
	functionID := uuid.New()
	collectionID := uuid.New().String()
	newCompletionOffset := uint64(50)
	witnessedLogOffset := int64(100) // Higher than new offset - needs repair

	attachedFunction := &dbmodel.AttachedFunction{
		ID:                attachedFunctionID,
		FunctionID:        functionID,
		InputCollectionID: collectionID,
		CompletionOffset:  40,
	}

	function := &dbmodel.Function{
		ID:      functionID,
		IsAsync: true,
	}

	collection := &dbmodel.CollectionAndMetadata{
		Collection: &dbmodel.Collection{
			ID:          collectionID,
			LogPosition: witnessedLogOffset,
		},
	}

	// Setup mocks
	mockTxImpl := &dbmodel_mocks.ITransaction{}
	mockMetaDomain := &dbmodel_mocks.IMetaDomain{}
	mockAttachedFunctionDb := &dbmodel_mocks.IAttachedFunctionDb{}
	mockCollectionDb := &dbmodel_mocks.ICollectionDb{}
	mockFunctionDb := &dbmodel_mocks.IFunctionDb{}

	coordinator := &Coordinator{
		ctx: ctx,
		catalog: Catalog{
			txImpl:     mockTxImpl,
			metaDomain: mockMetaDomain,
		},
	}

	// Transaction executes the function immediately
	mockTxImpl.On("Transaction", mock.Anything, mock.AnythingOfType("func(context.Context) error")).
		Return(func(ctx context.Context, fn func(context.Context) error) error {
			return fn(ctx)
		})

	// Mock all the DB calls
	mockMetaDomain.On("AttachedFunctionDb", mock.Anything).Return(mockAttachedFunctionDb)
	mockAttachedFunctionDb.On("GetAttachedFunctions", &attachedFunctionID, (*string)(nil), (*string)(nil), true).
		Return([]*dbmodel.AttachedFunction{attachedFunction}, nil)

	mockMetaDomain.On("FunctionDb", mock.Anything).Return(mockFunctionDb)
	mockFunctionDb.On("GetByID", functionID).Return(function, nil)

	mockMetaDomain.On("CollectionDb", mock.Anything).Return(mockCollectionDb)
	mockCollectionDb.On("GetCollections", []string{collectionID}, (*string)(nil), "", "", (*int32)(nil), (*int32)(nil), false).
		Return([]*dbmodel.CollectionAndMetadata{collection}, nil)

	// Should update with heap_entry_pending=true since repair is needed
	// The operation is idempotent (same final state) but performs the update each time
	mockAttachedFunctionDb.On("UpdateCompletionOffsetAndHeapEntry", attachedFunctionID, collectionID, int64(newCompletionOffset)).
		Return(nil).Times(3)

	req := &coordinatorpb.TryFinishAsyncAttachedFunctionInvocationRequest{
		AttachedFunctionId:  attachedFunctionID.String(),
		CollectionId:        collectionID,
		NewCompletionOffset: newCompletionOffset,
	}

	// Call the endpoint 3 times - should get same result each time
	for i := 0; i < 3; i++ {
		resp, err := coordinator.TryFinishAsyncAttachedFunctionInvocation(ctx, req)

		// Verify same response each time
		assert.NoError(t, err)
		assert.NotNil(t, resp)
		assert.NotNil(t, resp.Result)

		repairResp, ok := resp.Result.(*coordinatorpb.TryFinishAsyncAttachedFunctionInvocationResponse_NeedsRepair)
		assert.True(t, ok, "Expected NeedsRepair response on call %d", i+1)
		if ok {
			assert.Equal(t, uint64(witnessedLogOffset), repairResp.NeedsRepair.CurrentCollectionLogOffset)
		}
	}

	// Verify UpdateCompletionOffsetAndHeapEntry was called 3 times (idempotent in result, not in execution)
	mockAttachedFunctionDb.AssertNumberOfCalls(t, "UpdateCompletionOffsetAndHeapEntry", 3)
}

// Test idempotency of FinalizeAsyncAttachedFunctionRepair
func TestAsyncFunctionFinalizeRepairIdempotent(t *testing.T) {
	zap.ReplaceGlobals(zap.Must(zap.NewDevelopment()))

	ctx := context.Background()
	attachedFunctionID := uuid.New()

	// Setup mocks
	mockTxImpl := &dbmodel_mocks.ITransaction{}
	mockMetaDomain := &dbmodel_mocks.IMetaDomain{}
	mockAttachedFunctionDb := &dbmodel_mocks.IAttachedFunctionDb{}

	coordinator := &Coordinator{
		ctx: ctx,
		catalog: Catalog{
			txImpl:     mockTxImpl,
			metaDomain: mockMetaDomain,
		},
	}

	mockMetaDomain.On("AttachedFunctionDb", mock.Anything).Return(mockAttachedFunctionDb)

	// The operation is idempotent (same final state) but performs the update each time
	mockAttachedFunctionDb.On("UpdateHeapEntryPending", attachedFunctionID, false).Return(nil).Times(3)

	req := &coordinatorpb.FinalizeAsyncAttachedFunctionRepairRequest{
		AttachedFunctionId: attachedFunctionID.String(),
	}

	// Call the endpoint 3 times - should succeed each time
	for i := 0; i < 3; i++ {
		resp, err := coordinator.FinalizeAsyncAttachedFunctionRepair(ctx, req)
		assert.NoError(t, err, "Call %d should succeed", i+1)
		assert.NotNil(t, resp, "Call %d should return response", i+1)
	}

	// Verify UpdateHeapEntryPending was called 3 times (idempotent in result, not in execution)
	mockAttachedFunctionDb.AssertNumberOfCalls(t, "UpdateHeapEntryPending", 3)
}

// Test that completion offset can only move forward
func TestAsyncFunctionOffsetOnlyMovesForward(t *testing.T) {
	zap.ReplaceGlobals(zap.Must(zap.NewDevelopment()))

	ctx := context.Background()
	attachedFunctionID := uuid.New()
	functionID := uuid.New()
	collectionID := uuid.New().String()
	currentOffset := int64(100)

	// Test case 1: Try to move offset backwards (should fail)
	backwardOffset := uint64(50)
	witnessedLogOffset := int64(40) // No repair needed

	attachedFunction := &dbmodel.AttachedFunction{
		ID:                attachedFunctionID,
		FunctionID:        functionID,
		InputCollectionID: collectionID,
		CompletionOffset:  currentOffset, // Current offset is 100
	}

	function := &dbmodel.Function{
		ID:      functionID,
		IsAsync: true,
	}

	collection := &dbmodel.CollectionAndMetadata{
		Collection: &dbmodel.Collection{
			ID:          collectionID,
			LogPosition: witnessedLogOffset,
		},
	}

	// Setup mocks
	mockTxImpl := &dbmodel_mocks.ITransaction{}
	mockMetaDomain := &dbmodel_mocks.IMetaDomain{}
	mockAttachedFunctionDb := &dbmodel_mocks.IAttachedFunctionDb{}
	mockCollectionDb := &dbmodel_mocks.ICollectionDb{}
	mockFunctionDb := &dbmodel_mocks.IFunctionDb{}

	coordinator := &Coordinator{
		ctx: ctx,
		catalog: Catalog{
			txImpl:     mockTxImpl,
			metaDomain: mockMetaDomain,
		},
	}

	// Transaction executes the function immediately
	mockTxImpl.On("Transaction", mock.Anything, mock.AnythingOfType("func(context.Context) error")).
		Return(func(ctx context.Context, fn func(context.Context) error) error {
			return fn(ctx)
		})

	// Mock all the DB calls
	mockMetaDomain.On("AttachedFunctionDb", mock.Anything).Return(mockAttachedFunctionDb)
	mockAttachedFunctionDb.On("GetAttachedFunctions", &attachedFunctionID, (*string)(nil), (*string)(nil), true).
		Return([]*dbmodel.AttachedFunction{attachedFunction}, nil)

	mockMetaDomain.On("FunctionDb", mock.Anything).Return(mockFunctionDb)
	mockFunctionDb.On("GetByID", functionID).Return(function, nil)

	mockMetaDomain.On("CollectionDb", mock.Anything).Return(mockCollectionDb)
	mockCollectionDb.On("GetCollections", []string{collectionID}, (*string)(nil), "", "", (*int32)(nil), (*int32)(nil), false).
		Return([]*dbmodel.CollectionAndMetadata{collection}, nil)

	// The update should be called but due to WHERE clause protection, it won't actually update
	// We simulate this by returning ErrAttachedFunctionOffsetWouldRegress (no rows affected)
	mockAttachedFunctionDb.On("UpdateCompletionOffsetAndHeapEntry", attachedFunctionID, collectionID, int64(backwardOffset)).
		Return(common.ErrAttachedFunctionOffsetWouldRegress)

	req := &coordinatorpb.TryFinishAsyncAttachedFunctionInvocationRequest{
		AttachedFunctionId:  attachedFunctionID.String(),
		CollectionId:        collectionID,
		NewCompletionOffset: backwardOffset,
	}

	// Should fail because offset would move backwards
	_, err := coordinator.TryFinishAsyncAttachedFunctionInvocation(ctx, req)
	assert.Error(t, err)
	// The error happens inside the transaction and gets wrapped, so we just check it's an error

	// Test case 2: Move offset forward (should succeed)
	forwardOffset := uint64(150)

	// Update mock for forward movement
	mockAttachedFunctionDb.On("UpdateCompletionOffsetAndHeapEntry", attachedFunctionID, collectionID, int64(forwardOffset)).
		Return(nil).Once()

	req2 := &coordinatorpb.TryFinishAsyncAttachedFunctionInvocationRequest{
		AttachedFunctionId:  attachedFunctionID.String(),
		CollectionId:        collectionID,
		NewCompletionOffset: forwardOffset,
	}

	resp, err := coordinator.TryFinishAsyncAttachedFunctionInvocation(ctx, req2)
	assert.NoError(t, err)
	assert.NotNil(t, resp)

	successResp, ok := resp.Result.(*coordinatorpb.TryFinishAsyncAttachedFunctionInvocationResponse_Success)
	assert.True(t, ok, "Expected Success response")
	if ok {
		assert.Equal(t, forwardOffset, successResp.Success.UpdatedCompletionOffset)
	}
}
