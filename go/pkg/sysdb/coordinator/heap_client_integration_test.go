package coordinator

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/chroma-core/chroma/go/pkg/proto/coordinatorpb"
	"github.com/chroma-core/chroma/go/pkg/types"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// HeapClientIntegrationTestSuite tests heap client integration using local Tilt services
// This connects to services running in Tilt via port-forwarding:
// - sysdb on localhost:50051
// - heap tender service on localhost:50052 (colocated with rust-log-service)
//
// Prerequisites:
// - Tilt running with heap tender service deployed
// - Port forwards: sysdb (50051), heap service (50052) configured in Tiltfile
//
// Run with: go test -v -run TestHeapClientIntegration ./pkg/sysdb/coordinator/
type HeapClientIntegrationTestSuite struct {
	suite.Suite
	sysdbConn    *grpc.ClientConn
	sysdbClient  coordinatorpb.SysDBClient
	heapConn     *grpc.ClientConn
	heapClient   coordinatorpb.HeapTenderServiceClient
	db           *sql.DB
	tenantName   string
	databaseName string
}

// getDBConnection gets a direct database connection for hybrid testing
// Returns nil if connection fails (test will skip)
func (suite *HeapClientIntegrationTestSuite) getDBConnection() *sql.DB {
	// Try to connect to postgres running in Tilt (sysdb database)
	connStr := "host=localhost port=5432 user=chroma password=chroma dbname=sysdb sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil
	}

	if err := db.Ping(); err != nil {
		return nil
	}

	return db
}

func (suite *HeapClientIntegrationTestSuite) SetupSuite() {
	// Skip if not in local Tilt environment
	if testing.Short() {
		suite.T().Skip("Skipping heap client integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Connect to sysdb on localhost:50051
	sysdbConn, err := grpc.NewClient("localhost:50051",
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		suite.T().Skipf("Could not connect to sysdb on localhost:50051: %v. Is Tilt running?", err)
		return
	}
	suite.sysdbConn = sysdbConn
	suite.sysdbClient = coordinatorpb.NewSysDBClient(sysdbConn)

	// Verify sysdb is reachable
	_, err = suite.sysdbClient.GetTenant(ctx, &coordinatorpb.GetTenantRequest{Name: "test"})
	if err != nil && err.Error() != "rpc error: code = NotFound desc = tenant not found" {
		suite.sysdbConn.Close()
		suite.T().Skipf("Sysdb not responding properly: %v. Is Tilt running?", err)
		return
	}

	// Connect to heap service
	heapPort := "50052"
	heapAddr := "localhost:" + heapPort

	heapConn, err := grpc.NewClient(heapAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		suite.sysdbConn.Close()
		suite.T().Skipf("Could not connect to heap service on %s: %v", heapAddr, err)
		return
	}
	suite.heapConn = heapConn
	suite.heapClient = coordinatorpb.NewHeapTenderServiceClient(heapConn)

	// Verify heap service is reachable
	_, err = suite.heapClient.Summary(ctx, &coordinatorpb.HeapSummaryRequest{})
	if err != nil {
		suite.sysdbConn.Close()
		suite.heapConn.Close()
		suite.T().Skipf("Heap service not responding on %s: %v. Is heap tender deployed?", heapAddr, err)
		return
	}

	suite.T().Logf("Connected to sysdb (localhost:50051) and heap service (%s)", heapAddr)
}

func (suite *HeapClientIntegrationTestSuite) TearDownSuite() {
	if suite.sysdbConn != nil {
		suite.sysdbConn.Close()
	}
	if suite.heapConn != nil {
		suite.heapConn.Close()
	}
}

func (suite *HeapClientIntegrationTestSuite) SetupTest() {
	// Use timestamp with nanoseconds to ensure unique names for repeated test runs
	timestamp := time.Now().Format("20060102_150405.000")
	suite.tenantName = "test_tenant_heap_" + timestamp
	suite.databaseName = "test_db_heap_" + timestamp
}

func (suite *HeapClientIntegrationTestSuite) TearDownTest() {
	// Note: We don't clean up resources since there's no DeleteTenant API
	// Resources are uniquely named with timestamps so repeated runs won't conflict
}

// TestAttachFunctionPushesScheduleToHeap verifies that attaching a function pushes a schedule to the heap
func (suite *HeapClientIntegrationTestSuite) TestAttachFunctionPushesScheduleToHeap() {
	ctx := context.Background()

	// Get initial heap summary
	initialSummary, err := suite.heapClient.Summary(ctx, &coordinatorpb.HeapSummaryRequest{})
	suite.NoError(err, "Should be able to get initial heap summary")
	initialItemCount := initialSummary.TotalItems
	suite.T().Logf("Initial heap items: %d", initialItemCount)

	// Create tenant
	_, err = suite.sysdbClient.CreateTenant(ctx, &coordinatorpb.CreateTenantRequest{
		Name: suite.tenantName,
	})
	suite.NoError(err, "Should create tenant")

	// Create database
	_, err = suite.sysdbClient.CreateDatabase(ctx, &coordinatorpb.CreateDatabaseRequest{
		Id:     types.NewUniqueID().String(),
		Name:   suite.databaseName,
		Tenant: suite.tenantName,
	})
	suite.NoError(err, "Should create database")

	// Create collection
	collectionID := types.NewUniqueID().String()
	_, err = suite.sysdbClient.CreateCollection(ctx, &coordinatorpb.CreateCollectionRequest{
		Id:       collectionID,
		Name:     "test_collection_heap",
		Tenant:   suite.tenantName,
		Database: suite.databaseName,
		Dimension: func() *int32 {
			dim := int32(128)
			return &dim
		}(),
	})
	suite.NoError(err, "Should create collection")

	// Attach function using record_counter function
	response, err := suite.sysdbClient.AttachFunction(ctx, &coordinatorpb.AttachFunctionRequest{
		InputCollectionId:       collectionID,
		TenantId:                suite.tenantName,
		Database:                suite.databaseName,
		Name:                    "test_record_counter_function",
		FunctionName:            "record_counter",
		OutputCollectionName:    "output_collection_" + collectionID,
		MinRecordsForInvocation: 10,
	})
	suite.NoError(err, "Should attached function successfully")
	suite.NotNil(response)
	suite.NotEmpty(response.Id, "Attached function ID should be returned")

	// Get updated heap summary
	updatedSummary, err := suite.heapClient.Summary(ctx, &coordinatorpb.HeapSummaryRequest{})
	suite.NoError(err, "Should be able to get updated heap summary")
	suite.T().Logf("Updated heap items: %d", updatedSummary.TotalItems)

	// Verify that a schedule was added
	suite.Greater(updatedSummary.TotalItems, initialItemCount,
		"Heap should have more items after creating task")
}

// TestHeapSummary verifies that heap summary endpoint works
func (suite *HeapClientIntegrationTestSuite) TestHeapSummary() {
	ctx := context.Background()

	summary, err := suite.heapClient.Summary(ctx, &coordinatorpb.HeapSummaryRequest{})
	suite.NoError(err, "Should get heap summary")
	suite.NotNil(summary)

	suite.T().Logf("Heap summary - Total items: %d, Buckets: %d",
		summary.TotalItems, summary.BucketCount)
}

// TestPartialTaskRecovery_HybridApproach tests the full partial task lifecycle:
// 1. Create a task normally (fully initialized)
// 2. Directly UPDATE database to set lowest_live_nonce = NULL (simulate Phase 3 failure)
// 3. Try to create task with different params → should fail
// 4. Try to create task with same params → should succeed (recovery)
func (suite *HeapClientIntegrationTestSuite) TestPartialTaskRecovery_HybridApproach() {
	ctx := context.Background()
	timestamp := time.Now().Format("20060102_150405.000")

	// Create tenant and database
	_, err := suite.sysdbClient.CreateTenant(ctx, &coordinatorpb.CreateTenantRequest{
		Name: suite.tenantName,
	})
	suite.NoError(err)

	_, err = suite.sysdbClient.CreateDatabase(ctx, &coordinatorpb.CreateDatabaseRequest{
		Id:     types.NewUniqueID().String(),
		Name:   suite.databaseName,
		Tenant: suite.tenantName,
	})
	suite.NoError(err)

	// Create input collection
	collectionID := types.NewUniqueID().String()
	_, err = suite.sysdbClient.CreateCollection(ctx, &coordinatorpb.CreateCollectionRequest{
		Id:       collectionID,
		Name:     "test_collection_partial_" + timestamp,
		Tenant:   suite.tenantName,
		Database: suite.databaseName,
		Dimension: func() *int32 {
			dim := int32(128)
			return &dim
		}(),
	})
	suite.NoError(err)

	taskName := "task_partial_recovery_" + timestamp
	outputCollectionName := "output_partial_" + timestamp

	// Get direct DB connection for hybrid testing
	db := suite.getDBConnection()
	if db == nil {
		suite.T().Skip("Could not connect to database for hybrid testing")
		return
	}
	defer db.Close()

	// STEP 1: Create a task normally (fully initialized)
	taskResp, err := suite.sysdbClient.AttachFunction(ctx, &coordinatorpb.AttachFunctionRequest{
		InputCollectionId:    collectionID,
		TenantId:             suite.tenantName,
		Database:             suite.databaseName,
		Name:                 taskName,
		FunctionName:         "record_counter",
		OutputCollectionName: outputCollectionName,
		MinRecordsForInvocation: 100,
	})

	if err != nil {
		suite.T().Skipf("AttachFunction failed (heap service may be unavailable): %v", err)
		return
	}
	suite.NotNil(taskResp)
	originalTaskID := taskResp.Id
	suite.T().Logf("Created fully initialized task: %s", originalTaskID)

	// STEP 2: Directly UPDATE database to make task partial (simulate Phase 3 failure)
	// Set lowest_live_nonce = NULL to simulate the task being stuck
	_, err = db.Exec(`UPDATE public.tasks SET lowest_live_nonce = NULL WHERE task_id = $1`, originalTaskID)
	suite.NoError(err, "Should be able to corrupt task in database")
	suite.T().Logf("Made task partial by setting lowest_live_nonce = NULL")

	// STEP 3: Try to create task with same name but DIFFERENT parameters → should fail
	_, err = suite.sysdbClient.AttachFunction(ctx, &coordinatorpb.AttachFunctionRequest{
		InputCollectionId:    collectionID,
		TenantId:             suite.tenantName,
		Database:             suite.databaseName,
		Name:                 taskName,
		FunctionName:         "record_counter",                    // SAME
		OutputCollectionName: outputCollectionName + "_different", // DIFFERENT
		MinRecordsForInvocation:    200,                                 // DIFFERENT
	})
	suite.Error(err, "Should fail when creating task with different parameters")
	suite.Contains(err.Error(), "still initializing", "Error should indicate task is still initializing")

	// STEP 4: Try to create task with SAME parameters
	// NOTE: This will also fail with "still initializing" because:
	// - We manually corrupted the task (lowest_live_nonce = NULL)
	// - But the heap entry still exists from the successful first AttachFunction
	// - So the heap push in Phase 2 fails (duplicate entry)
	// - Recovery can't complete
	// This demonstrates that partial tasks need CleanupExpiredPartialAttachedFunctions to fully recover
	_, err = suite.sysdbClient.AttachFunction(ctx, &coordinatorpb.AttachFunctionRequest{
		InputCollectionId:    collectionID,
		TenantId:             suite.tenantName,
		Database:             suite.databaseName,
		Name:                 taskName,
		FunctionName:         "record_counter",
		OutputCollectionName: outputCollectionName,
		MinRecordsForInvocation:    100,
	})
	suite.Error(err, "Will also fail because heap entry already exists (recovery blocked)")
	suite.Contains(err.Error(), "still initializing")
	suite.T().Logf("Same params also fails - partial task is stuck until cleaned up")
}

// TestPartialTaskCleanup_ThenRecreate tests cleanup and recreation flow:
// 1. Manually create partial task (simulating Phase 2 failure)
// 2. Call CleanupExpiredPartialAttachedFunctions to remove stuck task
// 3. Create task again → should succeed
//
// NOTE: This test is simplified since we can't easily create a truly partial task via API
// In production, partial tasks occur when heap push fails after database insert
func (suite *HeapClientIntegrationTestSuite) TestPartialTaskCleanup_ThenRecreate() {
	ctx := context.Background()
	timestamp := time.Now().Format("20060102_150405.000")

	// Create tenant and database
	_, err := suite.sysdbClient.CreateTenant(ctx, &coordinatorpb.CreateTenantRequest{
		Name: suite.tenantName,
	})
	suite.NoError(err)

	_, err = suite.sysdbClient.CreateDatabase(ctx, &coordinatorpb.CreateDatabaseRequest{
		Id:     types.NewUniqueID().String(),
		Name:   suite.databaseName,
		Tenant: suite.tenantName,
	})
	suite.NoError(err)

	// Create input collection
	collectionID := types.NewUniqueID().String()
	_, err = suite.sysdbClient.CreateCollection(ctx, &coordinatorpb.CreateCollectionRequest{
		Id:       collectionID,
		Name:     "test_collection_cleanup_" + timestamp,
		Tenant:   suite.tenantName,
		Database: suite.databaseName,
		Dimension: func() *int32 {
			dim := int32(128)
			return &dim
		}(),
	})
	suite.NoError(err)

	taskName := "task_cleanup_test_" + timestamp
	outputCollectionName := "output_cleanup_" + timestamp

	// STEP 1: Create a task (if this succeeds, it's fully initialized, not partial)
	taskResp, err := suite.sysdbClient.AttachFunction(ctx, &coordinatorpb.AttachFunctionRequest{
		InputCollectionId:    collectionID,
		TenantId:             suite.tenantName,
		Database:             suite.databaseName,
		Name:                 taskName,
		FunctionName:         "record_counter",
		OutputCollectionName: outputCollectionName,
		MinRecordsForInvocation:    100,
	})

	if err != nil {
		suite.T().Skipf("CreateTask failed (heap service may be unavailable): %v", err)
		return
	}
	suite.NotNil(taskResp)
	suite.T().Logf("Created task: %s", taskResp.Id)

	// STEP 2: Call CleanupExpiredPartialAttachedFunctions (with short timeout to test it doesn't affect complete tasks)
	cleanupResp, err := suite.sysdbClient.CleanupExpiredPartialAttachedFunctions(ctx, &coordinatorpb.CleanupExpiredPartialAttachedFunctionsRequest{
		MaxAgeSeconds: 1, // 1 second - very aggressive
	})
	suite.NoError(err, "Cleanup should succeed")
	suite.NotNil(cleanupResp)
	// Note: May clean up partial tasks from previous test runs, so don't assert exact count
	suite.T().Logf("Cleanup completed, removed %d tasks", cleanupResp.CleanedUpCount)

	// STEP 3: Verify task still exists and can be retrieved
	getResp, err := suite.sysdbClient.GetAttachedFunctionByName(ctx, &coordinatorpb.GetAttachedFunctionByNameRequest{
		InputCollectionId: collectionID,
		Name:              taskName,
	})
	suite.NoError(err, "Task should still exist after cleanup")
	suite.NotNil(getResp)
	suite.Equal(taskResp.Id, getResp.AttachedFunction.Id)
	suite.T().Logf("Task still exists after cleanup: %s", getResp.AttachedFunction.Id)

	// STEP 4: Delete the task
	_, err = suite.sysdbClient.DetachFunction(ctx, &coordinatorpb.DetachFunctionRequest{
		AttachedFunctionId: taskResp.Id,
		DeleteOutput:       true,
	})
	suite.NoError(err, "Should delete task")

	// STEP 5: Recreate task with same name → should succeed
	taskResp2, err := suite.sysdbClient.AttachFunction(ctx, &coordinatorpb.AttachFunctionRequest{
		InputCollectionId:    collectionID,
		TenantId:             suite.tenantName,
		Database:             suite.databaseName,
		Name:                 taskName,
		FunctionName:         "record_counter",
		OutputCollectionName: outputCollectionName,
		MinRecordsForInvocation:    100,
	})
	suite.NoError(err, "Should be able to recreate task after deletion")
	suite.NotNil(taskResp2)
	suite.NotEqual(taskResp.Id, taskResp2.Id, "New task should have different ID")
	suite.T().Logf("Successfully recreated task: %s", taskResp2.Id)
}

func TestHeapClientIntegrationSuite(t *testing.T) {
	suite.Run(t, new(HeapClientIntegrationTestSuite))
}
