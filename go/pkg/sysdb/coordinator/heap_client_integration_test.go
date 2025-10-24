package coordinator

import (
	"context"
	"testing"
	"time"

	"github.com/chroma-core/chroma/go/pkg/proto/coordinatorpb"
	"github.com/chroma-core/chroma/go/pkg/types"
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
	tenantName   string
	databaseName string
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
	response, err := suite.coordinator.AttachFunction(ctx, &coordinatorpb.AttachFunctionRequest{
		InputCollectionId:    collectionID.String(),
		TenantId:             suite.tenantName,
		Database:             suite.databaseName,
		Name:                 "test_record_counter_function",
		FunctionName:         "record_counter",
		OutputCollectionName: "output_collection_" + collectionID,
		MinRecordsForRun:     10,
	})
	suite.NoError(err, "Should attached function successfully")
	suite.NotNil(response)
	suite.NotEmpty(response.AttachedFunctionId, "Attached function ID should be returned")

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

func TestHeapClientIntegrationSuite(t *testing.T) {
	suite.Run(t, new(HeapClientIntegrationTestSuite))
}
