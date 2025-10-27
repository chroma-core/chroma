package coordinator

import (
	"context"
	"fmt"

	"github.com/chroma-core/chroma/go/pkg/memberlist_manager"
	"github.com/chroma-core/chroma/go/pkg/proto/coordinatorpb"
	"github.com/chroma-core/chroma/go/pkg/utils"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// HeapClient is the interface for pushing schedules to the heap service
type HeapClient interface {
	Push(ctx context.Context, collectionID string, schedules []*coordinatorpb.Schedule) error
	Summary(ctx context.Context) (*coordinatorpb.HeapSummaryResponse, error)
	Close() error
}

// grpcHeapClient implements HeapClient using gRPC with memberlist-based discovery
type grpcHeapClient struct {
	memberlistStore memberlist_manager.IMemberlistStore
	port            int    // 50052
	hasher          Hasher // Assignment policy hasher (e.g., Murmur3)
}

// Hasher is a function type for hashing keys to members
type Hasher func(member string, key string) uint64

// NewGrpcHeapClient creates a new heap service client that uses a memberlist store for service discovery
func NewGrpcHeapClient(memberlistStore memberlist_manager.IMemberlistStore, port int, hasher Hasher) HeapClient {
	return &grpcHeapClient{
		memberlistStore: memberlistStore,
		port:            port,
		hasher:          hasher,
	}
}

// Push sends schedules to the heap service for the given collection
func (c *grpcHeapClient) Push(ctx context.Context, collectionID string, schedules []*coordinatorpb.Schedule) error {
	// 1. Read memberlist from store
	memberlist, _, err := c.memberlistStore.GetMemberlist(ctx)
	if err != nil {
		return fmt.Errorf("failed to read memberlist: %w", err)
	}

	if len(memberlist) == 0 {
		return fmt.Errorf("no heap service nodes available in memberlist")
	}

	// 2. Use rendezvous hashing to select node for collection
	nodeIP := c.selectNodeForCollection(collectionID, memberlist)
	if nodeIP == "" {
		return fmt.Errorf("failed to select node for collection %s", collectionID)
	}

	// 3. Create gRPC connection to the selected node
	target := fmt.Sprintf("%s:%d", nodeIP, c.port)
	log.Info("Connecting to heap service",
		zap.String("collection_id", collectionID),
		zap.String("target", target))

	conn, err := grpc.NewClient(target,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to heap service at %s: %w", target, err)
	}
	defer conn.Close()

	// 4. Create client and make Push call
	client := coordinatorpb.NewHeapTenderServiceClient(conn)

	request := &coordinatorpb.PushRequest{
		Schedules: schedules,
	}

	response, err := client.Push(ctx, request)
	if err != nil {
		return fmt.Errorf("heap service Push failed: %w", err)
	}

	log.Info("Successfully pushed schedules to heap service",
		zap.String("collection_id", collectionID),
		zap.Uint32("schedules_added", response.SchedulesAdded))

	return nil
}

// Summary retrieves heap statistics from any available heap service node
func (c *grpcHeapClient) Summary(ctx context.Context) (*coordinatorpb.HeapSummaryResponse, error) {
	// Read memberlist from store
	memberlist, _, err := c.memberlistStore.GetMemberlist(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to read memberlist: %w", err)
	}

	if len(memberlist) == 0 {
		return nil, fmt.Errorf("no heap service nodes available in memberlist")
	}

	// Use first available node (summary is global across all nodes)
	var nodeIP string
	for _, member := range memberlist {
		if member.GetIP() != "" {
			nodeIP = member.GetIP()
			break
		}
	}

	if nodeIP == "" {
		return nil, fmt.Errorf("no heap service nodes with valid IP address")
	}

	// Create gRPC connection
	target := fmt.Sprintf("%s:%d", nodeIP, c.port)
	conn, err := grpc.NewClient(target,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to heap service at %s: %w", target, err)
	}
	defer conn.Close()

	// Create client and make Summary call
	client := coordinatorpb.NewHeapTenderServiceClient(conn)

	request := &coordinatorpb.HeapSummaryRequest{}
	response, err := client.Summary(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("heap service Summary failed: %w", err)
	}

	log.Debug("Retrieved heap summary",
		zap.Uint32("total_items", response.TotalItems),
		zap.Uint32("bucket_count", response.BucketCount))

	return response, nil
}

// Close performs cleanup (no-op for this implementation)
func (c *grpcHeapClient) Close() error {
	return nil
}


// selectNodeForCollection uses rendezvous hashing to select a node for the collection
// This matches the exact algorithm used in Rust and other Go services (utils.Assign with Murmur3Hasher)
func (c *grpcHeapClient) selectNodeForCollection(collectionID string, memberlist memberlist_manager.Memberlist) string {
	if len(memberlist) == 0 {
		return ""
	}

	// Build list of member IPs (matching the existing utils.Assign signature)
	members := make([]string, 0, len(memberlist))
	for _, member := range memberlist {
		if member.GetIP() != "" {
			members = append(members, member.GetIP())
		}
	}

	if len(members) == 0 {
		return ""
	}

	// Use the configured rendezvous hashing algorithm
	selectedMember, err := utils.Assign(collectionID, members, c.hasher)
	if err != nil {
		log.Error("Failed to assign collection to node", zap.Error(err))
		return ""
	}

	return selectedMember
}

// GetHasherFromString returns a hasher function based on the hasher name
func GetHasherFromString(hasherName string) (Hasher, error) {
	switch hasherName {
	case "murmur3", "": // Default to murmur3
		return utils.Murmur3Hasher, nil
	default:
		return nil, fmt.Errorf("unknown hasher: %s (supported: murmur3)", hasherName)
	}
}
