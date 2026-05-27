package coordinator

import (
	"testing"

	"github.com/chroma-core/chroma/go/pkg/sysdb/metastore/db/dbmodel"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func testAttachedFunctionEdge(inputCollectionID string, outputCollectionID *string) *dbmodel.AttachedFunction {
	return &dbmodel.AttachedFunction{
		InputCollectionID:  inputCollectionID,
		OutputCollectionID: outputCollectionID,
	}
}

func TestAttachedFunctionGraphStateCollectionIDs(t *testing.T) {
	t.Parallel()

	outputA := "output-a"
	outputB := "output-b"

	graphState := &attachedFunctionGraphState{
		coordinator: &Coordinator{},
		upstreamFunctions: map[string][]*dbmodel.AttachedFunction{
			"sink": {
				testAttachedFunctionEdge("input-a", &outputA),
				testAttachedFunctionEdge("input-b", &outputB),
			},
		},
		downstreamFunctions: map[string][]*dbmodel.AttachedFunction{
			"source": {
				testAttachedFunctionEdge("source", &outputA),
				testAttachedFunctionEdge("source", &outputB),
			},
		},
	}

	incoming, err := graphState.incomingCollectionIDs("sink")
	require.NoError(t, err)
	require.Equal(t, []string{"input-a", "input-b"}, incoming)

	outgoing, err := graphState.outgoingCollectionIDs("source")
	require.NoError(t, err)
	require.Equal(t, []string{"output-a", "output-b"}, outgoing)
}

func TestAttachedFunctionGraphStateDepthAndReachability(t *testing.T) {
	t.Parallel()

	collectionB := "collection-b"
	collectionC := "collection-c"
	collectionD := "collection-d"

	graphState := &attachedFunctionGraphState{
		coordinator: &Coordinator{},
		upstreamFunctions: map[string][]*dbmodel.AttachedFunction{
			collectionB: {
				testAttachedFunctionEdge("collection-a", &collectionB),
			},
			collectionC: {
				testAttachedFunctionEdge(collectionB, &collectionC),
			},
			collectionD: {
				testAttachedFunctionEdge("collection-a", &collectionD),
			},
			"collection-a": {},
		},
		downstreamFunctions: map[string][]*dbmodel.AttachedFunction{
			"collection-a": {
				testAttachedFunctionEdge("collection-a", &collectionB),
				testAttachedFunctionEdge("collection-a", &collectionD),
			},
			collectionB: {
				testAttachedFunctionEdge(collectionB, &collectionC),
			},
			collectionC: {},
			collectionD: {},
		},
	}

	depth, err := graphState.collectionDepth(collectionC, map[string]int{}, map[string]struct{}{})
	require.NoError(t, err)
	require.Equal(t, 2, depth)

	tailDepth, err := graphState.collectionTailDepth("collection-a", map[string]int{}, map[string]struct{}{})
	require.NoError(t, err)
	require.Equal(t, 2, tailDepth)

	reaches, err := graphState.reaches("collection-a", collectionC)
	require.NoError(t, err)
	require.True(t, reaches)

	reaches, err = graphState.reaches(collectionB, collectionD)
	require.NoError(t, err)
	require.False(t, reaches)
}

func TestAttachedFunctionGraphStateDetectsCycles(t *testing.T) {
	t.Parallel()

	collectionA := "collection-a"
	collectionB := "collection-b"

	graphState := &attachedFunctionGraphState{
		coordinator: &Coordinator{},
		upstreamFunctions: map[string][]*dbmodel.AttachedFunction{
			collectionA: {
				testAttachedFunctionEdge(collectionB, &collectionA),
			},
			collectionB: {
				testAttachedFunctionEdge(collectionA, &collectionB),
			},
		},
		downstreamFunctions: map[string][]*dbmodel.AttachedFunction{
			collectionA: {
				testAttachedFunctionEdge(collectionA, &collectionB),
			},
			collectionB: {
				testAttachedFunctionEdge(collectionB, &collectionA),
			},
		},
	}

	_, err := graphState.collectionDepth(collectionA, map[string]int{}, map[string]struct{}{})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Contains(t, err.Error(), "attached function cycle detected while computing depth")

	_, err = graphState.collectionTailDepth(collectionA, map[string]int{}, map[string]struct{}{})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Contains(t, err.Error(), "attached function cycle detected while computing downstream depth")
}
