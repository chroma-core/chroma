package coordinator

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/chroma-core/chroma/go/pkg/common"
	"github.com/chroma-core/chroma/go/pkg/grpcutils"
	"github.com/chroma-core/chroma/go/pkg/proto/coordinatorpb"
	"github.com/chroma-core/chroma/go/pkg/sysdb/coordinator/model"
	"github.com/chroma-core/chroma/go/pkg/sysdb/metastore/db/dbmodel"
	"github.com/chroma-core/chroma/go/pkg/types"
	"github.com/google/uuid"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

const maxAttachedFunctionDepth = 5

// validateAttachedFunctionMatchesRequest validates that an existing attached function's parameters match the request parameters.
// Returns (true, nil) if all parameters match (idempotent request).
// Returns (false, nil) if parameters don't match.
// Returns (false, err) if there's an error during validation (e.g., DB lookup failure).
func (s *Coordinator) validateAttachedFunctionMatchesRequest(ctx context.Context, attachedFunction *dbmodel.AttachedFunction, req *coordinatorpb.AttachFunctionRequest) (bool, error) {
	if attachedFunction.Name != req.Name {
		// Different attached function exists - error
		log.Error("validateAttachedFunctionMatchesRequest: collection already has an attached function with different name",
			zap.String("existing_name", attachedFunction.Name),
			zap.String("requested_name", req.Name))
		return false, nil
	}
	if attachedFunction.TenantID != req.TenantId {
		log.Error("validateAttachedFunctionMatchesRequest: attached function has different tenant")
		return false, nil
	}
	if attachedFunction.OutputCollectionName != req.OutputCollectionName {
		log.Error("validateAttachedFunctionMatchesRequest: attached function has different output collection name",
			zap.String("existing", attachedFunction.OutputCollectionName),
			zap.String("requested", req.OutputCollectionName))
		return false, nil
	}
	if attachedFunction.MinRecordsForInvocation != int64(req.MinRecordsForInvocation) {
		log.Error("validateAttachedFunctionMatchesRequest: attached function has different min_records_for_invocation",
			zap.Int64("existing", attachedFunction.MinRecordsForInvocation),
			zap.Uint64("requested", req.MinRecordsForInvocation))
		return false, nil
	}

	// Check if the function matches using the ID-to-name mapping
	existingFunctionName, err := dbmodel.GetFunctionNameByID(attachedFunction.FunctionID)
	if err != nil {
		log.Error("validateAttachedFunctionMatchesRequest: unknown function ID", zap.Error(err))
		return false, err
	}
	if existingFunctionName != req.FunctionName {
		log.Error("validateAttachedFunctionMatchesRequest: attached function has different function",
			zap.String("existing", existingFunctionName),
			zap.String("requested", req.FunctionName))
		return false, nil
	}

	// Look up database for comparison
	databases, err := s.catalog.metaDomain.DatabaseDb(ctx).GetDatabases(req.TenantId, req.Database)
	if err != nil {
		log.Error("validateAttachedFunctionMatchesRequest: failed to get database for validation", zap.Error(err))
		return false, err
	}
	if len(databases) == 0 {
		log.Error("validateAttachedFunctionMatchesRequest: database not found")
		return false, common.ErrDatabaseNotFound
	}

	if attachedFunction.DatabaseID != databases[0].ID {
		log.Error("validateAttachedFunctionMatchesRequest: attached function has different database")
		return false, nil
	}

	return true, nil
}

func (s *Coordinator) resolveAttachedFunctionOutputCollectionID(ctx context.Context, attachedFunction *dbmodel.AttachedFunction, databaseName string) (*string, error) {
	if attachedFunction.OutputCollectionID != nil {
		return attachedFunction.OutputCollectionID, nil
	}

	existingCollections, err := s.catalog.metaDomain.CollectionDb(ctx).GetCollections(nil, &attachedFunction.OutputCollectionName, attachedFunction.TenantID, databaseName, nil, nil, false)
	if err != nil {
		return nil, err
	}
	if len(existingCollections) == 0 {
		return nil, nil
	}

	outputCollectionID := existingCollections[0].Collection.ID
	return &outputCollectionID, nil
}

type attachedFunctionGraphState struct {
	coordinator  *Coordinator
	ctx          context.Context
	databaseName string
	// upstreamFunctions caches functions that flow into the to-be input collection.
	upstreamFunctions map[string][]*dbmodel.AttachedFunction
	// downstreamFunctions caches functions that fan out from the to-be output collection.
	downstreamFunctions map[string][]*dbmodel.AttachedFunction
}

func newAttachedFunctionGraphState(ctx context.Context, coordinator *Coordinator, databaseName string) *attachedFunctionGraphState {
	return &attachedFunctionGraphState{
		coordinator:         coordinator,
		ctx:                 ctx,
		databaseName:        databaseName,
		upstreamFunctions:   make(map[string][]*dbmodel.AttachedFunction),
		downstreamFunctions: make(map[string][]*dbmodel.AttachedFunction),
	}
}

func (g *attachedFunctionGraphState) incoming(collectionID string) ([]*dbmodel.AttachedFunction, error) {
	if incoming, ok := g.upstreamFunctions[collectionID]; ok {
		return incoming, nil
	}

	incoming, err := g.coordinator.catalog.metaDomain.AttachedFunctionDb(g.ctx).GetAttachedFunctions(nil, nil, nil, &collectionID, nil, false)
	if err != nil {
		return nil, err
	}
	g.upstreamFunctions[collectionID] = incoming
	return incoming, nil
}

func (g *attachedFunctionGraphState) outgoing(collectionID string) ([]*dbmodel.AttachedFunction, error) {
	if outgoing, ok := g.downstreamFunctions[collectionID]; ok {
		return outgoing, nil
	}

	outgoing, err := g.coordinator.catalog.metaDomain.AttachedFunctionDb(g.ctx).GetAttachedFunctions(nil, nil, &collectionID, nil, nil, false)
	if err != nil {
		return nil, err
	}
	g.downstreamFunctions[collectionID] = outgoing
	return outgoing, nil
}

func (g *attachedFunctionGraphState) outputCollectionID(attachedFunction *dbmodel.AttachedFunction) (*string, error) {
	return g.coordinator.resolveAttachedFunctionOutputCollectionID(g.ctx, attachedFunction, g.databaseName)
}

func (g *attachedFunctionGraphState) materializeIncoming(collectionID string, remainingDepth int, visited map[string]struct{}) error {
	if remainingDepth < 0 {
		return nil
	}
	if _, ok := visited[collectionID]; ok {
		return nil
	}
	visited[collectionID] = struct{}{}

	incoming, err := g.incoming(collectionID)
	if err != nil {
		return err
	}
	for _, attachedFunction := range incoming {
		if err := g.materializeIncoming(attachedFunction.InputCollectionID, remainingDepth-1, visited); err != nil {
			return err
		}
	}
	return nil
}

func (g *attachedFunctionGraphState) materializeOutgoing(collectionID string, remainingDepth int, visited map[string]struct{}) error {
	if remainingDepth < 0 {
		return nil
	}
	if _, ok := visited[collectionID]; ok {
		return nil
	}
	visited[collectionID] = struct{}{}

	outgoing, err := g.outgoing(collectionID)
	if err != nil {
		return err
	}
	for _, attachedFunction := range outgoing {
		outputCollectionID, err := g.outputCollectionID(attachedFunction)
		if err != nil {
			return err
		}
		if outputCollectionID == nil {
			continue
		}
		if err := g.materializeOutgoing(*outputCollectionID, remainingDepth-1, visited); err != nil {
			return err
		}
	}
	return nil
}

func (g *attachedFunctionGraphState) maxPathLength(
	collectionID string,
	memo map[string]int,
	visiting map[string]struct{},
	cycleMessage string,
	neighbors func(string) ([]string, error),
) (int, error) {
	if depth, ok := memo[collectionID]; ok {
		return depth, nil
	}
	if _, ok := visiting[collectionID]; ok {
		return 0, status.Errorf(codes.FailedPrecondition, cycleMessage)
	}

	visiting[collectionID] = struct{}{}
	defer delete(visiting, collectionID)

	nextCollections, err := neighbors(collectionID)
	if err != nil {
		return 0, err
	}
	if len(nextCollections) == 0 {
		memo[collectionID] = 0
		return 0, nil
	}

	maxDepth := 0
	for _, nextCollectionID := range nextCollections {
		childDepth, err := g.maxPathLength(nextCollectionID, memo, visiting, cycleMessage, neighbors)
		if err != nil {
			return 0, err
		}
		if childDepth+1 > maxDepth {
			maxDepth = childDepth + 1
		}
	}

	memo[collectionID] = maxDepth
	return maxDepth, nil
}

func (g *attachedFunctionGraphState) incomingCollectionIDs(collectionID string) ([]string, error) {
	incoming, err := g.incoming(collectionID)
	if err != nil {
		return nil, err
	}

	nextCollections := make([]string, 0, len(incoming))
	for _, attachedFunction := range incoming {
		nextCollections = append(nextCollections, attachedFunction.InputCollectionID)
	}
	return nextCollections, nil
}

func (g *attachedFunctionGraphState) outgoingCollectionIDs(collectionID string) ([]string, error) {
	outgoing, err := g.outgoing(collectionID)
	if err != nil {
		return nil, err
	}

	nextCollections := make([]string, 0, len(outgoing))
	for _, attachedFunction := range outgoing {
		outputCollectionID, err := g.outputCollectionID(attachedFunction)
		if err != nil {
			return nil, err
		}
		if outputCollectionID != nil {
			nextCollections = append(nextCollections, *outputCollectionID)
		}
	}
	return nextCollections, nil
}

func (g *attachedFunctionGraphState) collectionDepth(collectionID string, memo map[string]int, visiting map[string]struct{}) (int, error) {
	return g.maxPathLength(
		collectionID,
		memo,
		visiting,
		"attached function cycle detected while computing depth",
		g.incomingCollectionIDs,
	)
}

func (g *attachedFunctionGraphState) collectionTailDepth(collectionID string, memo map[string]int, visiting map[string]struct{}) (int, error) {
	return g.maxPathLength(
		collectionID,
		memo,
		visiting,
		"attached function cycle detected while computing downstream depth",
		g.outgoingCollectionIDs,
	)
}

func (g *attachedFunctionGraphState) reaches(startCollectionID string, targetCollectionID string) (bool, error) {
	if startCollectionID == targetCollectionID {
		return true, nil
	}

	queue := []string{startCollectionID}
	visited := map[string]struct{}{startCollectionID: {}}

	for len(queue) > 0 {
		currentCollectionID := queue[0]
		queue = queue[1:]

		outgoing, err := g.outgoing(currentCollectionID)
		if err != nil {
			return false, err
		}
		for _, attachedFunction := range outgoing {
			outputCollectionID, err := g.outputCollectionID(attachedFunction)
			if err != nil {
				return false, err
			}
			if outputCollectionID == nil {
				continue
			}
			if *outputCollectionID == targetCollectionID {
				return true, nil
			}
			if _, ok := visited[*outputCollectionID]; !ok {
				visited[*outputCollectionID] = struct{}{}
				queue = append(queue, *outputCollectionID)
			}
		}
	}

	return false, nil
}

func (g *attachedFunctionGraphState) allCollectionIDs() []string {
	collectionIDs := make(map[string]struct{})

	for outputCollectionID, incoming := range g.upstreamFunctions {
		collectionIDs[outputCollectionID] = struct{}{}
		for _, attachedFunction := range incoming {
			collectionIDs[attachedFunction.InputCollectionID] = struct{}{}
		}
	}

	for inputCollectionID, outgoing := range g.downstreamFunctions {
		collectionIDs[inputCollectionID] = struct{}{}
		for _, attachedFunction := range outgoing {
			if attachedFunction.OutputCollectionID != nil {
				collectionIDs[*attachedFunction.OutputCollectionID] = struct{}{}
			}
		}
	}

	result := make([]string, 0, len(collectionIDs))
	for collectionID := range collectionIDs {
		result = append(result, collectionID)
	}
	slices.Sort(result)
	return result
}

func (s *Coordinator) buildAttachFunctionGraph(ctx context.Context, inputCollectionID string, outputCollectionID string, databaseName string) (*attachedFunctionGraphState, error) {
	graphState := newAttachedFunctionGraphState(ctx, s, databaseName)
	if err := graphState.materializeIncoming(inputCollectionID, maxAttachedFunctionDepth, map[string]struct{}{}); err != nil {
		return nil, err
	}
	if outputCollectionID != "" {
		if err := graphState.materializeOutgoing(outputCollectionID, maxAttachedFunctionDepth, map[string]struct{}{}); err != nil {
			return nil, err
		}
	}
	return graphState, nil
}

func (s *Coordinator) lockAttachFunctionGraph(ctx context.Context, graphState *attachedFunctionGraphState, inputCollectionID string, outputCollectionID string) error {
	collectionIDsToLock := graphState.allCollectionIDs()
	if len(collectionIDsToLock) == 0 {
		collectionIDsToLock = []string{inputCollectionID}
		if outputCollectionID != "" && outputCollectionID != inputCollectionID {
			collectionIDsToLock = append(collectionIDsToLock, outputCollectionID)
			slices.Sort(collectionIDsToLock)
		}
	}

	for _, collectionID := range collectionIDsToLock {
		_, err := s.catalog.metaDomain.CollectionDb(ctx).LockCollection(collectionID)
		if err != nil {
			return err
		}
	}
	return nil
}

type attachedFunctionInsertSpec struct {
	AttachedFunctionID      uuid.UUID
	Name                    string
	TenantID                string
	DatabaseID              string
	DatabaseName            string
	InputCollectionID       string
	OutputCollectionName    string
	FunctionID              uuid.UUID
	FunctionParams          string
	MinRecordsForInvocation int64
}

// attached functions are stored per input collection, so async functions with
// multiple inputs can produce several rows that reference the same function ID.
func uniqueFunctionIDs(attachedFunctions []*dbmodel.AttachedFunction) []uuid.UUID {
	functionIDs := make([]uuid.UUID, 0, len(attachedFunctions))
	seenFunctionIDs := make(map[uuid.UUID]struct{}, len(attachedFunctions))
	for _, attachedFunction := range attachedFunctions {
		if _, ok := seenFunctionIDs[attachedFunction.FunctionID]; ok {
			continue
		}
		seenFunctionIDs[attachedFunction.FunctionID] = struct{}{}
		functionIDs = append(functionIDs, attachedFunction.FunctionID)
	}
	return functionIDs
}

func (s *Coordinator) loadFunctionsForAttachedFunctions(ctx context.Context, attachedFunctions []*dbmodel.AttachedFunction) (map[uuid.UUID]*dbmodel.Function, error) {
	functionsByID := make(map[uuid.UUID]*dbmodel.Function, len(attachedFunctions))
	functionIDs := uniqueFunctionIDs(attachedFunctions)
	if len(functionIDs) == 0 {
		return functionsByID, nil
	}

	functions, err := s.catalog.metaDomain.FunctionDb(ctx).GetByIDs(functionIDs)
	if err != nil {
		return nil, err
	}
	for _, function := range functions {
		functionsByID[function.ID] = function
	}
	return functionsByID, nil
}

// insertAttachedFunctionForInputCollection assumes the caller already holds the
// lock for spec.InputCollectionID. AttachFunction takes that lock as part of the
// full graph lock, while AddAttachedFunctionInput locks the new input collection
// directly before calling this helper.
func (s *Coordinator) insertAttachedFunctionForInputCollection(
	ctx context.Context,
	spec attachedFunctionInsertSpec,
	existingAttachedFunctions []*dbmodel.AttachedFunction,
) (bool, error) {
	if existingAttachedFunctions == nil {
		var err error
		existingAttachedFunctions, err = s.catalog.metaDomain.AttachedFunctionDb(ctx).GetAttachedFunctions(nil, nil, &spec.InputCollectionID, nil, nil, false)
		if err != nil {
			return false, err
		}
	}

	requestedFunction, err := s.catalog.metaDomain.FunctionDb(ctx).GetByID(spec.FunctionID)
	if err != nil {
		return false, err
	}
	if requestedFunction == nil {
		return false, common.ErrFunctionNotFound
	}

	existingFunctionsByID, err := s.loadFunctionsForAttachedFunctions(ctx, existingAttachedFunctions)
	if err != nil {
		return false, err
	}

	for _, attachedFunction := range existingAttachedFunctions {
		if attachedFunction.ID == spec.AttachedFunctionID {
			return !attachedFunction.IsReady, nil
		}

		existingFunction, ok := existingFunctionsByID[attachedFunction.FunctionID]
		if !ok {
			return false, common.ErrFunctionNotFound
		}

		if existingFunction.IsAsync == requestedFunction.IsAsync {
			return false, status.Errorf(codes.AlreadyExists,
				"collection already has an attached function with the same execution mode: name=%s, function=%s, output_collection=%s",
				attachedFunction.Name,
				existingFunction.Name,
				attachedFunction.OutputCollectionName)
		}
	}

	collections, err := s.catalog.metaDomain.CollectionDb(ctx).GetCollections(
		[]string{spec.InputCollectionID},
		nil,
		spec.TenantID,
		spec.DatabaseName,
		nil,
		nil,
		false,
	)
	if err != nil {
		return false, err
	}
	if len(collections) == 0 {
		return false, common.ErrCollectionNotFound
	}

	now := time.Now()
	attachedFunction := &dbmodel.AttachedFunction{
		ID:                      spec.AttachedFunctionID,
		Name:                    spec.Name,
		TenantID:                spec.TenantID,
		DatabaseID:              spec.DatabaseID,
		InputCollectionID:       spec.InputCollectionID,
		OutputCollectionName:    spec.OutputCollectionName,
		OutputCollectionID:      nil,
		FunctionID:              spec.FunctionID,
		FunctionParams:          spec.FunctionParams,
		CompletionOffset:        0,
		LastRun:                 nil,
		MinRecordsForInvocation: spec.MinRecordsForInvocation,
		CurrentAttempts:         0,
		CreatedAt:               now,
		UpdatedAt:               now,
		OldestWrittenNonce:      nil,
		IsReady:                 false,
	}

	if err := s.catalog.metaDomain.AttachedFunctionDb(ctx).Insert(attachedFunction); err != nil {
		return false, err
	}

	return true, nil
}

// AttachFunction creates an output collection and attached function in a single transaction
func (s *Coordinator) AttachFunction(ctx context.Context, req *coordinatorpb.AttachFunctionRequest) (*coordinatorpb.AttachFunctionResponse, error) {
	log := log.With(zap.String("method", "AttachFunction"))

	// Validate attached function name doesn't use reserved prefix
	if strings.HasPrefix(req.Name, "_deleted_") {
		log.Error("AttachFunction: attached function name cannot start with _deleted_")
		return nil, common.ErrInvalidAttachedFunctionName
	}

	var attachedFunctionID uuid.UUID = uuid.New()
	var created bool = true // Track if we created a new function or reused existing

	// ===== Step 1: Create attached function with is_ready = false =====
	err := s.catalog.txImpl.Transaction(ctx, func(txCtx context.Context) error {
		// Look up function by name up front so we can validate coexistence with any existing
		// attached functions on the same input collection.
		function, err := s.catalog.metaDomain.FunctionDb(txCtx).GetByName(req.FunctionName)
		if err != nil {
			log.Error("AttachFunction: failed to get function", zap.Error(err))
			return err
		}
		if function == nil {
			log.Error("AttachFunction: function not found", zap.String("function_name", req.FunctionName))
			return common.ErrFunctionNotFound
		}

		// Fast path for idempotent requests and conservative same-mode
		// conflicts. This is repeated under graph locks below before insert so
		// the final decision does not rely on this pre-lock snapshot.
		existingAttachedFunctions, err := s.catalog.metaDomain.AttachedFunctionDb(txCtx).GetAttachedFunctions(nil, nil, &req.InputCollectionId, nil, nil, false)
		if err != nil {
			log.Error("AttachFunction: failed to check for existing attached function", zap.Error(err))
			return err
		}
		existingFunctionsByID, err := s.loadFunctionsForAttachedFunctions(txCtx, existingAttachedFunctions)
		if err != nil {
			log.Error("AttachFunction: failed to load existing functions for input collection validation", zap.Error(err))
			return err
		}

		for _, attachedFunction := range existingAttachedFunctions {
			matches, err := s.validateAttachedFunctionMatchesRequest(txCtx, attachedFunction, req)
			if err != nil {
				return err
			}
			if matches {
				// If the attached function matches the request, use it
				attachedFunctionID = attachedFunction.ID
				created = !attachedFunction.IsReady // This was an idempotent request, not a new creation
				return nil
			}

			existingFunction, ok := existingFunctionsByID[attachedFunction.FunctionID]
			if !ok {
				log.Error("AttachFunction: unknown function ID on existing attached function",
					zap.Stringer("function_id", attachedFunction.FunctionID))
				return common.ErrFunctionNotFound
			}
			if existingFunction.IsAsync == function.IsAsync {
				log.Error("AttachFunction: collection already has an attached function with the same execution mode",
					zap.String("name", attachedFunction.Name),
					zap.String("existing_function", existingFunction.Name),
					zap.String("requested_function", function.Name),
					zap.Bool("is_async", function.IsAsync),
					zap.Bool("is_ready", attachedFunction.IsReady))
				return status.Errorf(codes.AlreadyExists,
					"collection already has an attached function with the same execution mode: name=%s, function=%s, output_collection=%s",
					attachedFunction.Name,
					existingFunction.Name,
					attachedFunction.OutputCollectionName)
			}
		}

		// Look up database_id
		databases, err := s.catalog.metaDomain.DatabaseDb(txCtx).GetDatabases(req.TenantId, req.Database)
		if err != nil {
			log.Error("AttachFunction: failed to get database", zap.Error(err))
			return err
		}
		if len(databases) == 0 {
			log.Error("AttachFunction: database not found")
			return common.ErrDatabaseNotFound
		}

		// Check if input collection exists
		collections, err := s.catalog.metaDomain.CollectionDb(txCtx).GetCollections([]string{req.InputCollectionId}, nil, req.TenantId, req.Database, nil, nil, false)
		if err != nil {
			log.Error("AttachFunction: failed to get input collection", zap.Error(err))
			return err
		}
		if len(collections) == 0 {
			log.Error("AttachFunction: input collection not found")
			return common.ErrCollectionNotFound
		}

		// Check if output collection already exists so we can materialize and then lock the full graph in a stable order.
		existingOutputCollection, err := s.catalog.metaDomain.CollectionDb(txCtx).GetCollections(nil, &req.OutputCollectionName, req.TenantId, req.Database, nil, nil, false)
		if err != nil {
			log.Error("AttachFunction: failed to check for existing output collection", zap.Error(err))
			return err
		}

		var existingOutputCollectionID string
		if len(existingOutputCollection) > 0 {
			existingOutputCollectionID = existingOutputCollection[0].Collection.ID
		}

		graphState, err := s.buildAttachFunctionGraph(txCtx, req.InputCollectionId, existingOutputCollectionID, req.Database)
		if err != nil {
			log.Error("AttachFunction: failed to materialize attached function graph", zap.Error(err))
			return err
		}
		if err := s.lockAttachFunctionGraph(txCtx, graphState, req.InputCollectionId, existingOutputCollectionID); err != nil {
			log.Error("AttachFunction: failed to lock attached function graph", zap.Error(err))
			return err
		}

		// Rebuild the graph under locks before validating/inserting.
		graphState, err = s.buildAttachFunctionGraph(txCtx, req.InputCollectionId, existingOutputCollectionID, req.Database)
		if err != nil {
			log.Error("AttachFunction: failed to rebuild attached function graph under locks", zap.Error(err))
			return err
		}

		// Re-read same-input attached functions after taking graph locks. This
		// keeps the final same-mode validation from using a stale pre-lock
		// snapshot if another attach raced with this one.
		existingAttachedFunctions, err = s.catalog.metaDomain.AttachedFunctionDb(txCtx).GetAttachedFunctions(nil, nil, &req.InputCollectionId, nil, nil, false)
		if err != nil {
			log.Error("AttachFunction: failed to check for existing attached function under graph lock", zap.Error(err))
			return err
		}

		// Validate that the input collection can accept another upstream edge.
		inputCollectionIDStr := req.InputCollectionId
		attachedFunctionsUsingAsOutput := graphState.upstreamFunctions[inputCollectionIDStr]
		if len(attachedFunctionsUsingAsOutput) > 0 {
			functionsByID, err := s.loadFunctionsForAttachedFunctions(txCtx, attachedFunctionsUsingAsOutput)
			if err != nil {
				log.Error("AttachFunction: failed to load functions for output collection validation", zap.Error(err))
				return err
			}

			for _, attachedFunction := range attachedFunctionsUsingAsOutput {
				existingFunction, ok := functionsByID[attachedFunction.FunctionID]
				if !ok {
					log.Error("AttachFunction: attached function references unknown function during output collection validation",
						zap.String("collection_id", req.InputCollectionId),
						zap.Stringer("function_id", attachedFunction.FunctionID))
					return common.ErrFunctionNotFound
				}
				if !existingFunction.IsAsync {
					log.Error("AttachFunction: cannot attach function to a collection that is already an output collection with sync upstream functions",
						zap.String("collection_id", req.InputCollectionId),
						zap.Stringer("function_id", attachedFunction.FunctionID),
						zap.String("function_name", existingFunction.Name))
					return common.ErrCannotAttachToOutputCollection
				}
			}
		}

		// Validate the output side of the new edge against the existing graph.
		outputTailDepth := 0
		if len(existingOutputCollection) > 0 {
			wouldCreateCycle, err := graphState.reaches(existingOutputCollectionID, req.InputCollectionId)
			if err != nil {
				log.Error("AttachFunction: failed while checking for attached function cycles", zap.Error(err))
				return err
			}
			if wouldCreateCycle {
				log.Error("AttachFunction: cannot attach function because it would create a cycle",
					zap.String("input_collection_id", req.InputCollectionId),
					zap.String("output_collection_name", req.OutputCollectionName),
					zap.String("output_collection_id", existingOutputCollectionID))
				return common.ErrCannotAttachToOutputCollection
			}

			outputTailDepth, err = graphState.collectionTailDepth(existingOutputCollectionID, map[string]int{}, map[string]struct{}{})
			if err != nil {
				log.Error("AttachFunction: failed to compute output collection downstream depth", zap.Error(err))
				return err
			}
		}

		// Enforce the maximum chain length after splicing in the new function.
		inputCollectionDepth, err := graphState.collectionDepth(req.InputCollectionId, map[string]int{}, map[string]struct{}{})
		if err != nil {
			log.Error("AttachFunction: failed to compute input collection depth", zap.Error(err))
			return err
		}

		totalAttachedFunctionDepth := inputCollectionDepth + 1 + outputTailDepth
		if totalAttachedFunctionDepth > maxAttachedFunctionDepth {
			log.Error("AttachFunction: attached function depth exceeds maximum",
				zap.String("input_collection_id", req.InputCollectionId),
				zap.Int("input_collection_depth", inputCollectionDepth),
				zap.Int("output_tail_depth", outputTailDepth),
				zap.Int("total_attached_function_depth", totalAttachedFunctionDepth),
				zap.Int("max_attached_function_depth", maxAttachedFunctionDepth))
			return status.Errorf(codes.InvalidArgument, "attached function depth exceeds maximum of %d", maxAttachedFunctionDepth)
		}

		if len(existingOutputCollection) > 0 {
			// Output collection exists - we now allow reusing any existing collection
			log.Info("AttachFunction: output collection already exists, will reuse it",
				zap.String("output_collection_name", req.OutputCollectionName))
		}

		// Serialize params
		var paramsJSON string
		if req.Params != nil && len(req.Params.Fields) > 0 {
			// Convert protobuf Struct to JSON
			paramsBytes, err := protojson.Marshal(req.Params)
			if err != nil {
				log.Error("AttachFunction: failed to serialize params", zap.Error(err))
				return status.Errorf(codes.InvalidArgument, "failed to serialize params: %v", err)
			}
			paramsJSON = string(paramsBytes)
		} else {
			paramsJSON = "{}"
		}

		created, err = s.insertAttachedFunctionForInputCollection(txCtx, attachedFunctionInsertSpec{
			AttachedFunctionID:      attachedFunctionID,
			Name:                    req.Name,
			TenantID:                req.TenantId,
			DatabaseID:              databases[0].ID,
			DatabaseName:            req.Database,
			InputCollectionID:       req.InputCollectionId,
			OutputCollectionName:    req.OutputCollectionName,
			FunctionID:              function.ID,
			FunctionParams:          paramsJSON,
			MinRecordsForInvocation: int64(req.MinRecordsForInvocation),
		}, existingAttachedFunctions)
		if err != nil {
			log.Error("AttachFunction: failed to insert attached function", zap.Error(err))
			return err
		}

		log.Debug("AttachFunction: attached function created with is_ready=false",
			zap.String("attached_function_id", attachedFunctionID.String()),
			zap.String("output_collection_name", req.OutputCollectionName),
			zap.String("name", req.Name))
		return nil
	})

	if err != nil {
		return nil, err
	}

	return &coordinatorpb.AttachFunctionResponse{
		AttachedFunction: &coordinatorpb.AttachedFunction{
			Id: attachedFunctionID.String(),
		},
		Created: created,
	}, nil
}

func (s *Coordinator) AddAttachedFunctionInput(ctx context.Context, req *coordinatorpb.AddAttachedFunctionInputRequest) (*coordinatorpb.AddAttachedFunctionInputResponse, error) {
	log := log.With(zap.String("method", "AddAttachedFunctionInput"))

	attachedFunctionID, err := uuid.Parse(req.AttachedFunctionId)
	if err != nil {
		log.Error("AddAttachedFunctionInput: invalid attached_function_id", zap.Error(err))
		return nil, status.Errorf(codes.InvalidArgument, "invalid attached_function_id: %v", err)
	}

	var created bool = true

	err = s.catalog.txImpl.Transaction(ctx, func(txCtx context.Context) error {
		attachedFunctions, err := s.catalog.metaDomain.AttachedFunctionDb(txCtx).GetAttachedFunctions(&attachedFunctionID, nil, nil, nil, nil, false)
		if err != nil {
			log.Error("AddAttachedFunctionInput: failed to get attached function", zap.Error(err))
			return err
		}
		if len(attachedFunctions) == 0 {
			log.Error("AddAttachedFunctionInput: attached function not found")
			return common.ErrAttachedFunctionNotFound
		}

		baseAttachedFunction := attachedFunctions[0]

		function, err := s.catalog.metaDomain.FunctionDb(txCtx).GetByID(baseAttachedFunction.FunctionID)
		if err != nil {
			log.Error("AddAttachedFunctionInput: failed to get function", zap.Error(err))
			return err
		}
		if function == nil {
			log.Error("AddAttachedFunctionInput: function not found", zap.String("function_id", baseAttachedFunction.FunctionID.String()))
			return common.ErrFunctionNotFound
		}
		if !function.IsAsync {
			log.Error("AddAttachedFunctionInput: attached function is not async")
			return status.Errorf(codes.InvalidArgument, "multiple input collections are only supported for async attached functions")
		}

		database, err := s.catalog.metaDomain.DatabaseDb(txCtx).GetByID(baseAttachedFunction.DatabaseID)
		if err != nil {
			log.Error("AddAttachedFunctionInput: failed to get database", zap.Error(err))
			return err
		}
		if database == nil {
			log.Error("AddAttachedFunctionInput: database not found", zap.String("database_id", baseAttachedFunction.DatabaseID))
			return common.ErrDatabaseNotFound
		}

		_, err = s.catalog.metaDomain.CollectionDb(txCtx).LockCollection(req.InputCollectionId)
		if err != nil {
			log.Error("AddAttachedFunctionInput: failed to lock input collection", zap.Error(err))
			return err
		}

		created, err = s.insertAttachedFunctionForInputCollection(txCtx, attachedFunctionInsertSpec{
			AttachedFunctionID:      attachedFunctionID,
			Name:                    baseAttachedFunction.Name,
			TenantID:                baseAttachedFunction.TenantID,
			DatabaseID:              baseAttachedFunction.DatabaseID,
			DatabaseName:            database.Name,
			InputCollectionID:       req.InputCollectionId,
			OutputCollectionName:    baseAttachedFunction.OutputCollectionName,
			FunctionID:              baseAttachedFunction.FunctionID,
			FunctionParams:          baseAttachedFunction.FunctionParams,
			MinRecordsForInvocation: baseAttachedFunction.MinRecordsForInvocation,
		}, nil)
		if err != nil {
			log.Error("AddAttachedFunctionInput: failed to insert attached function", zap.Error(err))
			return err
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return &coordinatorpb.AddAttachedFunctionInputResponse{
		AttachedFunction: &coordinatorpb.AttachedFunction{
			Id: attachedFunctionID.String(),
		},
		Created: created,
	}, nil
}

func attachedFunctionToProto(attachedFunction *dbmodel.AttachedFunction, function *dbmodel.Function) (*coordinatorpb.AttachedFunction, error) {
	if attachedFunction == nil {
		return nil, status.Error(codes.Internal, "attached function is nil")
	}
	if function == nil {
		return nil, status.Error(codes.Internal, "function is nil")
	}

	var paramsStruct *structpb.Struct
	if attachedFunction.FunctionParams != "" {
		paramsStruct = &structpb.Struct{}
		if err := paramsStruct.UnmarshalJSON([]byte(attachedFunction.FunctionParams)); err != nil {
			return nil, err
		}
	}

	if attachedFunction.CompletionOffset < 0 {
		return nil, status.Errorf(codes.Internal, "attached function has invalid completion_offset: %d", attachedFunction.CompletionOffset)
	}

	if !attachedFunction.IsReady {
		return nil, status.Errorf(codes.Internal, "serialized attached function is not ready")
	}

	attachedFunctionProto := &coordinatorpb.AttachedFunction{
		Id:                      attachedFunction.ID.String(),
		Name:                    attachedFunction.Name,
		FunctionName:            function.Name,        // Human-readable name for user-facing API
		FunctionId:              function.ID.String(), // UUID for internal use
		InputCollectionId:       attachedFunction.InputCollectionID,
		OutputCollectionName:    attachedFunction.OutputCollectionName,
		Params:                  paramsStruct,
		CompletionOffset:        uint64(attachedFunction.CompletionOffset),
		MinRecordsForInvocation: uint64(attachedFunction.MinRecordsForInvocation),
		TenantId:                attachedFunction.TenantID,
		DatabaseId:              attachedFunction.DatabaseID,
		CreatedAt:               uint64(attachedFunction.CreatedAt.UnixMicro()),
		UpdatedAt:               uint64(attachedFunction.UpdatedAt.UnixMicro()),
		IsAsync:                 function.IsAsync,
	}
	if attachedFunction.OutputCollectionID != nil {
		attachedFunctionProto.OutputCollectionId = attachedFunction.OutputCollectionID
	}

	return attachedFunctionProto, nil
}

// GetAttachedFunctions retrieves attached functions using flexible query parameters
// All parameters are optional - nil means don't filter on that field
func (s *Coordinator) GetAttachedFunctions(ctx context.Context, req *coordinatorpb.GetAttachedFunctionsRequest) (*coordinatorpb.GetAttachedFunctionsResponse, error) {
	// Validate that both id and ids are not provided together
	if req.Id != nil && len(req.Ids) > 0 {
		log.Error("GetAttachedFunctions: cannot provide both 'id' and 'ids' parameters")
		return nil, status.Errorf(codes.InvalidArgument, "cannot provide both 'id' and 'ids' parameters")
	}

	// Parse optional ID parameter
	var idPtr *uuid.UUID
	if req.Id != nil {
		parsed, err := uuid.Parse(*req.Id)
		if err != nil {
			log.Error("GetAttachedFunctions: invalid attached_function_id", zap.Error(err))
			return nil, status.Errorf(codes.InvalidArgument, "invalid attached_function_id: %v", err)
		}
		idPtr = &parsed
	}

	// Parse multiple IDs if provided
	var ids []uuid.UUID
	if len(req.Ids) > 0 {
		// Enforce a reasonable limit on the number of IDs to prevent overly large queries
		const maxIDs = 100
		if len(req.Ids) > maxIDs {
			log.Error("GetAttachedFunctions: too many IDs provided", zap.Int("count", len(req.Ids)), zap.Int("max", maxIDs))
			return nil, status.Errorf(codes.InvalidArgument, "too many IDs provided: %d (maximum: %d)", len(req.Ids), maxIDs)
		}

		ids = make([]uuid.UUID, 0, len(req.Ids))
		for _, idStr := range req.Ids {
			parsed, err := uuid.Parse(idStr)
			if err != nil {
				log.Error("GetAttachedFunctions: invalid id in ids array", zap.String("id", idStr), zap.Error(err))
				return nil, status.Errorf(codes.InvalidArgument, "invalid id in ids array: %v", err)
			}
			ids = append(ids, parsed)
		}
	}

	// Default onlyReady to true if not specified
	onlyReady := true
	if req.OnlyReady != nil {
		onlyReady = *req.OnlyReady
	}

	attachedFunctions, err := s.catalog.metaDomain.AttachedFunctionDb(ctx).GetAttachedFunctions(idPtr, req.Name, req.InputCollectionId, nil, ids, onlyReady)
	if err != nil {
		log.Error("GetAttachedFunctions: failed to get attached functions", zap.Error(err))
		return nil, err
	}

	if len(attachedFunctions) == 0 {
		return &coordinatorpb.GetAttachedFunctionsResponse{AttachedFunctions: []*coordinatorpb.AttachedFunction{}}, nil
	}

	// Collect unique function IDs
	functionIDsSet := make(map[uuid.UUID]struct{})
	functionIDs := make([]uuid.UUID, 0, len(attachedFunctions))
	for _, attachedFunction := range attachedFunctions {
		if _, exists := functionIDsSet[attachedFunction.FunctionID]; !exists {
			functionIDsSet[attachedFunction.FunctionID] = struct{}{}
			functionIDs = append(functionIDs, attachedFunction.FunctionID)
		}
	}

	functions, err := s.catalog.metaDomain.FunctionDb(ctx).GetByIDs(functionIDs)
	if err != nil {
		log.Error("GetAttachedFunctions: failed to get functions", zap.Error(err))
		return nil, err
	}

	functionsByID := make(map[uuid.UUID]*dbmodel.Function, len(functions))
	for _, function := range functions {
		if function == nil {
			continue
		}
		functionsByID[function.ID] = function
	}

	for _, functionID := range functionIDs {
		if _, ok := functionsByID[functionID]; !ok {
			log.Error("GetAttachedFunctions: function not found", zap.String("function_id", functionID.String()))
			return nil, common.ErrFunctionNotFound
		}
	}

	protoFunctions := make([]*coordinatorpb.AttachedFunction, 0, len(attachedFunctions))
	for _, attachedFunction := range attachedFunctions {
		function := functionsByID[attachedFunction.FunctionID]

		attachedFunctionProto, err := attachedFunctionToProto(attachedFunction, function)
		if err != nil {
			log.Error("GetAttachedFunctions: failed to convert attached function to proto", zap.Error(err), zap.String("attached_function_id", attachedFunction.ID.String()))
			return nil, err
		}

		protoFunctions = append(protoFunctions, attachedFunctionProto)
	}

	log.Info("GetAttachedFunctions succeeded",
		zap.Any("id", req.Id),
		zap.Any("name", req.Name),
		zap.Any("input_collection_id", req.InputCollectionId),
		zap.Bool("only_ready", onlyReady),
		zap.Int("count", len(protoFunctions)))

	return &coordinatorpb.GetAttachedFunctionsResponse{
		AttachedFunctions: protoFunctions,
	}, nil
}

// DetachFunction soft deletes an attached function by name
func (s *Coordinator) DetachFunction(ctx context.Context, req *coordinatorpb.DetachFunctionRequest) (*coordinatorpb.DetachFunctionResponse, error) {
	// First get the attached function to check if we need to delete the output collection
	attachedFunctions, err := s.catalog.metaDomain.AttachedFunctionDb(ctx).GetAttachedFunctions(nil, &req.Name, &req.InputCollectionId, nil, nil, true)
	if err != nil {
		log.Error("DetachFunction: failed to get attached function", zap.Error(err))
		return nil, err
	}
	if len(attachedFunctions) == 0 {
		log.Error("DetachFunction: attached function not found")
		return nil, common.ErrAttachedFunctionNotFound
	}
	attachedFunction := attachedFunctions[0]

	// Execute collection and attached function deletion in a single transaction
	err = s.catalog.txImpl.Transaction(ctx, func(txCtx context.Context) error {
		// If delete_output is true and output_collection_id is set, soft-delete the output collection
		if req.DeleteOutput && attachedFunction.OutputCollectionID != nil && *attachedFunction.OutputCollectionID != "" {
			collectionUUID, err := types.ToUniqueID(attachedFunction.OutputCollectionID)
			if err != nil {
				log.Error("DetachFunction: invalid output_collection_id", zap.Error(err))
				return status.Errorf(codes.InvalidArgument, "invalid output_collection_id: %v", err)
			}

			deleteCollection := &model.DeleteCollection{
				ID:       collectionUUID,
				TenantID: attachedFunction.TenantID,
				// Database name isn't available but also isn't needed since we supplied a collection id
				DatabaseName: "",
			}

			err = s.SoftDeleteCollection(txCtx, deleteCollection)
			if err != nil {
				// If collection doesn't exist, that's fine - still delete the attached function
				if errors.Is(err, common.ErrCollectionDeleteNonExistingCollection) {
					log.Info("DetachFunction: output collection already deleted", zap.String("collection_id", *attachedFunction.OutputCollectionID))
				} else {
					// Other errors should fail the transaction
					log.Error("DetachFunction: failed to delete output collection", zap.Error(err), zap.String("collection_id", *attachedFunction.OutputCollectionID))
					return err
				}
			} else {
				log.Info("DetachFunction: deleted output collection", zap.String("collection_id", *attachedFunction.OutputCollectionID))
			}
		}

		// Now soft-delete the attached function by name
		err := s.catalog.metaDomain.AttachedFunctionDb(txCtx).SoftDelete(req.InputCollectionId, req.Name)
		if err != nil {
			log.Error("DetachFunction: failed to delete attached function", zap.Error(err))
			return err
		}

		log.Info("DetachFunction: successfully deleted attached function", zap.String("name", req.Name))
		return nil
	})

	if err != nil {
		return nil, err
	}

	log.Info("Attached function deleted", zap.String("name", req.Name))

	return &coordinatorpb.DetachFunctionResponse{
		Success: true,
	}, nil
}

// GetFunctions retrieves all functions from the database
func (s *Coordinator) GetFunctions(ctx context.Context, req *coordinatorpb.GetFunctionsRequest) (*coordinatorpb.GetFunctionsResponse, error) {
	functions, err := s.catalog.metaDomain.FunctionDb(ctx).GetAll()
	if err != nil {
		log.Error("GetFunctions failed", zap.Error(err))
		return nil, err
	}

	// Convert to proto response
	protoFunctions := make([]*coordinatorpb.Function, len(functions))
	for i, op := range functions {
		protoFunctions[i] = &coordinatorpb.Function{
			Id:   op.ID.String(),
			Name: op.Name,
		}
	}

	log.Info("GetFunctions succeeded", zap.Int("count", len(functions)))

	return &coordinatorpb.GetFunctionsResponse{
		Functions: protoFunctions,
	}, nil
}

// FinishCreateAttachedFunction creates the output collection and sets is_ready to true in a single transaction
func (s *Coordinator) FinishCreateAttachedFunction(ctx context.Context, req *coordinatorpb.FinishCreateAttachedFunctionRequest) (*coordinatorpb.FinishCreateAttachedFunctionResponse, error) {
	attachedFunctionID, err := uuid.Parse(req.Id)
	if err != nil {
		log.Error("FinishCreateAttachedFunction: invalid attached_function_id", zap.Error(err))
		return nil, status.Errorf(codes.InvalidArgument, "invalid attached_function_id: %v", err)
	}

	var created bool = true // Track if we created output collection or it already existed

	// Execute all operations in a transaction for atomicity
	err = s.catalog.txImpl.Transaction(ctx, func(txCtx context.Context) error {
		// 1. Get the attached function to retrieve metadata
		attachedFunctions, err := s.catalog.metaDomain.AttachedFunctionDb(txCtx).GetAttachedFunctions(&attachedFunctionID, nil, nil, nil, nil, false)
		if err != nil {
			log.Error("FinishCreateAttachedFunction: failed to get attached function", zap.Error(err))
			return err
		}
		if len(attachedFunctions) == 0 {
			log.Error("FinishCreateAttachedFunction: attached function not found")
			return status.Errorf(codes.NotFound, "attached function not found")
		}
		attachedFunction := attachedFunctions[0]
		var readyAttachedFunction *dbmodel.AttachedFunction
		for _, af := range attachedFunctions {
			if af.IsReady {
				readyAttachedFunction = af
				break
			}
		}

		// 2. If any row for this attached function id is already ready, use its output collection
		// and mark all rows with the same attached function id ready as well.
		if readyAttachedFunction != nil {
			created = false
			now := time.Now()
			dbAttachedFunction := &dbmodel.AttachedFunction{
				ID:                 attachedFunctionID,
				OutputCollectionID: readyAttachedFunction.OutputCollectionID,
				IsReady:            true,
				UpdatedAt:          now,
			}
			err = s.catalog.metaDomain.AttachedFunctionDb(txCtx).Update(dbAttachedFunction)
			if err != nil {
				log.Error("FinishCreateAttachedFunction: failed to update ready rows for shared attached function", zap.Error(err))
				return err
			}
			return nil
		}

		// 3. Look up database by ID to get its name
		database, err := s.catalog.metaDomain.DatabaseDb(txCtx).GetByID(attachedFunction.DatabaseID)
		if err != nil {
			log.Error("FinishCreateAttachedFunction: failed to get database", zap.Error(err))
			return err
		}
		if database == nil {
			log.Error("FinishCreateAttachedFunction: database not found", zap.String("database_id", attachedFunction.DatabaseID), zap.String("tenant_id", attachedFunction.TenantID))
			return common.ErrDatabaseNotFound
		}

		// 4. Check if output collection already exists
		existingCollections, err := s.catalog.metaDomain.CollectionDb(txCtx).GetCollections(nil, &attachedFunction.OutputCollectionName, attachedFunction.TenantID, database.Name, nil, nil, false)
		if err != nil {
			log.Error("FinishCreateAttachedFunction: failed to check for existing output collection", zap.Error(err))
			return err
		}

		var collectionID types.UniqueID
		if len(existingCollections) > 0 {
			// Output collection exists - reuse it
			existingCollection := existingCollections[0]
			log.Info("FinishCreateAttachedFunction: reusing existing output collection",
				zap.String("output_collection_name", attachedFunction.OutputCollectionName))

			collectionID, err = types.Parse(existingCollection.Collection.ID)
			if err != nil {
				log.Error("FinishCreateAttachedFunction: failed to parse existing collection ID", zap.Error(err))
				return grpcutils.BuildInternalGrpcError("invalid collection ID")
			}
			created = false // Collection already existed
		} else {
			// 5. Create new output collection with segments
			collectionID = types.NewUniqueID()
			dimension := int32(1) // Default dimension for attached function output collections

			// Use the schema string passed from Rust (contains default schema)
			schemaStr := req.OutputCollectionSchemaStr

			collection := &model.CreateCollection{
				ID:                   collectionID,
				Name:                 attachedFunction.OutputCollectionName,
				ConfigurationJsonStr: "{}", // Empty JSON object for default config
				SchemaStr:            &schemaStr,
				TenantID:             attachedFunction.TenantID,
				DatabaseName:         database.Name,
				Dimension:            &dimension,
			}

			// Create segments for the collection (distributed setup with HNSW)
			segments := []*model.Segment{
				{
					ID:           types.NewUniqueID(),
					Type:         "urn:chroma:segment/vector/hnsw-distributed",
					Scope:        "VECTOR",
					CollectionID: collectionID,
				},
				{
					ID:           types.NewUniqueID(),
					Type:         "urn:chroma:segment/metadata/blockfile",
					Scope:        "METADATA",
					CollectionID: collectionID,
				},
				{
					ID:           types.NewUniqueID(),
					Type:         "urn:chroma:segment/record/blockfile",
					Scope:        "RECORD",
					CollectionID: collectionID,
				},
			}

			_, _, err = s.catalog.CreateCollectionAndSegments(txCtx, collection, segments, 0)
			if err != nil {
				log.Error("FinishCreateAttachedFunction: failed to create output collection", zap.Error(err))
				if err == common.ErrCollectionUniqueConstraintViolation {
					return grpcutils.BuildAlreadyExistsGrpcError(fmt.Sprintf("output collection '%s' already exists", collection.Name))
				}
				return err
			}
		}

		// 6. Update attached function with output_collection_id and set is_ready to true
		collectionIDStr := collectionID.String()
		now := time.Now()
		dbAttachedFunction := &dbmodel.AttachedFunction{
			ID:                 attachedFunctionID,
			OutputCollectionID: &collectionIDStr,
			IsReady:            true,
			UpdatedAt:          now,
		}
		err = s.catalog.metaDomain.AttachedFunctionDb(txCtx).Update(dbAttachedFunction)
		if err != nil {
			log.Error("FinishCreateAttachedFunction: failed to update output collection ID and set ready", zap.Error(err))
			return err
		}

		// 7. Validate that there is at most one ready sync function and one ready
		// async function for this collection.
		existingAttachedFunctions, err := s.catalog.metaDomain.AttachedFunctionDb(txCtx).GetAttachedFunctions(nil, nil, &attachedFunction.InputCollectionID, nil, nil, true)
		if err != nil {
			log.Error("FinishCreateAttachedFunction: failed to get attached functions", zap.Error(err))
			return err
		}

		functionsByID, err := s.loadFunctionsForAttachedFunctions(txCtx, existingAttachedFunctions)
		if err != nil {
			log.Error("FinishCreateAttachedFunction: failed to load functions", zap.Error(err))
			return err
		}

		readySyncCount := 0
		readyAsyncCount := 0
		for _, existingAttachedFunction := range existingAttachedFunctions {
			function, ok := functionsByID[existingAttachedFunction.FunctionID]
			if !ok {
				log.Error("FinishCreateAttachedFunction: unknown function on attached function",
					zap.Stringer("function_id", existingAttachedFunction.FunctionID))
				return common.ErrFunctionNotFound
			}

			if function.IsAsync {
				readyAsyncCount++
			} else {
				readySyncCount++
			}
		}

		if readySyncCount > 1 || readyAsyncCount > 1 {
			log.Error("FinishCreateAttachedFunction: too many ready attached functions found for collection",
				zap.String("collection_id", attachedFunction.InputCollectionID),
				zap.Int("ready_sync_count", readySyncCount),
				zap.Int("ready_async_count", readyAsyncCount))
			return common.ErrAttachedFunctionAlreadyExists
		}

		log.Info("FinishCreateAttachedFunction: successfully created output collection and set is_ready=true",
			zap.String("attached_function_id", attachedFunctionID.String()),
			zap.String("output_collection_id", collectionID.String()))
		return nil
	})

	if err != nil {
		return nil, err
	}

	return &coordinatorpb.FinishCreateAttachedFunctionResponse{
		Created: created,
	}, nil
}

// CleanupExpiredPartialAttachedFunctions finds and soft deletes attached functions that were partially created
// (output_collection_id IS NULL) and are older than the specified max age.
// This is used to clean up attached functions that got stuck during creation.
func (s *Coordinator) CleanupExpiredPartialAttachedFunctions(ctx context.Context, req *coordinatorpb.CleanupExpiredPartialAttachedFunctionsRequest) (*coordinatorpb.CleanupExpiredPartialAttachedFunctionsResponse, error) {
	log := log.With(zap.String("method", "CleanupExpiredPartialAttachedFunctions"))

	if req.MaxAgeSeconds == 0 {
		log.Error("CleanupExpiredPartialAttachedFunctions: max_age_seconds must be greater than 0")
		return nil, status.Errorf(codes.InvalidArgument, "max_age_seconds must be greater than 0")
	}

	log.Info("CleanupExpiredPartialAttachedFunctions: starting cleanup",
		zap.Uint64("max_age_seconds", req.MaxAgeSeconds))

	cleanedAttachedFunctionIDs, err := s.catalog.metaDomain.AttachedFunctionDb(ctx).CleanupExpiredPartial(req.MaxAgeSeconds)
	if err != nil {
		log.Error("CleanupExpiredPartialAttachedFunctions: failed to cleanup attached functions", zap.Error(err))
		return nil, err
	}

	// Convert UUIDs to strings for response
	cleanedAttachedFunctionIDStrings := make([]string, len(cleanedAttachedFunctionIDs))
	for i, attachedFunctionID := range cleanedAttachedFunctionIDs {
		cleanedAttachedFunctionIDStrings[i] = attachedFunctionID.String()
	}

	log.Info("CleanupExpiredPartialAttachedFunctions: completed successfully",
		zap.Uint64("cleaned_up_count", uint64(len(cleanedAttachedFunctionIDs))))

	return &coordinatorpb.CleanupExpiredPartialAttachedFunctionsResponse{
		CleanedUpCount: uint64(len(cleanedAttachedFunctionIDs)),
		CleanedUpIds:   cleanedAttachedFunctionIDStrings,
	}, nil
}

// GetAttachedFunctionsToGc retrieves attached functions eligible for garbage collection:
// soft deleted or stuck in non-ready state, and updated before the cutoff time
func (s *Coordinator) GetAttachedFunctionsToGc(ctx context.Context, req *coordinatorpb.GetAttachedFunctionsToGcRequest) (*coordinatorpb.GetAttachedFunctionsToGcResponse, error) {
	log := log.With(zap.String("method", "GetAttachedFunctionsToGc"))

	if req.CutoffTime == nil {
		log.Error("GetAttachedFunctionsToGc: cutoff_time is required")
		return nil, status.Errorf(codes.InvalidArgument, "cutoff_time is required")
	}

	if req.Limit <= 0 {
		log.Error("GetAttachedFunctionsToGc: limit must be greater than 0")
		return nil, status.Errorf(codes.InvalidArgument, "limit must be greater than 0")
	}

	cutoffTime := req.CutoffTime.AsTime()
	attachedFunctions, err := s.catalog.metaDomain.AttachedFunctionDb(ctx).GetAttachedFunctionsToGc(cutoffTime, req.Limit)
	if err != nil {
		log.Error("GetAttachedFunctionsToGc: failed to get attached functions to gc", zap.Error(err))
		return nil, err
	}

	// Convert to proto response
	protoAttachedFunctions := make([]*coordinatorpb.AttachedFunction, len(attachedFunctions))
	for i, af := range attachedFunctions {
		protoAttachedFunctions[i] = &coordinatorpb.AttachedFunction{
			Id:                      af.ID.String(),
			Name:                    af.Name,
			InputCollectionId:       af.InputCollectionID,
			OutputCollectionName:    af.OutputCollectionName,
			CompletionOffset:        uint64(af.CompletionOffset),
			MinRecordsForInvocation: uint64(af.MinRecordsForInvocation),
			CreatedAt:               uint64(af.CreatedAt.UnixMicro()),
			UpdatedAt:               uint64(af.UpdatedAt.UnixMicro()),
		}

		if af.OutputCollectionID != nil {
			protoAttachedFunctions[i].OutputCollectionId = proto.String(*af.OutputCollectionID)
		}
	}

	return &coordinatorpb.GetAttachedFunctionsToGcResponse{
		AttachedFunctions: protoAttachedFunctions,
	}, nil
}

// FinishAttachedFunctionDeletion permanently deletes an attached function from the database (hard delete)
// This should only be called after the soft delete grace period has passed
func (s *Coordinator) FinishAttachedFunctionDeletion(ctx context.Context, req *coordinatorpb.FinishAttachedFunctionDeletionRequest) (*coordinatorpb.FinishAttachedFunctionDeletionResponse, error) {
	log := log.With(zap.String("method", "FinishAttachedFunctionDeletion"))

	attachedFunctionID, err := uuid.Parse(req.AttachedFunctionId)
	if err != nil {
		log.Error("FinishAttachedFunctionDeletion: invalid attached_function_id", zap.Error(err))
		return nil, status.Errorf(codes.InvalidArgument, "invalid attached_function_id: %v", err)
	}

	err = s.catalog.metaDomain.AttachedFunctionDb(ctx).HardDeleteAttachedFunction(attachedFunctionID)
	if err != nil {
		log.Error("FinishAttachedFunctionDeletion: failed to hard delete attached function", zap.Error(err))
		return nil, err
	}

	log.Info("FinishAttachedFunctionDeletion: completed successfully",
		zap.String("attached_function_id", attachedFunctionID.String()))

	return &coordinatorpb.FinishAttachedFunctionDeletionResponse{}, nil
}

// TryFinishAsyncAttachedFunctionInvocation updates the completion offset for an async attached function
func (s *Coordinator) TryFinishAsyncAttachedFunctionInvocation(ctx context.Context, req *coordinatorpb.TryFinishAsyncAttachedFunctionInvocationRequest) (*coordinatorpb.TryFinishAsyncAttachedFunctionInvocationResponse, error) {
	log := log.With(zap.String("attached_function_id", req.AttachedFunctionId))

	// Parse UUIDs
	attachedFunctionID, err := uuid.Parse(req.AttachedFunctionId)
	if err != nil {
		log.Error("Invalid attached function ID", zap.Error(err))
		return nil, status.Errorf(codes.InvalidArgument, "invalid attached_function_id: %v", err)
	}

	collectionID, err := types.ToUniqueID(&req.CollectionId)
	if err != nil {
		log.Error("Invalid collection ID", zap.Error(err))
		return nil, status.Errorf(codes.InvalidArgument, "invalid collection_id: %v", err)
	}

	err = s.catalog.txImpl.Transaction(ctx, func(txCtx context.Context) error {
		attachedFunctions, err := s.catalog.metaDomain.AttachedFunctionDb(txCtx).GetAttachedFunctions(&attachedFunctionID, nil, nil, nil, nil, true)
		if err != nil {
			log.Error("Failed to get attached function", zap.Error(err))
			return err
		}
		if len(attachedFunctions) == 0 {
			log.Error("Attached function not found", zap.String("id", attachedFunctionID.String()))
			return status.Errorf(codes.NotFound, "attached function not found")
		}

		var attachedFunction *dbmodel.AttachedFunction
		for _, candidate := range attachedFunctions {
			if candidate.InputCollectionID == req.CollectionId {
				attachedFunction = candidate
				break
			}
		}
		if attachedFunction == nil {
			log.Error("Attached function not found for collection",
				zap.String("id", attachedFunctionID.String()),
				zap.String("collection_id", req.CollectionId))
			return status.Errorf(codes.NotFound, "attached function not found for collection")
		}

		// Get the associated function to check if it's async
		function, err := s.catalog.metaDomain.FunctionDb(txCtx).GetByID(attachedFunction.FunctionID)
		if err != nil {
			log.Error("Failed to get function", zap.Error(err))
			return err
		}

		if !function.IsAsync {
			log.Error("Attached function is not async", zap.Bool("is_async", function.IsAsync))
			return status.Errorf(codes.InvalidArgument, "attached function is not async")
		}

		if attachedFunction.InputCollectionID != req.CollectionId {
			log.Error("Collection ID mismatch",
				zap.String("expected", attachedFunction.InputCollectionID),
				zap.String("provided", req.CollectionId))
			return status.Errorf(codes.InvalidArgument, "collection_id does not match attached function's input_collection_id")
		}

		err = s.catalog.metaDomain.AttachedFunctionDb(txCtx).UpdateCompletionOffset(
			attachedFunctionID, collectionID.String(), int64(req.NewCompletionOffset))
		if err != nil {
			log.Error("Failed to update completion offset", zap.Error(err))
			return err
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return &coordinatorpb.TryFinishAsyncAttachedFunctionInvocationResponse{
		UpdatedCompletionOffset: req.NewCompletionOffset,
	}, nil
}

// FinalizeAsyncAttachedFunctionRepair sets heap_entry_pending back to false after repair
func (s *Coordinator) FinalizeAsyncAttachedFunctionRepair(ctx context.Context, req *coordinatorpb.FinalizeAsyncAttachedFunctionRepairRequest) (*coordinatorpb.FinalizeAsyncAttachedFunctionRepairResponse, error) {
	log := log.With(zap.String("attached_function_id", req.AttachedFunctionId))
	log.Info("FinalizeAsyncAttachedFunctionRepair called")

	// Parse UUID
	attachedFunctionID, err := uuid.Parse(req.AttachedFunctionId)
	if err != nil {
		log.Error("Invalid attached function ID", zap.Error(err))
		return nil, status.Errorf(codes.InvalidArgument, "invalid attached_function_id: %v", err)
	}

	collectionID, err := types.ToUniqueID(&req.CollectionId)
	if err != nil {
		log.Error("Invalid collection ID", zap.Error(err))
		return nil, status.Errorf(codes.InvalidArgument, "invalid collection_id: %v", err)
	}

	err = s.catalog.metaDomain.AttachedFunctionDb(ctx).UpdateHeapEntryPending(attachedFunctionID, collectionID.String(), false)
	if err != nil {
		log.Error("Failed to update heap_entry_pending", zap.Error(err))
		return nil, err
	}

	log.Info("Successfully finalized repair for attached function")
	return &coordinatorpb.FinalizeAsyncAttachedFunctionRepairResponse{}, nil
}

// CheckInvocationStatus checks the status of multiple attached function invocations
// by comparing current completion_offset against provided old completion offsets and checking heap_entry_pending flag.
// Returns one of three states for each invocation: NOT_DONE (default), DONE, or NEEDS_REPAIR.
func (s *Coordinator) CheckInvocationStatus(ctx context.Context, req *coordinatorpb.CheckInvocationStatusRequest) (*coordinatorpb.CheckInvocationStatusResponse, error) {
	log := log.With(zap.String("method", "CheckInvocationStatus"))

	if len(req.Items) == 0 {
		return &coordinatorpb.CheckInvocationStatusResponse{Results: []*coordinatorpb.InvocationStatusResult{}}, nil
	}

	// Convert proto request items to dbmodel items
	items := make([]dbmodel.InvocationCheckItem, len(req.Items))
	for i, item := range req.Items {
		functionID, err := uuid.Parse(item.FunctionId)
		if err != nil {
			log.Error("CheckInvocationStatus: invalid function_id", zap.Error(err), zap.String("function_id", item.FunctionId))
			return nil, status.Errorf(codes.InvalidArgument, "invalid function_id at index %d: %v", i, err)
		}

		// Validate input collection ID as UUID
		_, err = uuid.Parse(item.InputCollectionId)
		if err != nil {
			log.Error("CheckInvocationStatus: invalid input_collection_id", zap.Error(err), zap.String("input_collection_id", item.InputCollectionId))
			return nil, status.Errorf(codes.InvalidArgument, "invalid input_collection_id at index %d: %v", i, err)
		}

		items[i] = dbmodel.InvocationCheckItem{
			FunctionID:        functionID,
			InputCollectionID: item.InputCollectionId,
			CompletionOffset:  item.CompletionOffset,
		}
	}

	// Call the database method
	results, err := s.catalog.metaDomain.AttachedFunctionDb(ctx).CheckInvocationStatus(items)
	if err != nil {
		log.Error("CheckInvocationStatus: failed to check invocation status", zap.Error(err))
		return nil, err
	}

	// Convert dbmodel statuses to proto statuses
	protoResults := make([]*coordinatorpb.InvocationStatusResult, len(results))
	for i, result := range results {
		var protoStatus coordinatorpb.InvocationStatus
		switch result.Status {
		case dbmodel.InvocationStatusNotDone:
			protoStatus = coordinatorpb.InvocationStatus_INVOCATION_STATUS_NOT_DONE
		case dbmodel.InvocationStatusDone:
			protoStatus = coordinatorpb.InvocationStatus_INVOCATION_STATUS_DONE
		case dbmodel.InvocationStatusNeedsRepair:
			protoStatus = coordinatorpb.InvocationStatus_INVOCATION_STATUS_NEEDS_REPAIR
		}
		protoResults[i] = &coordinatorpb.InvocationStatusResult{
			Status:                  protoStatus,
			CurrentCompletionOffset: result.CurrentCompletionOffset,
		}
	}

	log.Debug("CheckInvocationStatus: completed successfully",
		zap.Int("items_count", len(items)),
		zap.Int("results_count", len(results)))

	return &coordinatorpb.CheckInvocationStatusResponse{Results: protoResults}, nil
}
