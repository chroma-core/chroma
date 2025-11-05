package coordinator

import (
	"context"
	"errors"
	"math"
	"strings"
	"time"

	"github.com/chroma-core/chroma/go/pkg/common"
	"github.com/chroma-core/chroma/go/pkg/proto/coordinatorpb"
	"github.com/chroma-core/chroma/go/pkg/sysdb/coordinator/model"
	"github.com/chroma-core/chroma/go/pkg/sysdb/metastore/db/dbmodel"
	"github.com/chroma-core/chroma/go/pkg/types"
	"github.com/google/uuid"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// minimalUUIDv7 represents the smallest possible UUIDv7.
// UUIDv7 format: [timestamp (48 bits)][version (4 bits)][random (12 bits)][variant (2 bits)][random (62 bits)]
// This UUID has all zeros for timestamp and random bits, making it the minimal valid UUIDv7.
// It is used as the initial value for lowest_live_nonce, guaranteed to be less than any UUIDv7 generated with current time.
var minimalUUIDv7 = uuid.UUID{
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // timestamp = 0 (bytes 0-5)
	0x70, 0x00, // version 7 (0x7) in high nibble, low nibble = 0 (bytes 6-7)
	0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // variant bits + rest = 0 (bytes 8-15)
}

// validateAttachedFunctionMatchesRequest validates that an existing attached function's parameters match the request parameters.
// Returns an error if any parameters don't match. This is used for idempotency and race condition handling.
func (s *Coordinator) validateAttachedFunctionMatchesRequest(ctx context.Context, attachedFunction *dbmodel.AttachedFunction, req *coordinatorpb.AttachFunctionRequest) error {
	// Look up the function for the existing attached function
	existingFunction, err := s.catalog.metaDomain.FunctionDb(ctx).GetByID(attachedFunction.FunctionID)
	if err != nil {
		log.Error("validateAttachedFunctionMatchesRequest: failed to get attached function's function", zap.Error(err))
		return err
	}
	if existingFunction == nil {
		log.Error("validateAttachedFunctionMatchesRequest: attached function's function not found")
		return common.ErrFunctionNotFound
	}

	// Look up database for comparison
	databases, err := s.catalog.metaDomain.DatabaseDb(ctx).GetDatabases(req.TenantId, req.Database)
	if err != nil {
		log.Error("validateAttachedFunctionMatchesRequest: failed to get database for validation", zap.Error(err))
		return err
	}
	if len(databases) == 0 {
		log.Error("validateAttachedFunctionMatchesRequest: database not found")
		return common.ErrDatabaseNotFound
	}

	// Validate attributes match
	if existingFunction.Name != req.FunctionName {
		log.Error("validateAttachedFunctionMatchesRequest: attached function has different function",
			zap.String("existing", existingFunction.Name),
			zap.String("requested", req.FunctionName))
		return status.Errorf(codes.AlreadyExists, "attached function already exists with different function: existing=%s, requested=%s", existingFunction.Name, req.FunctionName)
	}
	if attachedFunction.TenantID != req.TenantId {
		log.Error("validateAttachedFunctionMatchesRequest: attached function has different tenant")
		return status.Errorf(codes.AlreadyExists, "attached function already exists with different tenant")
	}
	if attachedFunction.DatabaseID != databases[0].ID {
		log.Error("validateAttachedFunctionMatchesRequest: attached function has different database")
		return status.Errorf(codes.AlreadyExists, "attached function already exists with different database")
	}
	if attachedFunction.OutputCollectionName != req.OutputCollectionName {
		log.Error("validateAttachedFunctionMatchesRequest: attached function has different output collection name",
			zap.String("existing", attachedFunction.OutputCollectionName),
			zap.String("requested", req.OutputCollectionName))
		return status.Errorf(codes.AlreadyExists, "attached function already exists with different output collection: existing=%s, requested=%s", attachedFunction.OutputCollectionName, req.OutputCollectionName)
	}
	if attachedFunction.MinRecordsForInvocation != int64(req.MinRecordsForInvocation) {
		log.Error("validateAttachedFunctionMatchesRequest: attached function has different min_records_for_invocation",
			zap.Int64("existing", attachedFunction.MinRecordsForInvocation),
			zap.Uint64("requested", req.MinRecordsForInvocation))
		return status.Errorf(codes.AlreadyExists, "attached function already exists with different min_records_for_invocation: existing=%d, requested=%d", attachedFunction.MinRecordsForInvocation, req.MinRecordsForInvocation)
	}

	return nil
}

// AttachFunction creates a new attached function in the database
func (s *Coordinator) AttachFunction(ctx context.Context, req *coordinatorpb.AttachFunctionRequest) (*coordinatorpb.AttachFunctionResponse, error) {
	log := log.With(zap.String("method", "AttachFunction"))

	// Validate attached function name doesn't use reserved prefix
	if strings.HasPrefix(req.Name, "_deleted_") {
		log.Error("AttachFunction: attached function name cannot start with _deleted_")
		return nil, common.ErrInvalidAttachedFunctionName
	}

	var attachedFunctionID uuid.UUID = uuid.New()
	var nextNonce uuid.UUID       // Store next_nonce to avoid re-fetching from DB
	var lowestLiveNonce uuid.UUID // Store lowest_live_nonce to set in Phase 3
	var nextRun time.Time
	var skipPhase2And3 bool // Flag to skip Phase 2 & 3 if task is already fully initialized

	// ===== Phase 1: Create attached function with lowest_live_nonce = NULL (if needed) =====
	err := s.catalog.txImpl.Transaction(ctx, func(txCtx context.Context) error {
		// Double-check attached function doesn't exist (race condition protection)
		concurrentAttachedFunction, err := s.catalog.metaDomain.AttachedFunctionDb(txCtx).GetByName(req.InputCollectionId, req.Name)
		if err != nil {
			log.Error("AttachFunction: failed to double-check attached function", zap.Error(err))
			return err
		}
		if concurrentAttachedFunction != nil {
			// Attached function was created concurrently, validate it matches our request
			log.Info("AttachFunction: attached function created concurrently, validating parameters",
				zap.String("attached_function_id", concurrentAttachedFunction.ID.String()))

			// Validate that concurrent attached function matches our request
			if err := s.validateAttachedFunctionMatchesRequest(txCtx, concurrentAttachedFunction, req); err != nil {
				return err
			}

			// Validation passed, reuse the concurrent attached function's data
			attachedFunctionID = concurrentAttachedFunction.ID
			nextNonce = concurrentAttachedFunction.NextNonce
			nextRun = concurrentAttachedFunction.NextRun

			// Set lowestLiveNonce for the concurrent case
			if concurrentAttachedFunction.LowestLiveNonce != nil {
				// Already initialized, skip Phase 2 & 3
				lowestLiveNonce = *concurrentAttachedFunction.LowestLiveNonce
				skipPhase2And3 = true
			} else {
				// Not initialized yet, generate minimal UUIDv7 and continue to Phase 2 & 3
				lowestLiveNonce = minimalUUIDv7
			}
			return nil
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

		// Look up function by name
		function, err := s.catalog.metaDomain.FunctionDb(txCtx).GetByName(req.FunctionName)
		if err != nil {
			log.Error("AttachFunction: failed to get function", zap.Error(err))
			return err
		}
		if function == nil {
			log.Error("AttachFunction: function not found", zap.String("function_name", req.FunctionName))
			return common.ErrFunctionNotFound
		}

		// Generate next_nonce as UUIDv7 with current time
		nextNonce, err = uuid.NewV7()
		if err != nil {
			return err
		}

		// Set lowest_live_nonce to minimal UUIDv7 (guaranteed < nextNonce)
		lowestLiveNonce = minimalUUIDv7

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

		// Check if output collection already exists
		outputCollectionName := req.OutputCollectionName
		existingOutputCollections, err := s.catalog.metaDomain.CollectionDb(txCtx).GetCollections(nil, &outputCollectionName, req.TenantId, req.Database, nil, nil, false)
		if err != nil {
			log.Error("AttachFunction: failed to check output collection", zap.Error(err))
			return err
		}
		if len(existingOutputCollections) > 0 {
			log.Error("AttachFunction: output collection already exists")
			return common.ErrCollectionUniqueConstraintViolation
		}

		// Serialize params
		var paramsJSON string
		if req.Params != nil {
			paramsBytes, err := req.Params.MarshalJSON()
			if err != nil {
				log.Error("AttachFunction: failed to marshal params", zap.Error(err))
				return err
			}
			paramsJSON = string(paramsBytes)
		} else {
			paramsJSON = "{}"
		}

		now := time.Now()
		attachedFunction := &dbmodel.AttachedFunction{
			ID:                      attachedFunctionID,
			Name:                    req.Name,
			TenantID:                req.TenantId,
			DatabaseID:              databases[0].ID,
			InputCollectionID:       req.InputCollectionId,
			OutputCollectionName:    req.OutputCollectionName,
			FunctionID:              function.ID,
			FunctionParams:          paramsJSON,
			CompletionOffset:        0,
			LastRun:                 nil,
			NextRun:                 now,
			MinRecordsForInvocation: int64(req.MinRecordsForInvocation),
			CurrentAttempts:         0,
			CreatedAt:               now,
			UpdatedAt:               now,
			NextNonce:               nextNonce,
			LowestLiveNonce:         nil, // **KEY: Set to NULL for 2PC**
			OldestWrittenNonce:      nil,
		}

		nextRun = attachedFunction.NextRun

		err = s.catalog.metaDomain.AttachedFunctionDb(txCtx).Insert(attachedFunction)
		if err != nil {
			log.Error("AttachFunction: failed to insert attached function", zap.Error(err))
			return err
		}

		log.Debug("AttachFunction: Phase 1: attached function created with lowest_live_nonce=NULL",
			zap.String("attached_function_id", attachedFunctionID.String()),
			zap.String("name", req.Name))
		return nil
	})

	if err != nil {
		return nil, err
	}

	// If function is already fully attached, return immediately (idempotency)
	if skipPhase2And3 {
		log.Info("AttachFunction: function already fully attached, skipping Phase 2 & 3",
			zap.String("attached_function_id", attachedFunctionID.String()))
		return &coordinatorpb.AttachFunctionResponse{
			Id: attachedFunctionID.String(),
		}, nil
	}

	// ===== Phase 2 =====
	// This phase runs for both new attached functions and recovered incomplete attached functions
	log.Debug("AttachFunction: Phase 2: doing initialization work",
		zap.String("attached_function_id", attachedFunctionID.String()))
	// Push initial schedule to heap service if enabled
	if s.heapClient == nil {
		return nil, common.ErrHeapServiceNotEnabled
	}

	// Create schedule for the attached function
	schedule := &coordinatorpb.Schedule{
		Triggerable: &coordinatorpb.Triggerable{
			PartitioningUuid: req.InputCollectionId,
			SchedulingUuid:   attachedFunctionID.String(),
		},
		NextScheduled: timestamppb.New(nextRun),
		Nonce:         lowestLiveNonce.String(),
	}

	err = s.heapClient.Push(ctx, req.InputCollectionId, []*coordinatorpb.Schedule{schedule})
	if err != nil {
		log.Error("AttachFunction: Phase 2: failed to push schedule to heap service",
			zap.Error(err),
			zap.String("attached_function_id", attachedFunctionID.String()),
			zap.String("collection_id", req.InputCollectionId))
		return nil, err
	}

	log.Debug("AttachFunction: Phase 2: pushed schedule to heap service",
		zap.String("attached_function_id", attachedFunctionID.String()),
		zap.String("collection_id", req.InputCollectionId))

	// ===== Phase 3: Update lowest_live_nonce to complete initialization =====
	// No database fetch needed - we already have lowestLiveNonce and nextNonce from Phase 1/Recovery
	err = s.catalog.metaDomain.AttachedFunctionDb(ctx).UpdateLowestLiveNonce(attachedFunctionID, lowestLiveNonce)
	if err != nil {
		log.Error("AttachFunction: Phase 3: failed to update lowest_live_nonce", zap.Error(err), zap.String("attached_function_id", attachedFunctionID.String()), zap.String("lowest_live_nonce", lowestLiveNonce.String()))
		return nil, err
	}

	log.Debug("AttachFunction: Phase 3: attached function initialization completed",
		zap.String("attached_function_id", attachedFunctionID.String()),
		zap.String("lowest_live_nonce", lowestLiveNonce.String()),
		zap.String("next_nonce", nextNonce.String()))

	return &coordinatorpb.AttachFunctionResponse{
		Id: attachedFunctionID.String(),
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
		NextRunAt:               uint64(attachedFunction.NextRun.UnixMicro()),
		NextNonce:               attachedFunction.NextNonce.String(),
		CreatedAt:               uint64(attachedFunction.CreatedAt.UnixMicro()),
		UpdatedAt:               uint64(attachedFunction.UpdatedAt.UnixMicro()),
	}

	if attachedFunction.LowestLiveNonce != nil {
		val := attachedFunction.LowestLiveNonce.String()
		attachedFunctionProto.LowestLiveNonce = &val
	}
	if attachedFunction.OutputCollectionID != nil {
		attachedFunctionProto.OutputCollectionId = attachedFunction.OutputCollectionID
	}

	return attachedFunctionProto, nil
}

// GetAttachedFunctionByName retrieves an attached function by name from the database
func (s *Coordinator) GetAttachedFunctionByName(ctx context.Context, req *coordinatorpb.GetAttachedFunctionByNameRequest) (*coordinatorpb.GetAttachedFunctionByNameResponse, error) {
	// Can do both calls with a JOIN
	attachedFunction, err := s.catalog.metaDomain.AttachedFunctionDb(ctx).GetByName(req.InputCollectionId, req.Name)
	if err != nil {
		return nil, err
	}

	// If attached function not found, return empty response
	if attachedFunction == nil {
		return nil, common.ErrAttachedFunctionNotFound
	}

	// Look up function name from functions table
	function, err := s.catalog.metaDomain.FunctionDb(ctx).GetByID(attachedFunction.FunctionID)
	if err != nil {
		log.Error("GetAttachedFunctionByName: failed to get function", zap.Error(err))
		return nil, err
	}
	if function == nil {
		log.Error("GetAttachedFunctionByName: function not found", zap.String("function_id", attachedFunction.FunctionID.String()))
		return nil, common.ErrFunctionNotFound
	}

	// Debug logging
	log.Info("Found attached function", zap.String("attached_function_id", attachedFunction.ID.String()), zap.String("name", attachedFunction.Name), zap.String("input_collection_id", attachedFunction.InputCollectionID), zap.String("output_collection_name", attachedFunction.OutputCollectionName))

	attachedFunctionProto, err := attachedFunctionToProto(attachedFunction, function)
	if err != nil {
		log.Error("GetAttachedFunctionByName: failed to convert attached function to proto", zap.Error(err), zap.String("attached_function_id", attachedFunction.ID.String()))
		return nil, err
	}

	return &coordinatorpb.GetAttachedFunctionByNameResponse{
		AttachedFunction: attachedFunctionProto,
	}, nil
}

// ListAttachedFunctions retrieves all attached functions for a given collection
func (s *Coordinator) ListAttachedFunctions(ctx context.Context, req *coordinatorpb.ListAttachedFunctionsRequest) (*coordinatorpb.ListAttachedFunctionsResponse, error) {
	attachedFunctions, err := s.catalog.metaDomain.AttachedFunctionDb(ctx).GetByCollectionID(req.InputCollectionId)
	if err != nil {
		log.Error("ListAttachedFunctions: failed to get attached functions", zap.Error(err))
		return nil, err
	}

	if len(attachedFunctions) == 0 {
		return &coordinatorpb.ListAttachedFunctionsResponse{AttachedFunctions: []*coordinatorpb.AttachedFunction{}}, nil
	}

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
		log.Error("ListAttachedFunctions: failed to get functions", zap.Error(err))
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
			log.Error("ListAttachedFunctions: function not found", zap.String("function_id", functionID.String()))
			return nil, common.ErrFunctionNotFound
		}
	}

	protoFunctions := make([]*coordinatorpb.AttachedFunction, 0, len(attachedFunctions))

	for _, attachedFunction := range attachedFunctions {
		function := functionsByID[attachedFunction.FunctionID]

		attachedFunctionProto, err := attachedFunctionToProto(attachedFunction, function)
		if err != nil {
			log.Error("ListAttachedFunctions: failed to convert attached function to proto", zap.Error(err), zap.String("attached_function_id", attachedFunction.ID.String()))
			return nil, err
		}

		protoFunctions = append(protoFunctions, attachedFunctionProto)
	}

	log.Info("ListAttachedFunctions succeeded", zap.String("input_collection_id", req.InputCollectionId), zap.Int("count", len(protoFunctions)))

	return &coordinatorpb.ListAttachedFunctionsResponse{
		AttachedFunctions: protoFunctions,
	}, nil
}

// GetAttachedFunctionByUuid retrieves an attached function by UUID from the database
func (s *Coordinator) GetAttachedFunctionByUuid(ctx context.Context, req *coordinatorpb.GetAttachedFunctionByUuidRequest) (*coordinatorpb.GetAttachedFunctionByUuidResponse, error) {
	// Parse the attached function UUID
	attachedFunctionID, err := uuid.Parse(req.Id)
	if err != nil {
		log.Error("GetAttachedFunctionByUuid: invalid attached_function_id", zap.Error(err))
		return nil, status.Errorf(codes.InvalidArgument, "invalid attached_function_id: %v", err)
	}

	// Fetch attached function by ID
	attachedFunction, err := s.catalog.metaDomain.AttachedFunctionDb(ctx).GetByID(attachedFunctionID)
	if err != nil {
		// Map ErrAttachedFunctionNotReady to NotFound so it appears non-existent to clients
		if errors.Is(err, common.ErrAttachedFunctionNotReady) {
			return nil, status.Error(codes.NotFound, "attached function not ready")
		}
		return nil, err
	}

	// If attached function not found, return error
	if attachedFunction == nil {
		return nil, status.Error(codes.NotFound, "attached function not found")
	}

	// Look up function name from functions table
	function, err := s.catalog.metaDomain.FunctionDb(ctx).GetByID(attachedFunction.FunctionID)
	if err != nil {
		log.Error("GetAttachedFunctionByUuid: failed to get function", zap.Error(err))
		return nil, err
	}
	if function == nil {
		log.Error("GetAttachedFunctionByUuid: function not found", zap.String("function_id", attachedFunction.FunctionID.String()))
		return nil, common.ErrFunctionNotFound
	}

	// Debug logging
	log.Info("Found attached function by UUID", zap.String("attached_function_id", attachedFunction.ID.String()), zap.String("name", attachedFunction.Name), zap.String("input_collection_id", attachedFunction.InputCollectionID), zap.String("output_collection_name", attachedFunction.OutputCollectionName))

	attachedFunctionProto, err := attachedFunctionToProto(attachedFunction, function)
	if err != nil {
		log.Error("GetAttachedFunctionByUuid: failed to convert attached function to proto", zap.Error(err), zap.String("attached_function_id", attachedFunction.ID.String()))
		return nil, err
	}

	return &coordinatorpb.GetAttachedFunctionByUuidResponse{
		AttachedFunction: attachedFunctionProto,
	}, nil
}

// CreateOutputCollectionForAttachedFunction atomically creates an output collection and updates the attached function's output_collection_id
func (s *Coordinator) CreateOutputCollectionForAttachedFunction(ctx context.Context, req *coordinatorpb.CreateOutputCollectionForAttachedFunctionRequest) (*coordinatorpb.CreateOutputCollectionForAttachedFunctionResponse, error) {
	var collectionID types.UniqueID

	// Execute all operations in a transaction for atomicity
	err := s.catalog.txImpl.Transaction(ctx, func(txCtx context.Context) error {
		// 1. Parse attached function ID
		attachedFunctionID, err := uuid.Parse(req.AttachedFunctionId)
		if err != nil {
			log.Error("CreateOutputCollectionForAttachedFunction: invalid attached_function_id", zap.Error(err))
			return status.Errorf(codes.InvalidArgument, "invalid attached_function_id: %v", err)
		}

		// 2. Get the attached function to verify it exists and doesn't already have an output collection
		attachedFunction, err := s.catalog.metaDomain.AttachedFunctionDb(txCtx).GetByID(attachedFunctionID)
		if err != nil {
			log.Error("CreateOutputCollectionForAttachedFunction: failed to get attached function", zap.Error(err))
			return err
		}
		if attachedFunction == nil {
			log.Error("CreateOutputCollectionForAttachedFunction: attached function not found")
			return status.Errorf(codes.NotFound, "attached function not found")
		}

		// Check if output collection already exists
		if attachedFunction.OutputCollectionID != nil && *attachedFunction.OutputCollectionID != "" {
			log.Error("CreateOutputCollectionForAttachedFunction: output collection already exists",
				zap.String("existing_collection_id", *attachedFunction.OutputCollectionID))
			return status.Errorf(codes.AlreadyExists, "output collection already exists")
		}

		// 3. Generate new collection UUID
		collectionID = types.NewUniqueID()

		// 4. Look up database by ID to get its name
		database, err := s.catalog.metaDomain.DatabaseDb(txCtx).GetByID(req.DatabaseId)
		if err != nil {
			log.Error("CreateOutputCollectionForAttachedFunction: failed to get database", zap.Error(err))
			return err
		}
		if database == nil {
			log.Error("CreateOutputCollectionForAttachedFunction: database not found", zap.String("database_id", req.DatabaseId), zap.String("tenant_id", req.TenantId))
			return common.ErrDatabaseNotFound
		}

		// 5. Create the collection with segments
		// Set a default dimension to ensure segment writers can be initialized
		dimension := int32(1) // Default dimension for attached function output collections
		collection := &model.CreateCollection{
			ID:                   collectionID,
			Name:                 req.CollectionName,
			ConfigurationJsonStr: "{}", // Empty JSON object for default config
			TenantID:             req.TenantId,
			DatabaseName:         database.Name,
			Dimension:            &dimension,
			Metadata:             nil,
		}

		// Create segments for the collection (distributed setup)
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
			log.Error("CreateOutputCollectionForAttachedFunction: failed to create collection", zap.Error(err))
			return err
		}

		// 6. Update attached function with output_collection_id
		collectionIDStr := collectionID.String()
		err = s.catalog.metaDomain.AttachedFunctionDb(txCtx).UpdateOutputCollectionID(attachedFunctionID, &collectionIDStr)
		if err != nil {
			log.Error("CreateOutputCollectionForAttachedFunction: failed to update attached function", zap.Error(err))
			return err
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return &coordinatorpb.CreateOutputCollectionForAttachedFunctionResponse{
		CollectionId: collectionID.String(),
	}, nil
}

// DetachFunction soft deletes an attached function by ID
func (s *Coordinator) DetachFunction(ctx context.Context, req *coordinatorpb.DetachFunctionRequest) (*coordinatorpb.DetachFunctionResponse, error) {
	// Parse attached_function_id
	attachedFunctionID, err := uuid.Parse(req.AttachedFunctionId)
	if err != nil {
		log.Error("DetachFunction: invalid attached_function_id", zap.Error(err))
		return nil, status.Errorf(codes.InvalidArgument, "invalid attached_function_id: %v", err)
	}

	// First get the attached function to check if we need to delete the output collection
	attachedFunction, err := s.catalog.metaDomain.AttachedFunctionDb(ctx).GetByID(attachedFunctionID)
	if err != nil {
		// If attached function is not ready (lowest_live_nonce == NULL), treat it as not found
		if errors.Is(err, common.ErrAttachedFunctionNotReady) {
			log.Error("DetachFunction: attached function not ready (not initialized)")
			return nil, status.Error(codes.NotFound, "attached function not found")
		}
		log.Error("DetachFunction: failed to get attached function", zap.Error(err))
		return nil, err
	}
	if attachedFunction == nil {
		log.Error("DetachFunction: attached function not found")
		return nil, status.Errorf(codes.NotFound, "attached function not found")
	}

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

		// Now soft-delete the attached function
		err := s.catalog.metaDomain.AttachedFunctionDb(txCtx).SoftDeleteByID(attachedFunctionID)
		if err != nil {
			log.Error("DetachFunction: failed to delete attached function", zap.Error(err))
			return err
		}

		log.Info("DetachFunction: successfully deleted attached function", zap.String("attached_function_id", attachedFunctionID.String()))
		return nil
	})

	if err != nil {
		return nil, err
	}

	log.Info("Attached function deleted", zap.String("attached_function_id", req.AttachedFunctionId))

	return &coordinatorpb.DetachFunctionResponse{
		Success: true,
	}, nil
}

// Mark an attached function run as complete and set the nonce for the next run.
func (s *Coordinator) AdvanceAttachedFunction(ctx context.Context, req *coordinatorpb.AdvanceAttachedFunctionRequest) (*coordinatorpb.AdvanceAttachedFunctionResponse, error) {
	if req.Id == nil {
		log.Error("AdvanceAttachedFunction: id is required")
		return nil, status.Errorf(codes.InvalidArgument, "id is required")
	}

	if req.RunNonce == nil {
		log.Error("AdvanceAttachedFunction: run_nonce is required")
		return nil, status.Errorf(codes.InvalidArgument, "run_nonce is required")
	}

	attachedFunctionID, err := uuid.Parse(*req.Id)
	if err != nil {
		log.Error("AdvanceAttachedFunction: invalid attached_function_id", zap.Error(err))
		return nil, status.Errorf(codes.InvalidArgument, "invalid attached_function_id: %v", err)
	}

	runNonce, err := uuid.Parse(*req.RunNonce)
	if err != nil {
		log.Error("AdvanceAttachedFunction: invalid run_nonce", zap.Error(err))
		return nil, status.Errorf(codes.InvalidArgument, "invalid run_nonce: %v", err)
	}

	// Validate completion_offset fits in int64 before storing in database
	if *req.CompletionOffset > uint64(math.MaxInt64) { // math.MaxInt64
		log.Error("AdvanceAttachedFunction: completion_offset too large",
			zap.Uint64("completion_offset", *req.CompletionOffset))
		return nil, status.Errorf(codes.InvalidArgument,
			"completion_offset too large: %d", *req.CompletionOffset)
	}
	completionOffsetInt64 := int64(*req.CompletionOffset)

	advanceResult, err := s.catalog.metaDomain.AttachedFunctionDb(ctx).Advance(attachedFunctionID, runNonce, completionOffsetInt64, *req.NextRunDelaySecs)
	if err != nil {
		log.Error("AdvanceAttachedFunction failed", zap.Error(err), zap.String("attached_function_id", attachedFunctionID.String()))
		return nil, err
	}

	// Validate completion_offset from database is non-negative before converting to uint64
	if advanceResult.CompletionOffset < 0 {
		log.Error("AdvanceAttachedFunction: invalid completion_offset from database",
			zap.String("attached_function_id", attachedFunctionID.String()),
			zap.Int64("completion_offset", advanceResult.CompletionOffset))
		return nil, status.Errorf(codes.Internal,
			"attached function has invalid completion_offset: %d", advanceResult.CompletionOffset)
	}

	return &coordinatorpb.AdvanceAttachedFunctionResponse{
		NextRunNonce:     advanceResult.NextNonce.String(),
		NextRunAt:        uint64(advanceResult.NextRun.UnixMilli()),
		CompletionOffset: uint64(advanceResult.CompletionOffset),
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

// PeekScheduleByCollectionId gives, for a vector of collection IDs, a vector of schedule entries,
// including when to run and the nonce to use for said run.
func (s *Coordinator) PeekScheduleByCollectionId(ctx context.Context, req *coordinatorpb.PeekScheduleByCollectionIdRequest) (*coordinatorpb.PeekScheduleByCollectionIdResponse, error) {
	attachedFunctions, err := s.catalog.metaDomain.AttachedFunctionDb(ctx).PeekScheduleByCollectionId(req.CollectionId)
	if err != nil {
		log.Error("PeekScheduleByCollectionId failed", zap.Error(err))
		return nil, err
	}

	scheduleEntries := make([]*coordinatorpb.ScheduleEntry, 0, len(attachedFunctions))
	for _, attachedFunction := range attachedFunctions {
		attached_function_id := attachedFunction.ID.String()
		entry := &coordinatorpb.ScheduleEntry{
			CollectionId:       &attachedFunction.InputCollectionID,
			AttachedFunctionId: &attached_function_id,
			RunNonce:           proto.String(attachedFunction.NextNonce.String()),
			WhenToRun:          nil,
			LowestLiveNonce:    nil,
		}
		if !attachedFunction.NextRun.IsZero() {
			whenToRun := uint64(attachedFunction.NextRun.UnixMilli())
			entry.WhenToRun = &whenToRun
		}
		if attachedFunction.LowestLiveNonce != nil {
			entry.LowestLiveNonce = proto.String(attachedFunction.LowestLiveNonce.String())
		}
		scheduleEntries = append(scheduleEntries, entry)
	}

	return &coordinatorpb.PeekScheduleByCollectionIdResponse{
		Schedule: scheduleEntries,
	}, nil
}

func (s *Coordinator) FinishAttachedFunction(ctx context.Context, req *coordinatorpb.FinishAttachedFunctionRequest) (*coordinatorpb.FinishAttachedFunctionResponse, error) {
	attachedFunctionID, err := uuid.Parse(req.Id)
	if err != nil {
		log.Error("FinishAttachedFunction: invalid attached_function_id", zap.Error(err))
		return nil, err
	}

	err = s.catalog.metaDomain.AttachedFunctionDb(ctx).Finish(attachedFunctionID)
	if err != nil {
		log.Error("FinishAttachedFunction: failed to finish attached function", zap.Error(err))
		return nil, err
	}

	return &coordinatorpb.FinishAttachedFunctionResponse{}, nil
}

// CleanupExpiredPartialAttachedFunctions finds and soft deletes attached functions that were partially created
// (lowest_live_nonce IS NULL) and are older than the specified max age.
// This is used to clean up attached functions that got stuck during the 2-phase creation process.
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

// GetSoftDeletedAttachedFunctions retrieves attached functions that are soft deleted and were updated before the cutoff time
func (s *Coordinator) GetSoftDeletedAttachedFunctions(ctx context.Context, req *coordinatorpb.GetSoftDeletedAttachedFunctionsRequest) (*coordinatorpb.GetSoftDeletedAttachedFunctionsResponse, error) {
	log := log.With(zap.String("method", "GetSoftDeletedAttachedFunctions"))

	if req.CutoffTime == nil {
		log.Error("GetSoftDeletedAttachedFunctions: cutoff_time is required")
		return nil, status.Errorf(codes.InvalidArgument, "cutoff_time is required")
	}

	if req.Limit <= 0 {
		log.Error("GetSoftDeletedAttachedFunctions: limit must be greater than 0")
		return nil, status.Errorf(codes.InvalidArgument, "limit must be greater than 0")
	}

	cutoffTime := req.CutoffTime.AsTime()
	attachedFunctions, err := s.catalog.metaDomain.AttachedFunctionDb(ctx).GetSoftDeletedAttachedFunctions(cutoffTime, req.Limit)
	if err != nil {
		log.Error("GetSoftDeletedAttachedFunctions: failed to get soft deleted attached functions", zap.Error(err))
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

		protoAttachedFunctions[i].NextRunAt = uint64(af.NextRun.UnixMicro())
		if af.OutputCollectionID != nil {
			protoAttachedFunctions[i].OutputCollectionId = proto.String(*af.OutputCollectionID)
		}
	}

	log.Info("GetSoftDeletedAttachedFunctions: completed successfully",
		zap.Int("count", len(attachedFunctions)))

	return &coordinatorpb.GetSoftDeletedAttachedFunctionsResponse{
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
