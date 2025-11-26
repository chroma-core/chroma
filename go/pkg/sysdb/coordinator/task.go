package coordinator

import (
	"context"
	"errors"
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
)

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

// AttachFunction creates an output collection and attached function in a single transaction
func (s *Coordinator) AttachFunction(ctx context.Context, req *coordinatorpb.AttachFunctionRequest) (*coordinatorpb.AttachFunctionResponse, error) {
	log := log.With(zap.String("method", "AttachFunction"))

	// Validate attached function name doesn't use reserved prefix
	if strings.HasPrefix(req.Name, "_deleted_") {
		log.Error("AttachFunction: attached function name cannot start with _deleted_")
		return nil, common.ErrInvalidAttachedFunctionName
	}

	var attachedFunctionID uuid.UUID = uuid.New()

	// ===== Step 1: Create attached function with is_ready = false =====
	err := s.catalog.txImpl.Transaction(ctx, func(txCtx context.Context) error {
		// Double-check attached function doesn't exist (check both ready and not-ready)
		concurrentAttachedFunction, err := s.catalog.metaDomain.AttachedFunctionDb(txCtx).GetAnyByName(req.InputCollectionId, req.Name)
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

			// Validation passed, reuse the concurrent attached function ID (idempotent)
			attachedFunctionID = concurrentAttachedFunction.ID
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

		// Create attached function
		now := time.Now()
		attachedFunction := &dbmodel.AttachedFunction{
			ID:                      attachedFunctionID,
			Name:                    req.Name,
			TenantID:                req.TenantId,
			DatabaseID:              databases[0].ID,
			InputCollectionID:       req.InputCollectionId,
			OutputCollectionName:    req.OutputCollectionName,
			OutputCollectionID:      nil,
			FunctionID:              function.ID,
			FunctionParams:          paramsJSON,
			CompletionOffset:        0,
			LastRun:                 nil,
			MinRecordsForInvocation: int64(req.MinRecordsForInvocation),
			CurrentAttempts:         0,
			CreatedAt:               now,
			UpdatedAt:               now,
			OldestWrittenNonce:      nil,
			IsReady:                 false, // We will later set this to true in FinishAttachFunction
		}

		err = s.catalog.metaDomain.AttachedFunctionDb(txCtx).Insert(attachedFunction)
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

	// Validate that the input_collection_id matches the attached function's collection
	// This prevents silent failures where a user provides a valid function ID but wrong collection ID
	if attachedFunction.InputCollectionID != req.InputCollectionId {
		log.Error("DetachFunction: input_collection_id mismatch",
			zap.String("expected", attachedFunction.InputCollectionID),
			zap.String("provided", req.InputCollectionId))
		return nil, status.Error(codes.NotFound, "attached function not found")
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
		err := s.catalog.metaDomain.AttachedFunctionDb(txCtx).SoftDeleteByID(attachedFunctionID, req.InputCollectionId)
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

	// Execute all operations in a transaction for atomicity
	err = s.catalog.txImpl.Transaction(ctx, func(txCtx context.Context) error {
		// 1. Get the attached function to retrieve metadata
		attachedFunction, err := s.catalog.metaDomain.AttachedFunctionDb(txCtx).GetAnyByID(attachedFunctionID)
		if err != nil {
			log.Error("FinishCreateAttachedFunction: failed to get attached function", zap.Error(err))
			return err
		}
		if attachedFunction == nil {
			log.Error("FinishCreateAttachedFunction: attached function not found")
			return status.Errorf(codes.NotFound, "attached function not found")
		}

		// 2. Check if output collection already exists (idempotency)
		if attachedFunction.IsReady {
			log.Info("FinishCreateAttachedFunction: attached function is already ready", zap.String("attached_function_id", attachedFunctionID.String()))
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

		// 4. Generate new collection UUID
		collectionID := types.NewUniqueID()

		// 5. Create the output collection with segments
		dimension := int32(1) // Default dimension for attached function output collections
		collection := &model.CreateCollection{
			ID:                   collectionID,
			Name:                 attachedFunction.OutputCollectionName,
			ConfigurationJsonStr: "{}", // Empty JSON object for default config
			TenantID:             attachedFunction.TenantID,
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
			log.Error("FinishCreateAttachedFunction: failed to create output collection", zap.Error(err))
			return err
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

		log.Info("FinishCreateAttachedFunction: successfully created output collection and set is_ready=true",
			zap.String("attached_function_id", attachedFunctionID.String()),
			zap.String("output_collection_id", collectionID.String()))
		return nil
	})

	if err != nil {
		return nil, err
	}

	return &coordinatorpb.FinishCreateAttachedFunctionResponse{}, nil
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
