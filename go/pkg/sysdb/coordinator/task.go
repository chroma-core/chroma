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

// validateTaskMatchesRequest validates that an existing task's parameters match the request parameters.
// Returns an error if any parameters don't match. This is used for idempotency and race condition handling.
func (s *Coordinator) validateTaskMatchesRequest(ctx context.Context, task *dbmodel.Task, req *coordinatorpb.CreateTaskRequest) error {
	// Look up the operator for the existing task
	existingOperator, err := s.catalog.metaDomain.OperatorDb(ctx).GetByID(task.OperatorID)
	if err != nil {
		log.Error("validateTaskMatchesRequest: failed to get task's operator", zap.Error(err))
		return err
	}
	if existingOperator == nil {
		log.Error("validateTaskMatchesRequest: task's operator not found")
		return common.ErrOperatorNotFound
	}

	// Look up database for comparison
	databases, err := s.catalog.metaDomain.DatabaseDb(ctx).GetDatabases(req.TenantId, req.Database)
	if err != nil {
		log.Error("validateTaskMatchesRequest: failed to get database for validation", zap.Error(err))
		return err
	}
	if len(databases) == 0 {
		log.Error("validateTaskMatchesRequest: database not found")
		return common.ErrDatabaseNotFound
	}

	// Validate attributes match
	if existingOperator.OperatorName != req.OperatorName {
		log.Error("validateTaskMatchesRequest: task has different operator",
			zap.String("existing", existingOperator.OperatorName),
			zap.String("requested", req.OperatorName))
		return status.Errorf(codes.AlreadyExists, "task already exists with different operator: existing=%s, requested=%s", existingOperator.OperatorName, req.OperatorName)
	}
	if task.TenantID != req.TenantId {
		log.Error("validateTaskMatchesRequest: task has different tenant")
		return status.Errorf(codes.AlreadyExists, "task already exists with different tenant")
	}
	if task.DatabaseID != databases[0].ID {
		log.Error("validateTaskMatchesRequest: task has different database")
		return status.Errorf(codes.AlreadyExists, "task already exists with different database")
	}
	if task.OutputCollectionName != req.OutputCollectionName {
		log.Error("validateTaskMatchesRequest: task has different output collection name",
			zap.String("existing", task.OutputCollectionName),
			zap.String("requested", req.OutputCollectionName))
		return status.Errorf(codes.AlreadyExists, "task already exists with different output collection: existing=%s, requested=%s", task.OutputCollectionName, req.OutputCollectionName)
	}
	if task.MinRecordsForTask != int64(req.MinRecordsForTask) {
		log.Error("validateTaskMatchesRequest: task has different min_records_for_task",
			zap.Int64("existing", task.MinRecordsForTask),
			zap.Uint64("requested", req.MinRecordsForTask))
		return status.Errorf(codes.AlreadyExists, "task already exists with different min_records_for_task: existing=%d, requested=%d", task.MinRecordsForTask, req.MinRecordsForTask)
	}

	return nil
}

// CreateTask creates a new task in the database
func (s *Coordinator) CreateTask(ctx context.Context, req *coordinatorpb.CreateTaskRequest) (*coordinatorpb.CreateTaskResponse, error) {
	log := log.With(zap.String("method", "CreateTask"))

	// Validate task name doesn't use reserved prefix
	if strings.HasPrefix(req.Name, "_deleted_") {
		log.Error("CreateTask: task name cannot start with _deleted_")
		return nil, common.ErrInvalidTaskName
	}

	var taskID uuid.UUID = uuid.New()
	var nextNonce uuid.UUID       // Store next_nonce to avoid re-fetching from DB
	var lowestLiveNonce uuid.UUID // Store lowest_live_nonce to set in Phase 3
	var nextRun time.Time
	var skipPhase2And3 bool // Flag to skip Phase 2 & 3 if task is already fully initialized

	// ===== Phase 1: Create task with lowest_live_nonce = NULL (if needed) =====
	err := s.catalog.txImpl.Transaction(ctx, func(txCtx context.Context) error {
		// Double-check task doesn't exist (race condition protection)
		concurrentTask, err := s.catalog.metaDomain.TaskDb(txCtx).GetByName(req.InputCollectionId, req.Name)
		if err != nil {
			log.Error("CreateTask: failed to double-check task", zap.Error(err))
			return err
		}
		if concurrentTask != nil {
			// Task was created concurrently, validate it matches our request
			log.Info("CreateTask: task created concurrently, validating parameters",
				zap.String("task_id", concurrentTask.ID.String()))

			// Validate that concurrent task matches our request
			if err := s.validateTaskMatchesRequest(txCtx, concurrentTask, req); err != nil {
				return err
			}

			// Validation passed, reuse the concurrent task's data
			taskID = concurrentTask.ID
			nextNonce = concurrentTask.NextNonce
			nextRun = concurrentTask.NextRun

			// Set lowestLiveNonce for the concurrent case
			if concurrentTask.LowestLiveNonce != nil {
				// Already initialized, skip Phase 2 & 3
				lowestLiveNonce = *concurrentTask.LowestLiveNonce
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
			log.Error("CreateTask: failed to get database", zap.Error(err))
			return err
		}
		if len(databases) == 0 {
			log.Error("CreateTask: database not found")
			return common.ErrDatabaseNotFound
		}

		// Look up operator by name
		operator, err := s.catalog.metaDomain.OperatorDb(txCtx).GetByName(req.OperatorName)
		if err != nil {
			log.Error("CreateTask: failed to get operator", zap.Error(err))
			return err
		}
		if operator == nil {
			log.Error("CreateTask: operator not found", zap.String("operator_name", req.OperatorName))
			return common.ErrOperatorNotFound
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
			log.Error("CreateTask: failed to get input collection", zap.Error(err))
			return err
		}
		if len(collections) == 0 {
			log.Error("CreateTask: input collection not found")
			return common.ErrCollectionNotFound
		}

		// Check if output collection already exists
		outputCollectionName := req.OutputCollectionName
		existingOutputCollections, err := s.catalog.metaDomain.CollectionDb(txCtx).GetCollections(nil, &outputCollectionName, req.TenantId, req.Database, nil, nil, false)
		if err != nil {
			log.Error("CreateTask: failed to check output collection", zap.Error(err))
			return err
		}
		if len(existingOutputCollections) > 0 {
			log.Error("CreateTask: output collection already exists")
			return common.ErrCollectionUniqueConstraintViolation
		}

		// Serialize params
		var paramsJSON string
		if req.Params != nil {
			paramsBytes, err := req.Params.MarshalJSON()
			if err != nil {
				log.Error("CreateTask: failed to marshal params", zap.Error(err))
				return err
			}
			paramsJSON = string(paramsBytes)
		} else {
			paramsJSON = "{}"
		}

		now := time.Now()
		task := &dbmodel.Task{
			ID:                   taskID,
			Name:                 req.Name,
			TenantID:             req.TenantId,
			DatabaseID:           databases[0].ID,
			InputCollectionID:    req.InputCollectionId,
			OutputCollectionName: req.OutputCollectionName,
			OperatorID:           operator.OperatorID,
			OperatorParams:       paramsJSON,
			CompletionOffset:     0,
			LastRun:              nil,
			NextRun:              now,
			MinRecordsForTask:    int64(req.MinRecordsForTask),
			CurrentAttempts:      0,
			CreatedAt:            now,
			UpdatedAt:            now,
			NextNonce:            nextNonce,
			LowestLiveNonce:      nil, // **KEY: Set to NULL for 2PC**
			OldestWrittenNonce:   nil,
		}

		nextRun = task.NextRun

		err = s.catalog.metaDomain.TaskDb(txCtx).Insert(task)
		if err != nil {
			log.Error("CreateTask: failed to insert task", zap.Error(err))
			return err
		}

		log.Info("CreateTask Phase 1: task created with lowest_live_nonce=NULL",
			zap.String("task_id", taskID.String()),
			zap.String("name", req.Name))
		return nil
	})

	if err != nil {
		return nil, err
	}

	// If task is already fully initialized, return immediately (idempotent request)
	if skipPhase2And3 {
		log.Info("CreateTask: task already fully initialized, skipping Phase 2 & 3",
			zap.String("task_id", taskID.String()))
		return &coordinatorpb.CreateTaskResponse{
			TaskId: taskID.String(),
		}, nil
	}

	// Phase 2
	// This phase runs for both new tasks and recovered incomplete tasks
	log.Info("CreateTask Phase 2: doing initialization work",
		zap.String("task_id", taskID.String()))

	// Push initial schedule to heap service if enabled
	if s.heapClient == nil {
		return nil, common.ErrHeapServiceNotEnabled
	}
	// Create schedule for the task
	schedule := &coordinatorpb.Schedule{
		Triggerable: &coordinatorpb.Triggerable{
			PartitioningUuid: req.InputCollectionId,
			SchedulingUuid:   taskID.String(),
		},
		NextScheduled: timestamppb.New(nextRun),
		Nonce:         lowestLiveNonce.String(),
	}

	err = s.heapClient.Push(ctx, req.InputCollectionId, []*coordinatorpb.Schedule{schedule})
	if err != nil {
		log.Error("CreateTask Phase 2: failed to push schedule to heap service",
			zap.Error(err),
			zap.String("task_id", taskID.String()),
			zap.String("collection_id", req.InputCollectionId))
		return nil, err
	}

	log.Info("CreateTask Phase 2: pushed schedule to heap service",
		zap.String("task_id", taskID.String()),
		zap.String("collection_id", req.InputCollectionId))

	// ===== Phase 3: Update lowest_live_nonce to complete initialization =====
	// No database fetch needed - we already have lowestLiveNonce and nextNonce from Phase 1/Recovery
	err = s.catalog.metaDomain.TaskDb(ctx).UpdateLowestLiveNonce(taskID, lowestLiveNonce)
	if err != nil {
		log.Error("CreateTask Phase 3: failed to update lowest_live_nonce", zap.Error(err))
		return nil, err
	}

	log.Info("CreateTask Phase 3: task initialization completed",
		zap.String("task_id", taskID.String()),
		zap.String("lowest_live_nonce", lowestLiveNonce.String()),
		zap.String("next_nonce", nextNonce.String()))

	return &coordinatorpb.CreateTaskResponse{
		TaskId: taskID.String(),
	}, nil
}

// GetTaskByName retrieves a task by name from the database
func (s *Coordinator) GetTaskByName(ctx context.Context, req *coordinatorpb.GetTaskByNameRequest) (*coordinatorpb.GetTaskByNameResponse, error) {
	// Can do both calls with a JOIN
	task, err := s.catalog.metaDomain.TaskDb(ctx).GetByName(req.InputCollectionId, req.TaskName)
	if err != nil {
		return nil, err
	}

	// If task not found, return empty response
	if task == nil {
		return nil, common.ErrTaskNotFound
	}

	// Look up operator name from operators table
	operator, err := s.catalog.metaDomain.OperatorDb(ctx).GetByID(task.OperatorID)
	if err != nil {
		log.Error("GetTaskByName: failed to get operator", zap.Error(err))
		return nil, err
	}
	if operator == nil {
		log.Error("GetTaskByName: operator not found", zap.String("operator_id", task.OperatorID.String()))
		return nil, common.ErrOperatorNotFound
	}

	// Debug logging
	log.Info("Found task", zap.String("task_id", task.ID.String()), zap.String("name", task.Name), zap.String("input_collection_id", task.InputCollectionID), zap.String("output_collection_name", task.OutputCollectionName))

	// Deserialize params from JSON string to protobuf Struct
	var paramsStruct *structpb.Struct
	if task.OperatorParams != "" {
		paramsStruct = &structpb.Struct{}
		if err := paramsStruct.UnmarshalJSON([]byte(task.OperatorParams)); err != nil {
			log.Error("GetTaskByName: failed to unmarshal params", zap.Error(err))
			return nil, err
		}
	}

	// Validate completion_offset is non-negative before converting to uint64
	if task.CompletionOffset < 0 {
		log.Error("GetTaskByName: invalid completion_offset",
			zap.String("task_id", task.ID.String()),
			zap.Int64("completion_offset", task.CompletionOffset))
		return nil, status.Errorf(codes.Internal,
			"task has invalid completion_offset: %d", task.CompletionOffset)
	}

	// Convert task to response with nested Task message
	taskProto := &coordinatorpb.Task{
		TaskId:               task.ID.String(),
		Name:                 task.Name,
		OperatorName:         operator.OperatorName,
		InputCollectionId:    task.InputCollectionID,
		OutputCollectionName: task.OutputCollectionName,
		Params:               paramsStruct,
		CompletionOffset:     uint64(task.CompletionOffset),
		MinRecordsForTask:    uint64(task.MinRecordsForTask),
		TenantId:             task.TenantID,
		DatabaseId:           task.DatabaseID,
		NextRunAt:            uint64(task.NextRun.UnixMicro()),
		LowestLiveNonce:      "",
		NextNonce:            task.NextNonce.String(),
		CreatedAt:            uint64(task.CreatedAt.UnixMicro()),
		UpdatedAt:            uint64(task.UpdatedAt.UnixMicro()),
	}
	// Add lowest_live_nonce if it's set
	if task.LowestLiveNonce != nil {
		taskProto.LowestLiveNonce = task.LowestLiveNonce.String()
	}
	// Add output_collection_id if it's set
	if task.OutputCollectionID != nil {
		taskProto.OutputCollectionId = task.OutputCollectionID
	}

	return &coordinatorpb.GetTaskByNameResponse{
		Task: taskProto,
	}, nil
}

// GetTaskByUuid retrieves a task by UUID from the database
func (s *Coordinator) GetTaskByUuid(ctx context.Context, req *coordinatorpb.GetTaskByUuidRequest) (*coordinatorpb.GetTaskByUuidResponse, error) {
	// Parse the task UUID
	taskID, err := uuid.Parse(req.TaskId)
	if err != nil {
		log.Error("GetTaskByUuid: invalid task_id", zap.Error(err))
		return nil, status.Errorf(codes.InvalidArgument, "invalid task_id: %v", err)
	}

	// Fetch task by ID
	task, err := s.catalog.metaDomain.TaskDb(ctx).GetByID(taskID)
	if err != nil {
		// Map ErrTaskNotReady to NotFound so it appears non-existent to clients
		if errors.Is(err, common.ErrTaskNotReady) {
			return nil, status.Error(codes.NotFound, "task not ready")
		}
		return nil, err
	}

	// If task not found, return error
	if task == nil {
		return nil, status.Error(codes.NotFound, "task not found")
	}

	// Look up operator name from operators table
	operator, err := s.catalog.metaDomain.OperatorDb(ctx).GetByID(task.OperatorID)
	if err != nil {
		log.Error("GetTaskByUuid: failed to get operator", zap.Error(err))
		return nil, err
	}
	if operator == nil {
		log.Error("GetTaskByUuid: operator not found", zap.String("operator_id", task.OperatorID.String()))
		return nil, common.ErrOperatorNotFound
	}

	// Debug logging
	log.Info("Found task by UUID", zap.String("task_id", task.ID.String()), zap.String("name", task.Name), zap.String("input_collection_id", task.InputCollectionID), zap.String("output_collection_name", task.OutputCollectionName))

	// Deserialize params from JSON string to protobuf Struct
	var paramsStruct *structpb.Struct
	if task.OperatorParams != "" {
		paramsStruct = &structpb.Struct{}
		if err := paramsStruct.UnmarshalJSON([]byte(task.OperatorParams)); err != nil {
			log.Error("GetTaskByUuid: failed to unmarshal params", zap.Error(err))
			return nil, err
		}
	}

	// Validate completion_offset is non-negative before converting to uint64
	if task.CompletionOffset < 0 {
		log.Error("GetTaskByUuid: invalid completion_offset",
			zap.String("task_id", task.ID.String()),
			zap.Int64("completion_offset", task.CompletionOffset))
		return nil, status.Errorf(codes.Internal,
			"task has invalid completion_offset: %d", task.CompletionOffset)
	}

	// Convert task to response with nested Task message
	taskProto := &coordinatorpb.Task{
		TaskId:               task.ID.String(),
		Name:                 task.Name,
		OperatorName:         operator.OperatorName,
		InputCollectionId:    task.InputCollectionID,
		OutputCollectionName: task.OutputCollectionName,
		Params:               paramsStruct,
		CompletionOffset:     uint64(task.CompletionOffset),
		MinRecordsForTask:    uint64(task.MinRecordsForTask),
		TenantId:             task.TenantID,
		DatabaseId:           task.DatabaseID,
		NextRunAt:            uint64(task.NextRun.UnixMicro()),
		LowestLiveNonce:      "",
		NextNonce:            task.NextNonce.String(),
		CreatedAt:            uint64(task.CreatedAt.UnixMicro()),
		UpdatedAt:            uint64(task.UpdatedAt.UnixMicro()),
	}
	// Add lowest_live_nonce if it's set
	if task.LowestLiveNonce != nil {
		taskProto.LowestLiveNonce = task.LowestLiveNonce.String()
	}
	// Add output_collection_id if it's set
	if task.OutputCollectionID != nil {
		taskProto.OutputCollectionId = task.OutputCollectionID
	}

	return &coordinatorpb.GetTaskByUuidResponse{
		Task: taskProto,
	}, nil
}

// CreateOutputCollectionForTask atomically creates an output collection and updates the task's output_collection_id
func (s *Coordinator) CreateOutputCollectionForTask(ctx context.Context, req *coordinatorpb.CreateOutputCollectionForTaskRequest) (*coordinatorpb.CreateOutputCollectionForTaskResponse, error) {
	var collectionID types.UniqueID

	// Execute all operations in a transaction for atomicity
	err := s.catalog.txImpl.Transaction(ctx, func(txCtx context.Context) error {
		// 1. Parse task ID
		taskID, err := uuid.Parse(req.TaskId)
		if err != nil {
			log.Error("CreateOutputCollectionForTask: invalid task_id", zap.Error(err))
			return status.Errorf(codes.InvalidArgument, "invalid task_id: %v", err)
		}

		// 2. Get the task to verify it exists and doesn't already have an output collection
		task, err := s.catalog.metaDomain.TaskDb(txCtx).GetByID(taskID)
		if err != nil {
			log.Error("CreateOutputCollectionForTask: failed to get task", zap.Error(err))
			return err
		}
		if task == nil {
			log.Error("CreateOutputCollectionForTask: task not found")
			return status.Errorf(codes.NotFound, "task not found")
		}

		// Check if output collection already exists
		if task.OutputCollectionID != nil && *task.OutputCollectionID != "" {
			log.Error("CreateOutputCollectionForTask: output collection already exists",
				zap.String("existing_collection_id", *task.OutputCollectionID))
			return status.Errorf(codes.AlreadyExists, "output collection already exists")
		}

		// 3. Generate new collection UUID
		collectionID = types.NewUniqueID()

		// 4. Look up database by ID to get its name
		database, err := s.catalog.metaDomain.DatabaseDb(txCtx).GetByID(req.DatabaseId)
		if err != nil {
			log.Error("CreateOutputCollectionForTask: failed to get database", zap.Error(err))
			return err
		}
		if database == nil {
			log.Error("CreateOutputCollectionForTask: database not found", zap.String("database_id", req.DatabaseId), zap.String("tenant_id", req.TenantId))
			return common.ErrDatabaseNotFound
		}

		// 5. Create the collection with segments
		// Set a default dimension to ensure segment writers can be initialized
		dimension := int32(1) // Default dimension for task output collections
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
			log.Error("CreateOutputCollectionForTask: failed to create collection", zap.Error(err))
			return err
		}

		// 6. Update task with output_collection_id
		collectionIDStr := collectionID.String()
		err = s.catalog.metaDomain.TaskDb(txCtx).UpdateOutputCollectionID(taskID, &collectionIDStr)
		if err != nil {
			log.Error("CreateOutputCollectionForTask: failed to update task", zap.Error(err))
			return err
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return &coordinatorpb.CreateOutputCollectionForTaskResponse{
		CollectionId: collectionID.String(),
	}, nil
}

// DeleteTask soft deletes a task by name
func (s *Coordinator) DeleteTask(ctx context.Context, req *coordinatorpb.DeleteTaskRequest) (*coordinatorpb.DeleteTaskResponse, error) {
	// First get the task to check if we need to delete the output collection
	task, err := s.catalog.metaDomain.TaskDb(ctx).GetByName(req.InputCollectionId, req.TaskName)
	if err != nil {
		// If task is not ready (lowest_live_nonce == NULL), treat it as not found
		if errors.Is(err, common.ErrTaskNotReady) {
			log.Error("DeleteTask: task not ready (not initialized)")
			return nil, status.Error(codes.NotFound, "task not found")
		}
		log.Error("DeleteTask: failed to get task", zap.Error(err))
		return nil, err
	}
	if task == nil {
		log.Error("DeleteTask: task not found")
		return nil, status.Errorf(codes.NotFound, "task not found")
	}

	// Execute collection and task deletion in a single transaction
	err = s.catalog.txImpl.Transaction(ctx, func(txCtx context.Context) error {
		// If delete_output is true and output_collection_id is set, soft-delete the output collection
		if req.DeleteOutput && task.OutputCollectionID != nil && *task.OutputCollectionID != "" {
			collectionUUID, err := types.ToUniqueID(task.OutputCollectionID)
			if err != nil {
				log.Error("DeleteTask: invalid output_collection_id", zap.Error(err))
				return status.Errorf(codes.InvalidArgument, "invalid output_collection_id: %v", err)
			}

			deleteCollection := &model.DeleteCollection{
				ID:       collectionUUID,
				TenantID: task.TenantID,
				// Database name isn't available but also isn't needed since we supplied a collection id
				DatabaseName: "",
			}

			err = s.SoftDeleteCollection(txCtx, deleteCollection)
			if err != nil {
				// If collection doesn't exist, that's fine - still delete the task
				if errors.Is(err, common.ErrCollectionDeleteNonExistingCollection) {
					log.Info("DeleteTask: output collection already deleted", zap.String("collection_id", *task.OutputCollectionID))
				} else {
					// Other errors should fail the transaction
					log.Error("DeleteTask: failed to delete output collection", zap.Error(err), zap.String("collection_id", *task.OutputCollectionID))
					return err
				}
			} else {
				log.Info("DeleteTask: deleted output collection", zap.String("collection_id", *task.OutputCollectionID))
			}
		}

		// Now soft-delete the task
		err := s.catalog.metaDomain.TaskDb(txCtx).SoftDelete(req.InputCollectionId, req.TaskName)
		if err != nil {
			log.Error("DeleteTask: failed to delete task", zap.Error(err))
			return err
		}

		log.Info("DeleteTask: successfully deleted task", zap.String("task_name", req.TaskName))
		return nil
	})

	if err != nil {
		return nil, err
	}

	log.Info("Task deleted", zap.String("input_collection_id", req.InputCollectionId), zap.String("task_name", req.TaskName))

	return &coordinatorpb.DeleteTaskResponse{
		Success: true,
	}, nil
}

// Mark a task run as complete and set the nonce for the next task run.
func (s *Coordinator) AdvanceTask(ctx context.Context, req *coordinatorpb.AdvanceTaskRequest) (*coordinatorpb.AdvanceTaskResponse, error) {
	if req.TaskId == nil {
		log.Error("AdvanceTask: task_id is required")
		return nil, status.Errorf(codes.InvalidArgument, "task_id is required")
	}

	if req.TaskRunNonce == nil {
		log.Error("AdvanceTask: task_run_nonce is required")
		return nil, status.Errorf(codes.InvalidArgument, "task_run_nonce is required")
	}

	taskID, err := uuid.Parse(*req.TaskId)
	if err != nil {
		log.Error("AdvanceTask: invalid task_id", zap.Error(err))
		return nil, status.Errorf(codes.InvalidArgument, "invalid task_id: %v", err)
	}

	taskRunNonce, err := uuid.Parse(*req.TaskRunNonce)
	if err != nil {
		log.Error("AdvanceTask: invalid task_run_nonce", zap.Error(err))
		return nil, status.Errorf(codes.InvalidArgument, "invalid task_run_nonce: %v", err)
	}

	// Validate completion_offset fits in int64 before storing in database
	if *req.CompletionOffset > uint64(math.MaxInt64) { // math.MaxInt64
		log.Error("AdvanceTask: completion_offset too large",
			zap.Uint64("completion_offset", *req.CompletionOffset))
		return nil, status.Errorf(codes.InvalidArgument,
			"completion_offset too large: %d", *req.CompletionOffset)
	}
	completionOffsetInt64 := int64(*req.CompletionOffset)

	advanceTask, err := s.catalog.metaDomain.TaskDb(ctx).AdvanceTask(taskID, taskRunNonce, completionOffsetInt64, *req.NextRunDelaySecs)
	if err != nil {
		log.Error("AdvanceTask failed", zap.Error(err), zap.String("task_id", taskID.String()))
		return nil, err
	}

	// Validate completion_offset from database is non-negative before converting to uint64
	if advanceTask.CompletionOffset < 0 {
		log.Error("AdvanceTask: invalid completion_offset from database",
			zap.String("task_id", taskID.String()),
			zap.Int64("completion_offset", advanceTask.CompletionOffset))
		return nil, status.Errorf(codes.Internal,
			"task has invalid completion_offset: %d", advanceTask.CompletionOffset)
	}

	return &coordinatorpb.AdvanceTaskResponse{
		NextRunNonce:     advanceTask.NextNonce.String(),
		NextRunAt:        uint64(advanceTask.NextRun.UnixMilli()),
		CompletionOffset: uint64(advanceTask.CompletionOffset),
	}, nil
}

// GetOperators retrieves all operators from the database
func (s *Coordinator) GetOperators(ctx context.Context, req *coordinatorpb.GetOperatorsRequest) (*coordinatorpb.GetOperatorsResponse, error) {
	operators, err := s.catalog.metaDomain.OperatorDb(ctx).GetAll()
	if err != nil {
		log.Error("GetOperators failed", zap.Error(err))
		return nil, err
	}

	// Convert to proto response
	protoOperators := make([]*coordinatorpb.Operator, len(operators))
	for i, op := range operators {
		protoOperators[i] = &coordinatorpb.Operator{
			Id:   op.OperatorID.String(),
			Name: op.OperatorName,
		}
	}

	log.Info("GetOperators succeeded", zap.Int("count", len(operators)))

	return &coordinatorpb.GetOperatorsResponse{
		Operators: protoOperators,
	}, nil
}

// PeekScheduleByCollectionId gives, for a vector of collection IDs, a vector of schedule entries,
// including when to run and the nonce to use for said run.
func (s *Coordinator) PeekScheduleByCollectionId(ctx context.Context, req *coordinatorpb.PeekScheduleByCollectionIdRequest) (*coordinatorpb.PeekScheduleByCollectionIdResponse, error) {
	tasks, err := s.catalog.metaDomain.TaskDb(ctx).PeekScheduleByCollectionId(req.CollectionId)
	if err != nil {
		log.Error("PeekScheduleByCollectionId failed", zap.Error(err))
		return nil, err
	}

	scheduleEntries := make([]*coordinatorpb.ScheduleEntry, 0, len(tasks))
	for _, task := range tasks {
		task_id := task.ID.String()
		entry := &coordinatorpb.ScheduleEntry{
			CollectionId:    &task.InputCollectionID,
			TaskId:          &task_id,
			TaskRunNonce:    proto.String(task.NextNonce.String()),
			WhenToRun:       nil,
			LowestLiveNonce: nil,
		}
		if !task.NextRun.IsZero() {
			whenToRun := uint64(task.NextRun.UnixMilli())
			entry.WhenToRun = &whenToRun
		}
		if task.LowestLiveNonce != nil {
			entry.LowestLiveNonce = proto.String(task.LowestLiveNonce.String())
		}
		scheduleEntries = append(scheduleEntries, entry)
	}

	return &coordinatorpb.PeekScheduleByCollectionIdResponse{
		Schedule: scheduleEntries,
	}, nil
}

func (s *Coordinator) FinishTask(ctx context.Context, req *coordinatorpb.FinishTaskRequest) (*coordinatorpb.FinishTaskResponse, error) {
	taskID, err := uuid.Parse(req.TaskId)
	if err != nil {
		log.Error("FinishTask: invalid task_id", zap.Error(err))
		return nil, err
	}

	err = s.catalog.metaDomain.TaskDb(ctx).FinishTask(taskID)
	if err != nil {
		log.Error("FinishTask: failed to fin task", zap.Error(err))
		return nil, err
	}

	return &coordinatorpb.FinishTaskResponse{}, nil
}

// CleanupExpiredPartialTasks finds and soft deletes tasks that were partially created
// (lowest_live_nonce IS NULL) and are older than the specified max age.
// This is used to clean up tasks that got stuck during the 2-phase creation process.
func (s *Coordinator) CleanupExpiredPartialTasks(ctx context.Context, req *coordinatorpb.CleanupExpiredPartialTasksRequest) (*coordinatorpb.CleanupExpiredPartialTasksResponse, error) {
	log := log.With(zap.String("method", "CleanupExpiredPartialTasks"))

	if req.MaxAgeSeconds == 0 {
		log.Error("CleanupExpiredPartialTasks: max_age_seconds must be greater than 0")
		return nil, status.Errorf(codes.InvalidArgument, "max_age_seconds must be greater than 0")
	}

	log.Info("CleanupExpiredPartialTasks: starting cleanup",
		zap.Uint64("max_age_seconds", req.MaxAgeSeconds))

	cleanedTaskIDs, err := s.catalog.metaDomain.TaskDb(ctx).CleanupExpiredPartialTasks(req.MaxAgeSeconds)
	if err != nil {
		log.Error("CleanupExpiredPartialTasks: failed to cleanup tasks", zap.Error(err))
		return nil, err
	}

	// Convert UUIDs to strings for response
	cleanedTaskIDStrings := make([]string, len(cleanedTaskIDs))
	for i, taskID := range cleanedTaskIDs {
		cleanedTaskIDStrings[i] = taskID.String()
	}

	log.Info("CleanupExpiredPartialTasks: completed successfully",
		zap.Uint64("cleaned_up_count", uint64(len(cleanedTaskIDs))))

	return &coordinatorpb.CleanupExpiredPartialTasksResponse{
		CleanedUpCount:   uint64(len(cleanedTaskIDs)),
		CleanedUpTaskIds: cleanedTaskIDStrings,
	}, nil
}
