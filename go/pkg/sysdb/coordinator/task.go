package coordinator

import (
	"context"
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

// CreateTask creates a new task in the database
func (s *Coordinator) CreateTask(ctx context.Context, req *coordinatorpb.CreateTaskRequest) (*coordinatorpb.CreateTaskResponse, error) {
	// Validate task name doesn't start with soft-deletion reserved prefix
	if strings.HasPrefix(req.Name, "_deleted_") {
		log.Error("CreateTask: task name cannot start with _deleted_")
		return nil, common.ErrInvalidTaskName
	}

	var taskID uuid.UUID

	// Execute all database operations in a transaction
	err := s.catalog.txImpl.Transaction(ctx, func(txCtx context.Context) error {
		// Check if task already exists
		existingTask, err := s.catalog.metaDomain.TaskDb(txCtx).GetByName(req.InputCollectionId, req.Name)
		if err != nil {
			log.Error("CreateTask: failed to check task", zap.Error(err))
			return err
		}
		if existingTask != nil {
			log.Error("CreateTask: task already exists", zap.String("task_name", req.Name))
			return common.ErrTaskAlreadyExists
		}

		// Generate new task UUID
		taskID = uuid.New()
		outputCollectionName := req.OutputCollectionName

		// Look up database_id from databases table using database name and tenant
		databases, err := s.catalog.metaDomain.DatabaseDb(txCtx).GetDatabases(req.TenantId, req.Database)
		if err != nil {
			log.Error("CreateTask: failed to get database", zap.Error(err))
			return err
		}
		if len(databases) == 0 {
			log.Error("CreateTask: database not found")
			return common.ErrDatabaseNotFound
		}

		// Look up operator by name from the operators table
		operator, err := s.catalog.metaDomain.OperatorDb(txCtx).GetByName(req.OperatorName)
		if err != nil {
			log.Error("CreateTask: failed to get operator", zap.Error(err))
			return err
		}
		if operator == nil {
			log.Error("CreateTask: operator not found", zap.String("operator_name", req.OperatorName))
			return common.ErrOperatorNotFound
		}
		operatorID := operator.OperatorID

		// Generate UUIDv7 for time-ordered nonce
		nextNonce, err := uuid.NewV7()
		if err != nil {
			return err
		}

		// TODO(tanujnay112): Can combine the two collection checks into one
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
		existingOutputCollections, err := s.catalog.metaDomain.CollectionDb(txCtx).GetCollections(nil, &outputCollectionName, req.TenantId, req.Database, nil, nil, false)
		if err != nil {
			log.Error("CreateTask: failed to check output collection", zap.Error(err))
			return err
		}
		if len(existingOutputCollections) > 0 {
			log.Error("CreateTask: output collection already exists")
			return common.ErrCollectionUniqueConstraintViolation
		}

		// Serialize params from protobuf Struct to JSON string for database storage
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
			OperatorID:           operatorID,
			OperatorParams:       paramsJSON,
			CompletionOffset:     0,
			LastRun:              nil,
			NextRun:              nil, // Will be set to zero initially, scheduled by task scheduler
			MinRecordsForTask:    int64(req.MinRecordsForTask),
			CurrentAttempts:      0,
			CreatedAt:            now,
			UpdatedAt:            now,
			NextNonce:            nextNonce,
			OldestWrittenNonce:   nil,
		}

		// Try to insert task into database
		err = s.catalog.metaDomain.TaskDb(txCtx).Insert(task)
		if err != nil {
			// Check if it's a unique constraint violation (concurrent creation)
			if err == common.ErrTaskAlreadyExists {
				log.Error("CreateTask: task already exists")
				return common.ErrTaskAlreadyExists
			}
			log.Error("CreateTask: failed to insert task", zap.Error(err))
			return err
		}

		log.Info("Task created successfully", zap.String("task_id", taskID.String()), zap.String("name", req.Name), zap.String("output_collection_name", outputCollectionName))
		return nil
	})

	if err != nil {
		return nil, err
	}

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

	// Convert task to response
	response := &coordinatorpb.GetTaskByNameResponse{
		TaskId:               proto.String(task.ID.String()),
		Name:                 proto.String(task.Name),
		OperatorName:         proto.String(operator.OperatorName),
		InputCollectionId:    proto.String(task.InputCollectionID),
		OutputCollectionName: proto.String(task.OutputCollectionName),
		Params:               paramsStruct,
		CompletionOffset:     proto.Int64(task.CompletionOffset),
		MinRecordsForTask:    proto.Uint64(uint64(task.MinRecordsForTask)),
		TenantId:             proto.String(task.TenantID),
		DatabaseId:           proto.String(task.DatabaseID),
	}
	// Add output_collection_id if it's set
	if task.OutputCollectionID != nil {
		response.OutputCollectionId = task.OutputCollectionID
	}
	return response, nil
}

// DeleteTask soft deletes a task by name
func (s *Coordinator) DeleteTask(ctx context.Context, req *coordinatorpb.DeleteTaskRequest) (*coordinatorpb.DeleteTaskResponse, error) {
	// First get the task to check if we need to delete the output collection
	task, err := s.catalog.metaDomain.TaskDb(ctx).GetByName(req.InputCollectionId, req.TaskName)
	if err != nil {
		log.Error("DeleteTask: failed to get task", zap.Error(err))
		return nil, err
	}
	if task == nil {
		log.Error("DeleteTask: task not found")
		return nil, status.Errorf(codes.NotFound, "task not found")
	}

	// If delete_output is true and output_collection_id is set, soft-delete the output collection
	if req.DeleteOutput && task.OutputCollectionID != nil && *task.OutputCollectionID != "" {
		collectionUUID, err := types.ToUniqueID(task.OutputCollectionID)
		if err != nil {
			log.Error("DeleteTask: invalid output_collection_id", zap.Error(err))
			return nil, status.Errorf(codes.InvalidArgument, "invalid output_collection_id: %v", err)
		}

		deleteCollection := &model.DeleteCollection{
			ID:       collectionUUID,
			TenantID: task.TenantID,
			// Database name isn't available but also isn't needed since we supplied a collection id
			DatabaseName: "",
		}

		err = s.SoftDeleteCollection(ctx, deleteCollection)
		if err != nil {
			// Log but don't fail - we still want to delete the task
			log.Warn("DeleteTask: failed to delete output collection", zap.Error(err), zap.String("collection_id", *task.OutputCollectionID))
		} else {
			log.Info("DeleteTask: deleted output collection", zap.String("collection_id", *task.OutputCollectionID))
		}
	}

	// Now soft-delete the task
	err = s.catalog.metaDomain.TaskDb(ctx).SoftDelete(req.InputCollectionId, req.TaskName)
	if err != nil {
		log.Error("DeleteTask failed", zap.Error(err))
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

	err = s.catalog.metaDomain.TaskDb(ctx).AdvanceTask(taskID, taskRunNonce)
	if err != nil {
		log.Error("AdvanceTask failed", zap.Error(err), zap.String("task_id", taskID.String()))
		return nil, err
	}

	return &coordinatorpb.AdvanceTaskResponse{}, nil
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
			CollectionId:  &task.InputCollectionID,
			TaskId:        &task_id,
			TaskRunNonce:  proto.String(task.NextNonce.String()),
			WhenToRun:     nil,
		}
		if task.NextRun != nil {
			whenToRun := uint64(task.NextRun.UnixMilli())
			entry.WhenToRun = &whenToRun
		}
		scheduleEntries = append(scheduleEntries, entry)
	}

	return &coordinatorpb.PeekScheduleByCollectionIdResponse{
		Schedule: scheduleEntries,
	}, nil
}
