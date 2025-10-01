package coordinator

import (
	"context"

	"github.com/chroma-core/chroma/go/pkg/proto/coordinatorpb"
	"github.com/chroma-core/chroma/go/pkg/sysdb/metastore/db/dbmodel"
	"github.com/google/uuid"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

// CreateTask creates a new task in the database
func (s *Coordinator) CreateTask(ctx context.Context, req *coordinatorpb.CreateTaskRequest) (*coordinatorpb.CreateTaskResponse, error) {
	// Generate new task UUID
	taskID := uuid.New()

	// Look up operator by name to get its UUID
	// For now, we'll use a hardcoded mapping. In the future, this should query the operators table
	var operatorID uuid.UUID
	var err error
	switch req.OperatorId {
	case "record_counter":
		operatorID = uuid.MustParse("00000000-0000-0000-0000-000000000001")
	default:
		// Try to parse as UUID for backward compatibility
		operatorID, err = uuid.Parse(req.OperatorId)
		if err != nil {
			return nil, err
		}
	}

	// Generate UUIDv7 for time-ordered nonce
	nextNonce, err := uuid.NewV7()
	if err != nil {
		return nil, err
	}

	// Create the task model
	task := &dbmodel.Task{
		ID:                 taskID,
		Name:               req.Name,
		TenantID:           req.TenantId,
		DatabaseID:         req.DatabaseId,
		InputCollectionID:  req.InputCollectionId,
		OutputCollectionID: req.OutputCollectionId,
		OperatorID:         operatorID,
		OperatorParams:     req.Params,
		CompletionOffset:   0,
		LastRun:            nil,
		NextRun:            nil, // Will be scheduled by task scheduler
		MinRecordsForTask:  int64(req.MinRecordsForTask),
		CurrentAttempts:    0,
		NextNonce:          nextNonce,
		OldestWrittenNonce: nil,
	}

	// Insert into database using the DAO
	err = s.catalog.metaDomain.TaskDb(ctx).Insert(task)
	if err != nil {
		return nil, err
	}

	log.Info("Task created", zap.String("task_id", taskID.String()), zap.String("name", req.Name))

	return &coordinatorpb.CreateTaskResponse{
		TaskId: taskID.String(),
	}, nil
}

// GetTaskByName retrieves a task by name from the database
func (s *Coordinator) GetTaskByName(ctx context.Context, req *coordinatorpb.GetTaskByNameRequest) (*coordinatorpb.GetTaskByNameResponse, error) {
	task, err := s.catalog.metaDomain.TaskDb(ctx).GetByName(req.TenantId, req.DatabaseId, req.TaskName)
	if err != nil {
		return nil, err
	}

	// If task not found, return empty response
	if task == nil {
		return &coordinatorpb.GetTaskByNameResponse{}, nil
	}

	// Debug logging
	log.Info("Found task", zap.String("task_id", task.ID.String()), zap.String("name", task.Name), zap.String("input_collection_id", task.InputCollectionID), zap.String("output_collection_id", task.OutputCollectionID))

	// Convert task to response
	return &coordinatorpb.GetTaskByNameResponse{
		TaskId:             proto.String(task.ID.String()),
		Name:               proto.String(task.Name),
		OperatorId:         proto.String(task.OperatorID.String()),
		InputCollectionId:  proto.String(task.InputCollectionID),
		OutputCollectionId: proto.String(task.OutputCollectionID),
		Params:             proto.String(task.OperatorParams),
		CompletionOffset:   proto.Int64(task.CompletionOffset),
		MinRecordsForTask:  proto.Uint64(uint64(task.MinRecordsForTask)),
	}, nil
}

// DeleteTask soft deletes a task by name
func (s *Coordinator) DeleteTask(ctx context.Context, req *coordinatorpb.DeleteTaskRequest) (*coordinatorpb.DeleteTaskResponse, error) {
	err := s.catalog.metaDomain.TaskDb(ctx).SoftDelete(req.TenantId, req.DatabaseId, req.TaskName)
	if err != nil {
		log.Error("DeleteTask failed", zap.Error(err))
		return nil, err
	}

	log.Info("Task deleted", zap.String("tenant_id", req.TenantId), zap.String("database_id", req.DatabaseId), zap.String("task_name", req.TaskName))

	return &coordinatorpb.DeleteTaskResponse{
		Success: true,
	}, nil
}
