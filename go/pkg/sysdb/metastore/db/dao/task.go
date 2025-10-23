package dao

import (
	"errors"
	"time"

	"github.com/chroma-core/chroma/go/pkg/common"
	"github.com/chroma-core/chroma/go/pkg/sysdb/metastore/db/dbmodel"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type taskDb struct {
	db *gorm.DB
}

var _ dbmodel.ITaskDb = &taskDb{}

func (s *taskDb) DeleteAll() error {
	return s.db.Where("1 = 1").Delete(&dbmodel.Task{}).Error
}

func (s *taskDb) Insert(task *dbmodel.Task) error {
	err := s.db.Create(task).Error
	if err != nil {
		log.Error("insert task failed", zap.Error(err))
		var pgErr *pgconn.PgError
		ok := errors.As(err, &pgErr)
		if ok {
			switch pgErr.Code {
			case "23505":
				return common.ErrTaskAlreadyExists
			default:
				return err
			}
		}
		return err
	}
	return nil
}

func (s *taskDb) GetByName(inputCollectionID string, taskName string) (*dbmodel.Task, error) {
	var task dbmodel.Task
	err := s.db.
		Where("input_collection_id = ?", inputCollectionID).
		Where("task_name = ?", taskName).
		Where("is_deleted = ?", false).
		First(&task).Error

	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		log.Error("GetTaskByName failed", zap.Error(err))
		return nil, err
	}
	return &task, nil
}

func (s *taskDb) GetByID(taskID uuid.UUID) (*dbmodel.Task, error) {
	var task dbmodel.Task
	err := s.db.
		Where("task_id = ?", taskID).
		Where("is_deleted = ?", false).
		First(&task).Error

	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		log.Error("GetByID failed", zap.Error(err), zap.String("task_id", taskID.String()))
		return nil, err
	}
	return &task, nil
}

func (s *taskDb) UpdateOutputCollectionID(taskID uuid.UUID, outputCollectionID *string) error {
	now := time.Now()
	result := s.db.Exec(`
		UPDATE tasks
		SET output_collection_id = ?,
			updated_at = ?
		WHERE task_id = ?
			AND is_deleted = false
	`, outputCollectionID, now, taskID)

	if result.Error != nil {
		log.Error("UpdateOutputCollectionID failed", zap.Error(result.Error), zap.String("task_id", taskID.String()))
		return result.Error
	}

	if result.RowsAffected == 0 {
		log.Error("UpdateOutputCollectionID: no rows affected", zap.String("task_id", taskID.String()))
		return common.ErrTaskNotFound
	}

	return nil
}

func (s *taskDb) SoftDelete(inputCollectionID string, taskName string) error {
	// Update task name and is_deleted in a single query
	// Format: _deleted_<original_name>_<input_collection_id>_<task_id>
	result := s.db.Exec(`
		UPDATE tasks
		SET task_name = CONCAT('_deleted_', task_name, '_', input_collection_id, '_', task_id::text),
			is_deleted = true, updated_at = NOW()
		WHERE input_collection_id = ?
			AND task_name = ?
			AND is_deleted = false
	`, inputCollectionID, taskName)

	if result.Error != nil {
		log.Error("SoftDelete failed", zap.Error(result.Error))
		return result.Error
	}

	// If no rows were affected, task was not found (or already deleted)
	if result.RowsAffected == 0 {
		return nil // Idempotent - no error if already deleted or not found
	}

	return nil
}

// AdvanceTask updates task progress after register operator completes
// This bumps next_nonce and updates completion_offset/next_run
// Returns the authoritative values from the database
func (s *taskDb) AdvanceTask(taskID uuid.UUID, taskRunNonce uuid.UUID, completionOffset int64, nextRunDelaySecs uint64) (*dbmodel.AdvanceTask, error) {
	nextNonce, err := uuid.NewV7()
	if err != nil {
		log.Error("AdvanceTask: failed to generate next nonce", zap.Error(err))
		return nil, err
	}
	now := time.Now()
	// Bump next_nonce to mark a new run, but don't touch lowest_live_nonce yet
	// lowest_live_nonce will be updated later by finish_task when verification completes
	next_run := now.Add(time.Duration(nextRunDelaySecs) * time.Second)
	result := s.db.Model(&dbmodel.Task{}).Where("task_id = ?", taskID).Where("is_deleted = false").Where("next_nonce = ?", taskRunNonce).Where("completion_offset < ?", completionOffset).UpdateColumns(map[string]interface{}{
		"completion_offset": completionOffset,
		"next_run":          next_run,
		"last_run":          now,
		"next_nonce":        nextNonce,
		"current_attempts":  0,
		"updated_at":        gorm.Expr("GREATEST(updated_at, GREATEST(last_run, ?))", now),
	})

	if result.Error != nil {
		log.Error("AdvanceTask failed", zap.Error(result.Error), zap.String("task_id", taskID.String()))
		return nil, result.Error
	}

	if result.RowsAffected == 0 {
		log.Error("AdvanceTask: no rows affected", zap.String("task_id", taskID.String()))
		return nil, common.ErrTaskNotFound
	}

	// Return the authoritative values that were written to the database
	return &dbmodel.AdvanceTask{
		NextNonce:        nextNonce,
		NextRun:          next_run,
		CompletionOffset: completionOffset,
	}, nil
}

// UpdateCompletionOffset updates ONLY the completion_offset for a task
// This is called during flush_compaction_and_task after work is done
// NOTE: Does NOT update next_nonce (that was done earlier by AdvanceTask in PrepareTask)
func (s *taskDb) UpdateCompletionOffset(taskID uuid.UUID, taskRunNonce uuid.UUID, completionOffset int64) error {
	now := time.Now()
	// Update only completion_offset and last_run
	// Validate that we're updating the correct task run by checking lowest_live_nonce = taskRunNonce
	// This ensures we're updating the completion offset for the exact nonce we're working on
	result := s.db.Model(&dbmodel.Task{}).
		Where("task_id = ?", taskID).
		Where("is_deleted = false").
		Where("lowest_live_nonce = ?", taskRunNonce). // Ensure we're updating the correct nonce
		UpdateColumns(map[string]interface{}{
			"completion_offset": completionOffset,
			"last_run":          now,
			"updated_at":        now,
		})

	if result.Error != nil {
		log.Error("UpdateCompletionOffset failed", zap.Error(result.Error), zap.String("task_id", taskID.String()))
		return result.Error
	}

	if result.RowsAffected == 0 {
		log.Error("UpdateCompletionOffset: no rows affected - task not found or wrong nonce", zap.String("task_id", taskID.String()), zap.String("task_run_nonce", taskRunNonce.String()))
		return common.ErrTaskNotFound
	}

	return nil
}

// FinishTask updates lowest_live_nonce to mark the current nonce as verified
// This is called by the finish_task operator after scout_logs recheck completes
func (s *taskDb) FinishTask(taskID uuid.UUID) error {
	now := time.Now()
	// Set lowest_live_nonce = next_nonce to indicate this nonce is fully verified
	// If this fails, lowest_live_nonce < next_nonce will signal that we should skip
	// execution next time and only run the recheck phase
	result := s.db.Exec(`
		UPDATE tasks
		SET lowest_live_nonce = next_nonce,
			updated_at = ?
		WHERE task_id = ?
			AND is_deleted = false
	`, now, taskID)

	if result.Error != nil {
		log.Error("FinishTask failed", zap.Error(result.Error), zap.String("task_id", taskID.String()))
		return result.Error
	}

	if result.RowsAffected == 0 {
		log.Error("FinishTask: no rows affected", zap.String("task_id", taskID.String()))
		return common.ErrTaskNotFound
	}

	return nil
}

func (s *taskDb) PeekScheduleByCollectionId(collectionIDs []string) ([]*dbmodel.Task, error) {
	var tasks []*dbmodel.Task
	err := s.db.
		Where("input_collection_id IN ?", collectionIDs).
		Where("is_deleted = ?", false).
		Find(&tasks).Error

	if err != nil {
		log.Error("PeekScheduleByCollectionId failed", zap.Error(err))
		return nil, err
	}
	return tasks, nil
}

// GetMinCompletionOffsetForCollection returns the minimum completion_offset for all non-deleted tasks
// with the given input_collection_id. Returns nil if no tasks exist for the collection.
func (s *taskDb) GetMinCompletionOffsetForCollection(inputCollectionID string) (*int64, error) {
	var result struct {
		MinOffset *int64
	}

	err := s.db.Model(&dbmodel.Task{}).
		Select("MIN(completion_offset) as min_offset").
		Where("input_collection_id = ?", inputCollectionID).
		Where("is_deleted = ?", false).
		Scan(&result).Error

	if err != nil {
		log.Error("GetMinCompletionOffsetForCollection failed",
			zap.Error(err),
			zap.String("input_collection_id", inputCollectionID))
		return nil, err
	}

	return result.MinOffset, nil
}
