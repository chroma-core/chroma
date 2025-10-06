package dao

import (
	"errors"

	"github.com/chroma-core/chroma/go/pkg/common"
	"github.com/chroma-core/chroma/go/pkg/sysdb/metastore/db/dbmodel"
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
