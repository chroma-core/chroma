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
			log.Error("Postgres Error")
			switch pgErr.Code {
			case "23505":
				log.Error("task already exists")
				return common.ErrTaskUniqueConstraintViolation
			default:
				return err
			}
		}
		return err
	}
	return nil
}

func (s *taskDb) GetByName(tenantID string, databaseID string, taskName string) (*dbmodel.Task, error) {
	var task dbmodel.Task
	err := s.db.
		Where("tenant_id = ?", tenantID).
		Where("database_id = ?", databaseID).
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

func (s *taskDb) SoftDelete(tenantID string, databaseID string, taskName string) error {
	return s.db.Table("tasks").
		Where("tenant_id = ?", tenantID).
		Where("database_id = ?", databaseID).
		Where("task_name = ?", taskName).
		Updates(map[string]interface{}{
			"is_deleted": true,
		}).Error
}
