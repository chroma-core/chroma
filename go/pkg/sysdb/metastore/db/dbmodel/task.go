package dbmodel

import (
	"time"

	"github.com/google/uuid"
)

type Task struct {
	ID                 uuid.UUID  `gorm:"column:task_id;primaryKey"`
	Name               string     `gorm:"column:task_name;type:text;not null;uniqueIndex:unique_task_per_collection,priority:2"`
	TenantID           string     `gorm:"column:tenant_id;type:text;not null"`
	DatabaseID         string     `gorm:"column:database_id;type:text;not null"`
	InputCollectionID  string     `gorm:"column:input_collection_id;type:text;not null;uniqueIndex:unique_task_per_collection,priority:1"`
	OutputCollectionName string     `gorm:"column:output_collection_name;type:text;not null"`
	OutputCollectionID   *string    `gorm:"column:output_collection_id;type:text;default:null"`
	OperatorID         uuid.UUID  `gorm:"column:operator_id;type:uuid;not null"`
	OperatorParams     string     `gorm:"column:operator_params;type:jsonb;not null"`
	CompletionOffset   int64      `gorm:"column:completion_offset;type:bigint;not null;default:0"`
	LastRun            *time.Time `gorm:"column:last_run;type:timestamp"`
	NextRun            *time.Time `gorm:"column:next_run;type:timestamp"`
	MinRecordsForTask  int64      `gorm:"column:min_records_for_task;type:bigint;not null;default:100"`
	CurrentAttempts    int32      `gorm:"column:current_attempts;type:integer;not null;default:0"`
	IsAlive            bool       `gorm:"column:is_alive;type:boolean;not null;default:true"`
	IsDeleted          bool       `gorm:"column:is_deleted;type:boolean;not null;default:false"`
	CreatedAt          time.Time  `gorm:"column:created_at;type:timestamp;not null;default:CURRENT_TIMESTAMP"`
	UpdatedAt          time.Time  `gorm:"column:updated_at;type:timestamp;not null;default:CURRENT_TIMESTAMP"`
	TaskTemplateParent *uuid.UUID `gorm:"column:task_template_parent;type:uuid;default:null"`
	NextNonce          uuid.UUID  `gorm:"column:next_nonce;type:uuid;not null"`
	OldestWrittenNonce *uuid.UUID `gorm:"column:oldest_written_nonce;type:uuid;default:null"`
}

func (v Task) TableName() string {
	return "tasks"
}

//go:generate mockery --name=ITaskDb
type ITaskDb interface {
	Insert(task *Task) error
	GetByName(inputCollectionID string, taskName string) (*Task, error)
	SoftDelete(inputCollectionID string, taskName string) error
	DeleteAll() error
}
