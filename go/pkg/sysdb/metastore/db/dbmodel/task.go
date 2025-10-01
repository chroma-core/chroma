package dbmodel

import (
	"time"

	"github.com/google/uuid"
)

type Task struct {
	ID                 uuid.UUID  `gorm:"column:task_id;primaryKey"`
	Name               string     `gorm:"column:task_name;type:text;not null;uniqueIndex:unique_task_per_tenant_db,priority:3"`
	TenantID           string     `gorm:"column:tenant_id;type:text;not null;uniqueIndex:unique_task_per_tenant_db,priority:1"`
	DatabaseID         string     `gorm:"column:database_id;type:text;not null;uniqueIndex:unique_task_per_tenant_db,priority:2"`
	InputCollectionID  string     `gorm:"column:input_collection_id;type:text;not null;index:idx_tasks_input_collection"`
	OutputCollectionID string     `gorm:"column:output_collection_id;type:text;not null"`
	OperatorID         uuid.UUID  `gorm:"column:operator_id;type:uuid;not null"`
	OperatorParams     string     `gorm:"column:operator_params;type:jsonb;not null"`
	CompletionOffset   int64      `gorm:"column:completion_offset;type:bigint;not null;default:0"`
	LastRun            *time.Time `gorm:"column:last_run;type:timestamptz"`
	NextRun            *time.Time `gorm:"column:next_run;type:timestamptz"`
	MinRecordsForTask  int64      `gorm:"column:min_records_for_task;type:bigint;not null;default:100"`
	CurrentAttempts    int32      `gorm:"column:current_attempts;type:integer;not null;default:0"`
	IsAlive            bool       `gorm:"column:is_alive;type:boolean;not null;default:true"`
	IsDeleted          bool       `gorm:"column:is_deleted;type:boolean;not null;default:false"`
	CreatedAt          time.Time  `gorm:"column:created_at;type:timestamptz;not null;default:now()"`
	UpdatedAt          time.Time  `gorm:"column:updated_at;type:timestamptz;not null;default:now()"`
	TaskTemplateParent *uuid.UUID `gorm:"column:task_template_parent;type:uuid;default:null"`
	NextNonce          uuid.UUID  `gorm:"column:next_nonce;type:uuid;not null"`
	OldestWrittenNonce *uuid.UUID `gorm:"column:oldest_written_nonce;type:uuid:default:null"`
}

func (v Task) TableName() string {
	return "tasks"
}

//go:generate mockery --name=ITaskDb
type ITaskDb interface {
	Insert(task *Task) error
	GetByName(tenantID string, databaseID string, taskName string) (*Task, error)
	SoftDelete(tenantID string, databaseID string, taskName string) error
	DeleteAll() error
}
