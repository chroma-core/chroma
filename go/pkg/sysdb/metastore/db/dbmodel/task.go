package dbmodel

import (
	"time"

	"github.com/google/uuid"
)

type AttachedFunction struct {
	ID                      uuid.UUID  `gorm:"column:id;primaryKey"`
	Name                    string     `gorm:"column:name;type:text;not null;uniqueIndex:unique_attached_function_per_collection,priority:2"`
	TenantID                string     `gorm:"column:tenant_id;type:text;not null"`
	DatabaseID              string     `gorm:"column:database_id;type:text;not null"`
	InputCollectionID       string     `gorm:"column:input_collection_id;type:text;not null;uniqueIndex:unique_attached_function_per_collection,priority:1"`
	OutputCollectionName    string     `gorm:"column:output_collection_name;type:text;not null"`
	OutputCollectionID      *string    `gorm:"column:output_collection_id;type:text;default:null"`
	FunctionID              uuid.UUID  `gorm:"column:function_id;type:uuid;not null"`
	FunctionParams          string     `gorm:"column:function_params;type:jsonb;not null"`
	CompletionOffset        int64      `gorm:"column:completion_offset;type:bigint;not null;default:0"`
	LastRun                 *time.Time `gorm:"column:last_run;type:timestamp"`
	MinRecordsForInvocation int64      `gorm:"column:min_records_for_invocation;type:bigint;not null;default:100"`
	CurrentAttempts         int32      `gorm:"column:current_attempts;type:integer;not null;default:0"`
	IsAlive                 bool       `gorm:"column:is_alive;type:boolean;not null;default:true"`
	IsDeleted               bool       `gorm:"column:is_deleted;type:boolean;not null;default:false"`
	CreatedAt               time.Time  `gorm:"column:created_at;type:timestamp;not null;default:CURRENT_TIMESTAMP"`
	UpdatedAt               time.Time  `gorm:"column:updated_at;type:timestamp;not null;default:CURRENT_TIMESTAMP"`
	GlobalParent            *uuid.UUID `gorm:"column:global_parent;type:uuid;default:null"`
	OldestWrittenNonce      *uuid.UUID `gorm:"column:oldest_written_nonce;type:uuid;default:null"`
	IsReady                 bool       `gorm:"column:is_ready;type:boolean;not null;default:false"`
}

func (v AttachedFunction) TableName() string {
	return "attached_functions"
}

//go:generate mockery --name=IAttachedFunctionDb
type IAttachedFunctionDb interface {
	Insert(attachedFunction *AttachedFunction) error
	// GetAttachedFunctions is a consolidated getter that supports various query patterns
	// Parameters can be nil to indicate they should not be filtered on
	// - id: Filter by attached function ID
	// - name: Filter by attached function name
	// - inputCollectionID: Filter by input collection ID
	// - onlyReady: If true, only returns attached functions where is_ready = true
	GetAttachedFunctions(id *uuid.UUID, name *string, inputCollectionID *string, onlyReady bool) ([]*AttachedFunction, error)
	Update(attachedFunction *AttachedFunction) error
	Finish(id uuid.UUID) error
	SoftDelete(inputCollectionID string, name string) error
	SoftDeleteByID(id uuid.UUID, inputCollectionID uuid.UUID) error
	DeleteAll() error
	GetMinCompletionOffsetForCollection(inputCollectionID string) (*int64, error)
	CleanupExpiredPartial(maxAgeSeconds uint64) ([]uuid.UUID, error)
	GetSoftDeletedAttachedFunctions(cutoffTime time.Time, limit int32) ([]*AttachedFunction, error)
	HardDeleteAttachedFunction(id uuid.UUID) error
}
