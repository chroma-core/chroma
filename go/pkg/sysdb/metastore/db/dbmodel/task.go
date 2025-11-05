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
	NextRun                 time.Time  `gorm:"column:next_run;type:timestamp;not null"`
	MinRecordsForInvocation int64      `gorm:"column:min_records_for_invocation;type:bigint;not null;default:100"`
	CurrentAttempts         int32      `gorm:"column:current_attempts;type:integer;not null;default:0"`
	IsAlive                 bool       `gorm:"column:is_alive;type:boolean;not null;default:true"`
	IsDeleted               bool       `gorm:"column:is_deleted;type:boolean;not null;default:false"`
	CreatedAt               time.Time  `gorm:"column:created_at;type:timestamp;not null;default:CURRENT_TIMESTAMP"`
	UpdatedAt               time.Time  `gorm:"column:updated_at;type:timestamp;not null;default:CURRENT_TIMESTAMP"`
	GlobalParent            *uuid.UUID `gorm:"column:global_parent;type:uuid;default:null"`
	NextNonce               uuid.UUID  `gorm:"column:next_nonce;type:uuid;not null"`
	LowestLiveNonce         *uuid.UUID `gorm:"column:lowest_live_nonce;type:uuid;default:null"`
	OldestWrittenNonce      *uuid.UUID `gorm:"column:oldest_written_nonce;type:uuid;default:null"`
}

func (v AttachedFunction) TableName() string {
	return "attached_functions"
}

// AdvanceAttachedFunction contains the authoritative attached function data after AdvanceAttachedFunction
type AdvanceAttachedFunction struct {
	NextNonce        uuid.UUID
	NextRun          time.Time
	CompletionOffset int64
}

//go:generate mockery --name=IAttachedFunctionDb
type IAttachedFunctionDb interface {
	Insert(attachedFunction *AttachedFunction) error
	GetByName(inputCollectionID string, name string) (*AttachedFunction, error)
	GetByID(id uuid.UUID) (*AttachedFunction, error)
	GetByCollectionID(inputCollectionID string) ([]*AttachedFunction, error)
	Advance(id uuid.UUID, nextRunNonce uuid.UUID, completionOffset int64, nextRunDelaySecs uint64) (*AdvanceAttachedFunction, error)
	UpdateCompletionOffset(id uuid.UUID, runNonce uuid.UUID, completionOffset int64) error
	UpdateLowestLiveNonce(id uuid.UUID, lowestLiveNonce uuid.UUID) error
	Finish(id uuid.UUID) error
	UpdateOutputCollectionID(id uuid.UUID, outputCollectionID *string) error
	SoftDelete(inputCollectionID string, name string) error
	SoftDeleteByID(id uuid.UUID) error
	DeleteAll() error
	PeekScheduleByCollectionId(collectionIDs []string) ([]*AttachedFunction, error)
	GetMinCompletionOffsetForCollection(inputCollectionID string) (*int64, error)
	CleanupExpiredPartial(maxAgeSeconds uint64) ([]uuid.UUID, error)
	GetSoftDeletedAttachedFunctions(cutoffTime time.Time, limit int32) ([]*AttachedFunction, error)
	HardDeleteAttachedFunction(id uuid.UUID) error
}
