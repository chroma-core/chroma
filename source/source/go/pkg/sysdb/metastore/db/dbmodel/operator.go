package dbmodel

import (
	"github.com/google/uuid"
)

type Function struct {
	ID            uuid.UUID `gorm:"column:id;primaryKey;unique"`
	Name          string    `gorm:"column:name;type:text;not null;unique"`
	IsIncremental bool      `gorm:"column:is_incremental;type:bool;not null"`
	ReturnType    string    `gorm:"column:return_type;type:jsonb;not null"`
}

func (v Function) TableName() string {
	return "functions"
}

//go:generate mockery --name=IFunctionDb
type IFunctionDb interface {
	GetByName(name string) (*Function, error)
	GetByID(id uuid.UUID) (*Function, error)
	GetByIDs(ids []uuid.UUID) ([]*Function, error)
	GetAll() ([]*Function, error)
}
