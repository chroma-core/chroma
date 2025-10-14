package dbmodel

import (
	"github.com/google/uuid"
)

type Operator struct {
	OperatorID    uuid.UUID `gorm:"operator_id;primaryKey;unique"`
	OperatorName  string    `gorm:"operator_name;type:text;not null;unique"`
	IsIncremental bool      `gorm:"is_incremental;type:bool;not null"`
	ReturnType    string    `gorm:"return_type;type:jsonb;not null"`
}

func (v Operator) TableName() string {
	return "operators"
}

//go:generate mockery --name=IOperatorDb
type IOperatorDb interface {
	GetByName(operatorName string) (*Operator, error)
	GetByID(operatorID uuid.UUID) (*Operator, error)
	GetAll() ([]*Operator, error)
}
