package dao

import (
	"errors"

	"github.com/chroma-core/chroma/go/pkg/sysdb/metastore/db/dbmodel"
	"github.com/google/uuid"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type operatorDb struct {
	db *gorm.DB
}

var _ dbmodel.IOperatorDb = &operatorDb{}

func (s *operatorDb) GetByName(operatorName string) (*dbmodel.Operator, error) {
	var operator dbmodel.Operator
	err := s.db.
		Where("operator_name = ?", operatorName).
		First(&operator).Error

	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		log.Error("GetOperatorByName failed", zap.Error(err))
		return nil, err
	}
	return &operator, nil
}

func (s *operatorDb) GetByID(operatorID uuid.UUID) (*dbmodel.Operator, error) {
	var operator dbmodel.Operator
	err := s.db.
		Where("operator_id = ?", operatorID).
		First(&operator).Error

	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		log.Error("GetOperatorByID failed", zap.Error(err))
		return nil, err
	}
	return &operator, nil
}

func (s *operatorDb) GetAll() ([]*dbmodel.Operator, error) {
	var operators []*dbmodel.Operator
	err := s.db.Find(&operators).Error

	if err != nil {
		log.Error("GetAllOperators failed", zap.Error(err))
		return nil, err
	}
	return operators, nil
}
