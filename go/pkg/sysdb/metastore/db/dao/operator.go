package dao

import (
	"errors"

	"github.com/chroma-core/chroma/go/pkg/sysdb/metastore/db/dbmodel"
	"github.com/google/uuid"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type functionDb struct {
	db *gorm.DB
}

var _ dbmodel.IFunctionDb = &functionDb{}

func (s *functionDb) GetByName(name string) (*dbmodel.Function, error) {
	var function dbmodel.Function
	err := s.db.
		Where("name = ?", name).
		First(&function).Error

	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		log.Error("GetFunctionByName failed", zap.Error(err))
		return nil, err
	}
	return &function, nil
}

func (s *functionDb) GetByID(id uuid.UUID) (*dbmodel.Function, error) {
	var function dbmodel.Function
	err := s.db.
		Where("id = ?", id).
		First(&function).Error

	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		log.Error("GetFunctionByID failed", zap.Error(err))
		return nil, err
	}
	return &function, nil
}

func (s *functionDb) GetByIDs(ids []uuid.UUID) ([]*dbmodel.Function, error) {
	if len(ids) == 0 {
		return []*dbmodel.Function{}, nil
	}

	var functions []*dbmodel.Function
	err := s.db.
		Where("id IN ?", ids).
		Find(&functions).Error

	if err != nil {
		log.Error("GetFunctionsByIDs failed", zap.Error(err))
		return nil, err
	}
	return functions, nil
}

func (s *functionDb) GetAll() ([]*dbmodel.Function, error) {
	var functions []*dbmodel.Function
	err := s.db.Find(&functions).Error

	if err != nil {
		log.Error("GetAllFunctions failed", zap.Error(err))
		return nil, err
	}
	return functions, nil
}
