package dao

import (
	"errors"
	"time"

	"github.com/chroma-core/chroma/go/pkg/common"
	"github.com/chroma-core/chroma/go/pkg/sysdb/metastore/db/dbmodel"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type attachedFunctionDb struct {
	db *gorm.DB
}

var _ dbmodel.IAttachedFunctionDb = &attachedFunctionDb{}

func (s *attachedFunctionDb) DeleteAll() error {
	return s.db.Where("1 = 1").Delete(&dbmodel.AttachedFunction{}).Error
}

func (s *attachedFunctionDb) Insert(attachedFunction *dbmodel.AttachedFunction) error {
	err := s.db.Create(attachedFunction).Error
	if err != nil {
		log.Error("insert attached function failed", zap.Error(err))
		var pgErr *pgconn.PgError
		ok := errors.As(err, &pgErr)
		if ok {
			switch pgErr.Code {
			case "23505":
				return common.ErrAttachedFunctionAlreadyExists
			default:
				return err
			}
		}
		return err
	}
	return nil
}

func (s *attachedFunctionDb) Update(attachedFunction *dbmodel.AttachedFunction) error {
	result := s.db.Model(&dbmodel.AttachedFunction{}).
		Where("id = ?", attachedFunction.ID).
		Where("is_deleted = ?", false).
		Updates(attachedFunction)

	if result.Error != nil {
		log.Error("update attached function failed", zap.Error(result.Error))
		return result.Error
	}

	if result.RowsAffected == 0 {
		log.Error("update attached function: no rows affected", zap.String("id", attachedFunction.ID.String()))
		return common.ErrAttachedFunctionNotFound
	}

	return nil
}

// GetAttachedFunctions is a consolidated getter that supports various query patterns
// Parameters can be nil to indicate they should not be filtered on
// - id: Filter by attached function ID
// - name: Filter by attached function name
// - inputCollectionID: Filter by input collection ID
// - onlyReady: If true, only returns attached functions where is_ready = true
func (s *attachedFunctionDb) GetAttachedFunctions(id *uuid.UUID, name *string, inputCollectionID *string, onlyReady bool) ([]*dbmodel.AttachedFunction, error) {
	var attachedFunctions []*dbmodel.AttachedFunction

	query := s.db.Where("is_deleted = ?", false)

	if id != nil {
		query = query.Where("id = ?", *id)
	}

	if name != nil {
		query = query.Where("name = ?", *name)
	}

	if inputCollectionID != nil {
		query = query.Where("input_collection_id = ?", *inputCollectionID)
	}

	if onlyReady {
		query = query.Where("is_ready = ?", true)
	}

	err := query.Find(&attachedFunctions).Error
	if err != nil {
		log.Error("GetAttachedFunctions failed",
			zap.Error(err),
			zap.Any("id", id),
			zap.Any("name", name),
			zap.Any("input_collection_id", inputCollectionID),
			zap.Bool("only_ready", onlyReady))
		return nil, err
	}

	return attachedFunctions, nil
}

func (s *attachedFunctionDb) SoftDelete(inputCollectionID string, name string) error {
	// Update name and is_deleted in a single query
	// Format: _deleted_<original_name>_<id>
	result := s.db.Model(&dbmodel.AttachedFunction{}).
		Where("input_collection_id = ? AND name = ? AND is_deleted = false", inputCollectionID, name).
		Updates(map[string]interface{}{
			"name":       gorm.Expr("CONCAT('_deleted_', name, '_', id::text)"),
			"is_deleted": true,
			"updated_at": gorm.Expr("NOW()"),
		})

	if result.Error != nil {
		log.Error("SoftDelete failed", zap.Error(result.Error))
		return result.Error
	}

	// If no rows were affected, attached function was not found (or already deleted)
	if result.RowsAffected == 0 {
		return nil // Idempotent - no error if already deleted or not found
	}

	return nil
}

func (s *attachedFunctionDb) SoftDeleteByID(id uuid.UUID, inputCollectionID uuid.UUID) error {
	// Update name and is_deleted in a single query
	// Format: _deleted_<original_name>_<id>
	result := s.db.Model(&dbmodel.AttachedFunction{}).
		Where("id = ? AND input_collection_id = ? AND is_deleted = false", id, inputCollectionID.String()).
		Updates(map[string]interface{}{
			"name":       gorm.Expr("CONCAT('_deleted_', name, '_', id::text)"),
			"is_deleted": true,
			"updated_at": gorm.Expr("NOW()"),
		})

	if result.Error != nil {
		log.Error("SoftDeleteByID failed", zap.Error(result.Error))
		return result.Error
	}

	// If no rows were affected, attached function was not found (or already deleted)
	if result.RowsAffected == 0 {
		return nil // Idempotent - no error if already deleted or not found
	}

	return nil
}

// Finish marks work as complete
func (s *attachedFunctionDb) Finish(id uuid.UUID) error {
	now := time.Now()
	result := s.db.Model(&dbmodel.AttachedFunction{}).
		Where("id = ?", id).
		Where("is_deleted = false").
		UpdateColumns(map[string]interface{}{
			"updated_at": now,
		})

	if result.Error != nil {
		log.Error("Finish failed", zap.Error(result.Error), zap.String("id", id.String()))
		return result.Error
	}

	if result.RowsAffected == 0 {
		log.Error("Finish: no rows affected", zap.String("id", id.String()))
		return common.ErrAttachedFunctionNotFound
	}

	return nil
}

// GetMinCompletionOffsetForCollection returns the minimum completion_offset for all non-deleted attached functions
// with the given input_collection_id. Returns nil if no attached functions exist for the collection.
func (s *attachedFunctionDb) GetMinCompletionOffsetForCollection(inputCollectionID string) (*int64, error) {
	var result struct {
		MinOffset *int64
	}

	err := s.db.Model(&dbmodel.AttachedFunction{}).
		Select("MIN(completion_offset) as min_offset").
		Where("input_collection_id = ?", inputCollectionID).
		Where("is_deleted = ?", false).
		Scan(&result).Error

	if err != nil {
		log.Error("GetMinCompletionOffsetForCollection failed",
			zap.Error(err),
			zap.String("input_collection_id", inputCollectionID))
		return nil, err
	}

	return result.MinOffset, nil
}

// CleanupExpiredPartial finds and soft deletes attached functions that were partially created
// (lowest_live_nonce IS NULL) and are older than maxAgeSeconds.
// Returns the list of IDs that were soft deleted.
func (s *attachedFunctionDb) CleanupExpiredPartial(maxAgeSeconds uint64) ([]uuid.UUID, error) {
	// Calculate the cutoff time
	cutoffTime := time.Now().Add(-time.Duration(maxAgeSeconds) * time.Second)

	// First, find attached functions that match the criteria
	var attachedFunctions []dbmodel.AttachedFunction
	err := s.db.
		Where("output_collection_id IS NULL").
		Where("is_deleted = ?", false).
		Where("updated_at < ?", cutoffTime).
		Find(&attachedFunctions).Error

	if err != nil {
		log.Error("CleanupExpiredPartial: failed to find expired partial attached functions",
			zap.Error(err),
			zap.Uint64("max_age_seconds", maxAgeSeconds))
		return nil, err
	}

	if len(attachedFunctions) == 0 {
		log.Info("CleanupExpiredPartial: no expired partial attached functions found",
			zap.Uint64("max_age_seconds", maxAgeSeconds))
		return []uuid.UUID{}, nil
	}

	// Extract IDs
	ids := make([]uuid.UUID, len(attachedFunctions))
	for i, af := range attachedFunctions {
		ids[i] = af.ID
	}

	// Soft delete these stuck attached functions in batches to avoid IN clause limits
	// Format: _deleted_<original_name>_<id>
	const batchSize = 1000
	now := time.Now()
	totalDeleted := int64(0)

	for i := 0; i < len(ids); i += batchSize {
		end := i + batchSize
		if end > len(ids) {
			end = len(ids)
		}
		batch := ids[i:end]

		result := s.db.Exec(`
			UPDATE attached_functions
			SET name = CONCAT('_deleted_', name, '_', id::text),
				is_deleted = true,
				updated_at = ?
			WHERE id IN ?
				AND output_collection_id IS NULL
				AND is_deleted = false
		`, now, batch)

		if result.Error != nil {
			log.Error("CleanupExpiredPartial: failed to soft delete batch",
				zap.Error(result.Error),
				zap.Int("batch_start", i),
				zap.Int("batch_size", len(batch)))
			return nil, result.Error
		}

		totalDeleted += result.RowsAffected
	}

	log.Info("CleanupExpiredPartial: successfully soft deleted expired partial attached functions",
		zap.Int64("cleaned_count", totalDeleted),
		zap.Uint64("max_age_seconds", maxAgeSeconds))

	return ids, nil
}

// GetAttachedFunctionsToGc returns attached functions eligible for garbage collection:
// either soft deleted OR stuck in non-ready state, and updated before the cutoff time
func (s *attachedFunctionDb) GetAttachedFunctionsToGc(cutoffTime time.Time, limit int32) ([]*dbmodel.AttachedFunction, error) {
	var attachedFunctions []*dbmodel.AttachedFunction
	err := s.db.
		Where("(is_deleted = ? OR is_ready = ?)", true, false).
		Where("updated_at < ?", cutoffTime).
		Limit(int(limit)).
		Find(&attachedFunctions).Error

	if err != nil {
		log.Error("GetAttachedFunctionsToGc failed",
			zap.Error(err),
			zap.Time("cutoff_time", cutoffTime))
		return nil, err
	}

	log.Debug("GetAttachedFunctionsToGc found attached functions",
		zap.Int("count", len(attachedFunctions)),
		zap.Time("cutoff_time", cutoffTime))

	return attachedFunctions, nil
}

// HardDeleteAttachedFunction permanently deletes an attached function from the database.
// Deletes records that are either soft-deleted or stuck in non-ready state.
// This should only be called after the grace period has passed (via GetAttachedFunctionsToGc).
func (s *attachedFunctionDb) HardDeleteAttachedFunction(id uuid.UUID) error {
	result := s.db.Unscoped().Delete(&dbmodel.AttachedFunction{}, "id = ? AND (is_deleted = ? OR is_ready = ?)", id, true, false)

	if result.Error != nil {
		log.Error("HardDeleteAttachedFunction failed",
			zap.Error(result.Error),
			zap.String("id", id.String()))
		return result.Error
	}

	if result.RowsAffected == 0 {
		log.Warn("HardDeleteAttachedFunction: no rows affected (attached function not found or not eligible for deletion)",
			zap.String("id", id.String()))
		return nil // Idempotent - no error if not found or not eligible
	}

	log.Info("HardDeleteAttachedFunction succeeded",
		zap.String("id", id.String()))

	return nil
}
