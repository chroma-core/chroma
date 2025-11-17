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

func (s *attachedFunctionDb) GetByName(inputCollectionID string, name string) (*dbmodel.AttachedFunction, error) {
	var attachedFunction dbmodel.AttachedFunction
	err := s.db.
		Where("input_collection_id = ?", inputCollectionID).
		Where("name = ?", name).
		Where("is_deleted = ?", false).
		First(&attachedFunction).Error

	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		log.Error("GetByName failed", zap.Error(err))
		return nil, err
	}

	// Check if attached function is initialized (lowest_live_nonce must be set after 2PC completion)
	if attachedFunction.LowestLiveNonce == nil {
		log.Debug("GetByName: attached function exists but not ready",
			zap.String("input_collection_id", inputCollectionID),
			zap.String("name", name))
		return &attachedFunction, common.ErrAttachedFunctionNotReady
	}

	return &attachedFunction, nil
}

func (s *attachedFunctionDb) GetByID(id uuid.UUID) (*dbmodel.AttachedFunction, error) {
	var attachedFunction dbmodel.AttachedFunction
	err := s.db.
		Where("id = ?", id).
		Where("is_deleted = ?", false).
		First(&attachedFunction).Error

	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		log.Error("GetByID failed", zap.Error(err), zap.String("id", id.String()))
		return nil, err
	}

	// Check if attached function is initialized (lowest_live_nonce must be set after 2PC completion)
	if attachedFunction.LowestLiveNonce == nil {
		log.Debug("GetByID: attached function exists but not ready",
			zap.String("id", id.String()))
		return &attachedFunction, common.ErrAttachedFunctionNotReady
	}

	return &attachedFunction, nil
}

func (s *attachedFunctionDb) GetByCollectionID(inputCollectionID string) ([]*dbmodel.AttachedFunction, error) {
	var attachedFunctions []*dbmodel.AttachedFunction
	err := s.db.
		Where("input_collection_id = ?", inputCollectionID).
		Where("is_deleted = ?", false).
		Where("lowest_live_nonce IS NOT NULL").
		Find(&attachedFunctions).Error

	if err != nil {
		log.Error("GetByCollectionID failed", zap.Error(err), zap.String("input_collection_id", inputCollectionID))
		return nil, err
	}

	return attachedFunctions, nil
}

func (s *attachedFunctionDb) UpdateOutputCollectionID(id uuid.UUID, outputCollectionID *string) error {
	now := time.Now()
	result := s.db.Model(&dbmodel.AttachedFunction{}).
		Where("id = ? AND is_deleted = false", id).
		Updates(map[string]interface{}{
			"output_collection_id": outputCollectionID,
			"updated_at":           now,
		})

	if result.Error != nil {
		log.Error("UpdateOutputCollectionID failed", zap.Error(result.Error), zap.String("id", id.String()))
		return result.Error
	}

	if result.RowsAffected == 0 {
		log.Error("UpdateOutputCollectionID: no rows affected", zap.String("id", id.String()))
		return common.ErrAttachedFunctionNotFound
	}

	return nil
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

func (s *attachedFunctionDb) SoftDeleteByID(id uuid.UUID) error {
	// Update name and is_deleted in a single query
	// Format: _deleted_<original_name>_<id>
	result := s.db.Model(&dbmodel.AttachedFunction{}).
		Where("id = ? AND is_deleted = false", id).
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

// Advance updates attached function progress after register function completes
// This bumps next_nonce and updates completion_offset/next_run
// Returns the authoritative values from the database
func (s *attachedFunctionDb) Advance(id uuid.UUID, runNonce uuid.UUID, completionOffset int64, nextRunDelaySecs uint64) (*dbmodel.AdvanceAttachedFunction, error) {
	nextNonce, err := uuid.NewV7()
	if err != nil {
		log.Error("Advance: failed to generate next nonce", zap.Error(err))
		return nil, err
	}
	now := time.Now()
	// Bump next_nonce to mark a new run, but don't touch lowest_live_nonce yet
	// lowest_live_nonce will be updated later by finish when verification completes
	next_run := now.Add(time.Duration(nextRunDelaySecs) * time.Second)
	result := s.db.Model(&dbmodel.AttachedFunction{}).Where("id = ?", id).Where("is_deleted = false").Where("next_nonce = ?", runNonce).Where("completion_offset <= ?", completionOffset).UpdateColumns(map[string]interface{}{
		"completion_offset": completionOffset,
		"next_run":          next_run,
		"last_run":          now,
		"next_nonce":        nextNonce,
		"current_attempts":  0,
		"updated_at":        gorm.Expr("GREATEST(updated_at, GREATEST(last_run, ?))", now),
	})

	if result.Error != nil {
		log.Error("Advance failed", zap.Error(result.Error), zap.String("id", id.String()))
		return nil, result.Error
	}

	if result.RowsAffected == 0 {
		log.Error("Advance: no rows affected", zap.String("id", id.String()))
		return nil, common.ErrAttachedFunctionNotFound
	}

	// Return the authoritative values that were written to the database
	return &dbmodel.AdvanceAttachedFunction{
		NextNonce:        nextNonce,
		NextRun:          next_run,
		CompletionOffset: completionOffset,
	}, nil
}

// UpdateCompletionOffset updates ONLY the completion_offset for an attached function
// This is called during flush_compaction_and_attached_function after work is done
// NOTE: Does NOT update next_nonce (that was done earlier by Advance)
func (s *attachedFunctionDb) UpdateCompletionOffset(id uuid.UUID, runNonce uuid.UUID, completionOffset int64) error {
	now := time.Now()
	// Update only completion_offset and last_run
	// Validate that we're updating the correct run by checking lowest_live_nonce = runNonce
	// This ensures we're updating the completion offset for the exact nonce we're working on
	result := s.db.Model(&dbmodel.AttachedFunction{}).
		Where("id = ?", id).
		Where("is_deleted = false").
		Where("lowest_live_nonce = ?", runNonce). // Ensure we're updating the correct nonce
		UpdateColumns(map[string]interface{}{
			"completion_offset": completionOffset,
			"last_run":          now,
			"updated_at":        now,
		})

	if result.Error != nil {
		log.Error("UpdateCompletionOffset failed", zap.Error(result.Error), zap.String("id", id.String()))
		return result.Error
	}

	if result.RowsAffected == 0 {
		log.Error("UpdateCompletionOffset: no rows affected - attached function not found or wrong nonce", zap.String("id", id.String()), zap.String("run_nonce", runNonce.String()))
		return common.ErrAttachedFunctionNotFound
	}

	return nil
}

// UpdateLowestLiveNonce updates the lowest_live_nonce for an attached function
// This is used during initialization (Phase 3 of 2PC create)
// Only updates if lowest_live_nonce is currently NULL (2PC safety)
func (s *attachedFunctionDb) UpdateLowestLiveNonce(id uuid.UUID, lowestLiveNonce uuid.UUID) error {
	now := time.Now()
	result := s.db.Model(&dbmodel.AttachedFunction{}).
		Where("id = ?", id).
		Where("is_deleted = false").
		Where("lowest_live_nonce IS NULL"). // Only update if still NULL (2PC marker)
		UpdateColumns(map[string]interface{}{
			"lowest_live_nonce": lowestLiveNonce,
			"updated_at":        now,
		})

	if result.Error != nil {
		log.Error("UpdateLowestLiveNonce failed", zap.Error(result.Error), zap.String("id", id.String()))
		return result.Error
	}

	if result.RowsAffected == 0 {
		log.Error("UpdateLowestLiveNonce: no rows affected - attached function not found or already initialized", zap.String("id", id.String()))
		return common.ErrAttachedFunctionNotFound
	}

	return nil
}

// Finish updates lowest_live_nonce to mark the current nonce as verified
// This is called after scout_logs recheck completes
func (s *attachedFunctionDb) Finish(id uuid.UUID) error {
	now := time.Now()
	// Set lowest_live_nonce = next_nonce to indicate this nonce is fully verified
	// If this fails, lowest_live_nonce < next_nonce will signal that we should skip
	// execution next time and only run the recheck phase
	result := s.db.Exec(`
		UPDATE attached_functions
		SET lowest_live_nonce = next_nonce,
			updated_at = ?
		WHERE id = ?
			AND is_deleted = false
	`, now, id)

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

func (s *attachedFunctionDb) PeekScheduleByCollectionId(collectionIDs []string) ([]*dbmodel.AttachedFunction, error) {
	var attachedFunctions []*dbmodel.AttachedFunction
	err := s.db.
		Where("input_collection_id IN ?", collectionIDs).
		Where("is_deleted = ?", false).
		Where("lowest_live_nonce IS NOT NULL").
		Find(&attachedFunctions).Error

	if err != nil {
		log.Error("PeekScheduleByCollectionId failed", zap.Error(err))
		return nil, err
	}
	return attachedFunctions, nil
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
		Where("lowest_live_nonce IS NOT NULL").
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
		Where("lowest_live_nonce IS NULL").
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
				AND lowest_live_nonce IS NULL
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

// GetSoftDeletedAttachedFunctions returns attached functions that are soft deleted
// and were updated before the cutoff time (eligible for hard deletion)
func (s *attachedFunctionDb) GetSoftDeletedAttachedFunctions(cutoffTime time.Time, limit int32) ([]*dbmodel.AttachedFunction, error) {
	var attachedFunctions []*dbmodel.AttachedFunction
	err := s.db.
		Where("is_deleted = ?", true).
		Where("updated_at < ?", cutoffTime).
		Limit(int(limit)).
		Find(&attachedFunctions).Error

	if err != nil {
		log.Error("GetSoftDeletedAttachedFunctions failed",
			zap.Error(err),
			zap.Time("cutoff_time", cutoffTime))
		return nil, err
	}

	log.Debug("GetSoftDeletedAttachedFunctions found attached functions",
		zap.Int("count", len(attachedFunctions)),
		zap.Time("cutoff_time", cutoffTime))

	return attachedFunctions, nil
}

// HardDeleteAttachedFunction permanently deletes an attached function from the database
// This should only be called after the soft delete grace period has passed
func (s *attachedFunctionDb) HardDeleteAttachedFunction(id uuid.UUID) error {
	result := s.db.Unscoped().Delete(&dbmodel.AttachedFunction{}, "id = ? AND is_deleted = true", id)

	if result.Error != nil {
		log.Error("HardDeleteAttachedFunction failed",
			zap.Error(result.Error),
			zap.String("id", id.String()))
		return result.Error
	}

	if result.RowsAffected == 0 {
		log.Warn("HardDeleteAttachedFunction: no rows affected (attached function not found)",
			zap.String("id", id.String()))
		return nil // Idempotent - no error if not found
	}

	log.Info("HardDeleteAttachedFunction succeeded",
		zap.String("id", id.String()))

	return nil
}
