package common

import (
	"errors"
)

var (
	// Tenant errors
	ErrTenantNotFound                  = errors.New("tenant not found")
	ErrTenantUniqueConstraintViolation = errors.New("tenant unique constraint violation")
	ErrTenantResourceNameAlreadySet    = errors.New("tenant resource name is already set")

	// Database errors
	ErrDatabaseNotFound                  = errors.New("database not found")
	ErrDatabaseUniqueConstraintViolation = errors.New("database unique constraint violation")
	ErrDatabaseNameEmpty                 = errors.New("database name is empty")

	// Collection errors
	ErrCollectionNotFound                    = errors.New("collection not found")
	ErrCollectionSoftDeleted                 = errors.New("collection soft deleted")
	ErrConcurrentDeleteCollection            = errors.New("a concurrent operation deleted the collection")
	ErrCollectionIDFormat                    = errors.New("collection id format error")
	ErrCollectionNameEmpty                   = errors.New("collection name is empty")
	ErrCollectionUniqueConstraintViolation   = errors.New("collection unique constraint violation")
	ErrCollectionDeleteNonExistingCollection = errors.New("delete non existing collection")
	ErrCollectionLogPositionStale            = errors.New("collection log position stale")
	ErrCollectionVersionStale                = errors.New("collection version stale")
	ErrCollectionVersionInvalid              = errors.New("collection version invalid")
	ErrCollectionVersionFileNameStale        = errors.New("collection version file name stale")
	ErrCollectionEntryIsStale                = errors.New("collection entry is stale - one of version, version_file_name, or log_position is stale")
	ErrCollectionTooManyFork                 = errors.New("collection entry has too many forks")
	ErrCollectionDeletedWithLocksHeld        = errors.New("collection got deleted concurrently even though select for update locks were held. Not possible unless corruption somehow")
	ErrMissingLineageFileName                = errors.New("missing lineage file name in root collection entry")
	ErrCollectionWasNotSoftDeleted           = errors.New("collection was not soft deleted")

	// Collection metadata errors
	ErrUnknownCollectionMetadataType = errors.New("collection metadata value type not supported")
	ErrInvalidMetadataUpdate         = errors.New("invalid metadata update, reest metadata true and metadata value not empty")

	// Segment errors
	ErrSegmentIDFormat                  = errors.New("segment id format error")
	ErrInvalidCollectionUpdate          = errors.New("invalid collection update, reset collection true and collection value not empty")
	ErrMissingCollectionID              = errors.New("missing collection id")
	ErrSegmentUniqueConstraintViolation = errors.New("unique constraint violation")
	ErrSegmentDeleteNonExistingSegment  = errors.New("delete non existing segment")
	ErrSegmentUpdateNonExistingSegment  = errors.New("update non existing segment")

	// Segment metadata errors
	ErrUnknownSegmentMetadataType = errors.New("segment metadata value type not supported")

	// AttachedFunction errors
	ErrAttachedFunctionAlreadyExists = errors.New("the attached function that was being created already exists for this collection")
	ErrAttachedFunctionNotFound      = errors.New("the requested attached function was not found")
	ErrAttachedFunctionNotReady      = errors.New("the requested attached function exists but is still initializing")
	ErrInvalidAttachedFunctionName   = errors.New("attached function name cannot start with reserved prefix '_deleted_'")
	ErrHeapServiceNotEnabled         = errors.New("heap service is not enabled")

	// Function errors
	ErrFunctionNotFound = errors.New("function not found")

	// Others
	ErrCompactionOffsetSomehowAhead = errors.New("system invariant was violated. Compaction offset in sysdb should always be behind or equal to offset in log")
)
