package common

import (
	"errors"
)

var (
	// Tenant errors
	ErrTenantNotFound                  = errors.New("tenant not found")
	ErrTenantUniqueConstraintViolation = errors.New("tenant unique constraint violation")

	// Database errors
	ErrDatabaseNotFound                  = errors.New("database not found")
	ErrDatabaseUniqueConstraintViolation = errors.New("database unique constraint violation")

	// Collection errors
	ErrCollectionNotFound                    = errors.New("collection not found")
	ErrCollectionIDFormat                    = errors.New("collection id format error")
	ErrCollectionNameEmpty                   = errors.New("collection name is empty")
	ErrCollectionTopicEmpty                  = errors.New("collection topic is empty")
	ErrCollectionUniqueConstraintViolation   = errors.New("collection unique constraint violation")
	ErrCollectionDeleteNonExistingCollection = errors.New("delete non existing collection")

	// Collection metadata errors
	ErrUnknownCollectionMetadataType = errors.New("collection metadata value type not supported")
	ErrInvalidMetadataUpdate         = errors.New("invalid metadata update, reest metadata true and metadata value not empty")

	// Segment errors
	ErrSegmentIDFormat                  = errors.New("segment id format error")
	ErrInvalidTopicUpdate               = errors.New("invalid topic update, reset topic true and topic value not empty")
	ErrInvalidCollectionUpdate          = errors.New("invalid collection update, reset collection true and collection value not empty")
	ErrSegmentUniqueConstraintViolation = errors.New("unique constraint violation")
	ErrSegmentDeleteNonExistingSegment  = errors.New("delete non existing segment")

	// Segment metadata errors
	ErrUnknownSegmentMetadataType = errors.New("segment metadata value type not supported")
)
