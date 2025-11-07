package dbmodel

import "github.com/google/uuid"

// Constants for pre-populated functions.
// These UUIDs must match what's in the database migrations.
//
// When adding a new function:
// 1. Add a migration to populate the functions table with the new function
// 2. Add the UUID constant below (must match migration)
// 3. Add the name constant below
// 4. Add matching constants to rust/types/src/functions.rs
var (
	// FunctionRecordCounter is the UUID for the built-in record_counter function
	// Must match: migration 20251023154800.sql and rust/types/src/functions.rs::FUNCTION_RECORD_COUNTER_ID
	FunctionRecordCounter = uuid.MustParse("ccf2e3ba-633e-43ba-9394-46b0c54c61e3")

	// FunctionStatistics is the UUID for the built-in statistics function
	// Must match: migration 20251029223300.sql and rust/types/src/functions.rs::FUNCTION_STATISTICS_ID
	FunctionStatistics = uuid.MustParse("304b58ad-a5cb-41dc-b88f-36dd3bf1d401")
)

// Function names - must stay in sync with database and Rust constants.
const (
	// FunctionNameRecordCounter must match rust/types/src/functions.rs::FUNCTION_RECORD_COUNTER_NAME
	FunctionNameRecordCounter = "record_counter"

	// FunctionNameStatistics must match rust/types/src/functions.rs::FUNCTION_STATISTICS_NAME
	FunctionNameStatistics = "statistics"
)

// Function metadata
const (
	// FunctionRecordCounterIsIncremental indicates record_counter is an incremental function
	FunctionRecordCounterIsIncremental = true
	// FunctionRecordCounterReturnType is the JSON schema for record_counter's return type
	FunctionRecordCounterReturnType = `{"type": "object", "properties": {"count": {"type": "integer", "description": "Number of records processed"}}}`
)
