package dbmodel

import (
	"fmt"

	"github.com/google/uuid"
)

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

	// FunctionCountAsync is the UUID for the built-in count_async function
	// Must match: migration 20260602120000.sql and rust/types/src/operators_generated.rs::FUNCTION_COUNT_ASYNC_ID
	FunctionCountAsync = uuid.MustParse("4caddd3b-d49d-4625-ae1b-d4183530824a")

	// FunctionStatistics is the UUID for the built-in statistics function
	// Must match: migration 20251029223300.sql and rust/types/src/functions.rs::FUNCTION_STATISTICS_ID
	FunctionStatistics = uuid.MustParse("304b58ad-a5cb-41dc-b88f-36dd3bf1d401")

	// FunctionDummyAsync is the UUID for the built-in dummy_async function
	// Must match: migration 20260501105846.sql and rust/types/src/functions.rs::FUNCTION_DUMMY_ASYNC_ID
	FunctionDummyAsync = uuid.MustParse("1db3d179-37a7-4c44-a301-687c1da69d7b")

	// FunctionHttpGenerate is the UUID for the built-in http_generate function
	// Must match: rust/types/src/operators_generated.rs::FUNCTION_HTTP_GENERATE_ID
	FunctionHttpGenerate = uuid.MustParse("9e3c7540-4ddd-40a2-bbff-ad9cb3f06efc")

	// FunctionRevisionHistory is the UUID for the built-in revision_history function
	// Must match: migration 20260525150000.sql and rust/types/src/operators_generated.rs::FUNCTION_REVISION_HISTORY_ID
	FunctionRevisionHistory = uuid.MustParse("2df4342c-5b5a-49aa-8345-c46503e85509")
)

// Function names - must stay in sync with database and Rust constants.
const (
	// FunctionNameRecordCounter must match rust/types/src/functions.rs::FUNCTION_RECORD_COUNTER_NAME
	FunctionNameRecordCounter = "record_counter"

	// FunctionNameCountAsync must match rust/types/src/operators_generated.rs::FUNCTION_COUNT_ASYNC_NAME
	FunctionNameCountAsync = "count_async"

	// FunctionNameStatistics must match rust/types/src/functions.rs::FUNCTION_STATISTICS_NAME
	FunctionNameStatistics = "statistics"

	// FunctionNameDummyAsync must match rust/types/src/functions.rs::FUNCTION_DUMMY_ASYNC_NAME
	FunctionNameDummyAsync = "dummy_async"

	// FunctionNameHttpGenerate must match rust/types/src/operators_generated.rs::FUNCTION_HTTP_GENERATE_NAME
	FunctionNameHttpGenerate = "http_generate"

	// FunctionNameRevisionHistory must match rust/types/src/operators_generated.rs::FUNCTION_REVISION_HISTORY_NAME
	FunctionNameRevisionHistory = "revision_history"
)

// functionIDToName maps function UUIDs to their names.
// This avoids DB lookups for known built-in functions.
var functionIDToName = map[uuid.UUID]string{
	FunctionRecordCounter:   FunctionNameRecordCounter,
	FunctionCountAsync:      FunctionNameCountAsync,
	FunctionStatistics:      FunctionNameStatistics,
	FunctionDummyAsync:      FunctionNameDummyAsync,
	FunctionHttpGenerate:    FunctionNameHttpGenerate,
	FunctionRevisionHistory: FunctionNameRevisionHistory,
}

// GetFunctionNameByID returns the function name for a given function ID.
// Returns an error if the function ID is not a known built-in.
func GetFunctionNameByID(id uuid.UUID) (string, error) {
	if name, ok := functionIDToName[id]; ok {
		return name, nil
	}
	return "", fmt.Errorf("unknown function ID: %s", id.String())
}

// Function metadata
const (
	// FunctionRecordCounterIsIncremental indicates record_counter is an incremental function
	FunctionRecordCounterIsIncremental = true
	// FunctionRecordCounterReturnType is the JSON schema for record_counter's return type
	FunctionRecordCounterReturnType = `{"type": "object", "properties": {"count": {"type": "integer", "description": "Number of records processed"}}}`
)
