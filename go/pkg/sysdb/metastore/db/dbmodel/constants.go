package dbmodel

import "github.com/google/uuid"

// operator IDs that are pre-populated in the database.
//
// IMPORTANT: These constants must stay in sync with:
// 1. Database migrations that insert operators (go/pkg/sysdb/metastore/db/migrations/*.sql)
// 2. Rust constants (rust/types/src/operators.rs)
//
// When adding a new operator:
// 1. Create a migration to INSERT the operator with a UUID
// 2. Add the UUID constant here
// 3. Add the name constant below
// 4. Add matching constants to rust/types/src/operators.rs
var (
	// OperatorRecordCounter is the UUID for the built-in record_counter operator
	// Must match: migration 20250930122132.sql and rust/types/src/operators.rs::OPERATOR_RECORD_COUNTER_ID
	OperatorRecordCounter = uuid.MustParse("ccf2e3ba-633e-43ba-9394-46b0c54c61e3")
)

// OperatorNames contains the names of pre-populated operators.
// Must stay in sync with database and Rust constants.
const (
	// OperatorNameRecordCounter must match rust/types/src/operators.rs::OPERATOR_RECORD_COUNTER_NAME
	OperatorNameRecordCounter = "record_counter"
)

// Operator metadata
const (
	// OperatorRecordCounterIsIncremental indicates record_counter is an incremental operator
	OperatorRecordCounterIsIncremental = true
	// OperatorRecordCounterReturnType is the JSON schema for record_counter's return type
	OperatorRecordCounterReturnType = `{"type": "object", "properties": {"count": {"type": "integer", "description": "Number of records processed"}}}`
)
