package common

const (
	DefaultTenant   = "default_tenant"
	DefaultDatabase = "default_database"

	// SourceAttachedFunctionIDKey is the schema field name used to mark output collections
	// and link them to their attached function. This is stored in the collection schema,
	// not in metadata.
	SourceAttachedFunctionIDKey = "source_attached_function_id"
)
