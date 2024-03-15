package schema

import "entgo.io/ent"

// TestBase holds the schema definition for the TestBase entity.
type TestBase struct {
	ent.Schema
}

// Fields of the TestBase.
func (TestBase) Fields() []ent.Field {
	return nil
}

// Edges of the TestBase.
func (TestBase) Edges() []ent.Edge {
	return nil
}
