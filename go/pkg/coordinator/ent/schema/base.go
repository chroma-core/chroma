package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
	"entgo.io/ent/schema/mixin"
	"github.com/google/uuid"
	"time"
)

// Base holds the schema definition for the Base entity.
type Base struct {
	mixin.Schema
}

// Fields of the Base.
func (b Base) Fields() []ent.Field {
	return []ent.Field{
		field.UUID("parent_id", uuid.UUID{}).Optional().Immutable(),
		field.UUID("id", uuid.UUID{}).Default(uuid.New).Unique().Immutable(),
		field.String("name").MaxLen(255).Optional().Nillable(),
		field.Int64("created_at").Optional().Default(time.Now().UnixMilli()).Immutable(),
		field.Int64("updated_at").Optional().Default(time.Now().UnixMilli()).UpdateDefault(time.Now().UnixMilli),
		field.Int64("deleted_at").Optional(),
		field.Int("version").Optional().Default(0),
	}
}

func (b Base) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("name", "id"),
		index.Fields("deleted_at", "id"),
	}
}

//// delete
//type softDeleteKey struct{}
//
//func SkipSoftDelete(parent context.Context) context.Context {
//	return context.WithValue(parent, softDeleteKey{}, true)
//}
//
//func (b Base) Interceptors() []ent.Interceptor {
//	return []ent.Interceptor{
//		intercept.TraverseFunc(func(ctx context.Context, q intercept.Query) error {
//			// Skip soft-delete, means include soft-deleted entities.
//			if skip, _ := ctx.Value(softDeleteKey{}).(bool); skip {
//				return nil
//			}
//			b.P(q)
//			return nil
//		}),
//	}
//}
//
//func (b Base) P(w interface{ WhereP(...func(*sql.Selector)) }) {
//	w.WhereP(
//		sql.FieldIsNull(b.Fields()[0].Descriptor().Name),
//	)
//}
