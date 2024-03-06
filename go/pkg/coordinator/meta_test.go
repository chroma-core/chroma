package coordinator

import (
	"context"
	"testing"

	"github.com/chroma-core/chroma/go/pkg/metastore/coordinator"
	"github.com/chroma-core/chroma/go/pkg/model"
	"github.com/chroma-core/chroma/go/pkg/types"
	"pgregory.net/rapid"
)

func testMeta(t *rapid.T) {
	catalog := coordinator.NewMemoryCatalog()
	mt, err := NewMetaTable(context.Background(), catalog)
	if err != nil {
		t.Fatalf("error creating meta table: %v", err)
	}
	t.Repeat(map[string]func(*rapid.T){
		"generate_collection": func(t *rapid.T) {
			collection := rapid.Custom[*model.CreateCollection](func(t *rapid.T) *model.CreateCollection {
				return &model.CreateCollection{
					ID:   genCollectinID(t),
					Name: rapid.String().Draw(t, "name"),
					// Dimension: rapid.Int32().Draw(t, "dimension"),
					Metadata: rapid.Custom[*model.CollectionMetadata[model.CollectionMetadataValueType]](func(t *rapid.T) *model.CollectionMetadata[model.CollectionMetadataValueType] {
						return &model.CollectionMetadata[model.CollectionMetadataValueType]{
							Metadata: rapid.MapOf[string, model.CollectionMetadataValueType](rapid.StringMatching(`[a-zA-Z0-9_]+`), drawMetadata(t)).Draw(t, "metadata"),
						}
					}).Draw(t, "metadata"),
				}
			}).Draw(t, "collection")
			if _, err := mt.catalog.CreateCollection(context.Background(), collection, 0); err != nil {
				t.Fatalf("error creating collection: %v", err)
			}
		},
		"reload": func(t *rapid.T) {
			if err := mt.reload(); err != nil {
				t.Fatalf("error reloading meta table: %v", err)
			}
		},
		"add_collection": func(t *rapid.T) {
			if err := mt.reload(); err != nil {
				t.Fatalf("error reloading meta table: %v", err)
			}
			collection := rapid.Custom[*model.CreateCollection](func(t *rapid.T) *model.CreateCollection {
				return &model.CreateCollection{
					ID:   genCollectinID(t),
					Name: rapid.String().Draw(t, "name"),
					//Dimension: rapid.Int32().Draw(t, "dimension"),
					Metadata: rapid.Custom[*model.CollectionMetadata[model.CollectionMetadataValueType]](func(t *rapid.T) *model.CollectionMetadata[model.CollectionMetadataValueType] {
						return &model.CollectionMetadata[model.CollectionMetadataValueType]{
							Metadata: rapid.MapOf[string, model.CollectionMetadataValueType](rapid.StringMatching(`[a-zA-Z0-9_]+`), drawMetadata(t)).Draw(t, "metadata"),
						}
					}).Draw(t, "metadata"),
				}
			}).Draw(t, "collection")

			if _, err := mt.AddCollection(context.Background(), collection); err != nil {
				t.Fatalf("error adding collection: %v", err)
			}
		},
	})
}

func drawMetadata(t *rapid.T) *rapid.Generator[model.CollectionMetadataValueType] {
	return rapid.OneOf[model.CollectionMetadataValueType](
		rapid.Custom[model.CollectionMetadataValueType](func(t *rapid.T) model.CollectionMetadataValueType {
			return &model.CollectionMetadataValueStringType{
				Value: rapid.String().Draw(t, "string_value"),
			}
		}),
		rapid.Custom[model.CollectionMetadataValueType](func(t *rapid.T) model.CollectionMetadataValueType {
			return &model.CollectionMetadataValueInt64Type{
				Value: rapid.Int64().Draw(t, "int_value"),
			}
		}),
		rapid.Custom[model.CollectionMetadataValueType](func(t *rapid.T) model.CollectionMetadataValueType {
			return &model.CollectionMetadataValueFloat64Type{
				Value: rapid.Float64().Draw(t, "float_value"),
			}
		}),
	)
}

func genCollectinID(t *rapid.T) types.UniqueID {
	return rapid.Custom[types.UniqueID](func(t *rapid.T) types.UniqueID {
		return types.MustParse(rapid.StringMatching(`[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}`).Draw(t, "uuid"))
	}).Draw(t, "collection_id")
}

func TestMeta(t *testing.T) {
	// rapid.Check(t, testMeta)
}
