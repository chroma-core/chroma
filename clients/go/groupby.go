package chroma

import (
	"encoding/json"

	"github.com/pkg/errors"
)

// GroupBy groups search results by metadata keys and aggregates within groups.
type GroupBy struct {
	Keys      []Key
	Aggregate Aggregate
}

// NewGroupBy creates a GroupBy that partitions results by metadata keys.
//
// Example:
//
//	result, err := collection.Search(ctx,
//	    NewSearchRequest(
//	        WithKnnRank(KnnQueryText("query"), WithKnnLimit(100)),
//	        WithGroupBy(NewGroupBy(NewMinK(3, KScore), K("category"))),
//	        NewPage(Limit(30)),
//	    ),
//	)
func NewGroupBy(aggregate Aggregate, keys ...Key) *GroupBy {
	return &GroupBy{Keys: keys, Aggregate: aggregate}
}

func (g *GroupBy) Validate() error {
	if g.Aggregate == nil {
		return errors.New("aggregate is required")
	}
	if err := g.Aggregate.Validate(); err != nil {
		return errors.Wrap(err, "invalid aggregate")
	}
	if len(g.Keys) == 0 {
		return errors.New("at least one key is required")
	}
	return nil
}

func (g *GroupBy) MarshalJSON() ([]byte, error) {
	if g.Aggregate == nil {
		return nil, errors.New("aggregate is required")
	}

	keys := make([]string, len(g.Keys))
	for i, k := range g.Keys {
		keys[i] = string(k)
	}

	aggregateData, err := g.Aggregate.MarshalJSON()
	if err != nil {
		return nil, err
	}
	var aggregateMap any
	if err := json.Unmarshal(aggregateData, &aggregateMap); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"keys":      keys,
		"aggregate": aggregateMap,
	})
}
