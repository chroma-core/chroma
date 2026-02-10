package chroma

import (
	"errors"
	"math"
)

/*
Page provides fluent pagination for Get and Search operations.

# Creating a Page

Use [NewPage] with [Limit] and [Offset] options:

	page := NewPage(Limit(20))

# Basic Usage

Page can be passed directly to [NewSearchRequest] or [Collection.Get]:

	result, err := collection.Search(ctx,
	    NewSearchRequest(
	        WithKnnRank(KnnQueryText("query")),
	        NewPage(Limit(20)),
	    ),
	)

# Iteration Pattern

Use [Page.Next] and [Page.Prev] for simple pagination without off-by-one errors:

	page := NewPage(Limit(20))
	for {
	    result, err := collection.Search(ctx,
	        NewSearchRequest(
	            WithKnnRank(KnnQueryText("query")),
	            page,
	        ),
	    )
	    if err != nil || len(result.(*SearchResultImpl).Rows()) == 0 {
	        break
	    }
	    // process results...
	    page = page.Next()
	}

# Works with Get

	results, err := collection.Get(ctx, NewPage(Limit(100)))
*/

// Page provides fluent pagination for Get and Search operations.
// Use [NewPage] to create this option.
//
// Page implements both [GetOption] and [SearchRequestOption], following
// the same pattern as [WithIDs]. Validation is performed via the [Page.Validate]
// method, which is called automatically when the option is applied.
type Page struct {
	limit  int
	offset int
}

// PageOption configures a [Page].
type PageOption func(*Page)

// Limit sets the page size (number of results per page).
// Must be > 0. Validation is deferred until [Page.Validate] is called.
//
// Example:
//
//	page := NewPage(Limit(20))
func Limit(n int) PageOption {
	return func(p *Page) {
		p.limit = n
	}
}

// Offset sets the starting offset (number of results to skip).
// Must be >= 0. Validation is deferred until [Page.Validate] is called.
//
// Example:
//
//	page := NewPage(Limit(20), Offset(40)) // Page 3 (0-indexed)
func Offset(n int) PageOption {
	return func(p *Page) {
		p.offset = n
	}
}

// NewPage creates a new Page with the given options.
// Default limit is 10 if not specified.
//
// Validation is deferred until the Page is applied to an operation
// (via [Page.Validate]), keeping the API ergonomic for inline usage.
//
// Example:
//
//	// Inline usage - validation happens at apply time
//	result, err := col.Search(ctx, NewSearchRequest(
//	    WithKnnRank(KnnQueryText("query")),
//	    NewPage(Limit(20)),
//	))
//
//	// Iteration pattern
//	page := NewPage(Limit(20))
//	page = page.Next()
func NewPage(opts ...PageOption) *Page {
	p := &Page{limit: 10}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// Validate checks that the Page has valid limit and offset values.
// Returns an error if limit <= 0 or offset < 0.
func (p *Page) Validate() error {
	if p.limit <= 0 {
		return errors.New("limit must be greater than 0")
	}
	if p.offset < 0 {
		return errors.New("offset must be greater than or equal to 0")
	}
	return nil
}

// Next returns a new Page for the next set of results.
// If the next offset would overflow, returns a Page at maximum offset.
//
// Example:
//
//	page := NewPage(Limit(20))
//	// page.GetOffset() == 0
//	page = page.Next()
//	// page.GetOffset() == 20
//	page = page.Next()
//	// page.GetOffset() == 40
func (p *Page) Next() *Page {
	if p.offset > math.MaxInt-p.limit {
		return &Page{limit: p.limit, offset: math.MaxInt}
	}
	return &Page{limit: p.limit, offset: p.offset + p.limit}
}

// Prev returns a new Page for the previous set of results.
// The offset is clamped to 0 if it would go negative.
//
// Example:
//
//	page := NewPage(Limit(20), Offset(40))
//	// page.GetOffset() == 40
//	page = page.Prev()
//	// page.GetOffset() == 20
//	page = page.Prev()
//	// page.GetOffset() == 0
//	page = page.Prev()
//	// page.GetOffset() == 0 (clamped)
func (p *Page) Prev() *Page {
	return &Page{limit: p.limit, offset: max(0, p.offset-p.limit)}
}

// Number returns the current page number (0-indexed).
//
// Example:
//
//	page := NewPage(Limit(20), Offset(40))
//	// page.Number() == 2 (third page, 0-indexed)
func (p *Page) Number() int {
	if p.limit == 0 {
		return 0
	}
	return p.offset / p.limit
}

// Size returns the page size (limit).
func (p *Page) Size() int {
	return p.limit
}

// GetOffset returns the current offset.
func (p *Page) GetOffset() int {
	return p.offset
}

// ApplyToGet implements [GetOption].
func (p *Page) ApplyToGet(op *CollectionGetOp) error {
	if err := p.Validate(); err != nil {
		return err
	}
	op.Limit = p.limit
	op.Offset = p.offset
	return nil
}

// ApplyToSearchRequest implements [SearchRequestOption].
func (p *Page) ApplyToSearchRequest(req *SearchRequest) error {
	if err := p.Validate(); err != nil {
		return err
	}
	if req.Limit == nil {
		req.Limit = &SearchPage{}
	}
	req.Limit.Limit = p.limit
	req.Limit.Offset = p.offset
	return nil
}
