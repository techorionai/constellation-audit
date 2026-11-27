// Package queries holds the use cases that read the audit entries.
package queries

import (
	"context"
	"errors"

	"github.com/techorionai/constellation-audit/internal/app/problems"
	apperrors "github.com/techorionai/constellation-audit/internal/common/errors"
	"github.com/techorionai/constellation-audit/internal/domain"
	"github.com/techorionai/constellation-audit/internal/ports/entries"
)

// Page limits.
const (
	// MaxPageBytes is the largest encoded size of a page.
	MaxPageBytes = 1 << 20
	// envelopeBytes is the space that a page keeps for the parts of the
	// response that are not items.
	envelopeBytes = 1 << 10
	// DefaultPageSize and MaxPageSize are the limits of AuditEntries_list.
	DefaultPageSize = 50
	MaxPageSize     = 200
)

// CodeEntryNotFound is the code of the 404 problem of the get operation.
const CodeEntryNotFound = "audit_entry_not_found"

// BuildPage takes the items of one page from items, which has at most
// pageSize+1 entries. The page ends before the item that makes the encoded
// size larger than MaxPageBytes. A page always has the first item. more is true
// when items has an entry that is not in the page.
func BuildPage[T any](items []T, pageSize int, size func(T) int) (page []T, more bool) {
	total := envelopeBytes
	for i, it := range items {
		if i >= pageSize {
			break
		}
		n := size(it)
		if i > 0 && total+n > MaxPageBytes {
			break
		}
		total += n
		page = append(page, it)
	}
	return page, len(page) < len(items)
}

// ListEntries searches the entries.
type ListEntries struct {
	Filters domain.Filters
	// After is the cursor of the previous page, or nil.
	After    *domain.Cursor
	PageSize int
}

// ListEntriesResult is one page. Next is the cursor of the last item when more
// entries follow, and nil on the last page.
type ListEntriesResult struct {
	Items []domain.StoredEntry
	Next  *domain.Cursor
}

// ListEntriesHandler runs ListEntries.
type ListEntriesHandler struct{ repo entries.Reader }

// NewListEntries makes the handler.
func NewListEntries(repo entries.Reader) *ListEntriesHandler { return &ListEntriesHandler{repo: repo} }

// Execute returns one page.
func (h *ListEntriesHandler) Execute(ctx context.Context, q ListEntries) (ListEntriesResult, error) {
	if err := q.Filters.Validate(); err != nil {
		return ListEntriesResult{}, problems.FromFilter(err)
	}
	size := q.PageSize
	if size <= 0 {
		size = DefaultPageSize
	}
	items, err := h.repo.Search(ctx, q.Filters, q.After, size+1)
	if err != nil {
		return ListEntriesResult{}, problems.FromStore(err)
	}
	page, more := BuildPage(items, size, domain.StoredEntry.ApproxSize)
	res := ListEntriesResult{Items: page}
	if more && len(page) > 0 {
		last := page[len(page)-1]
		res.Next = &domain.Cursor{Time: last.Time, ID: last.Seq}
	}
	return res, nil
}

// GetEntry reads one entry. Service is empty if the request has no service.
type GetEntry struct {
	ID      string
	Service string
}

// GetEntryHandler runs GetEntry.
type GetEntryHandler struct{ repo entries.Reader }

// NewGetEntry makes the handler.
func NewGetEntry(repo entries.Reader) *GetEntryHandler { return &GetEntryHandler{repo: repo} }

// Execute returns the entry. An entry that does not exist gives the 404 problem
// audit_entry_not_found.
func (h *GetEntryHandler) Execute(ctx context.Context, q GetEntry) (domain.StoredEntry, error) {
	if q.Service != "" && !domain.ValidService(q.Service) {
		return domain.StoredEntry{}, problems.InvalidParameter("service")
	}
	e, err := h.repo.Get(ctx, q.ID, q.Service)
	if errors.Is(err, entries.ErrNotFound) {
		return domain.StoredEntry{}, NotFound()
	}
	if err != nil {
		return domain.StoredEntry{}, problems.FromStore(err)
	}
	return e, nil
}

// NotFound is the 404 problem of the get operation.
func NotFound() error {
	return apperrors.NewNotFound(CodeEntryNotFound, "The audit entry does not exist.")
}
