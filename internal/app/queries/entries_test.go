package queries_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/techorionai/constellation-audit/internal/app/queries"
	apperrors "github.com/techorionai/constellation-audit/internal/common/errors"
	"github.com/techorionai/constellation-audit/internal/domain"
	"github.com/techorionai/constellation-audit/internal/ports/entries"
	"github.com/techorionai/constellation-audit/resources/test/mock"
)

func bigEntry(seq int64) domain.StoredEntry {
	details, _ := json.Marshal(map[string]string{"text": strings.Repeat("x", 10<<10)})
	return domain.StoredEntry{
		Seq: seq, ReceivedAt: time.Now(),
		Entry: domain.Entry{ID: "0192f4c0-7b1a-7c3e-9a55-3f2d1c0b9a88", Service: "helpdesk", Time: time.Unix(1000-seq, 0).UTC(),
			Action: "helpdesk:ticket:write", Result: "success", Actor: "usr_1", ActorType: "user", Details: details},
	}
}

// U3: the page builder with pageSize 200 and entries of 10 KiB gives a page of
// 1 MiB or less and a next cursor.
func TestU3_PageBuilder(t *testing.T) {
	var items []domain.StoredEntry
	for i := range 201 {
		items = append(items, bigEntry(int64(1000-i)))
	}
	repo := &mock.Reader{SearchFunc: func(_ context.Context, _ domain.Filters, _ *domain.Cursor, limit int) ([]domain.StoredEntry, error) {
		if limit != 201 {
			t.Errorf("limit = %d, want 201", limit)
		}
		return items, nil
	}}
	res, err := queries.NewListEntries(repo).Execute(context.Background(), queries.ListEntries{PageSize: 200})
	if err != nil {
		t.Fatal(err)
	}
	total := 0
	for _, e := range res.Items {
		total += e.ApproxSize()
	}
	if len(res.Items) == 0 || len(res.Items) >= 200 || total > queries.MaxPageBytes {
		t.Fatalf("items = %d, size = %d", len(res.Items), total)
	}
	last := res.Items[len(res.Items)-1]
	if res.Next == nil || res.Next.ID != last.Seq || !res.Next.Time.Equal(last.Time) {
		t.Fatalf("next = %+v, last = %+v", res.Next, last)
	}

	// A page has one item at least, also when the item is larger than the limit.
	huge := bigEntry(5)
	huge.Summary = strings.Repeat("y", 2<<20)
	page, more := queries.BuildPage([]domain.StoredEntry{huge, bigEntry(4)}, 10, domain.StoredEntry.ApproxSize)
	if len(page) != 1 || !more {
		t.Fatalf("page = %d, more = %v", len(page), more)
	}

	// The last page has no cursor.
	repo.SearchFunc = func(context.Context, domain.Filters, *domain.Cursor, int) ([]domain.StoredEntry, error) {
		return items[:3], nil
	}
	res, err = queries.NewListEntries(repo).Execute(context.Background(), queries.ListEntries{PageSize: 50})
	if err != nil || len(res.Items) != 3 || res.Next != nil {
		t.Fatalf("res = %+v, err = %v", res, err)
	}
}

func TestListAndGetErrors(t *testing.T) {
	ctx := context.Background()
	problem := func(err error, status int, code string) {
		t.Helper()
		pe, ok := apperrors.AsProblem(err)
		if !ok || pe.Status != status || pe.Code != code {
			t.Fatalf("error = %v, want %d %s", err, status, code)
		}
	}
	repo := &mock.Reader{SearchFunc: func(context.Context, domain.Filters, *domain.Cursor, int) ([]domain.StoredEntry, error) {
		return nil, entries.ErrUnavailable
	}}
	_, err := queries.NewListEntries(repo).Execute(ctx, queries.ListEntries{})
	problem(err, 503, "unavailable")
	repo.SearchFunc = func(context.Context, domain.Filters, *domain.Cursor, int) ([]domain.StoredEntry, error) {
		return nil, errors.Join(entries.ErrDataRefused, errors.New("x"))
	}
	_, err = queries.NewListEntries(repo).Execute(ctx, queries.ListEntries{})
	problem(err, 400, "invalid_parameter")
	_, err = queries.NewListEntries(repo).Execute(ctx, queries.ListEntries{Filters: domain.Filters{Result: "ok"}})
	problem(err, 400, "invalid_parameter")

	_, err = queries.NewGetEntry(repo).Execute(ctx, queries.GetEntry{ID: "0192f4c0-7b1a-7c3e-9a55-3f2d1c0b9a88"})
	problem(err, 404, "audit_entry_not_found")
	_, err = queries.NewGetEntry(repo).Execute(ctx, queries.GetEntry{ID: "0192f4c0-7b1a-7c3e-9a55-3f2d1c0b9a88", Service: "Bad-App"})
	problem(err, 400, "invalid_parameter")
}
