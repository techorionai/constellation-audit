package httpapi_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/techorionai/constellation-platform-go/auth"

	"github.com/techorionai/constellation-audit/api/openapi"
	httpapi "github.com/techorionai/constellation-audit/internal/handlers/http"
	"github.com/techorionai/constellation-audit/resources/test/mock"
)

// fakeVerifier gives the principal of the bearer token value.
type fakeVerifier map[string]auth.Principal

func (v fakeVerifier) Verify(_ context.Context, token string) (auth.Principal, error) {
	p, ok := v[token]
	if !ok {
		return auth.Principal{}, auth.ErrInvalidToken
	}
	return p, nil
}

const readScope = "audit:log:read"

var routes = map[string]string{
	"GET /v1/audit-entries":        "/v1/audit-entries",
	"GET /v1/audit-entries/export": "/v1/audit-entries/export?format=csv",
	"GET /v1/audit-entries/{id}":   "/v1/audit-entries/0192f4c0-7b1a-7c3e-9a55-3f2d1c0b9a88",
}

func do(h http.Handler, path, token string) *httptest.ResponseRecorder {
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, path, nil)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// U5: each of the 3 routes needs audit:log:read. A route with no entry in the
// table gives 500.
func TestU5_ScopeTable(t *testing.T) {
	table, err := httpapi.NewScopeTable(openapi.Spec)
	if err != nil {
		t.Fatal(err)
	}
	if len(table) != len(routes) {
		t.Fatalf("table = %v", table)
	}
	for route := range routes {
		if got := table[route]; len(got) != 1 || got[0] != readScope {
			t.Fatalf("%s: scopes = %v, want %s", route, got, readScope)
		}
	}

	verifier := fakeVerifier{
		"reader":  {ID: "s1", Type: auth.PrincipalService, App: "reader", Scopes: []string{readScope}},
		"noscope": {ID: "s2", Type: auth.PrincipalService, App: "noscope"},
	}
	h := newServer(t, table, verifier, &mock.Reader{}, &mock.Entries{}).Handler()
	for route, path := range routes {
		if rec := do(h, path, "reader"); rec.Code != wantReader[route] {
			t.Fatalf("%s: status = %d, want %d", route, rec.Code, wantReader[route])
		}
		rec := do(h, path, "noscope")
		if rec.Code != http.StatusForbidden || !contains(rec.Body.String(), `"missingScopes":["`+readScope+`"]`) {
			t.Fatalf("%s: status = %d, body = %s, want 403 with missingScopes", route, rec.Code, rec.Body.String())
		}
		if rec := do(h, path, "bad"); rec.Code != http.StatusUnauthorized {
			t.Fatalf("%s: status = %d, want 401", route, rec.Code)
		}
		if rec := do(h, path, ""); rec.Code != http.StatusUnauthorized {
			t.Fatalf("%s: no token: status = %d, want 401", route, rec.Code)
		}
	}

	// A table with no entry makes each /v1 route fail closed.
	closed := httpapi.New(httpapi.Deps{Scopes: httpapi.ScopeTable{}, Verifier: verifier}).Handler()
	for route, path := range routes {
		if rec := do(closed, path, "reader"); rec.Code != http.StatusInternalServerError {
			t.Fatalf("%s: status = %d, want 500", route, rec.Code)
		}
	}

	// A spec with an operation that has no scope list is an error.
	if _, err := httpapi.NewScopeTable([]byte("paths:\n  /v1/x:\n    get:\n      operationId: X_get\n")); err == nil {
		t.Fatal("an operation with no x-required-scopes must be an error")
	}
	if _, err := httpapi.NewScopeTable([]byte("paths:\n  /v1/x:\n    get:\n      operationId: X_get\n      x-required-scopes: []\n")); err == nil {
		t.Fatal("an operation with an empty x-required-scopes must be an error")
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
