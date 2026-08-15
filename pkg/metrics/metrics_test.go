package metrics

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

// An unready node is still a healthy one: /healthz answering 200 is what stops
// an orchestrator from restarting a daemon whose lease merely lapsed, while
// /readyz answering 503 is what stops it from being sent work.
func TestHealthAndReadinessAreSeparate(t *testing.T) {
	notReady := errors.New("the membership lease is not live")
	ready := error(nil)
	h := Handler(func() error { return ready })

	check := func(path string, want int) {
		t.Helper()
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != want {
			t.Fatalf("%s: got %d, want %d (body %q)", path, rec.Code, want, rec.Body.String())
		}
	}

	check("/healthz", http.StatusOK)
	check("/readyz", http.StatusOK)

	ready = notReady
	check("/readyz", http.StatusServiceUnavailable)
	check("/healthz", http.StatusOK)
	check("/metrics", http.StatusOK)
}
