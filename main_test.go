package main

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestLegacyBaselineAndDurableResults(t *testing.T) {
	now := time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)
	state := filepath.Join(t.TempDir(), "quota.json")
	if err := os.WriteFile(state, []byte(`{"chibigeo":{"day":"2026-09-07","month":"2026-09","daily":2500,"monthly":2500}}`), 0600); err != nil {
		t.Fatal(err)
	}
	provider := p("chibigeo")
	provider.Daily, provider.Monthly = 2500, 0
	r, err := newRelay(Config{Providers: []Provider{provider}}, state)
	if err != nil {
		t.Fatal(err)
	}
	checkpoint, err := os.ReadFile(state + ".pre-v1")
	if err != nil || !strings.Contains(string(checkpoint), `"daily":2500`) {
		t.Fatal("legacy checkpoint missing")
	}
	w := httptest.NewRecorder()
	r.metricsAt(w, now)
	for _, want := range []string{
		`photon_relay_daily_quota_used{provider="chibigeo"} 2500`,
		`photon_relay_daily_quota_baseline{provider="chibigeo"} 2500`,
		`photon_relay_daily_requests{provider="chibigeo"} 0`,
		`photon_relay_daily_remaining{provider="chibigeo"} 0`,
		`photon_relay_monthly_remaining{provider="chibigeo"} -1`,
		fmt.Sprintf(`photon_relay_quota_reset_timestamp_seconds{period="day"} %d`, time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC).Unix()),
	} {
		if !strings.Contains(w.Body.String(), want+"\n") {
			t.Fatalf("missing %s", want)
		}
	}
	if _, err := r.reserve(now, nil); err == nil {
		t.Fatal("legacy reservation lost")
	}
	if _, err := r.reserve(now.Add(24*time.Hour), nil); err != nil {
		t.Fatal(err)
	}
	r.observed(provider, "success", 200, 120*time.Millisecond)
	r2, err := newRelay(Config{Providers: []Provider{provider}}, state)
	if err != nil {
		t.Fatal(err)
	}
	u := r2.usage[provider.Name]
	if u.Daily != 1 || u.DailyBaseline != 0 || u.Monthly != 2501 || u.MonthlyBaseline != 2500 || u.Attempts != 1 || u.Successes != 1 || u.DurationCount != 1 || u.Outcomes["success"] != 1 {
		t.Fatalf("state not preserved: %+v", u)
	}
	if old := u.History["2026-09-07"]; old.QuotaUsed != 2500 || old.Baseline != 2500 || old.Requests != 0 {
		t.Fatal(old)
	}
}

func TestDayHistoryIsBoundedAndMonthRolloverPreservesTotals(t *testing.T) {
	provider := p("a")
	provider.Monthly = 0
	r := fixture(t, provider)
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 40; i++ {
		if _, err := r.reserve(now.AddDate(0, 0, i), nil); err != nil {
			t.Fatal(err)
		}
	}
	u := r.usage["a"]
	if len(u.History) != 35 || u.Attempts != 40 || u.Monthly != 9 || u.Daily != 1 {
		t.Fatalf("bad rollover: %+v", u)
	}
	if _, exists := u.History["2026-01-01"]; exists {
		t.Fatal("old history not pruned")
	}
	w := httptest.NewRecorder()
	r.metricsAt(w, time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC))
	if !strings.Contains(w.Body.String(), `photon_relay_monthly_requests{provider="a"} 0`+"\n") {
		t.Fatal("idle month not projected as reset")
	}
	if r.usage["a"].Monthly != 9 {
		t.Fatal("metrics mutated persistence")
	}
}

func TestMetricsHaveTypesAndCumulativeHistograms(t *testing.T) {
	provider := p("a")
	r := fixture(t, provider)
	now := time.Now().UTC()
	if _, err := r.reserve(now, nil); err != nil {
		t.Fatal(err)
	}
	r.observed(provider, "success", 200, 120*time.Millisecond)
	if _, err := r.reserve(now.Add(time.Second), nil); err != nil {
		t.Fatal(err)
	}
	r.observed(provider, "timeout", 0, 2*time.Second)
	w := httptest.NewRecorder()
	r.metricsAt(w, now.Add(2*time.Second))
	text := w.Body.String()
	for _, want := range []string{
		"# TYPE photon_relay_attempts_total counter",
		"# TYPE photon_relay_daily_requests gauge",
		"# TYPE photon_relay_upstream_request_duration_seconds histogram",
		`photon_relay_upstream_request_duration_seconds_bucket{provider="a",le="0.1"} 0`,
		`photon_relay_upstream_request_duration_seconds_bucket{provider="a",le="0.25"} 1`,
		`photon_relay_upstream_request_duration_seconds_bucket{provider="a",le="2.5"} 2`,
		`photon_relay_upstream_request_duration_seconds_bucket{provider="a",le="+Inf"} 2`,
		`photon_relay_upstream_request_duration_seconds_count{provider="a"} 2`,
		`photon_relay_upstream_request_duration_seconds_sum{provider="a"} 2.12`,
		`photon_relay_attempts_total{provider="a"} 2`,
		`photon_relay_outcomes_total{provider="a",outcome="timeout"} 1`,
	} {
		if strings.Count(text, want+"\n") != 1 {
			t.Fatalf("missing or duplicate: %s", want)
		}
	}
	seen := map[string]bool{}
	for _, line := range strings.Split(text, "\n") {
		if strings.HasPrefix(line, "# TYPE ") {
			key := strings.Fields(line)[2]
			if seen[key] {
				t.Fatal("duplicate TYPE", key)
			}
			seen[key] = true
		}
	}
	if strings.Contains(text, "example.invalid") || strings.Contains(text, "lat=") {
		t.Fatal("sensitive metrics label")
	}
}

func TestMetricsAndHealthDoNotCountAsLookups(t *testing.T) {
	r := fixture(t, p("a"))
	for _, path := range []string{"/healthz", "/metrics"} {
		r.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", path, nil))
	}
	if len(r.clientResponses) != 0 || len(r.usage) != 0 {
		t.Fatal("probe charged as request")
	}
	r.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("POST", "/reverse", nil))
	if r.clientResponses[405] != 1 {
		t.Fatal("client method error not counted")
	}
}

func TestUnknownSchemaFailsClosed(t *testing.T) {
	state := filepath.Join(t.TempDir(), "quota.json")
	if err := os.WriteFile(state, []byte(`{"a":{"version":2}}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := newRelay(Config{Providers: []Provider{p("a")}}, state); err == nil {
		t.Fatal("future schema silently reset")
	}
}

func fixture(t *testing.T, providers ...Provider) *Relay {
	t.Helper()
	r, e := newRelay(Config{Providers: providers}, filepath.Join(t.TempDir(), "quota.json"))
	if e != nil {
		t.Fatal(e)
	}
	return r
}
func p(name string) Provider {
	return Provider{Name: name, URL: "https://example.invalid", IntervalMS: 1000, Daily: 2, Monthly: 3}
}
func TestRoundRobinQuotaAndRestart(t *testing.T) {
	r := fixture(t, p("a"), p("b"))
	now := time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC)
	for i, want := range []string{"a", "b", "a", "b"} {
		got, e := r.reserve(now.Add(time.Duration(i)*time.Second), nil)
		if e != nil || got.Name != want {
			t.Fatalf("%s %v", got.Name, e)
		}
	}
	if _, e := r.reserve(now.Add(time.Hour), nil); e == nil {
		t.Fatal("daily cap exceeded")
	}
	r2, e := newRelay(Config{Providers: r.providers}, r.state)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = r2.reserve(now.Add(time.Hour), nil); e == nil {
		t.Fatal("restart reset quota")
	}
	for i := 0; i < 2; i++ {
		if _, e = r2.reserve(now.Add(24*time.Hour+time.Duration(i)*time.Second), nil); e != nil {
			t.Fatal(e)
		}
	}
	if _, e = r2.reserve(now.Add(48*time.Hour), nil); e == nil {
		t.Fatal("monthly cap exceeded")
	}
	if _, e = r2.reserve(now.AddDate(0, 1, 0), nil); e != nil {
		t.Fatal(e)
	}
}
func TestPacingAndClockRegression(t *testing.T) {
	r := fixture(t, p("a"))
	now := time.Now().UTC()
	if _, e := r.reserve(now, nil); e != nil {
		t.Fatal(e)
	}
	if _, e := r.reserve(now.Add(500*time.Millisecond), nil); e == nil {
		t.Fatal("pacing bypass")
	}
	if _, e := r.reserve(now.AddDate(0, 0, -1), nil); e == nil {
		t.Fatal("clock rollback reopened quota")
	}
}
func TestConcurrentReservation(t *testing.T) {
	r := fixture(t, p("a"))
	var accepted atomic.Int64
	var wg sync.WaitGroup
	now := time.Now()
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, e := r.reserve(now, nil); e == nil {
				accepted.Add(1)
			}
		}()
	}
	wg.Wait()
	if accepted.Load() != 1 {
		t.Fatal(accepted.Load())
	}
}
func TestFailClosedStorage(t *testing.T) {
	r := fixture(t, p("a"))
	r.state = filepath.Join(t.TempDir(), "missing", "quota.json")
	if _, e := r.reserve(time.Now(), nil); e == nil || !r.broken {
		t.Fatal("disk error allowed request")
	}
	state := filepath.Join(t.TempDir(), "quota.json")
	os.WriteFile(state, []byte("broken"), 0600)
	if _, e := newRelay(Config{Providers: []Provider{p("a")}}, state); e == nil {
		t.Fatal("corruption reset quota")
	}
}
func TestFailoverAndCredentialIsolation(t *testing.T) {
	var calls atomic.Int64
	a := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Header.Get("X-Api-Key") != "secret-a" || r.Header.Get("Authorization") != "" {
			t.Error("wrong first provider auth")
		}
		w.Header().Set("Retry-After", "7200")
		w.WriteHeader(429)
	}))
	defer a.Close()
	b := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Header.Get("X-Api-Key") != "" || r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" {
			t.Error("credential leaked")
		}
		if r.URL.Path != "/reverse" || r.URL.Query().Get("lat") != "0" {
			t.Error("query lost")
		}
		io.WriteString(w, `{"type":"FeatureCollection","features":[]}`)
	}))
	defer b.Close()
	pa, pb := p("a"), p("b")
	pa.URL = a.URL
	pb.URL = b.URL
	r := fixture(t, pa, pb)
	r.providers[0].key = "secret-a"
	r.client = a.Client()
	request := httptest.NewRequest("GET", "/reverse?lat=0&lon=0", nil)
	request.Header.Set("Authorization", "Bearer caller-secret")
	request.Header.Set("X-Api-Key", "caller-key")
	request.Header.Set("Cookie", "private")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, request)
	if w.Code != 200 || calls.Load() != 2 {
		t.Fatalf("%d calls=%d", w.Code, calls.Load())
	}
	if time.Until(r.usage["a"].Cooldown) < 119*time.Minute {
		t.Fatal("Retry-After ignored")
	}
	if r.usage["a"].Daily != 1 || r.usage["b"].Daily != 1 {
		t.Fatal("failed attempt not charged")
	}
}
func TestInvalidJSONFailsAndEmptyFeaturesSucceeds(t *testing.T) {
	for _, tc := range []struct {
		body string
		want int
	}{{`<html>blocked</html>`, 503}, {`{"features":[]}`, 503}, {`{"type":"FeatureCollection","features":null}`, 503}, {`{"type":"FeatureCollection","features":[]}`, 200}} {
		t.Run(tc.body, func(t *testing.T) {
			s := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, tc.body) }))
			defer s.Close()
			a := p("a")
			a.URL = s.URL
			r := fixture(t, a)
			r.client = s.Client()
			w := httptest.NewRecorder()
			r.ServeHTTP(w, httptest.NewRequest("GET", "/reverse?lat=0&lon=0", nil))
			if w.Code != tc.want {
				t.Fatal(w.Code)
			}
		})
	}
}
func TestRejectUnsupportedPathsAndSensitiveQuery(t *testing.T) {
	r := fixture(t, p("a"))
	for _, path := range []string{"/update", "/reverse?key=secret", "/reverse?url=https://other.invalid"} {
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		if w.Code == 200 {
			t.Fatal(path)
		}
	}
	if len(r.usage) != 0 {
		t.Fatal("invalid request reserved quota")
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
	if strings.Contains(w.Body.String(), "example.invalid") {
		t.Fatal("URL in metrics")
	}
}

func TestRedirectDoesNotForwardProviderKey(t *testing.T) {
	var leaked atomic.Int64
	destination := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { leaked.Add(1) }))
	defer destination.Close()
	source := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, destination.URL, http.StatusFound) }))
	defer source.Close()
	a := p("a")
	a.URL = source.URL
	r := fixture(t, a)
	r.providers[0].key = "must-not-leak"
	r.client.Transport = source.Client().Transport
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest("GET", "/reverse?lat=0&lon=0", nil))
	if w.Code != 503 || leaked.Load() != 0 {
		t.Fatal("redirect followed")
	}
}

func TestChibiGeoBasePathAndMountedKey(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/photon/reverse" || r.Header.Get("X-Api-Key") != "fixture-key" {
			t.Errorf("incorrect base path or mounted key")
		}
		io.WriteString(w, `{"type":"FeatureCollection","features":[]}`)
	}))
	defer server.Close()
	provider := p("chibigeo")
	provider.URL = server.URL + "/v1/photon"
	provider.KeyFile = filepath.Join(t.TempDir(), "key")
	if err := os.WriteFile(provider.KeyFile, []byte("fixture-key\n"), 0600); err != nil {
		t.Fatal(err)
	}
	r := fixture(t, provider)
	r.client = server.Client()
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest("GET", "/reverse?lat=0&lon=0", nil))
	if w.Code != 200 {
		t.Fatal(w.Code)
	}
}

func TestTwoTimeoutsStillReachHealthyThird(t *testing.T) {
	slow := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer slow.Close()
	good := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"type":"FeatureCollection","features":[]}`)
	}))
	defer good.Close()
	a, b, c := p("a"), p("b"), p("c")
	a.URL, b.URL, c.URL = slow.URL, slow.URL, good.URL
	r := fixture(t, a, b, c)
	r.client = slow.Client()
	r.client.Timeout = 100 * time.Millisecond
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest("GET", "/reverse?lat=0&lon=0", nil))
	if w.Code != 200 || r.failures["a"] != 1 || r.failures["b"] != 1 || r.successes["c"] != 1 {
		t.Fatalf("status=%d failures=%v successes=%v", w.Code, r.failures, r.successes)
	}
}

func TestWholeFailoverChainHasDeadline(t *testing.T) {
	slow := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer slow.Close()
	a := p("a")
	a.URL = slow.URL
	b, c, d := p("b"), p("c"), p("d")
	b.URL, c.URL, d.URL = slow.URL, slow.URL, slow.URL
	r := fixture(t, a, b, c, d)
	r.client = slow.Client() // No client timeout: exercise the complete chain budget.
	start := time.Now()
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest("GET", "/reverse?lat=0&lon=0", nil))
	if elapsed := time.Since(start); w.Code != 503 || elapsed > 5*time.Second {
		t.Fatalf("status=%d elapsed=%s", w.Code, elapsed)
	}
}

func TestProviderCanUseMoreThanOldTimeout(t *testing.T) {
	s := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(1850 * time.Millisecond)
		io.WriteString(w, `{"type":"FeatureCollection","features":[]}`)
	}))
	defer s.Close()
	a := p("a")
	a.URL = s.URL
	r := fixture(t, a)
	r.client.Transport = s.Client().Transport
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest("GET", "/reverse?lat=0&lon=0", nil))
	if w.Code != 200 {
		t.Fatal(w.Code)
	}
}
