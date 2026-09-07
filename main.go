// Photon relay: single replica, durable reservation-before-send quota accounting.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type Provider struct {
	StatusEnabled bool   `json:"status_enabled,omitempty"`
	Name          string `json:"name"`
	URL           string `json:"url"`
	KeyFile       string `json:"key_file,omitempty"`
	IntervalMS    int    `json:"interval_ms"`
	Daily         int    `json:"daily"`
	Monthly       int    `json:"monthly"`
	key           string
}
type Usage struct {
	MetadataChecked   time.Time           `json:"metadata_checked,omitempty"`
	MetadataSucceeded time.Time           `json:"metadata_succeeded,omitempty"`
	MetadataOK        bool                `json:"metadata_ok,omitempty"`
	DataUpdated       time.Time           `json:"data_updated,omitempty"`
	Version           int                 `json:"version"`
	Day               string              `json:"day"`
	Month             string              `json:"month"`
	Daily             int                 `json:"daily"`
	Monthly           int                 `json:"monthly"`
	Next              time.Time           `json:"next"`
	Cooldown          time.Time           `json:"cooldown"`
	DailyBaseline     int                 `json:"daily_baseline"`
	MonthlyBaseline   int                 `json:"monthly_baseline"`
	Attempts          uint64              `json:"attempts"`
	Successes         uint64              `json:"successes"`
	Failures          uint64              `json:"failures"`
	Outcomes          map[string]uint64   `json:"outcomes,omitempty"`
	LastSuccess       time.Time           `json:"last_success"`
	LastFailure       time.Time           `json:"last_failure"`
	LastStatus        int                 `json:"last_status"`
	DurationCount     uint64              `json:"duration_count"`
	DurationSum       float64             `json:"duration_sum"`
	DurationBuckets   [8]uint64           `json:"duration_buckets"`
	History           map[string]DayUsage `json:"history,omitempty"`
}
type DayUsage struct {
	QuotaUsed int `json:"quota_used"`
	Baseline  int `json:"baseline"`
	Requests  int `json:"tracked_requests"`
}

var durationBounds = [...]float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}
var outcomeNames = []string{"success", "timeout", "network_error", "canceled", "rate_limited", "upstream_4xx", "upstream_5xx", "invalid_response", "request_error"}

type Config struct {
	Providers []Provider `json:"providers"`
}
type Relay struct {
	mu                  sync.Mutex
	providers           []Provider
	usage               map[string]Usage
	cursor              int
	state               string
	broken              bool
	client              *http.Client
	slots               chan struct{}
	successes, failures map[string]int
	clientResponses     map[int]uint64
	started             time.Time
	writeFailures       uint64
}

func newRelay(c Config, state string) (*Relay, error) {
	if len(c.Providers) == 0 {
		return nil, errors.New("no providers")
	}
	names := map[string]bool{}
	for i := range c.Providers {
		p := &c.Providers[i]
		u, err := url.Parse(p.URL)
		if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || !regexp.MustCompile(`^[a-z][a-z0-9_-]{0,40}$`).MatchString(p.Name) || names[p.Name] || p.IntervalMS < 1000 || p.Daily < 0 || p.Monthly < 0 {
			return nil, errors.New("invalid provider configuration (HTTPS, unique name, >=1000ms required)")
		}
		names[p.Name] = true
		if p.KeyFile != "" {
			b, e := os.ReadFile(p.KeyFile)
			if e != nil {
				return nil, errors.New("cannot read provider key file")
			}
			p.key = strings.TrimSpace(string(b))
			if p.key == "" {
				return nil, errors.New("empty provider key")
			}
		}
	}
	r := &Relay{providers: c.Providers, usage: map[string]Usage{}, state: state, slots: make(chan struct{}, 4), successes: map[string]int{}, failures: map[string]int{}, client: &http.Client{Timeout: 2500 * time.Millisecond, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
	r.clientResponses = map[int]uint64{}
	r.started = time.Now()
	b, e := os.ReadFile(state)
	if e == nil {
		if e = json.Unmarshal(b, &r.usage); e != nil || r.usage == nil {
			return nil, errors.New("invalid quota state; refusing reset")
		}
	} else if !os.IsNotExist(e) {
		return nil, e
	}
	needsMigration := false
	for name, u := range r.usage {
		if u.Version < 0 || u.Version > 1 || u.Daily < 0 || u.Monthly < 0 || u.DailyBaseline < 0 || u.MonthlyBaseline < 0 || u.DailyBaseline > u.Daily || u.MonthlyBaseline > u.Monthly || u.DurationSum < 0 {
			return nil, errors.New("invalid or unsupported quota state")
		}
		if u.Day != "" {
			if _, err := time.Parse("2006-01-02", u.Day); err != nil {
				return nil, errors.New("invalid quota day")
			}
		}
		if u.Month != "" {
			if _, err := time.Parse("2006-01", u.Month); err != nil {
				return nil, errors.New("invalid quota month")
			}
		}
		if u.Version == 0 {
			needsMigration = true
			// Legacy quota can include pre-relay reservations. Preserve it, but
			// do not misrepresent it as newly observed outbound traffic.
			u.DailyBaseline, u.MonthlyBaseline = u.Daily, u.Monthly
			u.Version = 1
		}
		r.usage[name] = u
		r.successes[name], r.failures[name] = int(u.Successes), int(u.Failures)
	}
	if needsMigration {
		checkpoint := state + ".pre-v1"
		if _, err := os.Stat(checkpoint); os.IsNotExist(err) {
			if err := atomicWrite(checkpoint, b); err != nil {
				return nil, errors.New("cannot preserve pre-migration checkpoint")
			}
		} else if err != nil {
			return nil, err
		}
	}
	// Verify durable writes before reporting healthy, including a fresh volume.
	if err := r.save(); err != nil {
		return nil, errors.New("cannot persist quota state")
	}
	return r, nil
}

// Called under mu. Persist before network I/O; failed requests also consume quota.
// Never refund: upstream may have counted a request even after a timeout.
func (r *Relay) save() (err error) {
	defer func() {
		if err != nil {
			r.broken = true
			r.writeFailures++
		}
	}()
	b, e := json.Marshal(r.usage)
	if e != nil {
		return e
	}
	return atomicWrite(r.state, b)
}
func atomicWrite(path string, b []byte) error {
	f, e := os.CreateTemp(filepath.Dir(path), ".quota-*")
	if e != nil {
		return e
	}
	defer os.Remove(f.Name())
	if _, e = f.Write(b); e != nil {
		f.Close()
		return e
	}
	if e = f.Sync(); e != nil {
		f.Close()
		return e
	}
	if e = f.Close(); e != nil {
		return e
	}
	if e = os.Rename(f.Name(), path); e != nil {
		return e
	}
	d, e := os.Open(filepath.Dir(path))
	if e != nil {
		return e
	}
	defer d.Close()
	return d.Sync()
}
func (r *Relay) reserve(now time.Time, tried map[string]bool) (Provider, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.broken {
		return Provider{}, errors.New("quota storage unavailable")
	}
	now = now.UTC()
	for n := 0; n < len(r.providers); n++ {
		i := (r.cursor + n) % len(r.providers)
		p := r.providers[i]
		u := r.usage[p.Name]
		if tried[p.Name] {
			continue
		}
		// Clock regression must not reset a newer bucket and reopen its allowance.
		day, month := now.Format("2006-01-02"), now.Format("2006-01")
		if day < u.Day || month < u.Month {
			continue
		}
		if day != u.Day {
			if u.Day != "" {
				if u.History == nil {
					u.History = map[string]DayUsage{}
				}
				u.History[u.Day] = DayUsage{u.Daily, u.DailyBaseline, u.Daily - u.DailyBaseline}
				days := make([]string, 0, len(u.History))
				for d := range u.History {
					days = append(days, d)
				}
				sort.Strings(days)
				for len(days) > 35 {
					delete(u.History, days[0])
					days = days[1:]
				}
			}
			u.Day = day
			u.Daily = 0
			u.DailyBaseline = 0
		}
		if month != u.Month {
			u.Month = month
			u.Monthly = 0
			u.MonthlyBaseline = 0
		}
		if now.Before(u.Next) || now.Before(u.Cooldown) || (p.Daily > 0 && u.Daily >= p.Daily) || (p.Monthly > 0 && u.Monthly >= p.Monthly) {
			continue
		}
		u.Daily++
		u.Monthly++
		u.Version = 1
		u.Attempts++
		u.Next = now.Add(time.Duration(p.IntervalMS) * time.Millisecond)
		r.usage[p.Name] = u
		if e := r.save(); e != nil {
			r.broken = true
			return Provider{}, errors.New("quota storage unavailable")
		}
		r.cursor = (i + 1) % len(r.providers)
		return p, nil
	}
	return Provider{}, errors.New("no eligible provider")
}
func (r *Relay) failed(p Provider, status int, retry string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now().UTC()
	until := now.Add(time.Minute)
	if status == 429 {
		until = now.Add(time.Hour)
	} // Conservative fallback without Retry-After.
	if status == 401 || status == 403 {
		until = now.Add(24 * time.Hour)
	}
	if secs, e := strconv.Atoi(retry); e == nil && secs > 0 && secs <= 31536000 {
		t := now.Add(time.Duration(secs) * time.Second)
		if t.After(until) {
			until = t
		}
	} else if t, e := http.ParseTime(retry); e == nil && t.After(until) {
		until = t
	}
	u := r.usage[p.Name]
	u.Cooldown = until
	r.usage[p.Name] = u
	if e := r.save(); e != nil {
		r.broken = true
	}
	// No coordinates, URLs, credentials, provider bodies, or transport error text.
	slog.Warn("provider_failure", "provider", p.Name, "status", status)
}

func (r *Relay) observed(p Provider, outcome string, status int, elapsed time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	u := r.usage[p.Name]
	if u.Outcomes == nil {
		u.Outcomes = map[string]uint64{}
	}
	u.Outcomes[outcome]++
	u.LastStatus = status
	if outcome == "success" {
		u.Successes++
		u.LastSuccess = time.Now().UTC()
		r.successes[p.Name]++
	} else {
		u.Failures++
		u.LastFailure = time.Now().UTC()
		r.failures[p.Name]++
	}
	seconds := max(0, elapsed.Seconds())
	u.DurationCount++
	u.DurationSum += seconds
	for i, bound := range durationBounds {
		if seconds <= bound {
			u.DurationBuckets[i]++
		}
	}
	r.usage[p.Name] = u
	_ = r.save() // save latches fail-closed state; never refund a dispatched request.
}
func transportOutcome(err error) string {
	var ne net.Error
	if errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &ne) && ne.Timeout()) {
		return "timeout"
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	return "network_error"
}

type responseStatus struct {
	http.ResponseWriter
	status int
}

func (w *responseStatus) WriteHeader(code int) {
	if w.status == 0 {
		w.status = code
		w.ResponseWriter.WriteHeader(code)
	}
}
func (w *responseStatus) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(200)
	}
	return w.ResponseWriter.Write(b)
}
func (r *Relay) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	lookupPath := strings.TrimSuffix(req.URL.Path, "/")
	if lookupPath == "/api" || lookupPath == "/reverse" {
		statusWriter := &responseStatus{ResponseWriter: w}
		w = statusWriter
		defer func() {
			r.mu.Lock()
			defer r.mu.Unlock()
			code := statusWriter.status
			if code == 0 {
				code = 499
			}
			r.clientResponses[code]++
		}()
	}
	if req.Method != http.MethodGet {
		http.Error(w, "GET required", 405)
		return
	}
	if req.URL.Path == "/healthz" {
		r.mu.Lock()
		bad := r.broken
		r.mu.Unlock()
		if bad {
			http.Error(w, "quota storage unavailable", 503)
		} else {
			w.Write([]byte("ok\n"))
		}
		return
	}
	if req.URL.Path == "/metrics" {
		r.metrics(w)
		return
	}
	path := strings.TrimSuffix(req.URL.Path, "/")
	if path != "/api" && path != "/reverse" {
		http.NotFound(w, req)
		return
	}
	if len(req.URL.RawQuery) > 4096 {
		http.Error(w, "query too long", 414)
		return
	}
	q, e := url.ParseQuery(req.URL.RawQuery)
	if e != nil {
		http.Error(w, "invalid query", 400)
		return
	}
	allowed := map[string]bool{"q": true, "lat": true, "lon": true, "lang": true, "limit": true, "radius": true, "osm_tag": true, "layer": true, "bbox": true, "location_bias_scale": true, "zoom": true, "dedupe": true, "distance_sort": true}
	for k := range q {
		if !allowed[k] {
			http.Error(w, "unsupported query parameter", 400)
			return
		}
	}
	select {
	case r.slots <- struct{}{}:
		defer func() { <-r.slots }()
	default:
		w.Header().Set("Retry-After", "2")
		http.Error(w, "busy", 503)
		return
	}
	tried := map[string]bool{}
	// Two slow providers must not hide a healthy third one. Bound the entire
	// chain below the caller's five-second timeout, as well as each attempt.
	ctx, cancel := context.WithTimeout(req.Context(), 4300*time.Millisecond)
	defer cancel()
	for attempt := 0; attempt < 4; attempt++ {
		if ctx.Err() != nil {
			break
		}
		p, e := r.reserve(time.Now(), tried)
		if e != nil {
			break
		}
		tried[p.Name] = true
		attemptStarted := time.Now()
		upstream := strings.TrimRight(p.URL, "/") + path + "?" + q.Encode()
		deadline, _ := ctx.Deadline()
		budget := time.Until(deadline)
		// Reserve a short final fallback window when earlier attempts are slow.
		if budget > 800*time.Millisecond {
			budget -= 800 * time.Millisecond
		}
		attemptCtx, stopAttempt := context.WithTimeout(ctx, min(budget, 2500*time.Millisecond))
		defer stopAttempt() // At most four timers, all bounded by the outer deadline.
		out, e := http.NewRequestWithContext(attemptCtx, http.MethodGet, upstream, nil)
		if e != nil {
			r.observed(p, "request_error", 0, time.Since(attemptStarted))
			r.failed(p, 0, "")
			continue
		}
		out.Header.Set("Accept", "application/json")
		out.Header.Set("User-Agent", "photon-relay/0.1")
		if p.key != "" {
			out.Header.Set("X-Api-Key", p.key)
		}
		resp, e := r.client.Do(out)
		if e != nil {
			r.observed(p, transportOutcome(e), 0, time.Since(attemptStarted))
			r.failed(p, 0, "")
			continue
		}
		b, readErr := io.ReadAll(io.LimitReader(resp.Body, 2*1024*1024+1))
		resp.Body.Close()
		if resp.StatusCode == 200 && readErr == nil && len(b) <= 2*1024*1024 {
			var data struct {
				Type     string          `json:"type"`
				Features json.RawMessage `json:"features"`
			}
			if json.Unmarshal(b, &data) == nil && data.Type == "FeatureCollection" && len(data.Features) > 0 && data.Features[0] == '[' {
				r.observed(p, "success", resp.StatusCode, time.Since(attemptStarted))
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Cache-Control", "no-store")
				w.Write(b)
				return
			}
		}
		outcome := "invalid_response"
		if readErr != nil {
			outcome = transportOutcome(readErr)
		} else if resp.StatusCode == 429 {
			outcome = "rate_limited"
		} else if resp.StatusCode >= 500 {
			outcome = "upstream_5xx"
		} else if resp.StatusCode >= 400 {
			outcome = "upstream_4xx"
		}
		r.observed(p, outcome, resp.StatusCode, time.Since(attemptStarted))
		if resp.StatusCode == 400 || resp.StatusCode == 404 || resp.StatusCode == 422 {
			http.Error(w, "upstream rejected query", resp.StatusCode)
			return
		}
		r.failed(p, resp.StatusCode, resp.Header.Get("Retry-After"))
	}
	w.Header().Set("Retry-After", "60")
	http.Error(w, "no geocoding provider available", 503)
}
func (r *Relay) metrics(w http.ResponseWriter) {
	r.metricsAt(w, time.Now().UTC())
}

// Opt-in public metadata only: never called by /metrics, never sends credentials.
// Reserve the check timestamp durably before sending to avoid restart-driven polls.
func (r *Relay) checkMetadata(ctx context.Context, now time.Time) {
	for _, p := range r.providers {
		if !p.StatusEnabled {
			continue
		}
		if ctx.Err() != nil {
			return
		}
		r.mu.Lock()
		u := r.usage[p.Name]
		if r.broken || (!u.MetadataChecked.IsZero() && now.Sub(u.MetadataChecked) < 24*time.Hour) {
			r.mu.Unlock()
			continue
		}
		u.Version = 1
		u.MetadataChecked, u.MetadataOK = now.UTC(), false
		r.usage[p.Name] = u
		err := r.save()
		r.mu.Unlock()
		if err != nil {
			return
		}
		checkCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		req, err := http.NewRequestWithContext(checkCtx, http.MethodGet, strings.TrimRight(p.URL, "/")+"/status", nil)
		var updated time.Time
		if err == nil {
			req.Header.Set("Accept", "application/json")
			resp, e := r.client.Do(req)
			if e == nil {
				b, e := io.ReadAll(io.LimitReader(resp.Body, 65537))
				resp.Body.Close()
				var data struct {
					ImportDate string `json:"import_date"`
				}
				if resp.StatusCode == 200 && e == nil && len(b) <= 65536 && json.Unmarshal(b, &data) == nil {
					updated, _ = time.Parse(time.RFC3339, data.ImportDate)
					if updated.IsZero() {
						updated, _ = time.Parse("2006-01-02", data.ImportDate)
					}
					if updated.Unix() <= 0 || updated.After(now.Add(5*time.Minute)) {
						updated = time.Time{}
					}
				}
			}
		}
		cancel()
		r.mu.Lock()
		u = r.usage[p.Name]
		u.MetadataOK = !updated.IsZero()
		if u.MetadataOK {
			u.DataUpdated, u.MetadataSucceeded = updated.UTC(), now.UTC()
		}
		r.usage[p.Name] = u
		err = r.save()
		r.mu.Unlock()
		if err != nil {
			return
		}
		if updated.IsZero() {
			slog.Warn("provider_metadata_unavailable", "provider", p.Name)
		}
	}
}
func (r *Relay) metricsAt(w http.ResponseWriter, now time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	now = now.UTC()
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	meta := func(name, kind, help string) {
		fmt.Fprintf(w, "# HELP photon_relay_%s %s\n# TYPE photon_relay_%s %s\n", name, help, name, kind)
	}
	scalar := func(name, kind, help string, value float64) {
		meta(name, kind, help)
		fmt.Fprintf(w, "photon_relay_%s %g\n", name, value)
	}
	boolean := func(b bool) float64 {
		if b {
			return 1
		}
		return 0
	}
	dayReset := time.Date(now.Year(), now.Month(), now.Day()+1, 0, 0, 0, 0, time.UTC)
	monthReset := time.Date(now.Year(), now.Month()+1, 1, 0, 0, 0, 0, time.UTC)
	view := func(p Provider) Usage {
		u := r.usage[p.Name]
		if now.Format("2006-01-02") > u.Day {
			u.Daily, u.DailyBaseline = 0, 0
		}
		if now.Format("2006-01") > u.Month {
			u.Monthly, u.MonthlyBaseline = 0, 0
		}
		return u
	}
	eligible := func(p Provider, u Usage) bool {
		return !r.broken && now.Format("2006-01-02") >= u.Day && now.Format("2006-01") >= u.Month && !now.Before(u.Next) && !now.Before(u.Cooldown) && (p.Daily == 0 || u.Daily < p.Daily) && (p.Monthly == 0 || u.Monthly < p.Monthly)
	}
	remaining := func(limit, used int) float64 {
		if limit == 0 {
			return -1
		}
		return float64(max(0, limit-used))
	}
	scalar("quota_storage_healthy", "gauge", "One when quota persistence is healthy; zero fails closed.", boolean(!r.broken))
	scalar("quota_state_schema_version", "gauge", "Version of per-provider JSON state written by this process.", 1)
	scalar("quota_state_write_errors_total", "counter", "State write errors since process start.", float64(r.writeFailures))
	scalar("process_start_time_seconds", "gauge", "Process start Unix timestamp.", float64(r.started.Unix()))
	scalar("inflight_requests", "gauge", "Current admitted client lookups; excludes metrics and health probes.", float64(len(r.slots)))
	if info, err := os.Stat(r.state); err == nil {
		scalar("quota_state_size_bytes", "gauge", "Size of durable JSON quota state.", float64(info.Size()))
	}
	count := 0
	for _, p := range r.providers {
		if eligible(p, view(p)) {
			count++
		}
	}
	scalar("providers_eligible", "gauge", "Providers eligible for an attempt at scrape time.", float64(count))
	meta("quota_reset_timestamp_seconds", "gauge", "Next UTC calendar quota reset; day is 09:00 KST.")
	fmt.Fprintf(w, "photon_relay_quota_reset_timestamp_seconds{period=\"day\"} %d\nphoton_relay_quota_reset_timestamp_seconds{period=\"month\"} %d\n", dayReset.Unix(), monthReset.Unix())
	// Emit each metric family as one group, with exactly one HELP/TYPE pair.
	families := []struct {
		name, kind, help string
		get              func(Provider, Usage) float64
	}{
		{"upstream_metadata_enabled", "gauge", "One when public status metadata polling is configured.", func(p Provider, u Usage) float64 { return boolean(p.StatusEnabled) }},
		{"upstream_metadata_success", "gauge", "One when the latest metadata check parsed a valid date; see enabled and checked timestamp.", func(p Provider, u Usage) float64 { return boolean(p.StatusEnabled && u.MetadataOK) }},
		{"upstream_metadata_checked_timestamp_seconds", "gauge", "Last reserved status check Unix time; zero means never checked.", func(p Provider, u Usage) float64 { return float64(max(0, u.MetadataChecked.Unix())) }},
		{"upstream_metadata_last_success_timestamp_seconds", "gauge", "Last successful status check Unix time; zero means unknown.", func(p Provider, u Usage) float64 { return float64(max(0, u.MetadataSucceeded.Unix())) }},
		{"daily_quota_used", "gauge", "Current UTC day quota charged, including baseline reservations.", func(p Provider, u Usage) float64 { return float64(u.Daily) }},
		{"monthly_quota_used", "gauge", "Current UTC calendar month quota charged, including baseline reservations.", func(p Provider, u Usage) float64 { return float64(u.Monthly) }},
		{"daily_quota_baseline", "gauge", "Current day pre-tracking or conservative external usage reservation, not observed traffic.", func(p Provider, u Usage) float64 { return float64(u.DailyBaseline) }},
		{"monthly_quota_baseline", "gauge", "Current month pre-tracking or external usage reservation, not observed traffic.", func(p Provider, u Usage) float64 { return float64(u.MonthlyBaseline) }},
		{"daily_requests", "gauge", "Admitted attempts this UTC day since schema upgrade, excluding baseline; a crash before send can overcount.", func(p Provider, u Usage) float64 { return float64(u.Daily - u.DailyBaseline) }},
		{"monthly_requests", "gauge", "Admitted attempts this UTC month since schema upgrade, excluding baseline.", func(p Provider, u Usage) float64 { return float64(u.Monthly - u.MonthlyBaseline) }},
		{"daily_limit", "gauge", "Configured daily quota; zero means unlimited locally.", func(p Provider, u Usage) float64 { return float64(p.Daily) }},
		{"monthly_limit", "gauge", "Configured monthly quota; zero means unlimited locally.", func(p Provider, u Usage) float64 { return float64(p.Monthly) }},
		{"daily_remaining", "gauge", "Remaining daily budget; minus one means unlimited locally.", func(p Provider, u Usage) float64 { return remaining(p.Daily, u.Daily) }},
		{"monthly_remaining", "gauge", "Remaining monthly budget; minus one means unlimited locally.", func(p Provider, u Usage) float64 { return remaining(p.Monthly, u.Monthly) }},
		{"eligible", "gauge", "One when this provider is eligible at scrape time.", func(p Provider, u Usage) float64 { return boolean(eligible(p, u)) }},
		{"cooldown_until_seconds", "gauge", "Unix timestamp when error cooldown ends; zero means none.", func(p Provider, u Usage) float64 { return float64(max(0, u.Cooldown.Unix())) }},
		{"next_eligible_timestamp_seconds", "gauge", "Earliest quota, pacing and cooldown eligibility; not a health guarantee.", func(p Provider, u Usage) float64 {
			next := now
			for _, t := range []time.Time{u.Next, u.Cooldown} {
				if t.After(next) {
					next = t
				}
			}
			if p.Daily > 0 && u.Daily >= p.Daily && dayReset.After(next) {
				next = dayReset
			}
			if p.Monthly > 0 && u.Monthly >= p.Monthly && monthReset.After(next) {
				next = monthReset
			}
			return float64(next.Unix())
		}},
		{"last_success_timestamp_seconds", "gauge", "Last observed successful upstream result; zero means none since tracking began.", func(p Provider, u Usage) float64 { return float64(max(0, u.LastSuccess.Unix())) }},
		{"last_failure_timestamp_seconds", "gauge", "Last observed failed upstream result; zero means none since tracking began.", func(p Provider, u Usage) float64 { return float64(max(0, u.LastFailure.Unix())) }},
		{"last_http_status", "gauge", "Last upstream HTTP status; zero means transport failure or no response yet.", func(p Provider, u Usage) float64 { return float64(u.LastStatus) }},
		{"attempts_total", "counter", "Durable admitted upstream attempts since schema upgrade; reservation precedes send.", func(p Provider, u Usage) float64 { return float64(u.Attempts) }},
		{"success_total", "counter", "Durable observed successful upstream results since schema upgrade.", func(p Provider, u Usage) float64 { return float64(u.Successes) }},
		{"failure_total", "counter", "Durable observed failed upstream results since schema upgrade.", func(p Provider, u Usage) float64 { return float64(u.Failures) }},
	}
	for _, f := range families {
		meta(f.name, f.kind, f.help)
		for _, p := range r.providers {
			fmt.Fprintf(w, "photon_relay_%s{provider=%q} %g\n", f.name, p.Name, f.get(p, view(p)))
		}
	}
	meta("upstream_data_timestamp_seconds", "gauge", "Last known upstream import_date Unix timestamp; absent when unknown, not zero. Check metadata freshness separately.")
	for _, p := range r.providers {
		u := r.usage[p.Name]
		if p.StatusEnabled && !u.DataUpdated.IsZero() {
			fmt.Fprintf(w, "photon_relay_upstream_data_timestamp_seconds{provider=%q} %d\n", p.Name, u.DataUpdated.Unix())
		}
	}
	meta("outcomes_total", "counter", "Durable completed attempts by bounded outcome; no coordinates or error text labels.")
	for _, p := range r.providers {
		for _, outcome := range outcomeNames {
			fmt.Fprintf(w, "photon_relay_outcomes_total{provider=%q,outcome=%q} %d\n", p.Name, outcome, r.usage[p.Name].Outcomes[outcome])
		}
	}
	meta("upstream_request_duration_seconds", "histogram", "Observed attempt duration including failures; durable cumulative buckets.")
	for _, p := range r.providers {
		u := r.usage[p.Name]
		for i, bound := range durationBounds {
			fmt.Fprintf(w, "photon_relay_upstream_request_duration_seconds_bucket{provider=%q,le=%q} %d\n", p.Name, strconv.FormatFloat(bound, 'g', -1, 64), u.DurationBuckets[i])
		}
		fmt.Fprintf(w, "photon_relay_upstream_request_duration_seconds_bucket{provider=%q,le=\"+Inf\"} %d\nphoton_relay_upstream_request_duration_seconds_sum{provider=%q} %g\nphoton_relay_upstream_request_duration_seconds_count{provider=%q} %d\n", p.Name, u.DurationCount, p.Name, u.DurationSum, p.Name, u.DurationCount)
	}
	meta("client_requests_total", "counter", "Client geocoding responses since process start; health/metrics excluded; 499 is no response written.")
	for _, code := range []int{200, 400, 404, 405, 414, 422, 499, 503} {
		fmt.Fprintf(w, "photon_relay_client_requests_total{status=%q} %d\n", strconv.Itoa(code), r.clientResponses[code])
	}
}
func main() {
	config := os.Getenv("CONFIG_FILE")
	if config == "" {
		config = "/config/providers.json"
	}
	state := os.Getenv("STATE_FILE")
	if state == "" {
		state = "/data/quota.json"
	}
	b, e := os.ReadFile(config)
	if e != nil {
		slog.Error("cannot read configuration")
		os.Exit(1)
	}
	var c Config
	decoder := json.NewDecoder(bytes.NewReader(b))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&c) != nil {
		slog.Error("invalid configuration")
		os.Exit(1)
	}
	if os.MkdirAll(filepath.Dir(state), 0700) != nil {
		slog.Error("cannot create state directory")
		os.Exit(1)
	}
	lock, e := os.OpenFile(state+".lock", os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		slog.Error("cannot open state lock")
		os.Exit(1)
	}
	defer lock.Close()
	if syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) != nil {
		slog.Error("state already in use")
		os.Exit(1)
	}
	r, e := newRelay(c, state)
	if e != nil {
		slog.Error("relay initialization failed", "error", e)
		os.Exit(1)
	}
	server := &http.Server{Addr: ":8080", Handler: r, ReadHeaderTimeout: 3 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 8192}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	go func() {
		r.checkMetadata(ctx, time.Now().UTC())
		ticker := time.NewTicker(time.Hour)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				r.checkMetadata(ctx, now.UTC())
			}
		}
	}()
	go func() {
		<-ctx.Done()
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		server.Shutdown(c)
	}()
	slog.Info("relay_started", "providers", len(c.Providers))
	if e = server.ListenAndServe(); e != nil && !errors.Is(e, http.ErrServerClosed) {
		slog.Error("server_failed")
		os.Exit(1)
	}
}
