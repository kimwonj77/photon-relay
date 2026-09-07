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
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type Provider struct {
	Name       string `json:"name"`
	URL        string `json:"url"`
	KeyFile    string `json:"key_file,omitempty"`
	IntervalMS int    `json:"interval_ms"`
	Daily      int    `json:"daily"`
	Monthly    int    `json:"monthly"`
	key        string
}
type Usage struct {
	Day      string    `json:"day"`
	Month    string    `json:"month"`
	Daily    int       `json:"daily"`
	Monthly  int       `json:"monthly"`
	Next     time.Time `json:"next"`
	Cooldown time.Time `json:"cooldown"`
}
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
	b, e := os.ReadFile(state)
	if e == nil {
		if e = json.Unmarshal(b, &r.usage); e != nil || r.usage == nil {
			return nil, errors.New("invalid quota state; refusing reset")
		}
	} else if !os.IsNotExist(e) {
		return nil, e
	}
	for _, u := range r.usage {
		if u.Daily < 0 || u.Monthly < 0 {
			return nil, errors.New("negative quota state")
		}
	}
	return r, nil
}

// Called under mu. Persist before network I/O; failed requests also consume quota.
// Never refund: upstream may have counted a request even after a timeout.
func (r *Relay) save() error {
	b, e := json.Marshal(r.usage)
	if e != nil {
		return e
	}
	f, e := os.CreateTemp(filepath.Dir(r.state), ".quota-*")
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
	if e = os.Rename(f.Name(), r.state); e != nil {
		return e
	}
	d, e := os.Open(filepath.Dir(r.state))
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
			u.Day = day
			u.Daily = 0
		}
		if month != u.Month {
			u.Month = month
			u.Monthly = 0
		}
		if now.Before(u.Next) || now.Before(u.Cooldown) || (p.Daily > 0 && u.Daily >= p.Daily) || (p.Monthly > 0 && u.Monthly >= p.Monthly) {
			continue
		}
		u.Daily++
		u.Monthly++
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
	r.failures[p.Name]++
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
func (r *Relay) ServeHTTP(w http.ResponseWriter, req *http.Request) {
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
				r.mu.Lock()
				r.successes[p.Name]++
				r.mu.Unlock()
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Cache-Control", "no-store")
				w.Write(b)
				return
			}
		}
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
	r.mu.Lock()
	defer r.mu.Unlock()
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	fmt.Fprintf(w, "photon_relay_quota_storage_healthy %d\n", map[bool]int{true: 0, false: 1}[r.broken])
	now := time.Now().UTC()
	for _, p := range r.providers {
		u := r.usage[p.Name]
		daily, monthly := u.Daily, u.Monthly
		if now.Format("2006-01-02") > u.Day {
			daily = 0
		}
		if now.Format("2006-01") > u.Month {
			monthly = 0
		}
		for name, value := range map[string]int{"daily_requests": daily, "monthly_requests": monthly, "daily_limit": p.Daily, "monthly_limit": p.Monthly, "success_total": r.successes[p.Name], "failure_total": r.failures[p.Name]} {
			fmt.Fprintf(w, "photon_relay_%s{provider=%q} %d\n", name, p.Name, value)
		}
		fmt.Fprintf(w, "photon_relay_cooldown_until_seconds{provider=%q} %d\n", p.Name, max(0, u.Cooldown.Unix()))
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
