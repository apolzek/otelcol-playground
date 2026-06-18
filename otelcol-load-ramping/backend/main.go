package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	ossignal "os/signal"
	"syscall"
	"time"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// snapshot is the payload pushed over SSE and returned by /api/state.
type snapshot struct {
	Config     Config          `json:"config"`
	Running    map[string]bool `json:"running"`
	Configured ConfiguredRates `json:"configured"`
	Received   Rates           `json:"received"`
	Status     Status          `json:"status"`
	Logs       []string        `json:"logs"`
	Time       int64           `json:"time"`
}

// Status reports connectivity to the targets the backend talks to. (Whether the
// browser is connected to the backend is determined client-side from the SSE
// stream.)
type Status struct {
	Endpoint       string `json:"endpoint"`       // OTLP endpoint being targeted
	EndpointOnline bool   `json:"endpointOnline"` // TCP-reachable?
	MetricsURL     string `json:"metricsURL"`     // resolved scrape URL
	MetricsOnline  bool   `json:"metricsOnline"`  // last scrape succeeded?
}

// ConfiguredRates is the throughput telemetrygen is *targeting* per the config.
// A value of -1 means "unthrottled" (rate=0).
type ConfiguredRates struct {
	Spans        float64 `json:"spans"`
	MetricPoints float64 `json:"metricPoints"`
	LogRecords   float64 `json:"logRecords"`
	Total        float64 `json:"total"`
}

type server struct {
	mgr    *Manager
	stats  *CollectorStats
	health *EndpointHealth
	now    func() time.Time
}

func (s *server) snapshot() snapshot {
	cfg := s.mgr.Config()
	conf := ConfiguredRates{
		Spans:        clampUnlimited(cfg.recordsPerSec(sigTraces)),
		MetricPoints: clampUnlimited(cfg.recordsPerSec(sigMetrics)),
		LogRecords:   clampUnlimited(cfg.recordsPerSec(sigLogs)),
	}
	conf.Total = conf.Spans + conf.MetricPoints + conf.LogRecords

	running := map[string]bool{}
	for sig, on := range s.mgr.Running() {
		running[string(sig)] = on
	}

	received := s.stats.Rates()
	return snapshot{
		Config:     cfg,
		Running:    running,
		Configured: conf,
		Received:   received,
		Status: Status{
			Endpoint:       cfg.Endpoint,
			EndpointOnline: s.health.Online(),
			MetricsURL:     cfg.metricsURL(),
			MetricsOnline:  received.Available,
		},
		Logs: s.mgr.Logs(),
		Time: s.now().UnixMilli(),
	}
}

// clampUnlimited maps the -1 "unthrottled" sentinel to 0 for display purposes.
func clampUnlimited(v float64) float64 {
	if v < 0 {
		return 0
	}
	return v
}

func (s *server) handleState(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.snapshot())
}

func (s *server) handleConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var cfg Config
	if err := json.NewDecoder(r.Body).Decode(&cfg); err != nil {
		http.Error(w, "invalid config: "+err.Error(), http.StatusBadRequest)
		return
	}
	normalize(&cfg)
	s.mgr.Apply(cfg)
	writeJSON(w, http.StatusOK, s.snapshot())
}

// handleStop disables every signal without discarding the rest of the config.
func (s *server) handleStop(w http.ResponseWriter, r *http.Request) {
	cfg := s.mgr.Config()
	cfg.Traces.Enabled = false
	cfg.Metrics.Enabled = false
	cfg.Logs.Enabled = false
	s.mgr.Apply(cfg)
	writeJSON(w, http.StatusOK, s.snapshot())
}

// handleStream pushes a snapshot once per second over Server-Sent Events.
func (s *server) handleStream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	send := func() {
		b, _ := json.Marshal(s.snapshot())
		w.Write([]byte("data: "))
		w.Write(b)
		w.Write([]byte("\n\n"))
		flusher.Flush()
	}
	send() // immediate first frame
	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			send()
		}
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

// normalize fills in nil maps and clamps obviously-invalid values so the
// manager and rate math never have to guard against them.
func normalize(c *Config) {
	if c.Endpoint == "" {
		c.Endpoint = "otelcol:4317"
	}
	if c.ServiceName == "" {
		c.ServiceName = "telemetrygen"
	}
	if c.MetricsEndpoint == "" {
		c.MetricsEndpoint = "otelcol:8888"
	}
	if c.Headers == nil {
		c.Headers = map[string]string{}
	}
	for _, sc := range []*SignalConfig{&c.Traces, &c.Metrics, &c.Logs} {
		if sc.Attributes == nil {
			sc.Attributes = map[string]string{}
		}
		if sc.Workers < 1 {
			sc.Workers = 1
		}
		if sc.Rate < 0 {
			sc.Rate = 0
		}
	}
	if c.Traces.ChildSpans < 0 {
		c.Traces.ChildSpans = 0
	}
}

// healthz is a trivial liveness endpoint for the frontend/compose to probe.
func (s *server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

func (s *server) routes() *http.ServeMux {
	// API only — the static UI is served by the separate frontend (nginx)
	// service, which reverse-proxies /api/* to this backend.
	mux := http.NewServeMux()
	mux.HandleFunc("/api/state", s.handleState)
	mux.HandleFunc("/api/config", s.handleConfig)
	mux.HandleFunc("/api/stop", s.handleStop)
	mux.HandleFunc("/api/stream", s.handleStream)
	mux.HandleFunc("/healthz", s.handleHealthz)
	return mux
}

func main() {
	bin := env("TELEMETRYGEN_BIN", "telemetrygen")
	addr := env("LISTEN_ADDR", ":8080")

	mgr := NewManager(bin)
	// Optional initial metrics endpoint from env (full URL or host:port).
	if v := os.Getenv("COLLECTOR_METRICS_URL"); v != "" {
		cfg := mgr.Config()
		cfg.MetricsEndpoint = v
		mgr.Apply(cfg)
	}

	// Metrics URL and OTLP dial address are resolved live from the current
	// config, so editing them in the UI takes effect without a restart.
	stats := NewCollectorStats(func() string { return mgr.Config().metricsURL() })
	health := NewEndpointHealth(func() string { return dialAddr(mgr.Config().Endpoint) })

	ctx, cancel := context.WithCancel(context.Background())
	go stats.Run(ctx, time.Second)
	go health.Run(ctx, 2*time.Second)

	srv := &server{mgr: mgr, stats: stats, health: health, now: time.Now}
	httpSrv := &http.Server{Addr: addr, Handler: srv.routes()}

	go func() {
		log.Printf("backend listening on %s (telemetrygen=%s, metrics=%s)", addr, bin, mgr.Config().metricsURL())
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http server: %v", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	ossignal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("shutting down...")
	cancel()
	mgr.StopAll()
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutCancel()
	httpSrv.Shutdown(shutCtx)
}
