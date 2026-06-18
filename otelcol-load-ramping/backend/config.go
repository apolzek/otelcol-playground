package main

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// SignalConfig holds the knobs shared by every telemetrygen signal plus the
// signal-specific extras (carried in the typed sub-structs below).
type SignalConfig struct {
	Enabled    bool              `json:"enabled"`
	Rate       float64           `json:"rate"`    // records/sec PER WORKER (0 = unthrottled)
	Workers    int               `json:"workers"` // number of goroutines
	Attributes map[string]string `json:"attributes"`

	// traces-only
	ChildSpans   int    `json:"childSpans,omitempty"`
	StatusCode   string `json:"statusCode,omitempty"`
	SpanDuration string `json:"spanDuration,omitempty"`

	// metrics-only
	MetricType string `json:"metricType,omitempty"`

	// logs-only
	Body string `json:"body,omitempty"`

	// traces+logs: minimum payload size in MB per record
	SizeMB int `json:"sizeMB,omitempty"`
}

// Config is the full, hot-reloadable load definition driven by the UI.
type Config struct {
	Endpoint    string            `json:"endpoint"`
	Insecure    bool              `json:"insecure"`
	HTTP        bool              `json:"http"`
	ServiceName string            `json:"serviceName"`
	Headers     map[string]string `json:"headers"`

	// MetricsEndpoint is the collector's internal Prometheus endpoint the
	// backend scrapes for measured throughput. Accepts "host:port" (→
	// http://host:port/metrics) or a full URL.
	MetricsEndpoint string `json:"metricsEndpoint"`

	Traces  SignalConfig `json:"traces"`
	Metrics SignalConfig `json:"metrics"`
	Logs    SignalConfig `json:"logs"`
}

// metricsURL normalizes MetricsEndpoint into a full scrape URL.
func (c Config) metricsURL() string {
	m := strings.TrimSpace(c.MetricsEndpoint)
	if m == "" {
		m = "otelcol:8888"
	}
	if strings.HasPrefix(m, "http://") || strings.HasPrefix(m, "https://") {
		return m
	}
	return "http://" + m + "/metrics"
}

// dialAddr extracts a host:port suitable for a TCP reachability check from an
// OTLP endpoint (which may optionally carry a scheme and/or path).
func dialAddr(endpoint string) string {
	e := strings.TrimSpace(endpoint)
	if i := strings.Index(e, "://"); i >= 0 {
		e = e[i+3:]
	}
	if i := strings.IndexByte(e, '/'); i >= 0 {
		e = e[:i]
	}
	return e
}

// DefaultConfig returns a safe, idle starting point: nothing enabled, pointing
// at the bundled collector.
func DefaultConfig() Config {
	return Config{
		Endpoint:        "otelcol:4317",
		Insecure:        true,
		HTTP:            false,
		ServiceName:     "telemetrygen",
		Headers:         map[string]string{},
		MetricsEndpoint: "otelcol:8888",
		Traces: SignalConfig{
			Rate: 10, Workers: 1, ChildSpans: 1, StatusCode: "Ok",
			SpanDuration: "1ms", Attributes: map[string]string{},
		},
		Metrics: SignalConfig{
			Rate: 10, Workers: 1, MetricType: "Gauge", Attributes: map[string]string{},
		},
		Logs: SignalConfig{
			Rate: 10, Workers: 1, Body: "the message", Attributes: map[string]string{},
		},
	}
}

// signal identifies one of the three telemetrygen subcommands.
type signal string

const (
	sigTraces  signal = "traces"
	sigMetrics signal = "metrics"
	sigLogs    signal = "logs"
)

func (c Config) signalCfg(s signal) SignalConfig {
	switch s {
	case sigTraces:
		return c.Traces
	case sigMetrics:
		return c.Metrics
	default:
		return c.Logs
	}
}

// args builds the telemetrygen argv for a single signal. The first element is
// the subcommand. Returns nil if the signal is disabled.
func (c Config) args(s signal) []string {
	sc := c.signalCfg(s)
	if !sc.Enabled {
		return nil
	}

	workers := sc.Workers
	if workers < 1 {
		workers = 1
	}

	a := []string{
		string(s),
		"--otlp-endpoint", c.Endpoint,
		"--workers", strconv.Itoa(workers),
		"--rate", strconv.FormatFloat(sc.Rate, 'f', -1, 64),
		"--duration", "inf",
		"--interval", "1s",
		"--service", c.ServiceName,
	}
	if c.Insecure {
		a = append(a, "--otlp-insecure")
	}
	if c.HTTP {
		a = append(a, "--otlp-http")
	}

	for _, k := range sortedKeys(c.Headers) {
		a = append(a, "--otlp-header", fmt.Sprintf(`%s=%q`, k, c.Headers[k]))
	}
	for _, k := range sortedKeys(sc.Attributes) {
		a = append(a, "--telemetry-attributes", fmt.Sprintf(`%s=%q`, k, sc.Attributes[k]))
	}

	switch s {
	case sigTraces:
		if sc.ChildSpans > 0 {
			a = append(a, "--child-spans", strconv.Itoa(sc.ChildSpans))
		}
		if sc.StatusCode != "" {
			a = append(a, "--status-code", sc.StatusCode)
		}
		if sc.SpanDuration != "" {
			a = append(a, "--span-duration", sc.SpanDuration)
		}
		if sc.SizeMB > 0 {
			a = append(a, "--size", strconv.Itoa(sc.SizeMB))
		}
	case sigMetrics:
		if sc.MetricType != "" {
			a = append(a, "--metric-type", sc.MetricType)
		}
	case sigLogs:
		if sc.Body != "" {
			a = append(a, "--body", sc.Body)
		}
		if sc.SizeMB > 0 {
			a = append(a, "--size", strconv.Itoa(sc.SizeMB))
		}
	}
	return a
}

// spec is a stable fingerprint of everything that affects a signal's process.
// If two configs produce the same spec for a signal, the running process does
// not need to be restarted.
func (c Config) spec(s signal) string {
	return strings.Join(c.args(s), "\x00")
}

// recordsPerSec returns the *configured* throughput target for a signal,
// expressed in the unit the collector counts (spans, metric points, log
// records). Returns 0 for a disabled signal. When Rate is 0 (unthrottled) the
// target is unknown, so we return -1 to signal "unlimited".
func (c Config) recordsPerSec(s signal) float64 {
	sc := c.signalCfg(s)
	if !sc.Enabled {
		return 0
	}
	workers := sc.Workers
	if workers < 1 {
		workers = 1
	}
	if sc.Rate <= 0 {
		return -1 // unthrottled
	}
	// telemetrygen's rate limiter caps the records the collector counts at
	// rate*workers for every signal. Notably, for traces --child-spans does
	// NOT multiply throughput in rate mode (the limiter throttles per span),
	// so spans/sec == rate*workers regardless of child span count.
	return sc.Rate * float64(workers)
}

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
