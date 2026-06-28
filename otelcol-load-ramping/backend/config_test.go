package main

import (
	"strings"
	"testing"
)

// hasFlag reports whether args contains `flag` immediately followed by `value`.
func hasFlag(args []string, flag, value string) bool {
	for i := 0; i < len(args)-1; i++ {
		if args[i] == flag && args[i+1] == value {
			return true
		}
	}
	return false
}

func hasBool(args []string, flag string) bool {
	for _, a := range args {
		if a == flag {
			return true
		}
	}
	return false
}

func TestArgsDisabledReturnsNil(t *testing.T) {
	c := DefaultConfig() // all signals disabled by default
	for _, s := range []signal{sigTraces, sigMetrics, sigLogs} {
		if got := c.args(s); got != nil {
			t.Errorf("args(%s) for disabled signal = %v, want nil", s, got)
		}
	}
}

func TestArgsTracesCommon(t *testing.T) {
	c := DefaultConfig()
	c.Endpoint = "collector:4317"
	c.Insecure = true
	c.HTTP = true
	c.ServiceName = "svc"
	c.Headers = map[string]string{"authorization": "Bearer xyz"}
	c.Traces.Enabled = true
	c.Traces.Rate = 42.5
	c.Traces.Workers = 4
	c.Traces.ChildSpans = 3
	c.Traces.StatusCode = "Error"
	c.Traces.SpanDuration = "2ms"
	c.Traces.SizeMB = 1
	c.Traces.Attributes = map[string]string{"env": "prod"}

	a := c.args(sigTraces)

	if a[0] != "traces" {
		t.Fatalf("subcommand = %q, want traces", a[0])
	}
	checks := [][2]string{
		{"--otlp-endpoint", "collector:4317"},
		{"--workers", "4"},
		{"--rate", "42.5"},
		{"--duration", "inf"},
		{"--service", "svc"},
		{"--child-spans", "3"},
		{"--status-code", "Error"},
		{"--span-duration", "2ms"},
		{"--size", "1"},
		{"--otlp-header", `authorization="Bearer xyz"`},
		{"--telemetry-attributes", `env="prod"`},
	}
	for _, c2 := range checks {
		if !hasFlag(a, c2[0], c2[1]) {
			t.Errorf("missing flag %s %s in %v", c2[0], c2[1], a)
		}
	}
	if !hasBool(a, "--otlp-insecure") {
		t.Error("expected --otlp-insecure")
	}
	if !hasBool(a, "--otlp-http") {
		t.Error("expected --otlp-http")
	}
}

func TestArgsSecureGRPCOmitsToggles(t *testing.T) {
	c := DefaultConfig()
	c.Insecure = false
	c.HTTP = false
	c.Metrics.Enabled = true
	a := c.args(sigMetrics)
	if hasBool(a, "--otlp-insecure") {
		t.Error("did not expect --otlp-insecure when Insecure=false")
	}
	if hasBool(a, "--otlp-http") {
		t.Error("did not expect --otlp-http when HTTP=false")
	}
	if !hasFlag(a, "--metric-type", "Gauge") {
		t.Errorf("expected --metric-type Gauge, got %v", a)
	}
}

func TestArgsLogs(t *testing.T) {
	c := DefaultConfig()
	c.Logs.Enabled = true
	c.Logs.Body = "hello world"
	c.Logs.SizeMB = 2
	a := c.args(sigLogs)
	if a[0] != "logs" {
		t.Fatalf("subcommand = %q", a[0])
	}
	if !hasFlag(a, "--body", "hello world") {
		t.Errorf("missing --body, got %v", a)
	}
	if !hasFlag(a, "--size", "2") {
		t.Errorf("missing --size, got %v", a)
	}
}

func TestArgsWorkersFloor(t *testing.T) {
	c := DefaultConfig()
	c.Logs.Enabled = true
	c.Logs.Workers = 0
	a := c.args(sigLogs)
	if !hasFlag(a, "--workers", "1") {
		t.Errorf("expected workers floored to 1, got %v", a)
	}
}

func TestSpecSensitivity(t *testing.T) {
	c := DefaultConfig()
	c.Traces.Enabled = true
	base := c.spec(sigTraces)

	if base == "" {
		t.Fatal("enabled signal must have non-empty spec")
	}

	// changing traces rate changes the traces spec
	c2 := c
	c2.Traces.Rate = c.Traces.Rate + 1
	if c2.spec(sigTraces) == base {
		t.Error("spec did not change when rate changed")
	}

	// changing an unrelated signal must NOT change the traces spec
	c3 := c
	c3.Metrics.Rate = 999
	if c3.spec(sigTraces) != base {
		t.Error("traces spec changed due to unrelated metrics change")
	}

	// disabled signal has empty spec
	c4 := c
	c4.Traces.Enabled = false
	if c4.spec(sigTraces) != "" {
		t.Error("disabled signal should have empty spec")
	}
}

func TestRecordsPerSec(t *testing.T) {
	c := DefaultConfig()

	if got := c.recordsPerSec(sigTraces); got != 0 {
		t.Errorf("disabled traces rate = %v, want 0", got)
	}

	c.Traces.Enabled = true
	c.Traces.Rate = 10
	c.Traces.Workers = 2
	c.Traces.ChildSpans = 3
	// rate limiter caps spans/sec at rate*workers; child-spans does NOT multiply
	if got := c.recordsPerSec(sigTraces); got != 20 {
		t.Errorf("traces spans/sec = %v, want 20", got)
	}

	c.Metrics.Enabled = true
	c.Metrics.Rate = 5
	c.Metrics.Workers = 4
	if got := c.recordsPerSec(sigMetrics); got != 20 {
		t.Errorf("metrics points/sec = %v, want 20", got)
	}

	// rate 0 => unthrottled sentinel
	c.Logs.Enabled = true
	c.Logs.Rate = 0
	if got := c.recordsPerSec(sigLogs); got != -1 {
		t.Errorf("unthrottled logs = %v, want -1", got)
	}
}

func TestArgsAttributeQuoting(t *testing.T) {
	c := DefaultConfig()
	c.Traces.Enabled = true
	c.Traces.Attributes = map[string]string{"a": "1", "b": "two words"}
	a := strings.Join(c.args(sigTraces), " ")
	if !strings.Contains(a, `a="1"`) || !strings.Contains(a, `b="two words"`) {
		t.Errorf("attributes not quoted properly: %s", a)
	}
}

func TestMetricsURL(t *testing.T) {
	cases := map[string]string{
		"otelcol:8888":                 "http://otelcol:8888/metrics",
		"localhost:9999":               "http://localhost:9999/metrics",
		"":                             "http://otelcol:8888/metrics",
		"http://host:8888/metrics":     "http://host:8888/metrics",
		"https://collector:443/custom": "https://collector:443/custom",
		"  otelcol:8888  ":             "http://otelcol:8888/metrics",
	}
	for in, want := range cases {
		c := Config{MetricsEndpoint: in}
		if got := c.metricsURL(); got != want {
			t.Errorf("metricsURL(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestDialAddr(t *testing.T) {
	cases := map[string]string{
		"otelcol:4317":           "otelcol:4317",
		"http://otelcol:4318/v1": "otelcol:4318",
		"https://host:443":       "host:443",
		"  localhost:4317  ":     "localhost:4317",
	}
	for in, want := range cases {
		if got := dialAddr(in); got != want {
			t.Errorf("dialAddr(%q) = %q, want %q", in, got, want)
		}
	}
}
