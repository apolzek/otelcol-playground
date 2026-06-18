package main

import (
	"strings"
	"testing"
	"time"
)

const sampleExposition = `# HELP otelcol_receiver_accepted_spans Number of spans accepted.
# TYPE otelcol_receiver_accepted_spans counter
otelcol_receiver_accepted_spans{receiver="otlp",transport="grpc"} 120
otelcol_receiver_accepted_spans{receiver="otlp",transport="http"} 30
# TYPE otelcol_receiver_accepted_metric_points counter
otelcol_receiver_accepted_metric_points{receiver="otlp",transport="grpc"} 200
# TYPE otelcol_receiver_accepted_log_records counter
otelcol_receiver_accepted_log_records{receiver="otlp",transport="grpc"} 55
otelcol_receiver_refused_spans{receiver="otlp",transport="grpc"} 999
otelcol_process_uptime 12345
`

func TestParseCounters(t *testing.T) {
	c := parseCounters(strings.NewReader(sampleExposition))
	if c.Spans != 150 {
		t.Errorf("spans = %v, want 150 (120+30 across transports)", c.Spans)
	}
	if c.MetricPoints != 200 {
		t.Errorf("metric points = %v, want 200", c.MetricPoints)
	}
	if c.LogRecords != 55 {
		t.Errorf("log records = %v, want 55", c.LogRecords)
	}
}

func TestParseCountersIgnoresUnrelated(t *testing.T) {
	// refused_spans and process_uptime must not leak into accepted counts
	c := parseCounters(strings.NewReader(`otelcol_receiver_refused_spans{a="b"} 7
otelcol_process_uptime 100
`))
	if c.Spans != 0 || c.MetricPoints != 0 || c.LogRecords != 0 {
		t.Errorf("unrelated metrics leaked: %+v", c)
	}
}

func TestParseCountersEmpty(t *testing.T) {
	c := parseCounters(strings.NewReader(""))
	if c.Spans != 0 || c.MetricPoints != 0 || c.LogRecords != 0 {
		t.Errorf("empty input should yield zeros, got %+v", c)
	}
}

func TestSplitMetricLine(t *testing.T) {
	cases := []struct {
		line     string
		wantName string
		wantVal  float64
		wantOK   bool
	}{
		{`otelcol_receiver_accepted_spans{a="b"} 120`, "otelcol_receiver_accepted_spans", 120, true},
		{`metric_no_labels 3.5`, "metric_no_labels", 3.5, true},
		{`malformed_line`, "", 0, false},
		{`name notanumber`, "", 0, false},
	}
	for _, tc := range cases {
		name, val, ok := splitMetricLine(tc.line)
		if ok != tc.wantOK || name != tc.wantName || val != tc.wantVal {
			t.Errorf("splitMetricLine(%q) = (%q,%v,%v), want (%q,%v,%v)",
				tc.line, name, val, ok, tc.wantName, tc.wantVal, tc.wantOK)
		}
	}
}

func TestRatesFromWindow(t *testing.T) {
	base := time.Unix(1_000_000, 0)
	// not enough history yet
	if r := ratesFromWindow(nil); r.Available != true || r.Total != 0 {
		t.Errorf("empty window = %+v, want available zero", r)
	}

	// 4 seconds apart: spans 100->500 => 100/s; bursty metric points net 800 => 200/s
	samples := []sample{
		{t: base, c: Counters{Spans: 100, MetricPoints: 0, LogRecords: 40}},
		{t: base.Add(2 * time.Second), c: Counters{Spans: 300, MetricPoints: 0, LogRecords: 40}},
		{t: base.Add(4 * time.Second), c: Counters{Spans: 500, MetricPoints: 800, LogRecords: 280}},
	}
	r := ratesFromWindow(samples)
	if r.Spans != 100 {
		t.Errorf("spans/s = %v, want 100", r.Spans)
	}
	if r.MetricPoints != 200 {
		t.Errorf("metric points/s = %v, want 200 (smoothed across burst)", r.MetricPoints)
	}
	if r.LogRecords != 60 {
		t.Errorf("log records/s = %v, want 60", r.LogRecords)
	}
	if r.Total != 360 {
		t.Errorf("total/s = %v, want 360", r.Total)
	}
}

func TestRatesFromWindowCounterReset(t *testing.T) {
	base := time.Unix(2_000_000, 0)
	// counter goes backwards (collector restart) => clamp to 0, not negative
	samples := []sample{
		{t: base, c: Counters{Spans: 1000}},
		{t: base.Add(2 * time.Second), c: Counters{Spans: 10}},
	}
	if r := ratesFromWindow(samples); r.Spans != 0 {
		t.Errorf("spans/s after reset = %v, want 0", r.Spans)
	}
}

func TestTrimWindow(t *testing.T) {
	base := time.Unix(3_000_000, 0)
	now := base.Add(10 * time.Second)
	samples := []sample{
		{t: base},                      // 10s old — outside window
		{t: base.Add(4 * time.Second)}, // 6s old — outside window
		{t: base.Add(6 * time.Second)}, // 4s old — inside window (boundary keeper)
		{t: base.Add(9 * time.Second)}, // 1s old — inside
		{t: now},                       // now
	}
	got := trimWindow(samples, now)
	// rateWindow is 5s; cutoff = now-5s = base+5s. Only the 10s-old sample is
	// dropped; base+4s is kept as the boundary straddler so dt spans the window.
	if len(got) != 4 {
		t.Fatalf("len after trim = %d, want 4; got %v", len(got), got)
	}
	if !got[0].t.Equal(base.Add(4 * time.Second)) {
		t.Errorf("oldest kept = %v, want base+4s", got[0].t.Sub(base))
	}
}

func TestNonNeg(t *testing.T) {
	if nonNeg(-5) != 0 {
		t.Error("negative delta (counter reset) should clamp to 0")
	}
	if nonNeg(5) != 5 {
		t.Error("positive delta should pass through")
	}
}
