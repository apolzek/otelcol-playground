package main

import (
	"bufio"
	"context"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Counters holds the cumulative accepted counts scraped from the collector's
// internal Prometheus endpoint.
type Counters struct {
	Spans        float64
	MetricPoints float64
	LogRecords   float64
}

// Rates is the per-second throughput derived from two consecutive Counters
// snapshots.
type Rates struct {
	Spans        float64 `json:"spans"`
	MetricPoints float64 `json:"metricPoints"`
	LogRecords   float64 `json:"logRecords"`
	Total        float64 `json:"total"`
	Available    bool    `json:"available"` // false if the collector couldn't be scraped
}

// sample is a timestamped counter snapshot kept in the sliding window.
type sample struct {
	t time.Time
	c Counters
}

// rateWindow is how far back the throughput is averaged. telemetrygen exports
// in batches, so a 1s instantaneous delta aliases badly (0, then a spike).
// Averaging over a few seconds yields a stable records/sec figure.
const rateWindow = 5 * time.Second

// CollectorStats polls the collector's /metrics endpoint and computes the
// measured (server-side) throughput as a moving average over rateWindow. The
// URL is resolved per scrape via urlFn so a runtime change to the configured
// metrics endpoint takes effect live.
type CollectorStats struct {
	urlFn  func() string
	client *http.Client

	mu    sync.Mutex
	rates Rates
}

func NewCollectorStats(urlFn func() string) *CollectorStats {
	return &CollectorStats{
		urlFn:  urlFn,
		client: &http.Client{Timeout: 3 * time.Second},
	}
}

// ratesFromWindow derives per-second rates from the oldest and newest samples
// in the window. Returns an available-but-zero result when there isn't yet
// enough history.
func ratesFromWindow(samples []sample) Rates {
	if len(samples) < 2 {
		return Rates{Available: true}
	}
	first, last := samples[0], samples[len(samples)-1]
	dt := last.t.Sub(first.t).Seconds()
	if dt <= 0 {
		return Rates{Available: true}
	}
	r := Rates{
		Spans:        nonNeg(last.c.Spans-first.c.Spans) / dt,
		MetricPoints: nonNeg(last.c.MetricPoints-first.c.MetricPoints) / dt,
		LogRecords:   nonNeg(last.c.LogRecords-first.c.LogRecords) / dt,
		Available:    true,
	}
	r.Total = r.Spans + r.MetricPoints + r.LogRecords
	return r
}

// trimWindow drops samples that fall entirely before the window, keeping the
// one straddling the boundary so the averaged dt stays close to rateWindow.
func trimWindow(samples []sample, now time.Time) []sample {
	cutoff := now.Add(-rateWindow)
	// Drop the head while the second element is still older than the cutoff,
	// so we retain exactly one sample at/just before the window boundary.
	for len(samples) >= 2 && samples[1].t.Before(cutoff) {
		samples = samples[1:]
	}
	return samples
}

func (c *CollectorStats) Rates() Rates {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.rates
}

// Run polls every interval until ctx is cancelled.
func (c *CollectorStats) Run(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var samples []sample

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			cur, err := c.scrape(ctx)
			if err != nil {
				// collector unreachable: report unavailable and reset history
				c.set(Rates{Available: false})
				samples = samples[:0]
				continue
			}
			samples = append(samples, sample{t: now, c: *cur})
			samples = trimWindow(samples, now)
			c.set(ratesFromWindow(samples))
		}
	}
}

func (c *CollectorStats) set(r Rates) {
	c.mu.Lock()
	c.rates = r
	c.mu.Unlock()
}

func (c *CollectorStats) scrape(ctx context.Context) (*Counters, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.urlFn(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return parseCounters(resp.Body), nil
}

// parseCounters scans Prometheus text exposition and sums the three
// otelcol_receiver_accepted_* families across all label sets.
func parseCounters(r interface {
	Read([]byte) (int, error)
}) *Counters {
	const (
		spansPfx   = "otelcol_receiver_accepted_spans"
		pointsPfx  = "otelcol_receiver_accepted_metric_points"
		recordsPfx = "otelcol_receiver_accepted_log_records"
	)
	var c Counters
	sc := bufio.NewScanner(bufio.NewReader(r))
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || line[0] == '#' {
			continue
		}
		name, val, ok := splitMetricLine(line)
		if !ok {
			continue
		}
		switch {
		case strings.HasPrefix(name, recordsPfx):
			c.LogRecords += val
		case strings.HasPrefix(name, pointsPfx):
			c.MetricPoints += val
		case strings.HasPrefix(name, spansPfx):
			c.Spans += val
		}
	}
	return &c
}

// splitMetricLine extracts the metric name (without labels) and value from a
// single Prometheus exposition line: `name{labels} value [timestamp]`.
func splitMetricLine(line string) (name string, value float64, ok bool) {
	// value is the last whitespace-separated token (ignore optional timestamp
	// only if present — counters here never carry one in practice).
	fields := strings.Fields(line)
	if len(fields) < 2 {
		return "", 0, false
	}
	v, err := strconv.ParseFloat(fields[1], 64)
	if err != nil {
		return "", 0, false
	}
	name = fields[0]
	if i := strings.IndexByte(name, '{'); i >= 0 {
		name = name[:i]
	}
	return name, v, true
}

func nonNeg(f float64) float64 {
	if f < 0 {
		return 0 // counter reset (e.g. collector restart)
	}
	return f
}
