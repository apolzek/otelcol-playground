// Package stats aggregates counters for live progress reporting.
package stats

import (
	"fmt"
	"strings"
	"sync/atomic"
	"time"
)

type Counters struct {
	// Payload-level outcomes.
	PayloadsOK  atomic.Uint64
	PayloadsErr atomic.Uint64
	ParseErr    atomic.Uint64 // JSON objects that could not be decoded as OTLP

	// Transport-level outcomes. Per-transport counters are incremented
	// only on successful sends.
	HTTPRequests atomic.Uint64
	GRPCRequests atomic.Uint64
	BytesHTTP    atomic.Uint64
	BytesGRPC    atomic.Uint64

	// Signal-level tallies derived from the decoded payloads. These
	// represent what was ingested off the producer side; transports may
	// drop some on failure, but these numbers do not get re-attributed
	// per transport (it would double-count when both are enabled).
	Files             atomic.Uint64
	Spans             atomic.Uint64
	MetricInstruments atomic.Uint64
	DataPoints        atomic.Uint64
	LogRecords        atomic.Uint64

	// Per-signal payload counts (one payload == one OTLP ExportRequest).
	TracePayloads  atomic.Uint64
	MetricPayloads atomic.Uint64
	LogPayloads    atomic.Uint64

	Started time.Time
}

func New() *Counters { return &Counters{Started: time.Now()} }

// Snapshot is a copy of the current counter values, cheap to pass around.
type Snapshot struct {
	PayloadsOK, PayloadsErr, ParseErr uint64
	HTTPReq, GRPCReq                  uint64
	BytesHTTP, BytesGRPC              uint64
	Files                             uint64
	Spans, MetricInstruments          uint64
	DataPoints, LogRecords            uint64
	TracePayloads, MetricPayloads     uint64
	LogPayloads                       uint64
	Elapsed                           time.Duration
}

func (c *Counters) Snapshot() Snapshot {
	return Snapshot{
		PayloadsOK:        c.PayloadsOK.Load(),
		PayloadsErr:       c.PayloadsErr.Load(),
		ParseErr:          c.ParseErr.Load(),
		HTTPReq:           c.HTTPRequests.Load(),
		GRPCReq:           c.GRPCRequests.Load(),
		BytesHTTP:         c.BytesHTTP.Load(),
		BytesGRPC:         c.BytesGRPC.Load(),
		Files:             c.Files.Load(),
		Spans:             c.Spans.Load(),
		MetricInstruments: c.MetricInstruments.Load(),
		DataPoints:        c.DataPoints.Load(),
		LogRecords:        c.LogRecords.Load(),
		TracePayloads:     c.TracePayloads.Load(),
		MetricPayloads:    c.MetricPayloads.Load(),
		LogPayloads:       c.LogPayloads.Load(),
		Elapsed:           time.Since(c.Started),
	}
}

func humanBytes(b uint64) string {
	const unit = 1024.0
	if b < 1024 {
		return fmt.Sprintf("%d B", b)
	}
	f := float64(b)
	units := []string{"KiB", "MiB", "GiB", "TiB"}
	for _, u := range units {
		f /= unit
		if f < unit {
			return fmt.Sprintf("%.2f %s", f, u)
		}
	}
	return fmt.Sprintf("%.2f PiB", f/unit)
}

// Format renders the compact single-line ticker output.
func (s Snapshot) Format() string {
	secs := s.Elapsed.Seconds()
	if secs < 0.001 {
		secs = 0.001
	}
	total := s.PayloadsOK + s.PayloadsErr
	rps := float64(total) / secs
	httpBps := float64(s.BytesHTTP) / secs
	grpcBps := float64(s.BytesGRPC) / secs
	return fmt.Sprintf(
		"elapsed=%s ok=%d err=%d parse_err=%d http=%d grpc=%d throughput=%.0f obj/s  "+
			"spans=%d metrics=%d dp=%d logs=%d  "+
			"http=%s/s grpc=%s/s sent_http=%s sent_grpc=%s",
		s.Elapsed.Truncate(time.Millisecond),
		s.PayloadsOK, s.PayloadsErr, s.ParseErr,
		s.HTTPReq, s.GRPCReq, rps,
		s.Spans, s.MetricInstruments, s.DataPoints, s.LogRecords,
		humanBytes(uint64(httpBps)), humanBytes(uint64(grpcBps)),
		humanBytes(s.BytesHTTP), humanBytes(s.BytesGRPC),
	)
}

// FormatSummary renders a multi-line end-of-run report.
func (s Snapshot) FormatSummary() string {
	var b strings.Builder
	secs := s.Elapsed.Seconds()
	if secs < 0.001 {
		secs = 0.001
	}
	total := s.PayloadsOK + s.PayloadsErr
	fmt.Fprintf(&b, "==== summary ====\n")
	fmt.Fprintf(&b, "elapsed:          %s\n", s.Elapsed.Truncate(time.Millisecond))
	fmt.Fprintf(&b, "files processed:  %d\n", s.Files)
	fmt.Fprintf(&b, "payloads:         total=%d ok=%d send_err=%d parse_err=%d\n",
		total, s.PayloadsOK, s.PayloadsErr, s.ParseErr)
	fmt.Fprintf(&b, "  traces:         payloads=%d spans=%d\n", s.TracePayloads, s.Spans)
	fmt.Fprintf(&b, "  metrics:        payloads=%d instruments=%d data_points=%d\n",
		s.MetricPayloads, s.MetricInstruments, s.DataPoints)
	fmt.Fprintf(&b, "  logs:           payloads=%d records=%d\n", s.LogPayloads, s.LogRecords)
	fmt.Fprintf(&b, "transports:\n")
	fmt.Fprintf(&b, "  HTTP requests:  %d  sent=%s  (%s/s)\n",
		s.HTTPReq, humanBytes(s.BytesHTTP), humanBytes(uint64(float64(s.BytesHTTP)/secs)))
	fmt.Fprintf(&b, "  gRPC requests:  %d  sent=%s  (%s/s) [raw json size; wire is gzipped]\n",
		s.GRPCReq, humanBytes(s.BytesGRPC), humanBytes(uint64(float64(s.BytesGRPC)/secs)))
	fmt.Fprintf(&b, "throughput:       %.0f obj/s", float64(total)/secs)
	return b.String()
}
