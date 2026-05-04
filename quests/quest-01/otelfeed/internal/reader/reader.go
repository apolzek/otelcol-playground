// Package reader streams OTLP JSON payloads from a directory tree.
//
// Files may contain multiple JSON objects concatenated (separated by
// whitespace/newlines). We use encoding/json's streaming decoder to split
// the file into top-level JSON values, then pdata's OTLP/JSON unmarshalers
// to decode each into an ExportRequest.
//
// Why pdata and not protojson: the OTLP/JSON spec encodes trace_id and
// span_id as hex strings, but the stock google.golang.org/protobuf/encoding
// /protojson mapping treats every bytes field as base64. Feeding the hex
// IDs through protojson produces byte slices of the wrong length, and the
// collector then rejects them with "invalid SpanID length". pdata ships
// an OTLP-aware JSON unmarshaler that gets this right.
package reader

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
)

// Signal is the OTLP telemetry type carried by a payload.
type Signal string

const (
	Metrics Signal = "metrics"
	Traces  Signal = "traces"
	Logs    Signal = "logs"
)

// HTTPPath returns the OTLP/HTTP path for the signal.
func (s Signal) HTTPPath() string { return "/v1/" + string(s) }

// Payload is a single OTLP request decoded once and ready to ship by either
// transport. Raw preserves the source bytes for the OTLP/HTTP path (which
// forwards them verbatim, no re-marshal). TraceReq/MetricReq/LogReq hold
// the pdata-decoded form used by the gRPC path and for statistics. Exactly
// one of those is populated, determined by Signal.
type Payload struct {
	Signal   Signal
	Raw      json.RawMessage
	Source   string
	ObjIndex int

	TraceReq  ptraceotlp.ExportRequest
	MetricReq pmetricotlp.ExportRequest
	LogReq    plogotlp.ExportRequest

	// Counts derived from the decoded request — filled at parse time so
	// both transports see the same numbers without re-walking the data.
	SpanCount      int
	MetricCount    int
	DataPointCount int
	LogRecordCount int
}

// detectSignal picks the signal type from a filename.
// Falls back to a peek at the JSON root key if the name is ambiguous.
func detectSignal(name string) Signal {
	n := strings.ToLower(filepath.Base(name))
	switch {
	case strings.Contains(n, "metric"):
		return Metrics
	case strings.Contains(n, "trace") || strings.Contains(n, "span"):
		return Traces
	case strings.Contains(n, "log"):
		return Logs
	}
	return ""
}

func signalFromRoot(raw []byte) Signal {
	head := raw
	if len(head) > 128 {
		head = head[:128]
	}
	s := string(head)
	switch {
	case strings.Contains(s, "resourceMetrics"):
		return Metrics
	case strings.Contains(s, "resourceSpans"):
		return Traces
	case strings.Contains(s, "resourceLogs"):
		return Logs
	}
	return ""
}

// WalkDir lists OTLP JSON files under root (non-recursive by default; set
// recursive=true to descend).
func WalkDir(root string, recursive bool) ([]string, error) {
	var out []string
	if !recursive {
		entries, err := os.ReadDir(root)
		if err != nil {
			return nil, err
		}
		for _, e := range entries {
			if e.IsDir() || !strings.HasSuffix(strings.ToLower(e.Name()), ".json") {
				continue
			}
			out = append(out, filepath.Join(root, e.Name()))
		}
		return out, nil
	}
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if strings.HasSuffix(strings.ToLower(path), ".json") {
			out = append(out, path)
		}
		return nil
	})
	return out, err
}

// decode unmarshals the raw OTLP JSON into the pdata request matching
// p.Signal and fills the per-payload counts.
func (p *Payload) decode() error {
	switch p.Signal {
	case Traces:
		req := ptraceotlp.NewExportRequest()
		if err := req.UnmarshalJSON(p.Raw); err != nil {
			return fmt.Errorf("traces: %w", err)
		}
		p.TraceReq = req
		p.SpanCount = req.Traces().SpanCount()
	case Metrics:
		req := pmetricotlp.NewExportRequest()
		if err := req.UnmarshalJSON(p.Raw); err != nil {
			return fmt.Errorf("metrics: %w", err)
		}
		p.MetricReq = req
		p.MetricCount = req.Metrics().MetricCount()
		p.DataPointCount = req.Metrics().DataPointCount()
	case Logs:
		req := plogotlp.NewExportRequest()
		if err := req.UnmarshalJSON(p.Raw); err != nil {
			return fmt.Errorf("logs: %w", err)
		}
		p.LogReq = req
		p.LogRecordCount = req.Logs().LogRecordCount()
	default:
		return fmt.Errorf("unknown signal %q", p.Signal)
	}
	return nil
}

// StreamFile emits every top-level OTLP request in the file onto ch.
// It returns (emitted, parseErrs, err):
//   - emitted: objects successfully decoded and queued
//   - parseErrs: objects skipped due to OTLP decode failure
//   - err: fatal error framing the file (non-recoverable)
func StreamFile(ctx context.Context, path string, ch chan<- Payload) (int, int, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()

	sig := detectSignal(path)

	// 4 MiB buffered reader — files here are ~100 MiB with ~1–3 MiB objects.
	br := bufio.NewReaderSize(f, 4<<20)
	dec := json.NewDecoder(br)
	dec.UseNumber()

	emitted, parseErrs, idx := 0, 0, 0
	for {
		if err := ctx.Err(); err != nil {
			return emitted, parseErrs, err
		}
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			if err == io.EOF {
				return emitted, parseErrs, nil
			}
			return emitted, parseErrs, fmt.Errorf("%s: decode obj %d: %w", path, idx, err)
		}
		if sig == "" {
			sig = signalFromRoot(raw)
			if sig == "" {
				return emitted, parseErrs, fmt.Errorf("%s: could not infer OTLP signal", path)
			}
		}
		p := Payload{Signal: sig, Raw: raw, Source: filepath.Base(path), ObjIndex: idx}
		idx++
		if err := p.decode(); err != nil {
			parseErrs++
			continue
		}
		select {
		case <-ctx.Done():
			return emitted, parseErrs, ctx.Err()
		case ch <- p:
		}
		emitted++
	}
}
