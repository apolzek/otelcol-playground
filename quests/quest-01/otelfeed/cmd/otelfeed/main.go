// Command otelfeed is a high-throughput OTLP JSON replayer.
//
// It walks a directory of OTLP JSON files (metrics / traces / logs exported
// by the OpenTelemetry file exporter), streams each top-level request out of
// the files concurrently, and ships them to an OTel Collector via HTTP and
// optionally also via gRPC.
//
// Design notes for performance:
//   - One producer goroutine per input file — decode runs in parallel with
//     network I/O, and the std json.Decoder is already streaming (no need
//     to load a whole 100 MiB file in RAM).
//   - Unbounded parsing is bounded by a channel of configurable depth, which
//     provides natural backpressure without stalling the decoders.
//   - One worker pool fans out to the HTTP sender; when --grpc is set, an
//     independent fan-out ships the same payload over gRPC in parallel.
//   - Raw JSON bytes are shipped verbatim to HTTP (no re-marshaling); gRPC
//     path sends the pdata-decoded ExportRequest produced once at read time.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/apolzek/otelfeed/internal/reader"
	"github.com/apolzek/otelfeed/internal/sender"
	"github.com/apolzek/otelfeed/internal/stats"
)

// accountPayload increments the signal-level counters for a decoded
// payload. Called once per payload on the fan-out path so the figures
// reflect what was ingested (not what each transport individually shipped).
func accountPayload(c *stats.Counters, p reader.Payload) {
	switch p.Signal {
	case reader.Traces:
		c.TracePayloads.Add(1)
		c.Spans.Add(uint64(p.SpanCount))
	case reader.Metrics:
		c.MetricPayloads.Add(1)
		c.MetricInstruments.Add(uint64(p.MetricCount))
		c.DataPoints.Add(uint64(p.DataPointCount))
	case reader.Logs:
		c.LogPayloads.Add(1)
		c.LogRecords.Add(uint64(p.LogRecordCount))
	}
}

type headerFlag map[string]string

func (h headerFlag) String() string {
	var parts []string
	for k, v := range h {
		parts = append(parts, k+"="+v)
	}
	return strings.Join(parts, ",")
}

func (h headerFlag) Set(s string) error {
	kv := strings.SplitN(s, "=", 2)
	if len(kv) != 2 {
		return fmt.Errorf("expected key=value, got %q", s)
	}
	h[strings.TrimSpace(kv[0])] = strings.TrimSpace(kv[1])
	return nil
}

func main() {
	var (
		dir         = flag.String("dir", "samples/otlp-json", "directory containing OTLP JSON files")
		recursive   = flag.Bool("recursive", false, "recurse into subdirectories")
		httpURL     = flag.String("http", "http://localhost:4318", "OTLP/HTTP endpoint (empty to disable)")
		grpcAddr    = flag.String("grpc", "", "OTLP/gRPC endpoint host:port (empty to disable)")
		insec       = flag.Bool("grpc-insecure", true, "disable TLS on gRPC")
		gzipOn      = flag.Bool("gzip", true, "gzip request bodies (HTTP) and use gzip compressor (gRPC)")
		workers     = flag.Int("workers", runtime.NumCPU()*4, "concurrent senders per transport")
		grpcConns   = flag.Int("grpc-conns", 4, "parallel gRPC client connections (HTTP/2 multiplex)")
		queueSize   = flag.Int("queue", 4096, "channel buffer per transport")
		repeat      = flag.Int("repeat", 1, "repeat the dataset N times (load test)")
		timeout     = flag.Duration("timeout", 30*time.Second, "per-request timeout")
		tickEvery   = flag.Duration("progress", 2*time.Second, "interval for live progress output")
		maxRetries  = flag.Int("retries", 3, "max retries on 5xx / network errors (HTTP)")
		rate        = flag.Float64("rate", 0, "max payloads/second across the fan-out (0 = unlimited)")
		minInterval = flag.Duration("min-interval", 0, "minimum spacing between payloads (alternative to --rate; 0 = disabled)")
	)
	headers := headerFlag{}
	flag.Var(&headers, "header", "extra header key=value (repeatable)")
	flag.Parse()

	if *httpURL == "" && *grpcAddr == "" {
		log.Fatal("at least one of --http or --grpc must be set")
	}

	// Resolve rate-limit interval. --rate and --min-interval are two ways to
	// express the same thing; if both are set, use the stricter (larger) gap.
	var pacingInterval time.Duration
	if *rate > 0 {
		pacingInterval = time.Duration(float64(time.Second) / *rate)
	}
	if *minInterval > pacingInterval {
		pacingInterval = *minInterval
	}

	files, err := reader.WalkDir(*dir, *recursive)
	if err != nil {
		log.Fatalf("walk %s: %v", *dir, err)
	}
	if len(files) == 0 {
		log.Fatalf("no .json files under %s", *dir)
	}
	log.Printf("found %d files in %s; workers=%d queue=%d repeat=%d", len(files), *dir, *workers, *queueSize, *repeat)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// SIGINT / SIGTERM → graceful shutdown.
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		log.Printf("signal received, shutting down…")
		cancel()
	}()

	counters := stats.New()

	// --- transports -------------------------------------------------------

	var (
		httpCh   chan reader.Payload
		grpcCh   chan reader.Payload
		wgHTTP   sync.WaitGroup
		wgGRPC   sync.WaitGroup
		httpSnd  *sender.HTTPSender
		grpcSnd  *sender.GRPCSender
	)

	if *httpURL != "" {
		h := http.Header{}
		for k, v := range headers {
			h.Set(k, v)
		}
		httpSnd = sender.NewHTTPSender(sender.HTTPConfig{
			Endpoint:   *httpURL,
			Gzip:       *gzipOn,
			Headers:    h,
			Timeout:    *timeout,
			MaxConns:   *workers * 2,
			MaxRetries: *maxRetries,
		})
		defer httpSnd.Close()
		httpCh = make(chan reader.Payload, *queueSize)
		for i := 0; i < *workers; i++ {
			wgHTTP.Add(1)
			go func() {
				defer wgHTTP.Done()
				for p := range httpCh {
					n, err := httpSnd.Send(ctx, p)
					if err != nil {
						counters.PayloadsErr.Add(1)
						// Don't spam the log — sample errors.
						if counters.PayloadsErr.Load()%100 == 1 {
							log.Printf("http send %s#%d: %v", p.Source, p.ObjIndex, err)
						}
						continue
					}
					counters.HTTPRequests.Add(1)
					counters.BytesHTTP.Add(uint64(n))
					counters.PayloadsOK.Add(1)
				}
			}()
		}
		log.Printf("HTTP sender: %s (gzip=%v, workers=%d)", *httpURL, *gzipOn, *workers)
	}

	if *grpcAddr != "" {
		s, err := sender.NewGRPCSender(ctx, sender.GRPCConfig{
			Endpoint: *grpcAddr,
			Insecure: *insec,
			Gzip:     *gzipOn,
			Headers:  map[string]string(headers),
			Timeout:  *timeout,
			Conns:    *grpcConns,
		})
		if err != nil {
			log.Fatalf("grpc dial: %v", err)
		}
		grpcSnd = s
		defer grpcSnd.Close()
		grpcCh = make(chan reader.Payload, *queueSize)
		for i := 0; i < *workers; i++ {
			wgGRPC.Add(1)
			go func() {
				defer wgGRPC.Done()
				for p := range grpcCh {
					n, err := grpcSnd.Send(ctx, p)
					if err != nil {
						counters.PayloadsErr.Add(1)
						if counters.PayloadsErr.Load()%100 == 1 {
							log.Printf("grpc send %s#%d: %v", p.Source, p.ObjIndex, err)
						}
						continue
					}
					counters.GRPCRequests.Add(1)
					counters.BytesGRPC.Add(uint64(n))
					// When only gRPC is enabled we also credit payloads ok;
					// when both are enabled we already credit via HTTP, so
					// avoid double-counting.
					if httpCh == nil {
						counters.PayloadsOK.Add(1)
					}
				}
			}()
		}
		log.Printf("gRPC sender: %s (gzip=%v, conns=%d, workers=%d)", *grpcAddr, *gzipOn, *grpcConns, *workers)
	}

	// --- progress ticker --------------------------------------------------

	stopTicker := make(chan struct{})
	go func() {
		t := time.NewTicker(*tickEvery)
		defer t.Stop()
		for {
			select {
			case <-stopTicker:
				return
			case <-t.C:
				log.Printf("progress: %s", counters.Snapshot().Format())
			}
		}
	}()

	// --- producers --------------------------------------------------------

	var wgProd sync.WaitGroup
	// Fan out file readers: one goroutine per file. Decoder work is CPU
	// bound, but json.Decoder is fast enough that a handful of goroutines
	// will saturate any realistic collector anyway.
	producerCap := runtime.NumCPU()
	if producerCap > len(files) {
		producerCap = len(files)
	}
	sem := make(chan struct{}, producerCap)

	// Intermediate channel so we can fan out to both transports.
	fan := make(chan reader.Payload, *queueSize)

	// Fan-out goroutine: copy each payload to both transport channels.
	// Signal-level counters (spans / metrics / logs) are incremented here
	// so they represent ingestion totals regardless of which transport
	// is enabled, and to avoid double-counting across transports.
	//
	// When pacingInterval > 0, a ticker gates each payload so we emit at
	// most one every pacingInterval — prevents overwhelming a collector
	// that can't keep up with the natural fan-out rate.
	var wgFan sync.WaitGroup
	wgFan.Add(1)
	var pacer *time.Ticker
	if pacingInterval > 0 {
		pacer = time.NewTicker(pacingInterval)
		defer pacer.Stop()
		log.Printf("rate limit: 1 payload every %s (~%.2f payloads/s)", pacingInterval, float64(time.Second)/float64(pacingInterval))
	}
	go func() {
		defer wgFan.Done()
		for p := range fan {
			if pacer != nil {
				select {
				case <-pacer.C:
				case <-ctx.Done():
					return
				}
			}
			accountPayload(counters, p)
			if httpCh != nil {
				select {
				case httpCh <- p:
				case <-ctx.Done():
					return
				}
			}
			if grpcCh != nil {
				select {
				case grpcCh <- p:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	start := time.Now()
	for r := 0; r < *repeat; r++ {
		for _, path := range files {
			wgProd.Add(1)
			sem <- struct{}{}
			go func(path string) {
				defer wgProd.Done()
				defer func() { <-sem }()
				emitted, parseErrs, err := reader.StreamFile(ctx, path, fan)
				if parseErrs > 0 {
					counters.ParseErr.Add(uint64(parseErrs))
					log.Printf("parse %s: %d object(s) skipped (OTLP decode failed)", path, parseErrs)
				}
				if err != nil && ctx.Err() == nil {
					log.Printf("read %s: %v (emitted %d)", path, err, emitted)
					return
				}
				counters.Files.Add(1)
			}(path)
		}
	}
	wgProd.Wait()
	close(fan)
	wgFan.Wait()

	// Drain workers.
	if httpCh != nil {
		close(httpCh)
		wgHTTP.Wait()
	}
	if grpcCh != nil {
		close(grpcCh)
		wgGRPC.Wait()
	}
	close(stopTicker)

	snap := counters.Snapshot()
	log.Printf("done in %s", time.Since(start).Truncate(time.Millisecond))
	log.Printf("final:    %s", snap.Format())
	fmt.Fprintln(os.Stderr, snap.FormatSummary())
}
