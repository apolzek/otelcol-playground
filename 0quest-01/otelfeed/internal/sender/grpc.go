package sender

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apolzek/otelfeed/internal/reader"

	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/keepalive"
)

// GRPCConfig tunes the OTLP/gRPC sender.
type GRPCConfig struct {
	Endpoint string // host:port (no scheme), e.g. "localhost:4317"
	Insecure bool
	Gzip     bool
	Headers  map[string]string
	Timeout  time.Duration
	// Conns is the number of parallel client connections. gRPC multiplexes
	// over one HTTP/2 connection, but multiple conns let multiple streams
	// fan out across NIC queues for real throughput gains.
	Conns int
}

// GRPCSender ships OTLP payloads over gRPC using the pdata-provided
// collector clients. Payloads are expected to arrive already decoded into
// pdata ExportRequest values (see reader.Payload).
type GRPCSender struct {
	cfg   GRPCConfig
	conns []*grpc.ClientConn

	// round-robin counter for conn selection
	rr uint64

	traces  []ptraceotlp.GRPCClient
	metrics []pmetricotlp.GRPCClient
	logs    []plogotlp.GRPCClient
}

// NewGRPCSender dials `Conns` parallel connections to the endpoint.
func NewGRPCSender(ctx context.Context, cfg GRPCConfig) (*GRPCSender, error) {
	if cfg.Timeout == 0 {
		cfg.Timeout = 30 * time.Second
	}
	if cfg.Conns <= 0 {
		cfg.Conns = 4
	}
	cfg.Endpoint = strings.TrimPrefix(strings.TrimPrefix(cfg.Endpoint, "http://"), "https://")

	var creds credentials.TransportCredentials
	if cfg.Insecure {
		creds = insecure.NewCredentials()
	} else {
		creds = credentials.NewTLS(nil)
	}

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(creds),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: backoff.Config{
				BaseDelay:  200 * time.Millisecond,
				Multiplier: 1.6,
				MaxDelay:   5 * time.Second,
			},
			MinConnectTimeout: 5 * time.Second,
		}),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                30 * time.Second,
			Timeout:             10 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallSendMsgSize(64 << 20), // 64 MiB — OTLP bodies can be big
			grpc.MaxCallRecvMsgSize(16 << 20),
		),
		grpc.WithInitialWindowSize(1 << 24),     // 16 MiB
		grpc.WithInitialConnWindowSize(1 << 24), // 16 MiB
	}
	if cfg.Gzip {
		opts = append(opts, grpc.WithDefaultCallOptions(grpc.UseCompressor(gzip.Name)))
	}

	s := &GRPCSender{cfg: cfg}
	for i := 0; i < cfg.Conns; i++ {
		cc, err := grpc.NewClient(cfg.Endpoint, opts...)
		if err != nil {
			s.closeAll()
			return nil, fmt.Errorf("dial %s: %w", cfg.Endpoint, err)
		}
		s.conns = append(s.conns, cc)
		s.traces = append(s.traces, ptraceotlp.NewGRPCClient(cc))
		s.metrics = append(s.metrics, pmetricotlp.NewGRPCClient(cc))
		s.logs = append(s.logs, plogotlp.NewGRPCClient(cc))
	}
	return s, nil
}

// Send dispatches the already-decoded pdata request via gRPC.
// The returned byte count is the raw OTLP/JSON size of the source payload;
// the actual bytes on the wire depend on gzip compression applied by gRPC.
func (s *GRPCSender) Send(ctx context.Context, p reader.Payload) (int, error) {
	idx := int(atomic.AddUint64(&s.rr, 1)-1) % len(s.conns)

	callCtx, cancel := context.WithTimeout(ctx, s.cfg.Timeout)
	defer cancel()
	if len(s.cfg.Headers) > 0 {
		callCtx = withMetadata(callCtx, s.cfg.Headers)
	}

	switch p.Signal {
	case reader.Traces:
		if _, err := s.traces[idx].Export(callCtx, p.TraceReq); err != nil {
			return 0, err
		}
	case reader.Metrics:
		if _, err := s.metrics[idx].Export(callCtx, p.MetricReq); err != nil {
			return 0, err
		}
	case reader.Logs:
		if _, err := s.logs[idx].Export(callCtx, p.LogReq); err != nil {
			return 0, err
		}
	default:
		return 0, fmt.Errorf("unknown signal %q", p.Signal)
	}
	return len(p.Raw), nil
}

func (s *GRPCSender) Close() error {
	s.closeAll()
	return nil
}

func (s *GRPCSender) closeAll() {
	var wg sync.WaitGroup
	for _, c := range s.conns {
		wg.Add(1)
		go func(c *grpc.ClientConn) { defer wg.Done(); _ = c.Close() }(c)
	}
	wg.Wait()
}
