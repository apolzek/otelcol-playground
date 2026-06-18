package sender

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/apolzek/otelfeed/internal/reader"
)

// HTTPConfig tunes the OTLP/HTTP sender.
type HTTPConfig struct {
	Endpoint       string        // e.g. "http://localhost:4318"
	Gzip           bool          // gzip request bodies
	Headers        http.Header   // extra headers (auth, etc.)
	Timeout        time.Duration // per-request timeout
	MaxConns       int           // keep-alive conns per host
	MaxRetries     int           // retries on 5xx / network errors
	InitialBackoff time.Duration // retry base delay
}

// HTTPSender ships OTLP JSON payloads over HTTP with a shared, tuned client.
type HTTPSender struct {
	cfg    HTTPConfig
	client *http.Client

	// pooled buffers — hot path allocs dominate otherwise.
	bufPool sync.Pool
	gzPool  sync.Pool
}

// NewHTTPSender builds a sender with an HTTP client tuned for sustained
// throughput (generous connection pool, keep-alives, no idle timeout bounces).
func NewHTTPSender(cfg HTTPConfig) *HTTPSender {
	if cfg.Timeout == 0 {
		cfg.Timeout = 30 * time.Second
	}
	if cfg.MaxConns == 0 {
		cfg.MaxConns = 256
	}
	if cfg.MaxRetries == 0 {
		cfg.MaxRetries = 3
	}
	if cfg.InitialBackoff == 0 {
		cfg.InitialBackoff = 100 * time.Millisecond
	}
	cfg.Endpoint = strings.TrimRight(cfg.Endpoint, "/")

	tr := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          cfg.MaxConns,
		MaxIdleConnsPerHost:   cfg.MaxConns,
		MaxConnsPerHost:       cfg.MaxConns,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		WriteBufferSize:       64 << 10,
		ReadBufferSize:        64 << 10,
		DisableCompression:    true, // we compress ourselves; avoid double work
	}
	return &HTTPSender{
		cfg:    cfg,
		client: &http.Client{Transport: tr, Timeout: cfg.Timeout},
		bufPool: sync.Pool{New: func() any {
			b := bytes.NewBuffer(make([]byte, 0, 1<<20))
			return b
		}},
		gzPool: sync.Pool{New: func() any {
			// BestSpeed — CPU is cheaper than network on hot paths.
			w, _ := gzip.NewWriterLevel(io.Discard, gzip.BestSpeed)
			return w
		}},
	}
}

// Send posts a single payload, with retries on transient failures.
// Returns the uncompressed body size that was shipped.
func (s *HTTPSender) Send(ctx context.Context, p reader.Payload) (int, error) {
	url := s.cfg.Endpoint + p.Signal.HTTPPath()
	bodySize := len(p.Raw)

	var (
		body       io.Reader
		bodyBytes  []byte
		gzBuf      *bytes.Buffer
		releaseGz  func()
	)

	if s.cfg.Gzip {
		gzBuf = s.bufPool.Get().(*bytes.Buffer)
		gzBuf.Reset()
		gz := s.gzPool.Get().(*gzip.Writer)
		gz.Reset(gzBuf)
		if _, err := gz.Write(p.Raw); err != nil {
			s.gzPool.Put(gz)
			s.bufPool.Put(gzBuf)
			return 0, fmt.Errorf("gzip: %w", err)
		}
		if err := gz.Close(); err != nil {
			s.gzPool.Put(gz)
			s.bufPool.Put(gzBuf)
			return 0, fmt.Errorf("gzip close: %w", err)
		}
		s.gzPool.Put(gz)
		bodyBytes = gzBuf.Bytes()
		releaseGz = func() { s.bufPool.Put(gzBuf) }
	} else {
		bodyBytes = p.Raw
	}

	var lastErr error
	delay := s.cfg.InitialBackoff
	for attempt := 0; attempt <= s.cfg.MaxRetries; attempt++ {
		body = bytes.NewReader(bodyBytes)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, body)
		if err != nil {
			if releaseGz != nil {
				releaseGz()
			}
			return 0, err
		}
		req.Header.Set("Content-Type", "application/json")
		if s.cfg.Gzip {
			req.Header.Set("Content-Encoding", "gzip")
		}
		for k, vs := range s.cfg.Headers {
			for _, v := range vs {
				req.Header.Add(k, v)
			}
		}

		resp, err := s.client.Do(req)
		if err != nil {
			lastErr = err
		} else {
			// Always drain the body so the connection can be reused.
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode < 300 {
				if releaseGz != nil {
					releaseGz()
				}
				return bodySize, nil
			}
			lastErr = fmt.Errorf("status %d", resp.StatusCode)
			// Client errors (4xx) other than 429 are not retryable.
			if resp.StatusCode < 500 && resp.StatusCode != http.StatusTooManyRequests {
				break
			}
		}
		if attempt == s.cfg.MaxRetries {
			break
		}
		select {
		case <-ctx.Done():
			if releaseGz != nil {
				releaseGz()
			}
			return 0, ctx.Err()
		case <-time.After(delay):
		}
		delay *= 2
		if delay > 5*time.Second {
			delay = 5 * time.Second
		}
	}
	if releaseGz != nil {
		releaseGz()
	}
	return 0, lastErr
}

// Close releases idle connections.
func (s *HTTPSender) Close() error {
	s.client.CloseIdleConnections()
	return nil
}
