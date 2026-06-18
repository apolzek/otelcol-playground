package main

import (
	"context"
	"net"
	"sync"
	"time"
)

// EndpointHealth periodically TCP-dials an address (resolved per check via
// addrFn) to report whether the target OTLP collector is reachable.
type EndpointHealth struct {
	addrFn  func() string
	timeout time.Duration

	mu     sync.Mutex
	online bool
}

func NewEndpointHealth(addrFn func() string) *EndpointHealth {
	return &EndpointHealth{addrFn: addrFn, timeout: 2 * time.Second}
}

func (h *EndpointHealth) Online() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.online
}

// checkOnce dials the current address and records reachability.
func (h *EndpointHealth) checkOnce() {
	ok := false
	if addr := h.addrFn(); addr != "" {
		conn, err := net.DialTimeout("tcp", addr, h.timeout)
		if err == nil {
			ok = true
			conn.Close()
		}
	}
	h.mu.Lock()
	h.online = ok
	h.mu.Unlock()
}

// Run checks immediately, then every interval until ctx is cancelled.
func (h *EndpointHealth) Run(ctx context.Context, interval time.Duration) {
	h.checkOnce()
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			h.checkOnce()
		}
	}
}
