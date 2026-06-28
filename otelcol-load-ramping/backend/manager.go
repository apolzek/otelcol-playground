package main

import (
	"bufio"
	"context"
	"io"
	"log"
	"os/exec"
	"strings"
	"sync"
	"time"
)

// ringBuffer keeps the last N log lines emitted by telemetrygen processes so
// the UI can show what is actually happening under the hood.
type ringBuffer struct {
	mu    sync.Mutex
	lines []string
	max   int
}

func newRingBuffer(max int) *ringBuffer { return &ringBuffer{max: max} }

func (r *ringBuffer) add(line string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lines = append(r.lines, line)
	if len(r.lines) > r.max {
		r.lines = r.lines[len(r.lines)-r.max:]
	}
}

func (r *ringBuffer) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.lines))
	copy(out, r.lines)
	return out
}

// proc tracks one running telemetrygen subprocess.
type proc struct {
	cancel context.CancelFunc
	spec   string // fingerprint of the config that started it
	done   chan struct{}
}

// Manager owns the current config and the set of running telemetrygen
// processes (at most one per signal). Apply() reconciles running processes
// with a new config, restarting only the signals whose spec changed — this is
// the "hot reload" mechanism.
type Manager struct {
	bin string // path to the telemetrygen binary
	log *ringBuffer

	mu    sync.Mutex
	cfg   Config
	procs map[signal]*proc
}

func NewManager(bin string) *Manager {
	return &Manager{
		bin:   bin,
		log:   newRingBuffer(200),
		cfg:   DefaultConfig(),
		procs: map[signal]*proc{},
	}
}

func (m *Manager) Config() Config {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.cfg
}

func (m *Manager) Logs() []string { return m.log.snapshot() }

// Running reports which signals currently have a live process.
func (m *Manager) Running() map[signal]bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := map[signal]bool{}
	for s, p := range m.procs {
		out[s] = p != nil
	}
	return out
}

// Apply stores cfg and reconciles processes. For each signal it stops the
// running process if the desired spec changed (or the signal was disabled),
// then starts a fresh one if the signal is enabled.
func (m *Manager) Apply(cfg Config) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cfg = cfg

	for _, s := range []signal{sigTraces, sigMetrics, sigLogs} {
		want := cfg.spec(s) // "" when disabled
		cur := m.procs[s]

		if cur != nil && cur.spec == want {
			continue // already running with the right spec
		}
		if cur != nil {
			m.stopLocked(s)
		}
		if want != "" {
			m.startLocked(s, cfg.args(s), want)
		}
	}
}

// StopAll terminates every running process (used on shutdown).
func (m *Manager) StopAll() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for s := range m.procs {
		m.stopLocked(s)
	}
}

func (m *Manager) stopLocked(s signal) {
	p := m.procs[s]
	if p == nil {
		return
	}
	p.cancel()
	select {
	case <-p.done:
	case <-time.After(3 * time.Second):
		log.Printf("[%s] timed out waiting for process to exit", s)
	}
	delete(m.procs, s)
}

func (m *Manager) startLocked(s signal, args []string, spec string) {
	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, m.bin, args...)
	// telemetrygen ignores SIGTERM cleanly enough; Kill guarantees teardown.
	cmd.Cancel = func() error { return cmd.Process.Kill() }

	stdout, _ := cmd.StdoutPipe()
	stderr, _ := cmd.StderrPipe()

	if err := cmd.Start(); err != nil {
		cancel()
		m.log.add("[" + string(s) + "] failed to start: " + err.Error())
		log.Printf("[%s] start error: %v", s, err)
		return
	}
	m.log.add("[" + string(s) + "] started: telemetrygen " + join(args))

	done := make(chan struct{})
	p := &proc{cancel: cancel, spec: spec, done: done}
	m.procs[s] = p

	go m.pump(s, stdout)
	go m.pump(s, stderr)
	go func() {
		err := cmd.Wait()
		close(done)
		if ctx.Err() == nil && err != nil {
			// exited on its own (not because we cancelled) — surface it
			m.log.add("[" + string(s) + "] exited: " + err.Error())
		}
	}()
}

func (m *Manager) pump(s signal, r io.Reader) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		line := sc.Text()
		// telemetrygen routes gRPC's chatty channelz/resolver logs through its
		// logger. Keep only real, timestamped generator lines (e.g. "exported
		// batch") and drop the gRPC noise so the UI log panel stays useful.
		if !isGenLogLine(line) || isGRPCNoise(line) {
			continue
		}
		m.log.add("[" + string(s) + "] " + line)
	}
}

// isGenLogLine reports whether a line is a real zap log entry from
// telemetrygen, which always begins with an ISO-8601 timestamp (2026-06-18T…).
// Continuation lines of multi-line JSON dumps fail this check.
func isGenLogLine(s string) bool {
	if len(s) < 5 || s[4] != '-' {
		return false
	}
	for i := 0; i < 4; i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

func isGRPCNoise(s string) bool {
	return strings.Contains(s, "grpc_log") ||
		strings.Contains(s, "channelz/") ||
		strings.Contains(s, "grpclog/")
}

func join(args []string) string {
	out := ""
	for i, a := range args {
		if i > 0 {
			out += " "
		}
		out += a
	}
	return out
}
