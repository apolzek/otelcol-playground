package main

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// makeFakeBin writes an executable stand-in for telemetrygen that prints its
// args and then blocks until killed. Used to drive the Manager lifecycle
// without the real generator.
func makeFakeBin(t *testing.T) string {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("fake-bin test relies on a POSIX shell")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "fakegen.sh")
	script := "#!/bin/sh\necho \"fake $@\"\nexec sleep 30\n"
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake bin: %v", err)
	}
	return path
}

func countStarted(lines []string, sig signal) int {
	n := 0
	want := "[" + string(sig) + "] started"
	for _, l := range lines {
		if strings.Contains(l, want) {
			n++
		}
	}
	return n
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met within timeout")
}

func TestManagerStartStop(t *testing.T) {
	m := NewManager(makeFakeBin(t))
	t.Cleanup(m.StopAll)

	cfg := DefaultConfig()
	cfg.Traces.Enabled = true
	m.Apply(cfg)

	waitFor(t, func() bool { return m.Running()[sigTraces] })
	if m.Running()[sigMetrics] {
		t.Error("metrics should not be running")
	}
	waitFor(t, func() bool { return countStarted(m.Logs(), sigTraces) == 1 })
}

func TestManagerNoRestartOnUnchangedSpec(t *testing.T) {
	m := NewManager(makeFakeBin(t))
	t.Cleanup(m.StopAll)

	cfg := DefaultConfig()
	cfg.Traces.Enabled = true
	m.Apply(cfg)
	waitFor(t, func() bool { return countStarted(m.Logs(), sigTraces) == 1 })

	// Re-apply identical config: must NOT restart the traces process.
	m.Apply(cfg)
	time.Sleep(150 * time.Millisecond)
	if got := countStarted(m.Logs(), sigTraces); got != 1 {
		t.Errorf("traces restarted on unchanged spec: started count = %d, want 1", got)
	}
}

func TestManagerRestartsOnChange(t *testing.T) {
	m := NewManager(makeFakeBin(t))
	t.Cleanup(m.StopAll)

	cfg := DefaultConfig()
	cfg.Traces.Enabled = true
	m.Apply(cfg)
	waitFor(t, func() bool { return countStarted(m.Logs(), sigTraces) == 1 })

	cfg.Traces.Rate = 123
	m.Apply(cfg)
	waitFor(t, func() bool { return countStarted(m.Logs(), sigTraces) == 2 })
}

func TestManagerDisableStops(t *testing.T) {
	m := NewManager(makeFakeBin(t))
	t.Cleanup(m.StopAll)

	cfg := DefaultConfig()
	cfg.Logs.Enabled = true
	m.Apply(cfg)
	waitFor(t, func() bool { return m.Running()[sigLogs] })

	cfg.Logs.Enabled = false
	m.Apply(cfg)
	waitFor(t, func() bool { return !m.Running()[sigLogs] })
}

func TestManagerStopAll(t *testing.T) {
	m := NewManager(makeFakeBin(t))

	cfg := DefaultConfig()
	cfg.Traces.Enabled = true
	cfg.Metrics.Enabled = true
	cfg.Logs.Enabled = true
	m.Apply(cfg)
	waitFor(t, func() bool {
		r := m.Running()
		return r[sigTraces] && r[sigMetrics] && r[sigLogs]
	})

	m.StopAll()
	r := m.Running()
	if r[sigTraces] || r[sigMetrics] || r[sigLogs] {
		t.Errorf("StopAll left processes running: %+v", r)
	}
}

func TestLogFiltering(t *testing.T) {
	keep := []string{
		`2026-06-18T14:38:18.964Z	DEBUG	metrics/worker.go:277	exported batch	{"worker": 1, "count": 100}`,
		`2026-06-18T14:38:19.793Z	DEBUG	logs/worker.go:148	exported batched logs	{"worker": 0, "count": 100}`,
	}
	for _, l := range keep {
		if !isGenLogLine(l) || isGRPCNoise(l) {
			t.Errorf("should keep useful line: %q", l)
		}
	}

	drop := []string{
		`2026-06-18T14:38:18.962Z	INFO	channelz/trace.go:200	[core] [Channel #1] foo	{"grpc_log": true}`,
		`2026-06-18T14:38:18.962Z	INFO	grpclog/prefix_logger.go:42	bar	{"grpc_log": true}`,
		`          "Addr": "172.19.0.2:4317",`, // JSON continuation, no timestamp
		`}`,
		``,
	}
	for _, l := range drop {
		if isGenLogLine(l) && !isGRPCNoise(l) {
			t.Errorf("should drop noise line: %q", l)
		}
	}
}

func TestRingBuffer(t *testing.T) {
	rb := newRingBuffer(3)
	for _, s := range []string{"a", "b", "c", "d"} {
		rb.add(s)
	}
	got := rb.snapshot()
	if len(got) != 3 || got[0] != "b" || got[2] != "d" {
		t.Errorf("ring buffer = %v, want [b c d]", got)
	}
}
