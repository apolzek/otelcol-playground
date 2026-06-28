package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func newTestServer(t *testing.T) *server {
	t.Helper()
	m := NewManager(makeFakeBin(t))
	t.Cleanup(m.StopAll)
	return &server{
		mgr:    m,
		stats:  NewCollectorStats(func() string { return "http://127.0.0.1:0/nope" }),
		health: NewEndpointHealth(func() string { return "127.0.0.1:0" }),
		now:    func() time.Time { return time.Unix(0, 0) },
	}
}

func TestHandleState(t *testing.T) {
	s := newTestServer(t)
	rec := httptest.NewRecorder()
	s.handleState(rec, httptest.NewRequest(http.MethodGet, "/api/state", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	var snap snapshot
	if err := json.Unmarshal(rec.Body.Bytes(), &snap); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if snap.Config.Endpoint != "otelcol:4317" {
		t.Errorf("default endpoint = %q", snap.Config.Endpoint)
	}
	if snap.Received.Available {
		t.Error("received should be unavailable (no collector)")
	}
}

func TestHandleConfigAppliesAndComputesRates(t *testing.T) {
	s := newTestServer(t)

	cfg := DefaultConfig()
	cfg.Metrics.Enabled = true
	cfg.Metrics.Rate = 100
	cfg.Metrics.Workers = 2
	body, _ := json.Marshal(cfg)

	req := httptest.NewRequest(http.MethodPost, "/api/config", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	s.handleConfig(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	var snap snapshot
	json.Unmarshal(rec.Body.Bytes(), &snap)

	if snap.Configured.MetricPoints != 200 {
		t.Errorf("configured metric points = %v, want 200", snap.Configured.MetricPoints)
	}
	if snap.Configured.Total != 200 {
		t.Errorf("configured total = %v, want 200", snap.Configured.Total)
	}
	if !snap.Running["metrics"] {
		t.Error("metrics should be running after enabling")
	}
}

func TestHandleConfigRejectsBadJSON(t *testing.T) {
	s := newTestServer(t)
	req := httptest.NewRequest(http.MethodPost, "/api/config", bytes.NewReader([]byte("{not json")))
	rec := httptest.NewRecorder()
	s.handleConfig(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

func TestHandleConfigWrongMethod(t *testing.T) {
	s := newTestServer(t)
	rec := httptest.NewRecorder()
	s.handleConfig(rec, httptest.NewRequest(http.MethodGet, "/api/config", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want 405", rec.Code)
	}
}

func TestHandleStopDisablesAll(t *testing.T) {
	s := newTestServer(t)

	cfg := DefaultConfig()
	cfg.Traces.Enabled = true
	cfg.Logs.Enabled = true
	s.mgr.Apply(cfg)
	waitFor(t, func() bool { return s.mgr.Running()[sigTraces] })

	rec := httptest.NewRecorder()
	s.handleStop(rec, httptest.NewRequest(http.MethodPost, "/api/stop", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}

	c := s.mgr.Config()
	if c.Traces.Enabled || c.Metrics.Enabled || c.Logs.Enabled {
		t.Error("all signals should be disabled after stop")
	}
	waitFor(t, func() bool { return !s.mgr.Running()[sigTraces] })
}

func TestNormalize(t *testing.T) {
	c := Config{}
	c.Traces.Workers = 0
	c.Traces.Rate = -5
	c.Traces.ChildSpans = -1
	normalize(&c)

	if c.Endpoint == "" || c.ServiceName == "" {
		t.Error("normalize should fill endpoint/service defaults")
	}
	if c.Headers == nil || c.Traces.Attributes == nil {
		t.Error("normalize should init nil maps")
	}
	if c.Traces.Workers != 1 {
		t.Errorf("workers = %d, want floored to 1", c.Traces.Workers)
	}
	if c.Traces.Rate != 0 {
		t.Errorf("negative rate = %v, want clamped to 0", c.Traces.Rate)
	}
	if c.Traces.ChildSpans != 0 {
		t.Errorf("negative childSpans = %d, want 0", c.Traces.ChildSpans)
	}
}

func TestClampUnlimited(t *testing.T) {
	if clampUnlimited(-1) != 0 {
		t.Error("unlimited sentinel should display as 0")
	}
	if clampUnlimited(42) != 42 {
		t.Error("normal value should pass through")
	}
}

func TestSnapshotShape(t *testing.T) {
	s := newTestServer(t)
	snap := s.snapshot()
	if snap.Time != 0 {
		t.Errorf("time = %d, want 0 (mocked clock)", snap.Time)
	}
	if snap.Running == nil {
		t.Error("running map should be non-nil")
	}
}
