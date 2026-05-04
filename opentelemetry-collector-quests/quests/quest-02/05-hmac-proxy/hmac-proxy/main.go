// hmac-proxy: reverse proxy que valida HMAC-SHA256 com proteção contra replay
// e encaminha para o OTel Collector em loopback.
package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "modernc.org/sqlite"
)

type Server struct {
	db        *sql.DB
	upstream  *url.URL
	skew      time.Duration
	nonceTTL  time.Duration
	adminKey  string

	keyMu sync.RWMutex
	keys  map[string]keyEntry  // keyID -> entry

	nonceMu sync.Mutex
	nonces  map[string]time.Time
}

type keyEntry struct {
	Secret   []byte
	TenantID string
	Revoked  bool
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	cfg := struct {
		ListenPub, ListenAdm, Upstream, DBPath, AdminKey string
		Skew, NonceTTL                                   time.Duration
	}{
		ListenPub: getenv("LISTEN_PUBLIC", ":4318"),
		ListenAdm: getenv("LISTEN_ADMIN",  ":8082"),
		Upstream:  getenv("UPSTREAM", "http://127.0.0.1:14318"),
		DBPath:    getenv("DB_PATH", "/data/keys.db"),
		AdminKey:  getenv("ADMIN_KEY", ""),
		Skew:      time.Duration(parseInt(getenv("SKEW_SECONDS", "300"), 300)) * time.Second,
		NonceTTL:  time.Duration(parseInt(getenv("NONCE_TTL_SECONDS", "600"), 600)) * time.Second,
	}
	if cfg.AdminKey == "" || cfg.AdminKey == "change-me-admin-key" {
		slog.Warn("ADMIN_KEY default — change before exposing publicly")
	}
	if err := os.MkdirAll(filepath.Dir(cfg.DBPath), 0o700); err != nil { fatal("mkdir", err) }

	up, err := url.Parse(cfg.Upstream); if err != nil { fatal("upstream url", err) }

	db, err := sql.Open("sqlite", cfg.DBPath+"?_pragma=journal_mode(WAL)")
	if err != nil { fatal("db", err) }
	defer db.Close()
	if _, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS keys (
			key_id     TEXT PRIMARY KEY,
			secret     BLOB NOT NULL,        -- em produção: cifrado com KMS
			tenant_id  TEXT NOT NULL,
			created_at DATETIME NOT NULL,
			revoked    INTEGER NOT NULL DEFAULT 0
		)`); err != nil { fatal("schema", err) }

	srv := &Server{
		db: db, upstream: up, skew: cfg.Skew, nonceTTL: cfg.NonceTTL, adminKey: cfg.AdminKey,
		keys: map[string]keyEntry{}, nonces: map[string]time.Time{},
	}
	if err := srv.loadKeys(context.Background()); err != nil { fatal("load keys", err) }
	go srv.gcNonces()

	// public endpoint — HMAC validation + reverse proxy
	pubMux := http.NewServeMux()
	pubMux.Handle("/", srv.hmacGuard(httputil.NewSingleHostReverseProxy(up)))

	// admin endpoint — manage keys
	admMux := http.NewServeMux()
	admMux.HandleFunc("POST /admin/keys",          srv.requireAdmin(srv.createKey))
	admMux.HandleFunc("DELETE /admin/keys/{id}",   srv.requireAdmin(srv.revokeKey))
	admMux.HandleFunc("POST /admin/keys/{id}/rotate", srv.requireAdmin(srv.rotateKey))
	admMux.HandleFunc("GET /admin/keys",           srv.requireAdmin(srv.listKeys))
	admMux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })

	pub := &http.Server{
		Addr: cfg.ListenPub, Handler: pubMux,
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second,
		WriteTimeout: 30 * time.Second, IdleTimeout: 90 * time.Second,
	}
	adm := &http.Server{
		Addr: cfg.ListenAdm, Handler: admMux,
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second,
		WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		slog.Info("public listening", "addr", cfg.ListenPub, "upstream", cfg.Upstream)
		if err := pub.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) { fatal("pub", err) }
	}()
	go func() {
		slog.Info("admin listening", "addr", cfg.ListenAdm)
		if err := adm.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) { fatal("adm", err) }
	}()
	<-ctx.Done()
	sd, c := context.WithTimeout(context.Background(), 5*time.Second); defer c()
	_ = pub.Shutdown(sd); _ = adm.Shutdown(sd)
}

// ---------- HMAC validation ----------

func (s *Server) hmacGuard(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Limita tamanho do body para evitar memory exhaustion antes da auth
		body, err := io.ReadAll(io.LimitReader(r.Body, 16<<20))   // 16MB
		if err != nil { http.Error(w, "read body", 400); return }

		keyID := r.Header.Get("X-Sig-KeyId")
		tsStr := r.Header.Get("X-Sig-Timestamp")
		nonce := r.Header.Get("X-Sig-Nonce")
		sig   := r.Header.Get("X-Sig")
		if keyID == "" || tsStr == "" || nonce == "" || sig == "" {
			http.Error(w, "missing signature headers", 401); return
		}
		ts, err := strconv.ParseInt(tsStr, 10, 64)
		if err != nil { http.Error(w, "bad timestamp", 401); return }

		now := time.Now().Unix()
		if abs64(now-ts) > int64(s.skew.Seconds()) {
			http.Error(w, "timestamp out of window", 401); return
		}
		if len(nonce) < 16 || len(nonce) > 128 {
			http.Error(w, "bad nonce", 401); return
		}
		if !s.consumeNonce(nonce) {
			http.Error(w, "replay detected", 401); return
		}

		s.keyMu.RLock(); ke, ok := s.keys[keyID]; s.keyMu.RUnlock()
		if !ok || ke.Revoked {
			http.Error(w, "invalid key", 401); return
		}

		bodyHash := sha256.Sum256(body)
		canonical := tsStr + "\n" + nonce + "\n" + r.Method + "\n" + r.URL.Path + "\n" + hex.EncodeToString(bodyHash[:])
		mac := hmac.New(sha256.New, ke.Secret)
		mac.Write([]byte(canonical))
		expected := mac.Sum(nil)

		got, err := base64.StdEncoding.DecodeString(sig)
		if err != nil || subtle.ConstantTimeCompare(got, expected) != 1 {
			http.Error(w, "bad signature", 401); return
		}

		// auth ok — restaura body para o reverse proxy e injeta tenant
		r.Body = io.NopCloser(bytes.NewReader(body))
		r.ContentLength = int64(len(body))
		r.Header.Set("X-Tenant-Id", ke.TenantID)
		r.Header.Del("X-Sig-KeyId")
		r.Header.Del("X-Sig-Timestamp")
		r.Header.Del("X-Sig-Nonce")
		r.Header.Del("X-Sig")
		next.ServeHTTP(w, r)
	})
}

// consumeNonce retorna true se o nonce nunca foi visto na janela
func (s *Server) consumeNonce(n string) bool {
	s.nonceMu.Lock(); defer s.nonceMu.Unlock()
	if _, exists := s.nonces[n]; exists { return false }
	s.nonces[n] = time.Now()
	return true
}

func (s *Server) gcNonces() {
	t := time.NewTicker(30 * time.Second); defer t.Stop()
	for range t.C {
		cutoff := time.Now().Add(-s.nonceTTL)
		s.nonceMu.Lock()
		for k, ts := range s.nonces {
			if ts.Before(cutoff) { delete(s.nonces, k) }
		}
		s.nonceMu.Unlock()
	}
}

// ---------- key management ----------

func (s *Server) loadKeys(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx, `SELECT key_id, secret, tenant_id, revoked FROM keys`)
	if err != nil { return err }
	defer rows.Close()
	m := map[string]keyEntry{}
	for rows.Next() {
		var id, tenant string; var sec []byte; var rev int
		if err := rows.Scan(&id, &sec, &tenant, &rev); err != nil { continue }
		m[id] = keyEntry{Secret: sec, TenantID: tenant, Revoked: rev == 1}
	}
	s.keyMu.Lock(); s.keys = m; s.keyMu.Unlock()
	slog.Info("keys loaded", "count", len(m))
	return nil
}

func (s *Server) requireAdmin(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if subtle.ConstantTimeCompare([]byte(r.Header.Get("X-Admin-Key")), []byte(s.adminKey)) != 1 {
			http.Error(w, "unauthorized", 401); return
		}
		h(w, r)
	}
}

func (s *Server) createKey(w http.ResponseWriter, r *http.Request) {
	var req struct{ TenantID string }
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil { http.Error(w, "bad json", 400); return }
	if strings.TrimSpace(req.TenantID) == "" { http.Error(w, "tenant_id required", 400); return }
	keyID := newID(8)
	secret := make([]byte, 32); rand.Read(secret)
	_, err := s.db.ExecContext(r.Context(),
		`INSERT INTO keys(key_id, secret, tenant_id, created_at, revoked) VALUES (?,?,?,?,0)`,
		keyID, secret, req.TenantID, time.Now().UTC())
	if err != nil { http.Error(w, "db: "+err.Error(), 500); return }
	if err := s.loadKeys(r.Context()); err != nil { slog.Error("reload", "err", err) }
	writeJSON(w, 201, map[string]string{
		"key_id":    keyID,
		"secret":    base64.StdEncoding.EncodeToString(secret),
		"tenant_id": req.TenantID,
	})
}

func (s *Server) revokeKey(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	res, err := s.db.ExecContext(r.Context(), `UPDATE keys SET revoked=1 WHERE key_id=?`, id)
	if err != nil { http.Error(w, "db", 500); return }
	if n, _ := res.RowsAffected(); n == 0 { http.Error(w, "not found", 404); return }
	if err := s.loadKeys(r.Context()); err != nil { slog.Error("reload", "err", err) }
	w.WriteHeader(204)
}

func (s *Server) rotateKey(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	secret := make([]byte, 32); rand.Read(secret)
	res, err := s.db.ExecContext(r.Context(),
		`UPDATE keys SET secret=? WHERE key_id=? AND revoked=0`, secret, id)
	if err != nil { http.Error(w, "db", 500); return }
	if n, _ := res.RowsAffected(); n == 0 { http.Error(w, "not found", 404); return }
	if err := s.loadKeys(r.Context()); err != nil { slog.Error("reload", "err", err) }
	writeJSON(w, 200, map[string]string{
		"key_id": id,
		"secret": base64.StdEncoding.EncodeToString(secret),
	})
}

func (s *Server) listKeys(w http.ResponseWriter, r *http.Request) {
	rows, err := s.db.QueryContext(r.Context(),
		`SELECT key_id, tenant_id, created_at, revoked FROM keys ORDER BY created_at DESC`)
	if err != nil { http.Error(w, "db", 500); return }
	defer rows.Close()
	out := []map[string]any{}
	for rows.Next() {
		var id, tenant string; var ca time.Time; var rev int
		if err := rows.Scan(&id, &tenant, &ca, &rev); err != nil { continue }
		out = append(out, map[string]any{
			"key_id": id, "tenant_id": tenant, "created_at": ca, "revoked": rev == 1,
		})
	}
	writeJSON(w, 200, out)
}

// helpers
func newID(n int) string { b := make([]byte, n); rand.Read(b); return hex.EncodeToString(b) }
func abs64(x int64) int64 { if x < 0 { return -x }; return x }
func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(code); _ = json.NewEncoder(w).Encode(v)
}
func getenv(k, d string) string { if v := os.Getenv(k); v != "" { return v }; return d }
func parseInt(s string, d int) int { n, err := strconv.Atoi(s); if err != nil { return d }; return n }
func fatal(msg string, err error) { slog.Error(msg, "err", err); os.Exit(1) }
