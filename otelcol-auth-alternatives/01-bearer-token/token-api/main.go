package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/crypto/bcrypt"
	"gopkg.in/yaml.v3"
	_ "modernc.org/sqlite"
)

type Tenant struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Email     string    `json:"email"`
	TokenHash string    `json:"-"`
	TokenPlain string   `json:"token,omitempty"` // só preenchido em create/rotate
	CreatedAt time.Time `json:"created_at"`
	ExpiresAt time.Time `json:"expires_at"`
	Revoked   bool      `json:"revoked"`
}

type Server struct {
	db          *sql.DB
	tokensFile  string
	adminKey    string
	collectorPID func() int
	bootstrap   string // sentinela aleatório p/ o collector subir quando não há tokens ativos
	mu          sync.Mutex
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	cfg := struct {
		Listen     string
		DBPath     string
		TokensFile string
		AdminKey   string
		Collector  string
	}{
		Listen:     getenv("LISTEN", ":8080"),
		DBPath:     getenv("DB_PATH", "/data/tokens.db"),
		TokensFile: getenv("TOKENS_FILE", "/etc/otelcol/tokens.yaml"),
		AdminKey:   getenv("ADMIN_KEY", ""),
		Collector:  getenv("COLLECTOR_BIN", "otelcol-contrib"),
	}
	if cfg.AdminKey == "" || cfg.AdminKey == "change-me-admin-key" {
		slog.Warn("ADMIN_KEY is default — change it before running publicly")
	}

	if err := os.MkdirAll(filepath.Dir(cfg.DBPath), 0o700); err != nil {
		fatal("mkdir", err)
	}
	db, err := sql.Open("sqlite", cfg.DBPath+"?_pragma=journal_mode(WAL)&_pragma=foreign_keys(1)")
	if err != nil { fatal("open db", err) }
	defer db.Close()
	if err := initSchema(db); err != nil { fatal("init schema", err) }

	srv := &Server{
		db:         db,
		tokensFile: cfg.TokensFile,
		adminKey:   cfg.AdminKey,
		collectorPID: func() int { return findPID(cfg.Collector) },
		bootstrap:  newToken(), // descartado: ninguém recebe; só evita lista vazia
	}

	// Regrava tokens.yaml ao iniciar (mantém collector e DB em sync)
	if err := srv.writeTokensFile(context.Background()); err != nil {
		slog.Error("initial tokens.yaml write failed", "err", err)
	}

	// Cron para expirar tokens (regrava a cada 60s mesmo sem mutação)
	go srv.expirationLoop()

	mux := http.NewServeMux()
	mux.HandleFunc("POST /tenants",          srv.requireAdmin(srv.createTenant))
	mux.HandleFunc("GET /tenants",           srv.requireAdmin(srv.listTenants))
	mux.HandleFunc("DELETE /tenants/{id}",   srv.requireAdmin(srv.revokeTenant))
	mux.HandleFunc("POST /tenants/{id}/rotate", srv.requireAdmin(srv.rotateTenant))
	mux.HandleFunc("GET /healthz",           func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })

	httpSrv := &http.Server{
		Addr:              cfg.Listen,
		Handler:           logging(mux),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		slog.Info("listening", "addr", cfg.Listen)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fatal("listen", err)
		}
	}()
	<-ctx.Done()
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(shutdown)
}

func initSchema(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS tenants (
			id          TEXT PRIMARY KEY,
			name        TEXT NOT NULL,
			email       TEXT NOT NULL,
			token_hash  TEXT NOT NULL,
			token_short TEXT NOT NULL,        -- prefixo + sha256 hex truncado, para listar
			token_plain TEXT NOT NULL,        -- plaintext lido pelo collector (volume privado 0600)
			created_at  DATETIME NOT NULL,
			expires_at  DATETIME NOT NULL,
			revoked     INTEGER NOT NULL DEFAULT 0
		);
		CREATE INDEX IF NOT EXISTS idx_tenants_active ON tenants(revoked, expires_at);
	`)
	return err
}

func (s *Server) requireAdmin(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		got := r.Header.Get("X-Admin-Key")
		if subtle.ConstantTimeCompare([]byte(got), []byte(s.adminKey)) != 1 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		h(w, r)
	}
}

type createReq struct {
	Name     string `json:"name"`
	Email    string `json:"email"`
	TTLHours int    `json:"ttl_hours"`
}

func (s *Server) createTenant(w http.ResponseWriter, r *http.Request) {
	var req createReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest); return
	}
	if req.Name == "" || req.Email == "" {
		http.Error(w, "name and email required", http.StatusBadRequest); return
	}
	if req.TTLHours <= 0 || req.TTLHours > 24*365 {
		req.TTLHours = 24 * 30
	}
	id := newID()
	plain := newToken()
	hash, err := bcrypt.GenerateFromPassword([]byte(plain), bcrypt.DefaultCost)
	if err != nil { http.Error(w, "hash", http.StatusInternalServerError); return }
	now := time.Now().UTC()
	exp := now.Add(time.Duration(req.TTLHours) * time.Hour)

	_, err = s.db.ExecContext(r.Context(), `
		INSERT INTO tenants(id, name, email, token_hash, token_short, token_plain, created_at, expires_at, revoked)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)
	`, id, req.Name, req.Email, string(hash), shortToken(plain), plain, now, exp)
	if err != nil { http.Error(w, "db: "+err.Error(), http.StatusInternalServerError); return }

	if err := s.writeTokensFile(r.Context()); err != nil {
		slog.Error("write tokens.yaml", "err", err)
	}
	t := Tenant{ID: id, Name: req.Name, Email: req.Email, CreatedAt: now, ExpiresAt: exp, TokenPlain: plain}
	writeJSON(w, http.StatusCreated, t)
}

func (s *Server) listTenants(w http.ResponseWriter, r *http.Request) {
	rows, err := s.db.QueryContext(r.Context(), `
		SELECT id, name, email, token_short, created_at, expires_at, revoked
		FROM tenants ORDER BY created_at DESC
	`)
	if err != nil { http.Error(w, "db", http.StatusInternalServerError); return }
	defer rows.Close()
	var out []map[string]any
	for rows.Next() {
		var id, name, email, ts string
		var ca, ea time.Time
		var rev int
		if err := rows.Scan(&id, &name, &email, &ts, &ca, &ea, &rev); err != nil { continue }
		out = append(out, map[string]any{
			"id": id, "name": name, "email": email,
			"token_short": ts, "created_at": ca, "expires_at": ea, "revoked": rev == 1,
		})
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) revokeTenant(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	res, err := s.db.ExecContext(r.Context(), `UPDATE tenants SET revoked=1 WHERE id=?`, id)
	if err != nil { http.Error(w, "db", http.StatusInternalServerError); return }
	n, _ := res.RowsAffected()
	if n == 0 { http.Error(w, "not found", http.StatusNotFound); return }
	if err := s.writeTokensFile(r.Context()); err != nil {
		slog.Error("write tokens.yaml", "err", err)
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) rotateTenant(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	plain := newToken()
	hash, err := bcrypt.GenerateFromPassword([]byte(plain), bcrypt.DefaultCost)
	if err != nil { http.Error(w, "hash", http.StatusInternalServerError); return }
	res, err := s.db.ExecContext(r.Context(),
		`UPDATE tenants SET token_hash=?, token_short=?, token_plain=? WHERE id=? AND revoked=0`,
		string(hash), shortToken(plain), plain, id)
	if err != nil { http.Error(w, "db", http.StatusInternalServerError); return }
	if n, _ := res.RowsAffected(); n == 0 { http.Error(w, "not found", http.StatusNotFound); return }
	if err := s.writeTokensFile(r.Context()); err != nil {
		slog.Error("write tokens.yaml", "err", err)
	}
	writeJSON(w, http.StatusOK, map[string]string{"id": id, "token": plain})
}

// writeTokensFile regrava o arquivo lido pelo collector com os tokens plaintext
// válidos e ativos no momento. O bearertokenauth espera uma lista YAML pura
// (o file provider do collector substitui ${file:...} pelo conteúdo parseado).
// O plaintext mora num volume privado lido só pelo collector (perm 0600);
// a API expõe apenas o hash/short para auditoria. NA PRÁTICA: cifre o
// plaintext em repouso com KMS.
func (s *Server) writeTokensFile(ctx context.Context) error {
	s.mu.Lock(); defer s.mu.Unlock()

	rows, err := s.db.QueryContext(ctx, `
		SELECT token_plain FROM tenants
		WHERE revoked = 0 AND expires_at > ?
	`, time.Now().UTC())
	if err != nil { return err }
	defer rows.Close()

	var tokens []string
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil { continue }
		tokens = append(tokens, t)
	}
	// bearertokenauth recusa lista vazia; injeta o sentinela quando não há
	// tokens ativos para que o collector continue de pé (sentinela é aleatório
	// e nunca é entregue a ninguém).
	if len(tokens) == 0 { tokens = []string{s.bootstrap} }

	buf, err := yaml.Marshal(tokens)
	if err != nil { return err }

	tmp := s.tokensFile + ".tmp"
	if err := os.WriteFile(tmp, buf, 0o600); err != nil { return err }
	if err := os.Rename(tmp, s.tokensFile); err != nil { return err }
	slog.Info("tokens.yaml updated", "count", len(tokens))

	pid := s.collectorPID()
	if pid > 0 {
		if err := syscall.Kill(pid, syscall.SIGHUP); err != nil {
			slog.Warn("SIGHUP collector failed", "pid", pid, "err", err)
		} else {
			slog.Info("SIGHUP sent to collector", "pid", pid)
		}
	} else {
		slog.Warn("collector PID not found — skipping reload")
	}
	return nil
}

func (s *Server) expirationLoop() {
	t := time.NewTicker(60 * time.Second)
	defer t.Stop()
	for range t.C {
		if err := s.writeTokensFile(context.Background()); err != nil {
			slog.Error("expiration loop write", "err", err)
		}
	}
}

// ---------- helpers ----------

func newID() string { b := make([]byte, 12); rand.Read(b); return hex.EncodeToString(b) }
func newToken() string {
	b := make([]byte, 32); rand.Read(b)
	return "otel_" + hex.EncodeToString(b)
}
func shortToken(t string) string {
	h := sha256.Sum256([]byte(t))
	n := 9
	if len(t) < n { n = len(t) }
	return t[:n] + "…" + hex.EncodeToString(h[:4])
}
func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
func getenv(k, d string) string { if v := os.Getenv(k); v != "" { return v }; return d }
func fatal(msg string, err error) { slog.Error(msg, "err", err); os.Exit(1) }

func logging(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, code: 200}
		h.ServeHTTP(sw, r)
		slog.Info("req",
			"method", r.Method, "path", r.URL.Path,
			"status", sw.code, "dur_ms", time.Since(start).Milliseconds(),
			"remote", r.RemoteAddr,
		)
	})
}
type statusWriter struct{ http.ResponseWriter; code int }
func (s *statusWriter) WriteHeader(c int) { s.code = c; s.ResponseWriter.WriteHeader(c) }

// findPID procura no /proc um processo cujo comm corresponda ao nome dado.
// Necessário pid namespace compartilhado com o container do collector.
func findPID(name string) int {
	entries, err := os.ReadDir("/proc")
	if err != nil { return 0 }
	for _, e := range entries {
		if !e.IsDir() { continue }
		pid := 0
		fmt.Sscanf(e.Name(), "%d", &pid)
		if pid == 0 { continue }
		b, err := os.ReadFile("/proc/" + e.Name() + "/comm")
		if err != nil { continue }
		if strings.TrimSpace(string(b)) == name { return pid }
	}
	// fallback: pkill -HUP via shell
	if err := exec.Command("pkill", "-HUP", "--", name).Run(); err == nil { return -1 }
	return 0
}
