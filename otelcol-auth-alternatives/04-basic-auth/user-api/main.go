// user-api: gerencia o htpasswd lido pela basicauth extension do collector.
// CRUD de usuários + reload do collector via SIGHUP.
package main

import (
	"context"
	"crypto/rand"
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
	_ "modernc.org/sqlite"
)

type Server struct {
	db        *sql.DB
	htpasswd  string
	adminKey  string
	collBin   string
	mu        sync.Mutex
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	cfg := struct {
		Listen, HTPasswd, DBPath, AdminKey, Coll string
	}{
		Listen:   getenv("LISTEN", ":8081"),
		HTPasswd: getenv("HTPASSWD", "/etc/otelcol/htpasswd"),
		DBPath:   getenv("DB_PATH", "/data/users.db"),
		AdminKey: getenv("ADMIN_KEY", ""),
		Coll:     getenv("COLLECTOR_BIN", "otelcol-contrib"),
	}
	if cfg.AdminKey == "" || cfg.AdminKey == "change-me-admin-key" {
		slog.Warn("ADMIN_KEY default — change before exposing publicly")
	}
	if err := os.MkdirAll(filepath.Dir(cfg.DBPath), 0o700); err != nil { fatal("mkdir db", err) }

	db, err := sql.Open("sqlite", cfg.DBPath+"?_pragma=journal_mode(WAL)&_pragma=foreign_keys(1)")
	if err != nil { fatal("open db", err) }
	defer db.Close()
	if _, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS users (
			name        TEXT PRIMARY KEY,
			pass_hash   TEXT NOT NULL,
			created_at  DATETIME NOT NULL,
			expires_at  DATETIME NOT NULL,
			revoked     INTEGER NOT NULL DEFAULT 0
		)`); err != nil { fatal("schema", err) }

	srv := &Server{db: db, htpasswd: cfg.HTPasswd, adminKey: cfg.AdminKey, collBin: cfg.Coll}
	if err := srv.regen(context.Background()); err != nil { slog.Error("initial regen", "err", err) }
	go srv.expirationLoop()

	mux := http.NewServeMux()
	mux.HandleFunc("POST /users",                 srv.requireAdmin(srv.create))
	mux.HandleFunc("DELETE /users/{name}",        srv.requireAdmin(srv.revoke))
	mux.HandleFunc("POST /users/{name}/rotate",   srv.requireAdmin(srv.rotate))
	mux.HandleFunc("GET /users",                  srv.requireAdmin(srv.list))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })

	httpSrv := &http.Server{
		Addr: cfg.Listen, Handler: mux,
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second,
		WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second,
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		slog.Info("user-api listening", "addr", cfg.Listen)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) { fatal("listen", err) }
	}()
	<-ctx.Done()
	sd, c := context.WithTimeout(context.Background(), 5*time.Second); defer c()
	_ = httpSrv.Shutdown(sd)
}

func (s *Server) requireAdmin(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if subtle.ConstantTimeCompare([]byte(r.Header.Get("X-Admin-Key")), []byte(s.adminKey)) != 1 {
			http.Error(w, "unauthorized", 401); return
		}
		h(w, r)
	}
}

func (s *Server) create(w http.ResponseWriter, r *http.Request) {
	var req struct { Name string; TTLHours int }
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil { http.Error(w, "bad json", 400); return }
	if req.Name == "" || strings.ContainsAny(req.Name, ":\n\r\t ") {
		http.Error(w, "invalid name", 400); return
	}
	if req.TTLHours <= 0 || req.TTLHours > 24*365 { req.TTLHours = 24 * 30 }
	pw := newSecret()
	hash, err := bcrypt.GenerateFromPassword([]byte(pw), 12)
	if err != nil { http.Error(w, "hash", 500); return }
	now := time.Now().UTC()
	exp := now.Add(time.Duration(req.TTLHours) * time.Hour)
	_, err = s.db.ExecContext(r.Context(),
		`INSERT INTO users(name, pass_hash, created_at, expires_at, revoked) VALUES (?,?,?,?,0)`,
		req.Name, string(hash), now, exp)
	if err != nil { http.Error(w, "db: "+err.Error(), 500); return }
	if err := s.regen(r.Context()); err != nil { slog.Error("regen", "err", err) }
	writeJSON(w, 201, map[string]any{"name": req.Name, "password": pw, "expires_at": exp})
}

func (s *Server) revoke(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	res, err := s.db.ExecContext(r.Context(), `UPDATE users SET revoked=1 WHERE name=?`, name)
	if err != nil { http.Error(w, "db", 500); return }
	if n, _ := res.RowsAffected(); n == 0 { http.Error(w, "not found", 404); return }
	if err := s.regen(r.Context()); err != nil { slog.Error("regen", "err", err) }
	w.WriteHeader(204)
}

func (s *Server) rotate(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	pw := newSecret()
	hash, err := bcrypt.GenerateFromPassword([]byte(pw), 12)
	if err != nil { http.Error(w, "hash", 500); return }
	res, err := s.db.ExecContext(r.Context(),
		`UPDATE users SET pass_hash=? WHERE name=? AND revoked=0`, string(hash), name)
	if err != nil { http.Error(w, "db", 500); return }
	if n, _ := res.RowsAffected(); n == 0 { http.Error(w, "not found", 404); return }
	if err := s.regen(r.Context()); err != nil { slog.Error("regen", "err", err) }
	writeJSON(w, 200, map[string]string{"name": name, "password": pw})
}

func (s *Server) list(w http.ResponseWriter, r *http.Request) {
	rows, err := s.db.QueryContext(r.Context(),
		`SELECT name, created_at, expires_at, revoked FROM users ORDER BY created_at DESC`)
	if err != nil { http.Error(w, "db", 500); return }
	defer rows.Close()
	out := []map[string]any{}
	for rows.Next() {
		var n string; var ca, ea time.Time; var rev int
		if err := rows.Scan(&n, &ca, &ea, &rev); err != nil { continue }
		out = append(out, map[string]any{
			"name": n, "created_at": ca, "expires_at": ea, "revoked": rev == 1,
		})
	}
	writeJSON(w, 200, out)
}

// regen escreve atomicamente o htpasswd com todos os usuários ativos não expirados
// e envia SIGHUP para o collector recarregar a config.
func (s *Server) regen(ctx context.Context) error {
	s.mu.Lock(); defer s.mu.Unlock()
	rows, err := s.db.QueryContext(ctx, `
		SELECT name, pass_hash FROM users
		WHERE revoked = 0 AND expires_at > ?`, time.Now().UTC())
	if err != nil { return err }
	defer rows.Close()
	var sb strings.Builder
	for rows.Next() {
		var n, h string
		if err := rows.Scan(&n, &h); err != nil { continue }
		fmt.Fprintf(&sb, "%s:%s\n", n, h)
	}
	tmp := s.htpasswd + ".tmp"
	if err := os.WriteFile(tmp, []byte(sb.String()), 0o600); err != nil { return err }
	if err := os.Rename(tmp, s.htpasswd); err != nil { return err }
	slog.Info("htpasswd regenerated")

	pid := findPID(s.collBin)
	if pid > 0 {
		if err := syscall.Kill(pid, syscall.SIGHUP); err != nil {
			slog.Warn("SIGHUP failed", "pid", pid, "err", err)
		}
	}
	return nil
}

func (s *Server) expirationLoop() {
	t := time.NewTicker(60 * time.Second); defer t.Stop()
	for range t.C {
		_ = s.regen(context.Background())
	}
}

// helpers
func newSecret() string { b := make([]byte, 24); rand.Read(b); return hex.EncodeToString(b) }
func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(code); _ = json.NewEncoder(w).Encode(v)
}
func getenv(k, d string) string { if v := os.Getenv(k); v != "" { return v }; return d }
func fatal(msg string, err error) { slog.Error(msg, "err", err); os.Exit(1) }

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
	if err := exec.Command("pkill", "-HUP", "--", name).Run(); err == nil { return -1 }
	return 0
}
