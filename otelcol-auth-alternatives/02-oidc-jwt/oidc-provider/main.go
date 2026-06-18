// Provider OIDC mínimo — emite JWT RS256 via client_credentials grant.
// Expõe /.well-known/openid-configuration e /jwks como esperado pela
// extension oidcauth do OTel Collector.
//
// PoC. Em produção use Keycloak / Dex / Auth0.
package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/x509"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"log/slog"
	"math/big"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"golang.org/x/crypto/bcrypt"
	_ "modernc.org/sqlite"
)

type Server struct {
	issuer    string
	audience  string
	tokenTTL  time.Duration
	adminKey  string
	db        *sql.DB

	mu  sync.RWMutex
	key *rsa.PrivateKey
	kid string
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))
	cfg := struct {
		Listen    string
		Issuer    string
		Audience  string
		KeyPath   string
		DBPath    string
		TTL       time.Duration
		AdminKey  string
	}{
		Listen:   getenv("LISTEN", ":9000"),
		Issuer:   getenv("ISSUER", "http://localhost:9000"),
		Audience: getenv("AUDIENCE", "otel-collector"),
		KeyPath:  getenv("KEY_PATH", "/data/signing.pem"),
		DBPath:   getenv("DB_PATH", "/data/clients.db"),
		TTL:      time.Duration(parseInt(getenv("TOKEN_TTL_SECONDS", "900"), 900)) * time.Second,
		AdminKey: getenv("ADMIN_KEY", ""),
	}
	if cfg.AdminKey == "" || cfg.AdminKey == "change-me-admin-key" {
		slog.Warn("ADMIN_KEY default — change before exposing publicly")
	}

	if err := os.MkdirAll(filepath.Dir(cfg.KeyPath), 0o700); err != nil { fatal("mkdir", err) }

	key, err := loadOrCreateKey(cfg.KeyPath)
	if err != nil { fatal("load key", err) }

	db, err := sql.Open("sqlite", cfg.DBPath+"?_pragma=journal_mode(WAL)&_pragma=foreign_keys(1)")
	if err != nil { fatal("db", err) }
	defer db.Close()
	if _, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS clients (
			client_id     TEXT PRIMARY KEY,
			secret_hash   TEXT NOT NULL,
			tenant_id     TEXT NOT NULL,
			created_at    DATETIME NOT NULL,
			revoked       INTEGER NOT NULL DEFAULT 0
		)`); err != nil { fatal("schema", err) }

	srv := &Server{
		issuer: cfg.Issuer, audience: cfg.Audience, tokenTTL: cfg.TTL,
		adminKey: cfg.AdminKey, db: db, key: key, kid: thumbprint(&key.PublicKey),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /.well-known/openid-configuration", srv.discovery)
	mux.HandleFunc("GET /jwks",      srv.jwks)
	mux.HandleFunc("POST /token",    srv.token)
	mux.HandleFunc("POST /admin/clients",        srv.requireAdmin(srv.createClient))
	mux.HandleFunc("DELETE /admin/clients/{id}", srv.requireAdmin(srv.revokeClient))
	mux.HandleFunc("GET /healthz",               func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })

	httpSrv := &http.Server{
		Addr: cfg.Listen, Handler: mux,
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second,
		WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second,
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		slog.Info("oidc-provider listening", "addr", cfg.Listen, "issuer", cfg.Issuer, "kid", srv.kid)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) { fatal("listen", err) }
	}()
	<-ctx.Done()
	sd, c := context.WithTimeout(context.Background(), 5*time.Second); defer c()
	_ = httpSrv.Shutdown(sd)
}

// ---------- OIDC handlers ----------

func (s *Server) discovery(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, 200, map[string]any{
		"issuer":                                s.issuer,
		"jwks_uri":                              s.issuer + "/jwks",
		"token_endpoint":                        s.issuer + "/token",
		"grant_types_supported":                 []string{"client_credentials"},
		"id_token_signing_alg_values_supported": []string{"RS256"},
		"response_types_supported":              []string{"token"},
		"subject_types_supported":               []string{"public"},
	})
}

func (s *Server) jwks(w http.ResponseWriter, _ *http.Request) {
	s.mu.RLock(); pub := s.key.PublicKey; kid := s.kid; s.mu.RUnlock()
	writeJSON(w, 200, map[string]any{
		"keys": []map[string]string{{
			"kty": "RSA",
			"alg": "RS256",
			"use": "sig",
			"kid": kid,
			"n":   b64url(pub.N.Bytes()),
			"e":   b64url(big.NewInt(int64(pub.E)).Bytes()),
		}},
	})
}

func (s *Server) token(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil { http.Error(w, "bad form", 400); return }
	if r.PostForm.Get("grant_type") != "client_credentials" {
		writeJSON(w, 400, map[string]string{"error": "unsupported_grant_type"}); return
	}
	cid := r.PostForm.Get("client_id")
	csec := r.PostForm.Get("client_secret")
	if cid == "" || csec == "" {
		writeJSON(w, 400, map[string]string{"error": "invalid_request"}); return
	}
	var hash, tenant string; var revoked int
	err := s.db.QueryRowContext(r.Context(),
		`SELECT secret_hash, tenant_id, revoked FROM clients WHERE client_id=?`, cid,
	).Scan(&hash, &tenant, &revoked)
	if err != nil || revoked == 1 {
		writeJSON(w, 401, map[string]string{"error": "invalid_client"}); return
	}
	if bcrypt.CompareHashAndPassword([]byte(hash), []byte(csec)) != nil {
		writeJSON(w, 401, map[string]string{"error": "invalid_client"}); return
	}
	tok, err := s.signToken(cid, tenant)
	if err != nil { http.Error(w, "sign", 500); return }
	writeJSON(w, 200, map[string]any{
		"access_token": tok,
		"token_type":   "Bearer",
		"expires_in":   int(s.tokenTTL.Seconds()),
	})
}

func (s *Server) signToken(sub, tenant string) (string, error) {
	now := time.Now().UTC()
	claims := jwt.MapClaims{
		"iss":       s.issuer,
		"aud":       s.audience,
		"sub":       sub,
		"iat":       now.Unix(),
		"nbf":       now.Unix(),
		"exp":       now.Add(s.tokenTTL).Unix(),
		"tenant_id": tenant,
	}
	t := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	t.Header["kid"] = s.kid
	s.mu.RLock(); defer s.mu.RUnlock()
	return t.SignedString(s.key)
}

// ---------- admin handlers ----------

func (s *Server) requireAdmin(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if subtle.ConstantTimeCompare([]byte(r.Header.Get("X-Admin-Key")), []byte(s.adminKey)) != 1 {
			http.Error(w, "unauthorized", 401); return
		}
		h(w, r)
	}
}

func (s *Server) createClient(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ClientID string `json:"client_id"`
		TenantID string `json:"tenant_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil { http.Error(w, "bad json", 400); return }
	if req.ClientID == "" || req.TenantID == "" { http.Error(w, "client_id and tenant_id required", 400); return }
	secret := newSecret()
	hash, err := bcrypt.GenerateFromPassword([]byte(secret), bcrypt.DefaultCost)
	if err != nil { http.Error(w, "hash", 500); return }
	_, err = s.db.ExecContext(r.Context(),
		`INSERT INTO clients(client_id, secret_hash, tenant_id, created_at) VALUES (?,?,?,?)`,
		req.ClientID, string(hash), req.TenantID, time.Now().UTC())
	if err != nil { http.Error(w, "db: "+err.Error(), 500); return }
	writeJSON(w, 201, map[string]string{"client_id": req.ClientID, "client_secret": secret, "tenant_id": req.TenantID})
}

func (s *Server) revokeClient(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	res, err := s.db.ExecContext(r.Context(), `UPDATE clients SET revoked=1 WHERE client_id=?`, id)
	if err != nil { http.Error(w, "db", 500); return }
	if n, _ := res.RowsAffected(); n == 0 { http.Error(w, "not found", 404); return }
	w.WriteHeader(204)
}

// ---------- key + helpers ----------

func loadOrCreateKey(path string) (*rsa.PrivateKey, error) {
	if b, err := os.ReadFile(path); err == nil {
		blk, _ := pem.Decode(b)
		if blk == nil { return nil, errors.New("pem decode") }
		k, err := x509.ParsePKCS1PrivateKey(blk.Bytes)
		return k, err
	}
	k, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil { return nil, err }
	pemBlk := &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(k)}
	if err := os.WriteFile(path, pem.EncodeToMemory(pemBlk), 0o600); err != nil { return nil, err }
	return k, nil
}

// thumbprint determinístico para uso como kid (RFC 7638 simplificado)
func thumbprint(pub *rsa.PublicKey) string {
	canonical := `{"e":"` + b64url(big.NewInt(int64(pub.E)).Bytes()) +
		`","kty":"RSA","n":"` + b64url(pub.N.Bytes()) + `"}`
	h := sha256.Sum256([]byte(canonical))
	return b64url(h[:])
}
func b64url(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }
func newSecret() string { b := make([]byte, 32); rand.Read(b); return hex.EncodeToString(b) }
func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(code); _ = json.NewEncoder(w).Encode(v)
}
func getenv(k, d string) string { if v := os.Getenv(k); v != "" { return v }; return d }
func parseInt(s string, d int) int { n, err := strconv.Atoi(s); if err != nil { return d }; return n }
func fatal(msg string, err error) { slog.Error(msg, "err", err); os.Exit(1) }
