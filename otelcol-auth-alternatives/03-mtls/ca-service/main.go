// ca-service: assina CSRs de cliente com a CA configurada.
// Auth: X-Admin-Key (header). Em produção, troque por OIDC/mTLS.
package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/subtle"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	listen   := getenv("LISTEN", ":9100")
	caCertP  := getenv("CA_CERT", "/etc/ca/ca.crt")
	caKeyP   := getenv("CA_KEY",  "/etc/ca/ca.key")
	adminKey := getenv("ADMIN_KEY", "")
	ttl      := time.Duration(parseInt(getenv("CERT_TTL_HOURS", "24"), 24)) * time.Hour

	if adminKey == "" || adminKey == "change-me-admin-key" {
		slog.Warn("ADMIN_KEY default — change before exposing publicly")
	}

	caCert, caKey, err := loadCA(caCertP, caKeyP)
	if err != nil { fatal("load CA", err) }

	signer := &Signer{caCert: caCert, caKey: caKey, ttl: ttl}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /sign", requireAdmin(adminKey, signer.sign))
	mux.HandleFunc("GET /ca",    func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("content-type", "application/x-pem-file")
		pem.Encode(w, &pem.Block{Type: "CERTIFICATE", Bytes: caCert.Raw})
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })

	srv := &http.Server{
		Addr: listen, Handler: mux,
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second,
		WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second,
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		slog.Info("ca-service listening", "addr", listen)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) { fatal("listen", err) }
	}()
	<-ctx.Done()
	sd, c := context.WithTimeout(context.Background(), 5*time.Second); defer c()
	_ = srv.Shutdown(sd)
}

type Signer struct {
	caCert *x509.Certificate
	caKey  *rsa.PrivateKey
	ttl    time.Duration
}

func (s *Signer) sign(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<16))
	if err != nil { http.Error(w, "read body", 400); return }
	blk, _ := pem.Decode(body)
	if blk == nil || blk.Type != "CERTIFICATE REQUEST" {
		http.Error(w, "expecting PEM CERTIFICATE REQUEST", 400); return
	}
	csr, err := x509.ParseCertificateRequest(blk.Bytes)
	if err != nil { http.Error(w, "parse csr: "+err.Error(), 400); return }
	if err := csr.CheckSignature(); err != nil { http.Error(w, "csr signature invalid", 400); return }
	if csr.Subject.CommonName == "" { http.Error(w, "CN required in CSR", 400); return }

	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil { http.Error(w, "serial", 500); return }

	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName:   csr.Subject.CommonName,
			Organization: csr.Subject.Organization,
		},
		NotBefore:             time.Now().UTC().Add(-1 * time.Minute),
		NotAfter:              time.Now().UTC().Add(s.ttl),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  false,
		DNSNames:              csr.DNSNames,
		IPAddresses:           csr.IPAddresses,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, s.caCert, csr.PublicKey, s.caKey)
	if err != nil { http.Error(w, "sign: "+err.Error(), 500); return }

	w.Header().Set("content-type", "application/x-pem-file")
	pem.Encode(w, &pem.Block{Type: "CERTIFICATE", Bytes: der})
	slog.Info("issued client cert",
		"cn", csr.Subject.CommonName, "o", csr.Subject.Organization,
		"serial", serial.Text(16), "ttl_h", int(s.ttl.Hours()))
}

func loadCA(certPath, keyPath string) (*x509.Certificate, *rsa.PrivateKey, error) {
	cb, err := os.ReadFile(certPath); if err != nil { return nil, nil, err }
	cBlk, _ := pem.Decode(cb); if cBlk == nil { return nil, nil, errors.New("ca cert pem decode") }
	cert, err := x509.ParseCertificate(cBlk.Bytes); if err != nil { return nil, nil, err }

	kb, err := os.ReadFile(keyPath); if err != nil { return nil, nil, err }
	kBlk, _ := pem.Decode(kb); if kBlk == nil { return nil, nil, errors.New("ca key pem decode") }
	key, err := x509.ParsePKCS1PrivateKey(kBlk.Bytes)
	if err != nil {
		// tenta PKCS8
		k2, err2 := x509.ParsePKCS8PrivateKey(kBlk.Bytes)
		if err2 != nil { return nil, nil, err }
		rk, ok := k2.(*rsa.PrivateKey); if !ok { return nil, nil, errors.New("only RSA supported") }
		key = rk
	}
	return cert, key, nil
}

func requireAdmin(key string, h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if subtle.ConstantTimeCompare([]byte(r.Header.Get("X-Admin-Key")), []byte(key)) != 1 {
			http.Error(w, "unauthorized", 401); return
		}
		h(w, r)
	}
}

func getenv(k, d string) string { if v := os.Getenv(k); v != "" { return v }; return d }
func parseInt(s string, d int) int { n, err := strconv.Atoi(s); if err != nil { return d }; return n }
func fatal(msg string, err error) { slog.Error(msg, "err", err); os.Exit(1) }
