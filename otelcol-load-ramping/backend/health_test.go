package main

import (
	"net"
	"testing"
)

func TestEndpointHealthOnline(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	h := NewEndpointHealth(func() string { return ln.Addr().String() })
	h.checkOnce()
	if !h.Online() {
		t.Error("expected online for a listening address")
	}
}

func TestEndpointHealthOffline(t *testing.T) {
	// Bind then immediately close to get a port nothing listens on.
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	addr := ln.Addr().String()
	ln.Close()

	h := NewEndpointHealth(func() string { return addr })
	h.checkOnce()
	if h.Online() {
		t.Error("expected offline for a closed port")
	}
}

func TestEndpointHealthEmptyAddr(t *testing.T) {
	h := NewEndpointHealth(func() string { return "" })
	h.checkOnce()
	if h.Online() {
		t.Error("empty address should be offline, not dialed")
	}
}
