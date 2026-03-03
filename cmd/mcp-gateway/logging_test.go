package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRequestIDMiddleware_generatesID(t *testing.T) {
	var gotID string
	handler := RequestIDMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotID = RequestIDFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if gotID == "" {
		t.Fatal("expected request ID in context")
	}
	if rec.Header().Get("X-Request-Id") == "" {
		t.Fatal("expected X-Request-Id response header")
	}
	if rec.Header().Get("X-Request-Id") != gotID {
		t.Errorf("header %q != context %q", rec.Header().Get("X-Request-Id"), gotID)
	}
}

func TestRequestIDMiddleware_preservesClientID(t *testing.T) {
	var gotID string
	handler := RequestIDMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotID = RequestIDFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Request-Id", "client-provided-id")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if gotID != "client-provided-id" {
		t.Errorf("expected client-provided-id, got %s", gotID)
	}
	if rec.Header().Get("X-Request-Id") != "client-provided-id" {
		t.Errorf("expected client-provided-id in response header, got %s", rec.Header().Get("X-Request-Id"))
	}
}

func TestLoggingMiddleware_capturesStatus(t *testing.T) {
	handler := LoggingMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte("ok"))
	}))

	req := httptest.NewRequest(http.MethodPost, "/test", nil)
	req.RemoteAddr = "10.0.0.1:1234"
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Errorf("expected 201, got %d", rec.Code)
	}
}

func TestStatusWriter_defaultsTo200(t *testing.T) {
	rec := httptest.NewRecorder()
	sw := &statusWriter{ResponseWriter: rec, status: http.StatusOK}
	sw.Write([]byte("hello"))

	if sw.status != http.StatusOK {
		t.Errorf("expected 200, got %d", sw.status)
	}
}
