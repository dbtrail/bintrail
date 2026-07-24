package main

import (
	"errors"
	"strings"
	"testing"
)

func TestValidateBridgeFlags(t *testing.T) {
	tests := []struct {
		name    string
		connect string
		http    string
		tenants string
		token   string
		wantErr string // substring; "" = no error
	}{
		{name: "no bridge flags", wantErr: ""},
		{name: "plain http mode", http: ":8080", wantErr: ""},
		{name: "bridge alone", connect: "http://host:8090/mcp", wantErr: ""},
		{name: "bridge with token", connect: "https://host/mcp", token: "s3cret", wantErr: ""},
		{name: "token without connect", token: "s3cret", wantErr: "--token requires --connect"},
		{name: "connect plus http", connect: "http://host:8090/mcp", http: ":8080", wantErr: "mutually exclusive"},
		{name: "connect plus tenant dsns", connect: "http://host:8090/mcp", tenants: "dsns.json", wantErr: "mutually exclusive"},
		{name: "connect without scheme", connect: "host:8090/mcp", wantErr: "http://"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateBridgeFlags(tt.connect, tt.http, tt.tenants, tt.token)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error %q does not contain %q", err, tt.wantErr)
			}
		})
	}
}

func TestIsObjectSchema(t *testing.T) {
	if !isObjectSchema(map[string]any{"type": "object"}) {
		t.Error("object schema rejected")
	}
	if isObjectSchema(map[string]any{"type": "string"}) {
		t.Error("string schema accepted")
	}
	if isObjectSchema(nil) {
		t.Error("nil schema accepted")
	}
	if isObjectSchema(make(chan int)) {
		t.Error("unmarshalable schema accepted")
	}
}

func TestAuthHint(t *testing.T) {
	if hint := authHint(errors.New("unexpected status: 401 Unauthorized")); !strings.Contains(hint, "--token") {
		t.Errorf("401 error produced no token hint: %q", hint)
	}
	if hint := authHint(errors.New("dial tcp: connection refused")); hint != "" {
		t.Errorf("network error produced a token hint: %q", hint)
	}
}
