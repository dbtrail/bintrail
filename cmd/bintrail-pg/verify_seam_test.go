package main

import (
	"testing"

	"github.com/dbtrail/dbtrail/internal/cli"
)

// TestPGLiveVerifySeamIsFilled is the wiring guard for #1024: this binary's
// init() must install the PostgreSQL live-source verify provider
// (cli.SetPGLiveVerifyConnect(pgverifysource.LiveSource)). Delete that call
// and every other suite stays green while `bintrail-pg verify --source-dsn`
// refuses at runtime — with a message telling the operator to run the very
// binary they are already running. Mutation-detecting by construction: the
// test observes the seam's state after init, not the code that fills it.
func TestPGLiveVerifySeamIsFilled(t *testing.T) {
	if !cli.PGLiveVerifySeamFilled() {
		t.Fatal("the PG live-source verify seam is empty — cmd/bintrail-pg's init() must call cli.SetPGLiveVerifyConnect(pgverifysource.LiveSource)")
	}
}
