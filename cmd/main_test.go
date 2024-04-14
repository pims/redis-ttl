package main

import (
	"testing"

	"github.com/alicebob/miniredis/v2"
)

func TestRun(t *testing.T) {
	if err := run([]string{"redis-ttl"}); err == nil {
		t.Fatal("expected error, got nil")
	}

	if err := run([]string{"redis-ttl", "--desired-ttl=1w"}); err != nil {
		t.Fatalf("expected nil, got: %v", err)
	}

	if err := run([]string{"redis-ttl", "--desired-ttl=0s"}); err == nil {
		t.Fatal("expected error got nil")
	}

	s := miniredis.RunT(t)
	s.SetError("fault-injected")
	if err := run([]string{
		"redis-ttl",
		"--desired-ttl=1w",
		"--redis-addr=" + s.Addr(),
	}); err == nil {
		t.Fatal("expected error, got nil")
	}
}
