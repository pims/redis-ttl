package main

import (
	"errors"
	"testing"
	"time"
)

func TestValidTTL(t *testing.T) {
	m := map[string]time.Duration{
		"1h": time.Hour,
		"1w": time.Hour * 24 * 7,
	}
	for k, v := range m {
		dur := ttl{}
		if err := dur.UnmarshalText([]byte(k)); err != nil {
			t.Fatal(err)
		}
		if v != dur.AsDuration() {
			t.Fatalf("want: %v got: %v", v, dur.AsDuration())
		}
	}
}

func TestInvalidTTL(t *testing.T) {
	input := []string{
		"1w2h", // only simple suffixes are supported for days and weeks
		"1y",   // year is not supported
		"",
		"0w",   // a ttl can't be 0
		"foow", // non numerical prefix
	}
	for _, s := range input {
		dur := ttl{}
		if err := dur.UnmarshalText([]byte(s)); err == nil {
			t.Fatalf("expected error for invalid ttl: %s", s)
		}
	}
}

func TestRoundTrip(t *testing.T) {
	input := []string{
		"1h1m0s",
		"0s",
		"1m0s",
	}
	for _, s := range input {
		dur := ttl{}
		if err := dur.UnmarshalText([]byte(s)); err != nil {
			t.Fatalf("unexpected error for ttl %s: %v", s, err)
		}
		if dur.String() != s {
			t.Fatalf("got %s want: %s", dur.String(), s)
		}
		text, err := dur.MarshalText()
		if err != nil {
			t.Fatalf("expected error for invalid ttl: %s", s)
		}
		got := string(text)
		if s != got {
			t.Fatalf("got %s want: %s", got, s)
		}
	}
}

func TestConfig(t *testing.T) {

	testCases := map[string]struct {
		cfg config
		err error
	}{
		"default config is valid": {
			cfg: defaultConfig,
			err: nil,
		},
		"can't have 0 ttl except for one mode": {
			cfg: config{
				mode:       "noop",
				desiredTTL: newTTL(0),
			},
			err: errTTL,
		},
		"can't set rps to 0": {
			cfg: config{rps: 0, mode: "persist"},
			err: errRPS,
		},
	}

	for name, tc := range testCases {
		tc := tc
		t.Run(name, func(t *testing.T) {
			if !errors.Is(tc.cfg.Err(), tc.err) {
				t.Fatalf("got: %v, want: %v", tc.cfg.Err(), tc.err)
			}
		})
	}
}
