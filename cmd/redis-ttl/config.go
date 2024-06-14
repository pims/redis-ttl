package main

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

var (
	errTTL = errors.New("invalid ttl")
	errRPS = errors.New("invalid rps")
)

var defaultConfig = config{
	redisAddr:         ":6379",
	mode:              "noop",
	scanPrefix:        "not-found",
	desiredTTL:        ttl{dur: 1 * time.Hour},
	rps:               100,
	redisClusterAddrs: "",
	scanType:          "string",
}

type config struct {
	redisAddr         string
	scanPrefix        string
	mode              string
	desiredTTL        ttl
	rps               int
	redisClusterAddrs string
	scanType          string
}

func (c *config) Err() error {
	switch {
	case c.desiredTTL.dur <= 0 && c.mode != "persist":
		return fmt.Errorf("invalid desired-ttl value (%s) for mode %s: %w", &c.desiredTTL, c.mode, errTTL)
	case c.rps <= 0:
		return fmt.Errorf("rps must be greater than 0, got %d: %w", &c.rps, errRPS)
	case c.redisAddr == "" && c.redisClusterAddrs == "":
		return fmt.Errorf("both --redis-addr and --redis-cluster-addrs cannot be empty")
	}

	return nil
}

// ttl is a custom type to simplify parsing a TTL duration
type ttl struct {
	dur time.Duration
}

func newTTL(d time.Duration) ttl {
	return ttl{dur: d}
}

func (d *ttl) MarshalText() ([]byte, error) {
	return []byte(d.dur.String()), nil
}

func (d *ttl) UnmarshalText(text []byte) error {
	if len(text) == 0 {
		return errTTL
	}

	s := string(text)
	dur, err := time.ParseDuration(s)
	if err == nil {
		d.dur = dur
		return nil
	}

	suffix := s[len(s)-1:]

	extras := map[string]time.Duration{
		"d": time.Hour * 24,
		"w": time.Hour * 24 * 7,
	}

	mul, found := extras[suffix]
	if !found {
		return fmt.Errorf("unknown duration suffix %s: %w", suffix, errTTL)
	}
	n, err := strconv.ParseUint(strings.TrimSuffix(s, suffix), 10, 64)
	switch {
	case err != nil:
		return err
	case n == 0:
		return fmt.Errorf("duration has to be greater than 0: %w", errTTL)
	}

	d.dur = time.Duration(n) * mul
	return nil
}

func (d *ttl) AsDuration() time.Duration {
	return d.dur
}

func (d *ttl) String() string {
	return d.dur.String()
}
