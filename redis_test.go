package redisttl

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestScan(t *testing.T) {

	testCases := map[string]struct {
		db       map[string]time.Duration
		prefix   string
		mode     string
		expected map[string]time.Duration
		err      error
		errorMsg string
	}{
		"noop does nothing": {
			mode:   "noop",
			prefix: "*",
			db: map[string]time.Duration{
				"foo": 0,
				"zoo": 0,
			},
			expected: map[string]time.Duration{
				"foo": 0,
				"zoo": 0,
			},
		},
		"exp": {
			mode:   "exp",
			prefix: "f",
			db: map[string]time.Duration{
				"foo": 0,
				"zoo": 0,
			},
			expected: map[string]time.Duration{
				"foo": time.Hour,
				"zoo": 0,
			},
		},
		"nx should not set ttl for keys with an existing ttl": {
			mode:   "nx",
			prefix: "f",
			db: map[string]time.Duration{
				"foo": time.Second,
				"zoo": 0,
			},
			expected: map[string]time.Duration{
				"foo": time.Second,
				"zoo": 0,
			},
		},
		"xx should set new ttl for keys with an existing ttl": {
			mode:   "xx",
			prefix: "f",
			db: map[string]time.Duration{
				"foo": time.Second,
				"zoo": 0,
			},
			expected: map[string]time.Duration{
				"foo": time.Hour,
				"zoo": 0,
			},
		},
		"gt should not set ttl if existing ttl is less than ttl": {
			mode:   "gt",
			prefix: "f",
			db: map[string]time.Duration{
				"foo": time.Second,   // less than the desired ttl
				"far": 2 * time.Hour, // gt desired ttl
			},
			expected: map[string]time.Duration{
				"foo": time.Hour,
				"far": 2 * time.Hour,
			},
		},
		"persist unsets ttl": {
			mode:   "persist",
			prefix: "f",
			db: map[string]time.Duration{
				"foo": 0,
				"far": time.Minute,
			},
			expected: map[string]time.Duration{
				"foo": 0,
				"far": 0,
			},
		},
		"invalid mode returns error": {
			mode:   "zx", // invalid mode
			prefix: "f",
			db: map[string]time.Duration{
				"foo": 0,
			},
			expected: map[string]time.Duration{
				"foo": 0,
			},
			err: errInvalidMode,
		},
	}

	for name, tc := range testCases {
		tc := tc
		t.Run(name, func(t *testing.T) {
			rs := miniredis.RunT(t)
			if tc.err != nil {
				rs.SetError(tc.err.Error())
			}

			for k, ttl := range tc.db {
				_ = rs.Set(k, "some value")
				if ttl > 0 {
					rs.SetTTL(k, ttl)
				}
			}

			rdb := redis.NewClient(&redis.Options{
				Addr: rs.Addr(),
			})

			s := Scanner{
				Mode:       tc.mode,
				ScanPrefix: "f*",
				Client:     rdb,
				DesiredTTL: time.Hour,
			}

			ctx := context.Background()
			if err := s.Run(ctx); !errors.Is(err, tc.err) {
				t.Fatalf("want: %v got: %v", tc.err, err)
			}

			for k, dur := range tc.expected {
				ttl := rs.TTL(k)
				if ttl != dur {
					t.Fatalf("ttl don't match for: %s, got:%v want: %v", k, ttl, dur)
				}
			}
		})

	}

}

type dummyLimiter struct {
	err error
}

func (l *dummyLimiter) Wait(_ context.Context) error {
	return l.err
}

type hook struct {
	// possible values are “before” or “after”,
	//based on where we would want to run the hook Action
	err     error
	cmdName string
}

func (h *hook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if cmd.FullName() == h.cmdName {
			return h.err
		}
		return next(ctx, cmd)
	}
}

func (h *hook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func (h *hook) DialHook(hook redis.DialHook) redis.DialHook {
	return hook
}

func TestRunExpError(t *testing.T) {

	s := miniredis.RunT(t)

	rdb := redis.NewClient(&redis.Options{
		Addr: s.Addr(),
	})

	_ = s.Set("foo", "bar")

	rdb.AddHook(&hook{
		cmdName: "expire",
		err:     nil,
	})

	f := Scanner{
		Mode:       "exp",
		ScanPrefix: "f*",
		Client:     rdb,
		DesiredTTL: time.Hour,
	}

	ctx := context.Background()
	if err := f.Run(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRunIterError(t *testing.T) {

	s := miniredis.RunT(t)

	rdb := redis.NewClient(&redis.Options{
		Addr: s.Addr(),
	})

	_ = s.Set("foo", "bar")

	rdb.AddHook(&hook{
		cmdName: "scan",
		err:     fmt.Errorf("scan call failed"),
	})

	f := Scanner{
		Mode:       "exp",
		ScanPrefix: "f*",
		Client:     rdb,
		DesiredTTL: time.Hour,
	}

	ctx := context.Background()
	if err := f.Run(ctx); err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestLimitError(t *testing.T) {

	s := miniredis.RunT(t)

	rdb := redis.NewClient(&redis.Options{
		Addr: s.Addr(),
	})

	_ = s.Set("foo", "bar")

	var errLimit = errors.New("dummy limiter error")
	f := Scanner{
		Mode:       "exp",
		ScanPrefix: "f*",
		Client:     rdb,
		DesiredTTL: time.Hour,
		Limiter:    &dummyLimiter{err: errLimit},
	}

	ctx := context.Background()
	if err := f.Run(ctx); !errors.Is(errLimit, err) {
		t.Fatalf("expected error %v, got: %v", errLimit, err)
	}
}
