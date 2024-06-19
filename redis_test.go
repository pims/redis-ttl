package redisttl

import (
	"context"
	"errors"
	"fmt"
	"reflect"
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

func TestPrimaryNodesFromClusterNodes(t *testing.T) {
	input := `07c37dfeb235213a872192d90877d0cd55635b91 127.0.0.1:30004@31004,hostname4 slave e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca 0 1426238317239 4 connected
67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1 127.0.0.1:30002@31002,hostname2 master - 0 1426238316232 2 connected 5461-10922
292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f 127.0.0.1:30003@31003,hostname3 master - 0 1426238318243 3 connected 10923-16383
6ec23923021cf3ffec47632106199cb7f496ce01 127.0.0.1:30005@31005,hostname5 slave 67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1 0 1426238316232 5 connected
824fe116063bc5fcf9f4ffd895bc17aee7731ac3 127.0.0.1:30006@31006,hostname6 slave 292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f 0 1426238317741 6 connected
e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca 127.0.0.1:30001@31001,hostname1 myself,master - 0 0 1 connected 0-5460
`
	primaries := PrimaryNodesFromClusterNodes(input)
	expected := []string{"127.0.0.1:30002", "127.0.0.1:30003", "127.0.0.1:30001"}
	if !reflect.DeepEqual(primaries, expected) {
		t.Fatalf("got: %v, want : %v", primaries, expected)
	}
}
