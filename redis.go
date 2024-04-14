package redisttl

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

var errInvalidMode = errors.New("invalid mode")

type Scanner struct {
	Client     redis.Cmdable
	Mode       string
	ScanPrefix string
	DesiredTTL time.Duration
}

func (f *Scanner) Run(ctx context.Context) error {
	c := f.Client
	iter := c.Scan(ctx, 0, f.ScanPrefix, 0).Iterator()

	type ttlFunc func(ctx context.Context, key string, ttl time.Duration) *redis.BoolCmd

	ttlFuncs := map[string]ttlFunc{
		"exp": c.Expire,
		"gt":  c.ExpireGT,
		"lt":  c.ExpireLT,
		"nx":  c.ExpireNX,
		"xx":  c.ExpireXX,
		"noop": func(ctx context.Context, key string, ttl time.Duration) *redis.BoolCmd {
			return redis.NewBoolCmd(ctx)
		},
		"persist": func(ctx context.Context, key string, _ time.Duration) *redis.BoolCmd {
			return c.Persist(ctx, key)
		},
	}

	fn, found := ttlFuncs[f.Mode]
	if !found {
		return fmt.Errorf("mode %s is not supported: %w", f.Mode, errInvalidMode)
	}

	for iter.Next(ctx) {
		key := iter.Val()
		ok, err := fn(ctx, key, f.DesiredTTL).Result()
		if err != nil {
			return err
		}
		if ok {
			log.Println(key, ok)
		}
	}

	return iter.Err()
}
