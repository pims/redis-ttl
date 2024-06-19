package redisttl

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

var errInvalidMode = errors.New("invalid mode")

type limiter interface {
	Wait(ctx context.Context) (err error)
}

type Scanner struct {
	Client     redis.Cmdable
	ScanClient redis.Cmdable
	Mode       string
	ScanPrefix string
	DesiredTTL time.Duration
	Limiter    limiter
	ScanType   string
	ScanCount  int64
	Name       string
}

func (f *Scanner) Run(ctx context.Context) error {
	c := f.Client
	if f.ScanClient == nil {
		f.ScanClient = c
	}
	iter := f.ScanClient.ScanType(ctx, 0, f.ScanPrefix, f.ScanCount, f.ScanType).Iterator()

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
		return fmt.Errorf("[%s] mode %s is not supported: %w", f.Name, f.Mode, errInvalidMode)
	}

	for iter.Next(ctx) {

		if err := f.wait(ctx); err != nil {
			return err
		}

		key := iter.Val()
		ok, err := fn(ctx, key, f.DesiredTTL).Result()
		if err != nil {
			log.Printf("[%s] expFn error: %v\n", f.Name, errInvalidMode)
			continue
		}
		if ok {
			log.Printf("[%s] %s, ttl %s\n", f.Name, key, f.DesiredTTL)
		}
	}

	iterErr := iter.Err()
	if iterErr != nil {
		return fmt.Errorf("iter error: %w", iterErr)
	}
	return nil
}

func (s *Scanner) wait(ctx context.Context) error {
	if s.Limiter != nil {
		return s.Limiter.Wait(ctx)
	}
	return nil
}

func PrimaryNodesFromClusterNodes(s string) []string {
	// <id> <ip:port@cport[,hostname]> <flags> <master> <ping-sent> <pong-recv> <config-epoch> <link-state> <slot> <slot> ... <slot>
	scan := bufio.NewScanner(strings.NewReader(s))
	res := []string{}
	for scan.Scan() {
		line := scan.Text()
		if !strings.Contains(line, "master") {
			continue
		}

		parts := strings.SplitN(line, " ", 3)
		addr := strings.Split(parts[1], "@")[0]
		res = append(res, addr)
	}
	return res
}
