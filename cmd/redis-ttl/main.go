package main

import (
	"context"
	"flag"
	"log"
	"os"

	redisttl "github.com/pims/redis-ttl"
	"github.com/redis/go-redis/v9"
	"golang.org/x/time/rate"
)

func main() {

	if err := run(os.Args); err != nil {
		log.Println(err)
		os.Exit(1)
	}
	log.Println("done")
}

func run(args []string) error {
	cfg := config{}

	fs := flag.NewFlagSet("redis-ttl", flag.ExitOnError)

	fs.StringVar(&cfg.redisAddr, "redis-addr", ":6379", "--redis-addr=:6379")
	fs.StringVar(&cfg.scanPrefix, "scan-prefix", "not-found", "--scan-prefix=my-prefix")
	fs.StringVar(&cfg.mode, "mode", "noop", "--mode=exp|gt|lt|nx|xx|noop|persist")
	fs.TextVar(&cfg.desiredTTL, "desired-ttl", &cfg.desiredTTL, "--desired-ttl=24h")
	fs.IntVar(&cfg.rps, "rps", 100, "--rps=100")

	if err := fs.Parse(args[1:]); err != nil {
		return err
	}

	if err := cfg.Err(); err != nil {
		return err
	}

	// TODO: add support for redis cluster
	rdb := redis.NewClient(&redis.Options{
		Addr: cfg.redisAddr,
	})

	if _, err := rdb.Ping(context.Background()).Result(); err != nil {
		return err
	}

	f := &redisttl.Scanner{
		Client:     rdb,
		ScanPrefix: cfg.scanPrefix,
		Mode:       cfg.mode,
		DesiredTTL: cfg.desiredTTL.AsDuration(),
		Limiter:    rate.NewLimiter(rate.Limit(cfg.rps), cfg.rps),
	}

	return f.Run(context.Background())
}
