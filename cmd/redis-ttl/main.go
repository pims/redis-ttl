package main

import (
	"context"
	"flag"
	"log"
	"os"
	"strings"

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
	fs.StringVar(&cfg.redisClusterAddrs, "redis-cluster-addrs", "", "--redis-cluster-addrs=node1:6379,node2:6379")
	fs.StringVar(&cfg.scanType, "scan-type", "string", "--scan-type=set|string|list|hash")
	fs.Int64Var(&cfg.scanCount, "scan-count", 0, "--scan-count=0")

	if err := fs.Parse(args[1:]); err != nil {
		return err
	}

	if err := cfg.Err(); err != nil {
		return err
	}

	if cfg.redisClusterAddrs != "" {
		clusterClient := redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:      strings.Split(cfg.redisClusterAddrs, ","),
			ClientName: "redis-ttl-cluster",
		})
		ctx := context.Background()
		clusterClient.ReloadState(ctx)

		runScan := func(ctx context.Context, client *redis.Client) error {

			f := &redisttl.Scanner{
				Client:     client,
				ScanPrefix: cfg.scanPrefix,
				Mode:       cfg.mode,
				DesiredTTL: cfg.desiredTTL.AsDuration(),
				Limiter:    rate.NewLimiter(rate.Limit(cfg.rps), cfg.rps),
				ScanType:   cfg.scanType,
			}
			return f.Run(ctx)
		}

		return clusterClient.ForEachMaster(ctx, runScan)
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:       cfg.redisAddr,
		ClientName: "redis-ttl",
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
		ScanType:   cfg.scanType,
		ScanCount:  cfg.scanCount,
	}

	return f.Run(context.Background())
}
