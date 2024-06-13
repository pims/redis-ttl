package main

import (
	"context"
	"flag"
	"log"
	"os"
	"strings"

	redisttl "github.com/pims/redis-ttl"
	"github.com/redis/go-redis/v9"
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

	fs.StringVar(&cfg.redisAddr, "redis-addr", "", "--redis-addr=:6379")
	fs.StringVar(&cfg.scanPrefix, "scan-prefix", "not-found", "--scan-prefix=my-prefix")
	fs.StringVar(&cfg.mode, "mode", "noop", "--mode=exp|gt|lt|nx|xx|noop|persist")
	fs.TextVar(&cfg.desiredTTL, "desired-ttl", &cfg.desiredTTL, "--desired-ttl=24h")
	fs.StringVar(&cfg.redisClusterAddrs, "redis-cluster-addrs", "", "--redis-cluster-addrs=node1:6379,node2:6379")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}

	if err := cfg.Err(); err != nil {
		return err
	}

	var rdb redis.Cmdable
	rdb = redis.NewClient(&redis.Options{
		Addr:       cfg.redisAddr,
		ClientName: "redis-ttl",
	})

	if cfg.redisClusterAddrs != "" {
		rdb = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:      strings.Split(cfg.redisClusterAddrs, ","),
			ClientName: "redis-ttl-cluster",
		})
	}

	if _, err := rdb.Ping(context.Background()).Result(); err != nil {
		return err
	}

	f := &redisttl.Scanner{
		Client:     rdb,
		ScanPrefix: cfg.scanPrefix,
		Mode:       cfg.mode,
		DesiredTTL: cfg.desiredTTL.AsDuration(),
	}

	return f.Run(context.Background())
}
