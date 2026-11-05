// Copyright 2026, Offchain Labs, Inc.
// For license information, see https://github.com/OffchainLabs/nitro/blob/master/LICENSE.md

// orca-ancient-zstd moves a stopped node's storage from snappy to zstd.
//
//	train      sample a freezer table and install a zstd dictionary for it
//	recompress rewrite a freezer table with zstd, verify it, swap it in
//	compact    rewrite a pebble database's sstables with zstd
//	bench      time item reads of a freezer table
//
// Every command needs the node stopped. Run the node afterwards with
// --persistent.ancient-zstd.enable (and --persistent.pebble.zstd-databases for
// compacted databases) so new data is written the same way.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/ethereum/go-ethereum/log"
)

func main() {
	log.SetDefault(log.NewLogger(log.NewTerminalHandlerWithLevel(os.Stderr, log.LevelInfo, true)))
	if len(os.Args) < 2 {
		usage()
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var err error
	switch command, args := os.Args[1], os.Args[2:]; command {
	case "train":
		err = runTrain(args)
	case "recompress":
		err = runRecompress(ctx, args)
	case "compact":
		err = runCompact(args)
	case "bench":
		err = runBench(args)
	default:
		usage()
	}
	if err != nil {
		log.Crit("failed", "err", err)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: orca-ancient-zstd train|recompress|compact|bench [flags]  (-h for flags)")
	os.Exit(2)
}
