// Copyright 2026, Offchain Labs, Inc.
// For license information, see https://github.com/OffchainLabs/nitro/blob/master/LICENSE.md

// orca-ancient-zstd moves a stopped node's storage from snappy to zstd.
//
//	train      sample a freezer table and install a zstd dictionary for it
//	prepare    while the node runs: snapshot the sealed files, train, recompress them
//	finish     after the node stops: recompress the rest, verify, swap it in
//	recompress on a stopped node: train, prepare and finish in one go
//	compact    rewrite a pebble database's sstables with zstd
//	bench      time item reads of a freezer table
//
// Every command but prepare needs the node stopped. Run the node afterwards with
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
	case "prepare":
		err = runPrepare(ctx, args)
	case "finish":
		err = runFinish(ctx, args)
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
	fmt.Fprintln(os.Stderr, "usage: orca-ancient-zstd train|prepare|finish|recompress|compact|bench [flags]  (-h for flags)")
	os.Exit(2)
}
