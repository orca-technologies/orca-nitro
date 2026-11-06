// Copyright 2026, Offchain Labs, Inc.
// For license information, see https://github.com/OffchainLabs/nitro/blob/master/LICENSE.md

package main

import (
	"context"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/log"

	"github.com/offchainlabs/nitro/cmd/conf"
)

// recompressFlags are the flags shared by prepare, finish and recompress.
type recompressFlags struct {
	dir       *string
	table     *string
	level     *int
	workers   *int
	deleteOld *bool
	train     trainFlags
}

func parseRecompressFlags(name, dirHelp string, args []string) (recompressFlags, error) {
	f := flag.NewFlagSet(name, flag.ExitOnError)
	rf := recompressFlags{
		dir:       f.String("dir", "", dirHelp),
		table:     f.String("table", "", "table name (e.g. bodies)"),
		level:     f.Int("level", 3, "zstd encoder level: 1 fastest, 2 default, 3 better, 4 best"),
		workers:   f.Int("workers", 0, "encoding goroutines (0 = all cores)"),
		deleteOld: f.Bool("delete-old", false, "delete the old snappy files once the new table is verified and swapped in"),
		train:     addTrainFlags(f),
	}
	_ = f.Parse(args)
	if *rf.dir == "" || *rf.table == "" {
		return rf, errors.New("--dir and --table are required")
	}
	return rf, nil
}

func (rf recompressFlags) options(step string, start time.Time) (rawdb.RecompressOptions, error) {
	level, err := conf.ZstdEncoderLevel(*rf.level)
	if err != nil {
		return rawdb.RecompressOptions{}, err
	}
	lastLog := start
	progress := func(s rawdb.RecompressStats) {
		if time.Since(lastLog) < 10*time.Second {
			return
		}
		lastLog = time.Now()
		log.Info(step, "table", *rf.table, "items", s.Items, "total", s.Total,
			"raw", common.StorageSize(s.RawBytes), "disk", common.StorageSize(s.DiskBytes), "elapsed", common.PrettyDuration(time.Since(start)))
	}
	return rawdb.RecompressOptions{Zstd: rawdb.FreezerZstdConfig{Level: level}, Workers: *rf.workers, Progress: progress}, nil
}

func logStep(step, table string, stats rawdb.RecompressStats, start time.Time) {
	log.Info(step+" done", "table", table, "items", stats.Items,
		"raw", common.StorageSize(stats.RawBytes), "disk", common.StorageSize(stats.DiskBytes), "elapsed", common.PrettyDuration(time.Since(start)))
}

// runPrepare recompresses the sealed part of a table while the node runs.
func runPrepare(ctx context.Context, args []string) error {
	rf, err := parseRecompressFlags("prepare", "freezer directory holding the table; the node may keep running", args)
	if err != nil {
		return err
	}
	start := time.Now()
	opts, err := rf.options("preparing", start)
	if err != nil {
		return err
	}
	snap, items, err := rawdb.SnapshotFreezerTable(*rf.dir, *rf.table)
	if err != nil {
		return err
	}
	log.Info("snapshot taken", "table", *rf.table, "items", items, "path", snap)
	if err := ensureDict(snap, *rf.dir, *rf.table, rf.train); err != nil {
		return err
	}
	stats, err := rawdb.PrepareRecompress(ctx, *rf.dir, *rf.table, opts)
	if err != nil {
		return err
	}
	logStep("prepare", *rf.table, stats, start)
	return nil
}

// runFinish completes a prepared table after the node stopped and swaps it in.
func runFinish(ctx context.Context, args []string) error {
	rf, err := parseRecompressFlags("finish", "freezer directory holding the table; the node must be stopped", args)
	if err != nil {
		return err
	}
	start := time.Now()
	opts, err := rf.options("finishing", start)
	if err != nil {
		return err
	}
	stats, err := rawdb.FinishRecompress(ctx, *rf.dir, *rf.table, opts)
	if err != nil {
		return err
	}
	logStep("finish", *rf.table, stats, start)
	return removeOld(rf)
}

// runRecompress does train (when needed), prepare and finish on a stopped node.
func runRecompress(ctx context.Context, args []string) error {
	rf, err := parseRecompressFlags("recompress", "freezer directory holding the table; the node must be stopped", args)
	if err != nil {
		return err
	}
	start := time.Now()
	opts, err := rf.options("recompressing", start)
	if err != nil {
		return err
	}
	if err := ensureDict(*rf.dir, *rf.dir, *rf.table, rf.train); err != nil {
		return err
	}
	stats, err := rawdb.RecompressFreezerTable(ctx, *rf.dir, *rf.table, opts)
	if err != nil {
		return err
	}
	logStep("recompress", *rf.table, stats, start)
	return removeOld(rf)
}

func removeOld(rf recompressFlags) error {
	old := filepath.Join(*rf.dir, ".recompress", *rf.table+".old")
	if !*rf.deleteOld {
		log.Info("old snappy files kept", "path", old)
		return nil
	}
	if err := os.RemoveAll(old); err != nil {
		return err
	}
	log.Info("old table deleted", "path", old)
	return nil
}
