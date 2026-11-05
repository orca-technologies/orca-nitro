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

func runRecompress(ctx context.Context, args []string) error {
	f := flag.NewFlagSet("recompress", flag.ExitOnError)
	dir := f.String("dir", "", "freezer directory holding the table (e.g. <ancient>/chain)")
	table := f.String("table", "", "table name (e.g. bodies)")
	level := f.Int("level", 3, "zstd encoder level: 1 fastest, 2 default, 3 better, 4 best")
	workers := f.Int("workers", 0, "encoding goroutines (0 = all cores)")
	deleteOld := f.Bool("delete-old", false, "delete the old snappy files once the new table is verified and swapped in")
	_ = f.Parse(args)
	if *dir == "" || *table == "" {
		return errors.New("--dir and --table are required")
	}
	zstdLevel, err := conf.ZstdEncoderLevel(*level)
	if err != nil {
		return err
	}
	start := time.Now()
	lastLog := start
	progress := func(s rawdb.RecompressStats) {
		if time.Since(lastLog) < 10*time.Second {
			return
		}
		lastLog = time.Now()
		log.Info("recompressing", "table", *table, "items", s.Items, "total", s.Total,
			"raw", common.StorageSize(s.RawBytes), "disk", common.StorageSize(s.DiskBytes), "elapsed", common.PrettyDuration(time.Since(start)))
	}
	stats, err := rawdb.RecompressFreezerTable(ctx, *dir, *table, rawdb.RecompressOptions{
		Zstd:     rawdb.FreezerZstdConfig{Level: zstdLevel},
		Workers:  *workers,
		Progress: progress,
	})
	if err != nil {
		return err
	}
	old := filepath.Join(*dir, ".recompress", *table+".old")
	log.Info("table recompressed and swapped in", "table", *table, "items", stats.Items,
		"raw", common.StorageSize(stats.RawBytes), "disk", common.StorageSize(stats.DiskBytes),
		"elapsed", common.PrettyDuration(time.Since(start)), "old", old)
	if *deleteOld {
		if err := os.RemoveAll(old); err != nil {
			return err
		}
		log.Info("old table deleted", "path", old)
	}
	return nil
}
