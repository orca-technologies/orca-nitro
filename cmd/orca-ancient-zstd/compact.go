// Copyright 2026, Offchain Labs, Inc.
// For license information, see https://github.com/OffchainLabs/nitro/blob/master/LICENSE.md

package main

import (
	"errors"
	"flag"
	"path/filepath"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb/pebble"
	"github.com/ethereum/go-ethereum/log"

	"github.com/offchainlabs/nitro/cmd/conf"
)

// runCompact compacts the whole key range so every sstable is rewritten with
// the node's pebble options plus zstd.
func runCompact(args []string) error {
	f := flag.NewFlagSet("compact", flag.ExitOnError)
	db := f.String("db", "", "pebble database directory (e.g. <chain>/nitro/arbitrumdata)")
	cache := f.Int("cache", 2048, "block cache in MB")
	handles := f.Int("handles", 512, "file handles")
	_ = f.Parse(args)
	if *db == "" {
		return errors.New("--db is required")
	}
	namespace := filepath.Base(*db)
	pebbleConfig := conf.PebbleConfigDefault
	pebbleConfig.ZstdDatabases = []string{namespace}
	start := time.Now()
	database, err := pebble.New(*db, *cache, *handles, namespace, false, pebbleConfig.ExtraOptions(namespace))
	if err != nil {
		return err
	}
	before := dirSize(*db)
	log.Info("compacting", "db", *db, "size", common.StorageSize(before))
	if err := database.Compact(nil, nil); err != nil {
		database.Close()
		return err
	}
	if err := database.Close(); err != nil {
		return err
	}
	log.Info("compacted", "db", *db, "before", common.StorageSize(before), "after", common.StorageSize(dirSize(*db)), "elapsed", common.PrettyDuration(time.Since(start)))
	return nil
}
