// Copyright 2026, Offchain Labs, Inc.
// For license information, see https://github.com/OffchainLabs/nitro/blob/master/LICENSE.md

package main

import (
	"errors"
	"flag"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/log"
)

// runBench times random and sequential item reads of a table, to compare the
// read cost of snappy and zstd copies of the same data.
func runBench(args []string) error {
	f := flag.NewFlagSet("bench", flag.ExitOnError)
	dir := f.String("dir", "", "freezer directory holding the table")
	table := f.String("table", "", "table name")
	reads := f.Int("reads", 20000, "items read in each pattern")
	_ = f.Parse(args)
	if *dir == "" || *table == "" {
		return errors.New("--dir and --table are required")
	}
	for _, pattern := range []string{"random", "sequential"} {
		result, err := rawdb.BenchFreezerReads(*dir, *table, *reads, pattern == "random")
		if err != nil {
			return err
		}
		log.Info("read bench", "table", *table, "pattern", pattern, "items", result.Items,
			"raw", common.StorageSize(result.RawBytes), "elapsed", common.PrettyDuration(result.Elapsed),
			"us/item", float64(result.Elapsed.Microseconds())/float64(result.Items),
			"rawMB/s", float64(result.RawBytes)/result.Elapsed.Seconds()/1e6)
	}
	return nil
}
