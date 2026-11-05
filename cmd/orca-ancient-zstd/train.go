// Copyright 2026, Offchain Labs, Inc.
// For license information, see https://github.com/OffchainLabs/nitro/blob/master/LICENSE.md

package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/log"
)

// runTrain trains the dictionary with the zstd CLI: its COVER trainer picks the
// dictionary content, which the Go zstd library leaves to the caller.
func runTrain(args []string) error {
	f := flag.NewFlagSet("train", flag.ExitOnError)
	dir := f.String("dir", "", "freezer directory holding the table (e.g. <ancient>/chain)")
	table := f.String("table", "", "table name (e.g. bodies)")
	samples := f.Uint64("samples", 20000, "items sampled evenly over the table")
	dictSize := f.Int("dict-size", 112640, "dictionary size in bytes")
	zstdBin := f.String("zstd", "zstd", "zstd CLI used for training")
	_ = f.Parse(args)
	if *dir == "" || *table == "" {
		return errors.New("--dir and --table are required")
	}
	items, err := rawdb.SampleFreezerItems(*dir, *table, *samples)
	if err != nil {
		return err
	}
	if len(items) == 0 {
		return fmt.Errorf("table %s is empty", *table)
	}
	work, err := os.MkdirTemp(*dir, ".zdict-train-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(work)
	files := make([]string, len(items))
	var sampleBytes int
	for i, item := range items {
		files[i] = filepath.Join(work, fmt.Sprintf("%06d", i))
		if err := os.WriteFile(files[i], item, 0600); err != nil {
			return err
		}
		sampleBytes += len(item)
	}
	out := filepath.Join(work, "dict")
	cmd := exec.Command(*zstdBin, append([]string{"--train", "-q", fmt.Sprintf("--maxdict=%d", *dictSize), "-o", out}, files...)...)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("zstd --train: %w", err)
	}
	dict, err := os.ReadFile(out)
	if err != nil {
		return err
	}
	path, err := rawdb.AddFreezerZstdDict(*dir, *table, dict)
	if err != nil {
		return err
	}
	log.Info("dictionary installed", "table", *table, "path", path, "bytes", len(dict), "samples", len(items), "sampleBytes", sampleBytes)
	return nil
}
