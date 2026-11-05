// Copyright 2026, Offchain Labs, Inc.
// For license information, see https://github.com/OffchainLabs/nitro/blob/master/LICENSE.md

package conf

import (
	"fmt"

	"github.com/klauspost/compress/zstd"
	"github.com/spf13/pflag"

	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/log"
)

// AncientZstdConfig turns on zstd for new items of the compressed ancient
// (freezer) tables. Snappy and zstd items are told apart per item, so it can be
// switched on and off on an existing database.
type AncientZstdConfig struct {
	Enable bool     `koanf:"enable"`
	Level  int      `koanf:"level"`
	Tables []string `koanf:"tables"`
}

var AncientZstdConfigDefault = AncientZstdConfig{
	Enable: false,
	Level:  3,
	Tables: nil,
}

func AncientZstdConfigAddOptions(prefix string, f *pflag.FlagSet, defaultConfig *AncientZstdConfig) {
	f.Bool(prefix+".enable", defaultConfig.Enable, "write new ancient items with zstd, using the table's newest dictionary (<table>.<seq>.zdict) when present")
	f.Int(prefix+".level", defaultConfig.Level, "zstd encoder level: 1 fastest, 2 default, 3 better, 4 best")
	f.StringSlice(prefix+".tables", defaultConfig.Tables, "ancient tables to write with zstd (empty = every compressed table)")
}

func ZstdEncoderLevel(level int) (zstd.EncoderLevel, error) {
	if level < int(zstd.SpeedFastest) || level > int(zstd.SpeedBestCompression) {
		return 0, fmt.Errorf("zstd level %d out of range %d..%d", level, zstd.SpeedFastest, zstd.SpeedBestCompression)
	}
	return zstd.EncoderLevel(level), nil
}

func (c *AncientZstdConfig) Validate() error {
	_, err := ZstdEncoderLevel(c.Level)
	return err
}

// Apply must run before the databases are opened.
func (c *AncientZstdConfig) Apply() error {
	if !c.Enable {
		return nil
	}
	level, err := ZstdEncoderLevel(c.Level)
	if err != nil {
		return err
	}
	rawdb.EnableFreezerZstd(rawdb.FreezerZstdConfig{Level: level, Tables: c.Tables})
	log.Info("ancient zstd enabled", "level", level, "tables", c.Tables)
	return nil
}
