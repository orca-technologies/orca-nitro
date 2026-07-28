package orcafeed

import (
	"errors"

	"github.com/spf13/pflag"
)

type Config struct {
	Enable     bool   `koanf:"enable"`
	SocketPath string `koanf:"socket-path"`
	Mode       string `koanf:"mode"`        // "tx" | "block"
	BufferSize int    `koanf:"buffer-size"` // ring 슬롯 수
}

var DefaultConfig = Config{
	Enable:     false,
	SocketPath: "",
	Mode:       "tx",
	BufferSize: 4096,
}

func ConfigAddOptions(prefix string, f *pflag.FlagSet) {
	f.Bool(prefix+".enable", DefaultConfig.Enable, "enable orca receipt dispatch over unix socket")
	f.String(prefix+".socket-path", DefaultConfig.SocketPath, "unix socket path to listen on")
	f.String(prefix+".mode", DefaultConfig.Mode, "dispatch timing: tx (per-tx, pre-seal) or block (per-block, pre-commit)")
	f.Int(prefix+".buffer-size", DefaultConfig.BufferSize, "dispatch ring buffer slots (drop-oldest on overflow)")
}

func (c *Config) Validate() error {
	if !c.Enable {
		return nil
	}
	if c.SocketPath == "" {
		return errors.New("orca-feed.socket-path required when enabled")
	}
	if c.Mode != "tx" && c.Mode != "block" {
		return errors.New("orca-feed.mode must be tx or block")
	}
	if c.BufferSize <= 0 {
		return errors.New("orca-feed.buffer-size must be positive")
	}
	return nil
}
