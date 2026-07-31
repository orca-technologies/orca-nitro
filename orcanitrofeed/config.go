package orcanitrofeed

import (
	"errors"

	"github.com/spf13/pflag"
)

type Config struct {
	Enable      bool   `koanf:"enable"`
	SocketPath  string `koanf:"socket-path"`
	Mode        string `koanf:"mode"`         // "tx" | "block"
	BufferSize  int    `koanf:"buffer-size"`  // staging ring 슬롯 수 (인코딩 대기열)
	BufferBytes int    `koanf:"buffer-bytes"` // retention log 바이트 예산 (컨슈머 다운타임 backlog)
}

var DefaultConfig = Config{
	Enable:      false,
	SocketPath:  "",
	Mode:        "tx",
	BufferSize:  4096,
	BufferBytes: 1 << 30, // 1GiB — feeder 재배포 다운타임 동안 무손실 재접속
}

func ConfigAddOptions(prefix string, f *pflag.FlagSet) {
	f.Bool(prefix+".enable", DefaultConfig.Enable, "enable orca receipt dispatch over unix socket")
	f.String(prefix+".socket-path", DefaultConfig.SocketPath, "unix socket path to listen on")
	f.String(prefix+".mode", DefaultConfig.Mode, "dispatch timing: tx (per-tx, pre-seal) or block (per-block, pre-commit)")
	f.Int(prefix+".buffer-size", DefaultConfig.BufferSize, "encoding staging ring slots (drop-oldest on overflow)")
	f.Int(prefix+".buffer-bytes", DefaultConfig.BufferBytes, "retained backlog byte budget — encoded frames kept for consumer reconnect replay (drop-oldest)")
}

func (c *Config) Validate() error {
	if !c.Enable {
		return nil
	}
	if c.SocketPath == "" {
		return errors.New("orcanitrofeed.socket-path required when enabled")
	}
	if c.Mode != "tx" && c.Mode != "block" {
		return errors.New("orcanitrofeed.mode must be tx or block")
	}
	if c.BufferSize <= 0 {
		return errors.New("orcanitrofeed.buffer-size must be positive")
	}
	if c.BufferBytes <= 0 {
		return errors.New("orcanitrofeed.buffer-bytes must be positive")
	}
	return nil
}
