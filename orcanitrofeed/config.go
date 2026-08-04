package orcanitrofeed

import (
	"errors"
	"time"

	"github.com/spf13/pflag"
)

type Config struct {
	Enable      bool          `koanf:"enable"`
	SocketPath  string        `koanf:"socket-path"`
	Mode        string        `koanf:"mode"`         // "tx" | "block"
	BufferSize  int           `koanf:"buffer-size"`  // staging ring 슬롯 수 (인코딩 대기열)
	BufferBytes int           `koanf:"buffer-bytes"` // retention log 바이트 예산
	BufferAge   time.Duration `koanf:"buffer-age"`   // retention log 최대 보관 시간 (0 = age 제한 없음)
	// SameTimestampLookback — SameTimestampIndex warm-up 시 헤더를 뒤로 걷는
	// 최대 칸. feed order block_rel cap(31)을 덮도록 기본 32.
	SameTimestampLookback uint64 `koanf:"same-timestamp-lookback"`
}

var DefaultConfig = Config{
	Enable:                false,
	SocketPath:            "",
	Mode:                  "tx",
	BufferSize:            4096,
	BufferBytes:           1 << 30, // 1GiB
	BufferAge:             30 * time.Minute,
	SameTimestampLookback: DefaultSameTimestampLookback,
}

func ConfigAddOptions(prefix string, f *pflag.FlagSet) {
	f.Bool(prefix+".enable", DefaultConfig.Enable, "enable orca receipt dispatch over unix socket")
	f.String(prefix+".socket-path", DefaultConfig.SocketPath, "unix socket path to listen on")
	f.String(prefix+".mode", DefaultConfig.Mode, "dispatch timing: tx (per-tx, pre-seal) or block (per-block, pre-commit)")
	f.Int(prefix+".buffer-size", DefaultConfig.BufferSize, "encoding staging ring slots (drop-oldest on overflow)")
	f.Int(prefix+".buffer-bytes", DefaultConfig.BufferBytes, "retained backlog byte budget — encoded frames kept for consumer reconnect replay (drop-oldest)")
	f.Duration(prefix+".buffer-age", DefaultConfig.BufferAge, "retained backlog max age — drop-oldest when exceeded (0 disables)")
	f.Uint64(prefix+".same-timestamp-lookback", DefaultConfig.SameTimestampLookback,
		"max headers to walk back when resolving SameTimestampIndex (block_rel); 0 uses default")
}

func (c *Config) Validate() error {
	if !c.Enable {
		return nil
	}
	if c.SocketPath == "" {
		return errors.New("orca-nitro-feed.socket-path required when enabled")
	}
	if c.Mode != "tx" && c.Mode != "block" {
		return errors.New("orca-nitro-feed.mode must be tx or block")
	}
	if c.BufferSize <= 0 {
		return errors.New("orca-nitro-feed.buffer-size must be positive")
	}
	if c.BufferBytes <= 0 {
		return errors.New("orca-nitro-feed.buffer-bytes must be positive")
	}
	if c.BufferAge < 0 {
		return errors.New("orca-nitro-feed.buffer-age must be non-negative")
	}
	return nil
}
