// orca-band-patch — 재실행 불가 블록(receipts mismatch 대역)의 FEED 복구 CLI.
//
// blocks_reexecutor --blocks-reexecutor.skip-on-mismatch가 남긴 mismatch-report를
// 입력으로, 로컬 archive RPC(블록·receipts·code) + Alchemy(debug trace)에서 데이터를
// 모아 orca-nitro-feed wire로 기존 consumer(unix socket)에 dispatch한다.
//
//	orca-band-patch \
//	  --blocks-file /data2/rhc/logs/3w-mismatch.txt \
//	  --socket-path /data2/rhc/orca-feed-patch.sock \
//	  --rpc http://127.0.0.1:8547 \
//	  --alchemy-key-file ~/.config/alchemy/rhc_gazua
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rpc"

	"github.com/offchainlabs/nitro/orcanitrofeed"
	"github.com/offchainlabs/nitro/orcanitrofeed/bandpatch"
	"github.com/offchainlabs/nitro/orcanitrofeed/orcasock"
)

const alchemyURLTemplate = "https://robinhood-mainnet.g.alchemy.com/v2/%s"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		blocksArg      = flag.String("blocks", "", "comma-separated block numbers or ranges (e.g. 100,200-205)")
		blocksFile     = flag.String("blocks-file", "", "file with block numbers: bare integers or 'block=N ...' lines (mismatch-report)")
		dataRPC        = flag.String("rpc", "http://127.0.0.1:8547", "data RPC for blocks/receipts/code (local archive recommended)")
		alchemyURL     = flag.String("alchemy-url", "", "full trace RPC url (overrides --alchemy-key-file)")
		alchemyKeyFile = flag.String("alchemy-key-file", defaultKeyFile(), "file containing a bare Alchemy API key")
		socketPath     = flag.String("socket-path", "", "unix socket to serve orca-nitro-feed on (consumer connects)")
		lookback       = flag.Uint64("same-timestamp-lookback", orcanitrofeed.DefaultSameTimestampLookback, "SameTimestampIndex header walk limit")
		rps            = flag.Int("rps", 8, "max RPC requests per second (data+trace combined)")
		dryRun         = flag.Bool("dry-run", false, "fetch and reconstruct only; count messages instead of dispatching")
	)
	flag.Parse()
	log.SetDefault(log.NewLogger(log.NewTerminalHandlerWithLevel(os.Stderr, log.LevelInfo, false)))

	if *socketPath == "" && !*dryRun {
		return fmt.Errorf("--socket-path is required (or use --dry-run)")
	}
	blocks, err := parseBlocks(*blocksArg, *blocksFile)
	if err != nil {
		return err
	}
	if len(blocks) == 0 {
		return fmt.Errorf("no blocks to patch (--blocks or --blocks-file)")
	}

	traceURL := *alchemyURL
	if traceURL == "" {
		key, err := os.ReadFile(*alchemyKeyFile)
		if err != nil {
			return fmt.Errorf("reading alchemy key file: %w", err)
		}
		traceURL = fmt.Sprintf(alchemyURLTemplate, strings.TrimSpace(string(key)))
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	dataClient, err := rpc.DialContext(ctx, *dataRPC)
	if err != nil {
		return fmt.Errorf("dialing data rpc: %w", err)
	}
	defer dataClient.Close()
	traceClient, err := rpc.DialContext(ctx, traceURL)
	if err != nil {
		return fmt.Errorf("dialing trace rpc: %w", err)
	}
	defer traceClient.Close()

	var sink orcanitrofeed.Sink
	if *dryRun {
		sink = &countingSink{}
	} else {
		cfg := orcanitrofeed.DefaultConfig
		cfg.Enable = true
		cfg.SocketPath = *socketPath
		cfg.SameTimestampLookback = *lookback
		dispatcher, err := orcasock.NewDispatcher(&cfg, orcanitrofeed.ModeSweep)
		if err != nil {
			return fmt.Errorf("starting dispatcher: %w", err)
		}
		// Close는 ring 잔량 flush를 기다린다 — 반드시 완주 후 호출
		defer dispatcher.Close()
		sink = dispatcher
	}

	log.Info("orca-band-patch starting", "blocks", len(blocks), "first", blocks[0], "last", blocks[len(blocks)-1], "rps", *rps, "dryRun", *dryRun)
	patcher := bandpatch.NewPatcher(dataClient, traceClient, sink, *lookback, *rps)
	stats, err := patcher.PatchBlocks(ctx, blocks)
	log.Info("orca-band-patch done", "blocks", stats.Blocks, "receipts", stats.Receipts,
		"degradedTxs", stats.DegradedTxs, "logFallbacks", stats.LogFallbacks, "err", err)
	if cs, ok := sink.(*countingSink); ok {
		log.Info("dry-run message counts", "receipt", cs.receipts, "blockSeal", cs.seals, "rangeDone", cs.ranges)
	}
	return err
}

// countingSink — dry-run용 메모리 sink.
type countingSink struct {
	receipts, seals, ranges int
}

func (s *countingSink) Enqueue(typ orcanitrofeed.MsgType, _ orcanitrofeed.SeqSetter) {
	switch typ {
	case orcanitrofeed.MsgReceipt:
		s.receipts++
	case orcanitrofeed.MsgBlockSeal:
		s.seals++
	case orcanitrofeed.MsgRangeDone:
		s.ranges++
	}
}

func defaultKeyFile() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".config", "alchemy", "rhc_gazua")
}

var blockLineRe = regexp.MustCompile(`block=(\d+)`)

func parseBlocks(arg, file string) ([]uint64, error) {
	var out []uint64
	if arg != "" {
		for _, part := range strings.Split(arg, ",") {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			if lo, hi, ok := strings.Cut(part, "-"); ok {
				a, err1 := strconv.ParseUint(lo, 10, 64)
				b, err2 := strconv.ParseUint(hi, 10, 64)
				if err1 != nil || err2 != nil || b < a {
					return nil, fmt.Errorf("invalid range %q", part)
				}
				for n := a; n <= b; n++ {
					out = append(out, n)
				}
				continue
			}
			n, err := strconv.ParseUint(part, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid block %q", part)
			}
			out = append(out, n)
		}
	}
	if file != "" {
		f, err := os.Open(file)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			if m := blockLineRe.FindStringSubmatch(line); m != nil {
				n, err := strconv.ParseUint(m[1], 10, 64)
				if err != nil {
					return nil, fmt.Errorf("invalid line %q", line)
				}
				out = append(out, n)
				continue
			}
			if n, err := strconv.ParseUint(line, 10, 64); err == nil {
				out = append(out, n)
			}
		}
		if err := scanner.Err(); err != nil {
			return nil, err
		}
	}
	return out, nil
}
