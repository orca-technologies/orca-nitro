// Copyright 2024-2026, Offchain Labs, Inc.
// For license information, see https://github.com/OffchainLabs/nitro/blob/master/LICENSE.md
package blocksreexecutor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spf13/pflag"

	"github.com/ethereum/go-ethereum/arbitrum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/triedb"
	"github.com/ethereum/go-ethereum/triedb/hashdb"

	"github.com/offchainlabs/nitro/orcanitrofeed"
	"github.com/offchainlabs/nitro/util"
	"github.com/offchainlabs/nitro/util/arbmath"
	"github.com/offchainlabs/nitro/util/stopwaiter"
)

// lint:require-exhaustive-initialization
type Config struct {
	Enable             bool   `koanf:"enable"`
	Mode               string `koanf:"mode"`
	Blocks             string `koanf:"blocks"` // Range of blocks to be executed in json format
	CommitStateToDisk  bool   `koanf:"commit-state-to-disk"`
	Room               int    `koanf:"room"`
	MinBlocksPerThread uint64 `koanf:"min-blocks-per-thread"`
	TrieCleanLimit     int    `koanf:"trie-clean-limit"`
	ValidateMultiGas   bool   `koanf:"validate-multigas"`
	// path 모드 전용: receipts root·gasUsed 불일치 블록을 fatal 대신 건너뛴다
	// (기록 + dispatch 생략 + 다음 블록은 fresh state). 불일치 블록은 로그와
	// MismatchReport 파일에 남는다 — orca-band-patch로 별도 복구.
	SkipOnMismatch bool   `koanf:"skip-on-mismatch"`
	MismatchReport string `koanf:"mismatch-report"`

	blocks [][2]uint64
}

func (c *Config) Validate() error {
	c.Mode = strings.ToLower(c.Mode)
	if c.Enable && c.Mode != "random" && c.Mode != "full" {
		return errors.New("invalid mode for blocks re-execution")
	}
	if c.Blocks == "" {
		return errors.New("list of block ranges to be re-executed cannot be empty")
	}
	var blocks [][2]uint64
	if err := json.Unmarshal([]byte(c.Blocks), &blocks); err != nil {
		return fmt.Errorf("failed to parse blocks re-execution's blocks string: %w", err)
	}
	c.blocks = blocks
	for _, blockRange := range c.blocks {
		if blockRange[1] < blockRange[0] {
			return errors.New("invalid block range for blocks re-execution")
		}
	}
	if c.Room <= 0 {
		return errors.New("room for blocks re-execution should be greater than 0")
	}
	return nil
}

var DefaultConfig = Config{
	Enable:             false,
	Mode:               "random",
	Room:               util.GoMaxProcs(),
	Blocks:             `[[0,0]]`, // execute from chain start to chain end
	CommitStateToDisk:  false,
	MinBlocksPerThread: 0,
	TrieCleanLimit:     0,
	ValidateMultiGas:   false,
	SkipOnMismatch:     false,
	MismatchReport:     "",
	blocks:             nil,
}

var TestConfig = Config{
	Enable:             true,
	Mode:               "full",
	Blocks:             `[[0,0]]`, // execute from chain start to chain end
	CommitStateToDisk:  false,
	Room:               util.GoMaxProcs(),
	TrieCleanLimit:     600,
	MinBlocksPerThread: 0,
	ValidateMultiGas:   true,
	SkipOnMismatch:     false,
	MismatchReport:     "",

	blocks: [][2]uint64{},
}

func ConfigAddOptions(prefix string, f *pflag.FlagSet) {
	f.Bool(prefix+".enable", DefaultConfig.Enable, "enables re-execution of a range of blocks against historic state")
	f.String(prefix+".mode", DefaultConfig.Mode, "mode to run the blocks-reexecutor on. Valid modes full and random. full - execute all the blocks in the given range. random - execute a random sample range of blocks with in a given range")
	f.String(prefix+".blocks", DefaultConfig.Blocks, "json encoded list of block ranges in the form of start and end block numbers in a list of size 2")
	f.Bool(prefix+".commit-state-to-disk", DefaultConfig.CommitStateToDisk, "if set, blocks-reexecutor not only re-executes blocks but it also commits their state to triedb")
	f.Int(prefix+".room", DefaultConfig.Room, "number of threads to parallelize blocks re-execution")
	f.Uint64(prefix+".min-blocks-per-thread", DefaultConfig.MinBlocksPerThread, "minimum number of blocks to execute per thread. When mode is random this acts as the size of random block range sample. In path (historic) mode this is the fixed chunk size per worker (default 2000)")
	f.Int(prefix+".trie-clean-limit", DefaultConfig.TrieCleanLimit, "memory allowance (MB) to use for caching trie nodes in memory")
	f.Bool(prefix+".validate-multigas", DefaultConfig.ValidateMultiGas, "if set, validate the sum of multi-gas dimensions match the single-gas")
	f.Bool(prefix+".skip-on-mismatch", DefaultConfig.SkipOnMismatch, "path mode only: on receipts-root/gas-used mismatch, record and skip the block instead of failing the run")
	f.String(prefix+".mismatch-report", DefaultConfig.MismatchReport, "append skipped mismatch blocks to this file (block=N kind=... got=... want=...)")
}

// path 모드 chunk 크기 기본값 (min-blocks-per-thread로 재정의 가능).
// chunk는 배분 단위이자 carry-forward statedb의 dirty set 상한 — 1k~4k가
// tail 손실(chunk가 클수록 커짐)과 메모리·cold-cache 비용(작을수록 커짐)의 균형점.
const defaultHistoricChunkBlocks = 2000

// worker별 블록 prefetch 깊이 — 실행 중에 다음 블록의 freezer 읽기·RLP decode·
// sender ecrecover를 겹치기 위한 버퍼.
const historicPrefetchDepth = 8

// lint:require-exhaustive-initialization
type BlocksReExecutor struct {
	stopwaiter.StopWaiter
	config        *Config
	db            state.Database
	blockchain    *core.BlockChain
	stateFor      arbitrum.StateForHeaderFunction
	done          chan struct{}
	fatalErrChan  chan error
	fatalReported atomic.Bool // set by reportFatalErr; checked by Impl and Start to stop launching work
	blocks        [][3]uint64 // start, end and minBlocksPerThread of block ranges
	mutex         sync.Mutex
	success       chan struct{}

	// orca sweep dispatch (옵션). sink는 worker 간 공유, observer는 worker별 생성.
	orcaSink                  orcanitrofeed.Sink
	orcaRanges                [][2]uint64 // dispatch 대상 원본 범위 (pre-state용 start-- 이전 값)
	orcaSameTimestampLookback uint64

	// path 스킴 archive (Titan 등): HistoricReader로 블록별 과거 state를 직접 열어
	// 실행한다 — hash 모드의 state 전진(FindLastAvailableState)이 불필요.
	pathMode   bool
	historicDB state.Database

	// skip-on-mismatch: worker들이 공유하는 report 파일 (nil = 미기록)
	mismatchMu   sync.Mutex
	mismatchFile *os.File
	skippedCount atomic.Uint64
}

// SetOrcaSink — Start 이전 1회 주입. 설정 시 재실행 블록의 receipt·transfer를 dispatch한다.
func (s *BlocksReExecutor) SetOrcaSink(sink orcanitrofeed.Sink, sameTimestampLookback uint64) {
	s.orcaSink = sink
	s.orcaSameTimestampLookback = sameTimestampLookback
}

func (s *BlocksReExecutor) newOrcaSweepObserver() *orcanitrofeed.SweepObserver {
	if s.orcaSink == nil {
		return nil
	}
	return orcanitrofeed.NewSweepObserver(s.orcaSink, s.orcaRanges, s.orcaSameTimestampLookback, func(n uint64) (uint64, bool) {
		h := s.blockchain.GetHeaderByNumber(n)
		if h == nil {
			return 0, false
		}
		return h.Time, true
	})
}

func New(c *Config, blockchain *core.BlockChain, ethDb ethdb.Database) (*BlocksReExecutor, error) {
	pathMode := blockchain.TrieDB().Scheme() == rawdb.PathScheme
	chainStart := blockchain.Config().ArbitrumChainParams.GenesisBlockNum
	chainEnd := blockchain.CurrentBlock().Number.Uint64()
	minBlocksPerThread := uint64(10000)
	if pathMode {
		minBlocksPerThread = defaultHistoricChunkBlocks
	}
	if c.MinBlocksPerThread != 0 {
		minBlocksPerThread = c.MinBlocksPerThread
	}
	var blocks [][3]uint64
	var orcaRanges [][2]uint64
	for _, blockRange := range c.blocks {
		start := blockRange[0]
		end := blockRange[1]
		if start == 0 && end == 0 {
			start = chainStart
			end = chainEnd
		}
		if start < chainStart || start > chainEnd {
			log.Warn("invalid state reexecutor's start block number, resetting to genesis", "start", start, "genesis", chainStart)
			start = chainStart
		}
		if end > chainEnd || end < chainStart {
			log.Warn("invalid state reexecutor's end block number, resetting to latest", "end", end, "latest", chainEnd)
			end = chainEnd
		}
		if c.Mode == "random" && end != start {
			// Reexecute a range of 10000 or (non-zero) c.MinBlocksPerThread number of blocks between start to end picked randomly
			rng := minBlocksPerThread
			if rng > end-start {
				rng = end - start
			}
			// #nosec G115
			start += uint64(rand.Int63n(int64(end - start - rng + 1)))
			end = start + rng
		}
		// Inclusive of block reexecution [start, end]
		// Do not reexecute genesis block i,e chainStart
		orcaRanges = append(orcaRanges, [2]uint64{start, end})
		if start > 0 && start != chainStart {
			start--
		}
		// Divide work equally among available threads when MinBlocksPerThread is zero.
		// path 모드는 고정 chunk를 쓴다 — Room 기준 등분은 chunk를 키워 tail 손실만 늘린다.
		var work uint64
		if c.MinBlocksPerThread == 0 && !pathMode {
			// #nosec G115
			work = (end - start) / uint64(c.Room*2)
		}
		if work > 0 {
			blocks = append(blocks, [3]uint64{start, end, work})
		} else {
			blocks = append(blocks, [3]uint64{start, end, minBlocksPerThread})
		}
	}
	if pathMode {
		// path 모드는 오름차순으로 소진한다 — consumer의 contiguous progress(중단 후
		// 재개 granularity)가 낮은 블록부터 부드럽게 전진하도록.
		sort.Slice(blocks, func(i, j int) bool {
			return blocks[i][0] < blocks[j][0]
		})
	} else {
		// We sort the block ranges in descending order of their endBlocks to avoid duplicate reexecution of blocks
		sort.Slice(blocks, func(i, j int) bool {
			return blocks[i][1] > blocks[j][1]
		})
	}
	var blocksReExecutor *BlocksReExecutor
	var stateDatabase state.Database
	var historicDB state.Database
	var stateForFunc arbitrum.StateForHeaderFunction

	if pathMode {
		// path archive: 과거 state는 state history freezer에서 직접 읽는다.
		// (EnableStateIndexing은 archive 모드에서 켜짐 — core/blockchain.go)
		historicDB = state.NewHistoricDatabase(ethDb, blockchain.TrieDB())
	} else {
		hashConfig := *hashdb.Defaults
		hashConfig.CleanCacheSize = c.TrieCleanLimit * 1024 * 1024
		trieConfig := triedb.Config{
			Preimages: false,
			HashDB:    &hashConfig,
		}
		stateDatabase = state.NewDatabase(triedb.NewDatabase(ethDb, &trieConfig), nil)
		stateForFunc = func(header *types.Header) (*state.StateDB, arbitrum.StateReleaseFunc, error) {
			blocksReExecutor.mutex.Lock()
			defer blocksReExecutor.mutex.Unlock()
			sdb, err := state.New(header.Root, blocksReExecutor.db)
			if err == nil {
				_ = blocksReExecutor.db.TrieDB().Reference(header.Root, common.Hash{}) // Will be dereferenced later in advanceStateUpToBlock
				return sdb, func() { blocksReExecutor.dereferenceRoot(header.Root) }, nil
			}
			return sdb, arbitrum.NoopStateRelease, err
		}
	}

	var mismatchFile *os.File
	if c.SkipOnMismatch && c.MismatchReport != "" {
		var err error
		mismatchFile, err = os.OpenFile(c.MismatchReport, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			return nil, fmt.Errorf("opening mismatch report %s: %w", c.MismatchReport, err)
		}
	}
	blocksReExecutor = &BlocksReExecutor{
		StopWaiter:    stopwaiter.StopWaiter{},
		config:        c,
		db:            stateDatabase,
		blockchain:    blockchain,
		stateFor:      stateForFunc,
		blocks:        blocks,
		done:          make(chan struct{}, c.Room),
		fatalErrChan:  make(chan error, c.Room),
		fatalReported: atomic.Bool{},
		success:       make(chan struct{}),
		mutex:         sync.Mutex{},
		orcaSink:      nil,
		orcaRanges:    orcaRanges,
		pathMode:      pathMode,
		historicDB:    historicDB,
		mismatchMu:    sync.Mutex{},
		mismatchFile:  mismatchFile,
		skippedCount:  atomic.Uint64{},
	}
	return blocksReExecutor, nil
}

// recordMismatch — skip-on-mismatch로 건너뛴 블록을 로그·report 파일에 남긴다.
// 형식은 orca-band-patch --blocks-file이 그대로 파싱한다.
func (s *BlocksReExecutor) recordMismatch(blockNum uint64, kind string, got, want string) {
	s.skippedCount.Add(1)
	log.Warn("skipping mismatched block", "block", blockNum, "kind", kind, "got", got, "want", want)
	if s.mismatchFile == nil {
		return
	}
	s.mismatchMu.Lock()
	defer s.mismatchMu.Unlock()
	fmt.Fprintf(s.mismatchFile, "block=%d kind=%s got=%s want=%s\n", blockNum, kind, got, want)
}

func logState(header *types.Header, hasState bool) {
	if height := header.Number.Uint64(); height%1_000_000 == 0 {
		log.Info("Finding last available state.", "block", height, "hash", header.Hash(), "hasState", hasState)
	}
}

// reportFatalErr marks a fatal error and attempts to send it to the fatal error channel.
// The fatalReported flag is set unconditionally so that Impl and Start stop launching
// new work. The error is always logged; if the channel is full, the error is dropped
// from the channel but remains visible in logs.
func (s *BlocksReExecutor) reportFatalErr(err error) {
	s.fatalReported.Store(true)
	log.Error("blocksReExecutor: fatal error", "err", err)
	select {
	case s.fatalErrChan <- err:
	default:
	}
}

// LaunchBlocksReExecution launches a thread to re-execute blocks ending at currentBlock,
// starting from at most minBlocksPerThread blocks before currentBlock (clamped to startBlock
// and adjusted to the last block with available state). It returns the block number from
// which re-execution actually starts (after state-availability adjustment), which callers
// use as the next upper bound. Every call produces exactly one send on s.done, regardless
// of whether a goroutine is launched or an early error occurs.
func (s *BlocksReExecutor) LaunchBlocksReExecution(ctx context.Context, startBlock, currentBlock, minBlocksPerThread uint64) uint64 {
	launched := false
	defer func() {
		if !launched {
			s.done <- struct{}{}
		}
	}()
	start := arbmath.SaturatingUSub(currentBlock, minBlocksPerThread)
	if start < startBlock {
		start = startBlock
	}
	startHeader := s.blockchain.GetHeaderByNumber(start)
	if startHeader == nil {
		s.reportFatalErr(fmt.Errorf("blocksReExecutor failed to get start header at %d", start))
		return startBlock
	}
	startState, startHeader, release, err := arbitrum.FindLastAvailableState(ctx, s.blockchain, s.stateFor, startHeader, logState, -1)
	if err != nil {
		s.reportFatalErr(fmt.Errorf("blocksReExecutor failed to get last available state while searching for state at %d, err: %w", start, err))
		return startBlock
	}
	start = startHeader.Number.Uint64()
	targetHeader := s.blockchain.GetHeaderByNumber(currentBlock)
	if targetHeader == nil {
		release()
		s.reportFatalErr(fmt.Errorf("blocksReExecutor failed to get target header at %d", currentBlock))
		return startBlock
	}
	launched = true
	s.LaunchThread(func(ctx context.Context) {
		defer func() { s.done <- struct{}{} }()
		orcaObserver := s.newOrcaSweepObserver()
		log.Info("Starting reexecution of blocks against historic state", "stateAt", start, "startBlock", start+1, "endBlock", currentBlock)
		if err := s.advanceStateUpToBlock(ctx, startState, targetHeader, startHeader, release, orcaObserver); err != nil {
			if ctx.Err() == nil {
				s.reportFatalErr(fmt.Errorf("blocksReExecutor errored advancing state from block %d to block %d, err: %w", start, currentBlock, err))
			}
		} else {
			log.Info("Successfully reexecuted blocks against historic state", "stateAt", start, "startBlock", start+1, "endBlock", currentBlock)
			if orcaObserver != nil {
				orcaObserver.OnRangeDone(start+1, currentBlock)
			}
		}
	})
	return start
}

// prefetchResult — prefetch 파이프라인이 실행 worker에 넘기는 단위.
type prefetchResult struct {
	block *types.Block
	err   error
}

// prefetchBlocks — (from, to] 블록을 freezer에서 미리 읽고 sender ecrecover까지
// 수행해 채널로 넘긴다. types.Sender는 결과를 tx 내부에 캐시하므로 Process의
// 실행 루프는 ecrecover 없이 캐시를 재사용한다. ctx 취소 시 채널을 닫고 종료.
func (s *BlocksReExecutor) prefetchBlocks(ctx context.Context, from, to uint64) <-chan prefetchResult {
	out := make(chan prefetchResult, historicPrefetchDepth)
	go func() {
		defer close(out)
		config := s.blockchain.Config()
		for n := from; n <= to; n++ {
			block := s.blockchain.GetBlockByNumber(n)
			var result prefetchResult
			if block == nil {
				result = prefetchResult{block: nil, err: fmt.Errorf("block %d not found", n)}
			} else {
				arbosVersion := types.DeserializeHeaderExtraInformation(block.Header()).ArbOSFormatVersion
				signer := types.MakeSigner(config, block.Number(), block.Time(), arbosVersion)
				for _, tx := range block.Transactions() {
					// 캐시 워밍 전용 — 서명 오류는 Process가 같은 tx에서 다시 만나 처리한다
					_, _ = types.Sender(signer, tx)
				}
				result = prefetchResult{block: block, err: nil}
			}
			select {
			case out <- result:
			case <-ctx.Done():
				return
			}
			if result.err != nil {
				return
			}
		}
	}()
	return out
}

// launchHistoricChunk — path archive용 chunk worker. (lo, hi] 블록들을 HistoricReader
// 기반 state로 실행한다 (state 전진·release 관리 불필요). statedb는 chunk 전체에
// carry-forward — dirty set이 chunk 크기로 바운드되므로 중간 재오픈이 필요 없다
// (touch되지 않은 계정은 pinned reader가, 변경분은 in-memory가 답한다. 블록마다
// historic reader를 새로 여는 것이 path 모드의 지배적 비용이었다).
func (s *BlocksReExecutor) launchHistoricChunk(ctx context.Context, lo, hi uint64) {
	s.LaunchThread(func(ctx context.Context) {
		defer func() { s.done <- struct{}{} }()
		orcaObserver := s.newOrcaSweepObserver()
		log.Info("Starting historic reexecution of blocks", "startBlock", lo+1, "endBlock", hi)
		prefetchCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		prefetched := s.prefetchBlocks(prefetchCtx, lo+1, hi)
		var statedb *state.StateDB
		var skippedBlocks []uint64
		for n := lo + 1; n <= hi; n++ {
			if ctx.Err() != nil {
				return
			}
			pf, ok := <-prefetched
			if !ok {
				if ctx.Err() == nil {
					s.reportFatalErr(fmt.Errorf("blocksReExecutor historic prefetch closed unexpectedly at block %d", n))
				}
				return
			}
			if pf.err != nil {
				s.reportFatalErr(fmt.Errorf("blocksReExecutor historic prefetch failed: %w", pf.err))
				return
			}
			next, skipped, err := s.reExecuteHistoricBlock(pf.block, statedb, orcaObserver)
			if err != nil {
				if ctx.Err() == nil {
					s.reportFatalErr(fmt.Errorf("blocksReExecutor historic reexecution failed at block %d: %w", n, err))
				}
				return
			}
			if skipped {
				skippedBlocks = append(skippedBlocks, n)
			}
			statedb = next
		}
		log.Info("Successfully reexecuted historic blocks", "startBlock", lo+1, "endBlock", hi, "skipped", len(skippedBlocks))
		if orcaObserver != nil {
			emitRangeDoneExcluding(orcaObserver, lo+1, hi, skippedBlocks)
		}
	})
}

// emitRangeDoneExcluding — skip된 블록을 제외한 연속 구간별 RangeDone.
// RangeDone은 "구간의 모든 블록이 dispatch됐다"는 완료 신호여야 한다 —
// skip 블록을 포함하면 progress·regen이 결손을 완료로 오인한다.
func emitRangeDoneExcluding(o *orcanitrofeed.SweepObserver, lo, hi uint64, skipped []uint64) {
	start := lo
	for _, b := range skipped { // 실행 순서상 오름차순
		if b > start {
			o.OnRangeDone(start, b-1)
		}
		start = b + 1
	}
	if start <= hi {
		o.OnRangeDone(start, hi)
	}
}

// reExecuteHistoricBlock — 블록 하나를 재실행하고 receipts root·gas 정합을 검증한다.
// statedb가 nil이 아니면 (직전 블록의 post-state) 재사용하고, nil이면 부모 root에서 연다.
// 반환값: 다음 블록에 물려줄 post-state (skip 시 nil — fresh open), skip 여부.
func (s *BlocksReExecutor) reExecuteHistoricBlock(block *types.Block, statedb *state.StateDB, orcaObserver *orcanitrofeed.SweepObserver) (*state.StateDB, bool, error) {
	blockNum := block.NumberU64()
	if statedb == nil {
		parent := s.blockchain.GetHeader(block.ParentHash(), blockNum-1)
		if parent == nil {
			return nil, false, fmt.Errorf("parent header of block %d not found", blockNum)
		}
		// 최근 블록은 live pathdb(diff layer·persistent)에서, 오래된 블록은 state history
		// freezer(HistoricReader)에서 읽는다.
		var err error
		statedb, err = s.blockchain.StateAt(parent.Root)
		if err != nil {
			statedb, err = state.New(parent.Root, s.historicDB)
		}
		if err != nil {
			return nil, false, fmt.Errorf("historic state at block %d (root %v): %w", blockNum-1, parent.Root, err)
		}
	}
	vmConfig := vm.Config{ExposeMultiGas: s.config.ValidateMultiGas}
	if orcaObserver != nil {
		vmConfig.Tracer = orcaObserver.Hooks()
	}
	result, err := s.blockchain.Processor().Process(block, statedb, vmConfig)
	if err != nil {
		return nil, false, fmt.Errorf("processing block %d: %w", blockNum, err)
	}
	receipts := result.Receipts
	if got := types.DeriveSha(receipts, trie.NewStackTrie(nil)); got != block.ReceiptHash() {
		if s.config.SkipOnMismatch {
			s.recordMismatch(blockNum, "receipts-root", got.Hex(), block.ReceiptHash().Hex())
			if orcaObserver != nil {
				orcaObserver.OnBlockSkipped(block)
			}
			// 불일치 실행의 post-state는 신뢰 불가 — 다음 블록은 fresh open
			return nil, true, nil
		}
		return nil, false, fmt.Errorf("receipts root mismatch at block %d: got %v want %v", blockNum, got, block.ReceiptHash())
	}
	if result.GasUsed != block.GasUsed() {
		if s.config.SkipOnMismatch {
			s.recordMismatch(blockNum, "gas-used", fmt.Sprintf("%d", result.GasUsed), fmt.Sprintf("%d", block.GasUsed()))
			if orcaObserver != nil {
				orcaObserver.OnBlockSkipped(block)
			}
			return nil, true, nil
		}
		return nil, false, fmt.Errorf("gas used mismatch at block %d: got %d want %d", blockNum, result.GasUsed, block.GasUsed())
	}
	if orcaObserver != nil {
		orcaObserver.OnBlockExecuted(block, receipts, statedb)
	}
	return statedb, false, nil
}

// implAscending — path 모드 chunk 스케줄러. (startBlock, endBlock]을 chunkSize 단위
// 오름차순으로 배분하는 풀 큐 — worker가 비는 즉시 다음 chunk를 떼어 준다. 낮은
// 블록부터 소진해야 consumer의 contiguous progress(중단 후 재개 지점)가 부드럽게
// 전진한다. launch는 (lo, hi] chunk를 비동기 실행하고 완료 시 s.done에 정확히 한 번
// 신호해야 한다.
func (s *BlocksReExecutor) implAscending(ctx context.Context, startBlock, endBlock, chunkSize uint64, launch func(lo, hi uint64)) {
	cursor := startBlock
	inflight := 0
	next := func() {
		hi := min(cursor+chunkSize, endBlock)
		launch(cursor, hi)
		cursor = hi
		inflight++
	}
	for inflight < s.config.Room && cursor < endBlock {
		if s.fatalReported.Load() || ctx.Err() != nil {
			break
		}
		next()
	}
	for inflight > 0 {
		<-s.done
		inflight--
		if !s.fatalReported.Load() && ctx.Err() == nil && cursor < endBlock {
			next()
		}
	}
}

// waitForStateIndexing — path archive 복원 직후 state history 인덱싱이 진행 중이면
// historic read가 전부 "not fully indexed"로 실패한다. 완료까지 폴링 대기한다.
// remaining==0은 indexer의 done 채널 기준이라 (pathdb indexIniter.remain) inited와
// race가 없다. index metadata가 freezer tip보다 앞선 복구 상태면 remaining이 0으로
// 내려오지 않는다 — geth가 "State indexer is in recovery"를 남기며, 스냅샷 재복원이
// 올바른 대응이다.
func (s *BlocksReExecutor) waitForStateIndexing(ctx context.Context) error {
	for {
		remaining, err := s.blockchain.StateIndexProgress()
		if err != nil {
			return fmt.Errorf("state index progress: %w", err)
		}
		if remaining == 0 {
			return nil
		}
		log.Info("Waiting for state history indexing before historic reexecution", "remaining", remaining)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Second):
		}
	}
}

// runAscending — path 모드 전체 실행. range들을 시작 블록 오름차순으로 처리하고,
// 겹치는 range는 이미 커버한 상한(highestCovered) 이후만 실행한다.
func (s *BlocksReExecutor) runAscending(ctx context.Context) {
	if err := s.waitForStateIndexing(ctx); err != nil {
		if ctx.Err() == nil {
			s.reportFatalErr(fmt.Errorf("blocksReExecutor waiting for state indexing: %w", err))
		}
		return
	}
	var highestCovered uint64
	for _, blocks := range s.blocks {
		if s.fatalReported.Load() || ctx.Err() != nil {
			return
		}
		lo := max(blocks[0], highestCovered)
		if lo >= blocks[1] {
			log.Info("BlocksReExecutor range already covered by previous ranges", "startBlock", blocks[0]+1, "endBlock", blocks[1])
			continue
		}
		s.implAscending(ctx, lo, blocks[1], blocks[2], func(chunkLo, chunkHi uint64) {
			s.launchHistoricChunk(ctx, chunkLo, chunkHi)
		})
		if s.fatalReported.Load() || ctx.Err() != nil {
			return
		}
		log.Info("BlocksReExecutor successfully completed re-execution of blocks against historic state", "startBlock", lo+1, "endBlock", blocks[1])
		highestCovered = blocks[1]
	}
	if skipped := s.skippedCount.Load(); skipped > 0 {
		log.Warn("BlocksReExecutor skipped mismatched blocks — recover them with orca-band-patch", "count", skipped, "report", s.config.MismatchReport)
	}
}

func (s *BlocksReExecutor) Impl(ctx context.Context, startBlock, currentBlock, minBlocksPerThread uint64) uint64 {
	var threadsLaunched uint64
	end := currentBlock
	for i := 0; i < s.config.Room && currentBlock > startBlock; i++ {
		if s.fatalReported.Load() {
			break
		}
		threadsLaunched++
		currentBlock = s.LaunchBlocksReExecution(ctx, startBlock, currentBlock, minBlocksPerThread)
	}
	// Wait for all launched threads to complete, launching new work as threads
	// finish unless a fatal error has been reported or the context is cancelled.
	// No special drain path is needed: launched goroutines respect ctx and will
	// finish promptly on cancellation, so the loop naturally drains.
	for threadsLaunched > 0 {
		<-s.done
		threadsLaunched--
		if !s.fatalReported.Load() && ctx.Err() == nil && currentBlock > startBlock {
			threadsLaunched++
			currentBlock = s.LaunchBlocksReExecution(ctx, startBlock, currentBlock, minBlocksPerThread)
		}
	}
	if s.fatalReported.Load() || ctx.Err() != nil {
		return 0
	}
	log.Info("BlocksReExecutor successfully completed re-execution of blocks against historic state", "stateAt", startBlock, "startBlock", startBlock+1, "endBlock", end)
	return currentBlock
}

func (s *BlocksReExecutor) Start(ctx context.Context) {
	s.StopWaiter.Start(ctx, s)
	s.LaunchThread(func(ctx context.Context) {
		if s.pathMode {
			s.runAscending(ctx)
		} else {
			s.runDescending(ctx)
		}
		if s.success != nil && !s.fatalReported.Load() && ctx.Err() == nil {
			close(s.success)
		}
	})
}

// runDescending — hash 모드 전체 실행 (상태 가용성 조정 때문에 내림차순 유지).
func (s *BlocksReExecutor) runDescending(ctx context.Context) {
	// Using returned value from Impl we can avoid duplicate reexecution of blocks
	// lowestBlockNotReExecuted represents the block after which either all the blocks have already been reexecuted or not in scope of reexecution
	lowestBlockNotReExecuted := s.blocks[0][1] + 1
	for _, blocks := range s.blocks {
		if s.fatalReported.Load() || ctx.Err() != nil {
			break
		}
		if lowestBlockNotReExecuted > blocks[0] {
			lowestBlockNotReExecuted = s.Impl(ctx, blocks[0], min(lowestBlockNotReExecuted, blocks[1]), blocks[2])
		} else {
			log.Info("BlocksReExecutor successfully completed re-execution of blocks against historic state", "stateAt", blocks[0], "startBlock", blocks[0]+1, "endBlock", blocks[1])
		}
	}
}

func (s *BlocksReExecutor) WaitForReExecution(ctx context.Context) error {
	select {
	case err := <-s.fatalErrChan:
		return s.wrapFatalErr(err)
	case <-s.success:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *BlocksReExecutor) wrapFatalErr(err error) error {
	return fmt.Errorf("shutting BlocksReExecutor down due to fatal error: %w", err)
}

func (s *BlocksReExecutor) dereferenceRoot(root common.Hash) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	_ = s.db.TrieDB().Dereference(root)
}

func (s *BlocksReExecutor) commitStateAndVerify(statedb *state.StateDB, expected common.Hash, blockNumber uint64) (*state.StateDB, arbitrum.StateReleaseFunc, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	result, err := statedb.Commit(blockNumber, true, false)
	if err != nil {
		return nil, arbitrum.NoopStateRelease, err
	}
	if result != expected {
		return nil, arbitrum.NoopStateRelease, fmt.Errorf("bad root hash expected: %v got: %v", expected, result)
	}

	if s.config.CommitStateToDisk {
		err = s.db.TrieDB().Commit(expected, false)
		if err != nil {
			return nil, arbitrum.NoopStateRelease, fmt.Errorf("trieDB commit failed in commitStateAndVerify, number %d root %v: %w", blockNumber, expected, err)
		}
	}

	sdb, err := state.New(result, s.db)
	if err == nil {
		_ = s.db.TrieDB().Reference(result, common.Hash{})
		return sdb, func() { s.dereferenceRoot(result) }, nil
	}
	return sdb, arbitrum.NoopStateRelease, err
}

func (s *BlocksReExecutor) advanceStateUpToBlock(ctx context.Context, state *state.StateDB, targetHeader *types.Header, lastAvailableHeader *types.Header, lastRelease arbitrum.StateReleaseFunc, orcaObserver *orcanitrofeed.SweepObserver) error {
	targetBlockNumber := targetHeader.Number.Uint64()
	blockToRecreate := lastAvailableHeader.Number.Uint64() + 1
	prevHash := lastAvailableHeader.Hash()
	var stateRelease arbitrum.StateReleaseFunc
	defer func() {
		lastRelease()
	}()
	var block *types.Block
	var err error
	vmConfig := vm.Config{
		ExposeMultiGas: s.config.ValidateMultiGas,
	}
	if orcaObserver != nil {
		vmConfig.Tracer = orcaObserver.Hooks()
	}
	for ctx.Err() == nil {
		var receipts types.Receipts
		// Recover from panics in AdvanceStateByBlock caused by trie-cache
		// eviction races: one goroutine dereferences a root (dropping its
		// refcount to zero and allowing eviction) while another goroutine
		// is still traversing shared nodes under a different root.
		func() {
			defer func() {
				if r := recover(); r != nil {
					log.Error("panic during block re-execution", "block", blockToRecreate, "recover", r, "stack", string(debug.Stack()))
					state = nil
					err = fmt.Errorf("panic during block re-execution at block %d: %v", blockToRecreate, r)
				}
			}()
			state, block, receipts, err = arbitrum.AdvanceStateByBlock(ctx, s.blockchain, state, blockToRecreate, prevHash, nil, vmConfig)
		}()
		if err != nil {
			return err
		}

		if vmConfig.ExposeMultiGas {
			for _, receipt := range receipts {
				if receipt.GasUsed != receipt.MultiGasUsed.SingleGas() {
					return fmt.Errorf("multi-dimensional gas mismatch in block %d, txHash %s: gasUsed=%d, multiGasUsed=%d",
						block.NumberU64(), receipt.TxHash, receipt.GasUsed, receipt.MultiGasUsed.SingleGas())
				}
			}
		}

		if orcaObserver != nil {
			orcaObserver.OnBlockExecuted(block, receipts, state)
		}

		prevHash = block.Hash()
		state, stateRelease, err = s.commitStateAndVerify(state, block.Root(), block.NumberU64())
		if err != nil {
			return fmt.Errorf("failed committing state for block %d : %w", blockToRecreate, err)
		}
		lastRelease()
		lastRelease = stateRelease
		if blockToRecreate >= targetBlockNumber {
			if block.Hash() != targetHeader.Hash() {
				return fmt.Errorf("blockHash doesn't match when recreating number: %d expected: %v got: %v", blockToRecreate, targetHeader.Hash(), block.Hash())
			}
			return nil
		}
		blockToRecreate++
	}
	return ctx.Err()
}
