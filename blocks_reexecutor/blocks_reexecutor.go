// Copyright 2024-2026, Offchain Labs, Inc.
// For license information, see https://github.com/OffchainLabs/nitro/blob/master/LICENSE.md
package blocksreexecutor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

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

	blocks: [][2]uint64{},
}

func ConfigAddOptions(prefix string, f *pflag.FlagSet) {
	f.Bool(prefix+".enable", DefaultConfig.Enable, "enables re-execution of a range of blocks against historic state")
	f.String(prefix+".mode", DefaultConfig.Mode, "mode to run the blocks-reexecutor on. Valid modes full and random. full - execute all the blocks in the given range. random - execute a random sample range of blocks with in a given range")
	f.String(prefix+".blocks", DefaultConfig.Blocks, "json encoded list of block ranges in the form of start and end block numbers in a list of size 2")
	f.Bool(prefix+".commit-state-to-disk", DefaultConfig.CommitStateToDisk, "if set, blocks-reexecutor not only re-executes blocks but it also commits their state to triedb")
	f.Int(prefix+".room", DefaultConfig.Room, "number of threads to parallelize blocks re-execution")
	f.Uint64(prefix+".min-blocks-per-thread", DefaultConfig.MinBlocksPerThread, "minimum number of blocks to execute per thread. When mode is random this acts as the size of random block range sample")
	f.Int(prefix+".trie-clean-limit", DefaultConfig.TrieCleanLimit, "memory allowance (MB) to use for caching trie nodes in memory")
	f.Bool(prefix+".validate-multigas", DefaultConfig.ValidateMultiGas, "if set, validate the sum of multi-gas dimensions match the single-gas")
}

// path 모드에서 statedb를 재사용하는 최대 블록 수 — dirty set 누적(메모리)과
// historic reader 재오픈 비용(시간)의 절충. 1000이면 재오픈 비용은 0.1%로 상각된다.
const historicReopenInterval = 1000

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
	orcaSink   orcanitrofeed.Sink
	orcaRanges [][2]uint64 // dispatch 대상 원본 범위 (pre-state용 start-- 이전 값)

	// path 스킴 archive (Titan 등): HistoricReader로 블록별 과거 state를 직접 열어
	// 실행한다 — hash 모드의 state 전진(FindLastAvailableState)이 불필요.
	pathMode   bool
	historicDB state.Database
}

// SetOrcaSink — Start 이전 1회 주입. 설정 시 재실행 블록의 receipt·transfer를 dispatch한다.
func (s *BlocksReExecutor) SetOrcaSink(sink orcanitrofeed.Sink) {
	s.orcaSink = sink
}

func New(c *Config, blockchain *core.BlockChain, ethDb ethdb.Database) (*BlocksReExecutor, error) {
	pathMode := blockchain.TrieDB().Scheme() == rawdb.PathScheme
	chainStart := blockchain.Config().ArbitrumChainParams.GenesisBlockNum
	chainEnd := blockchain.CurrentBlock().Number.Uint64()
	minBlocksPerThread := uint64(10000)
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
		// Divide work equally among available threads when MinBlocksPerThread is zero
		var work uint64
		if c.MinBlocksPerThread == 0 {
			// #nosec G115
			work = (end - start) / uint64(c.Room*2)
		}
		if work > 0 {
			blocks = append(blocks, [3]uint64{start, end, work})
		} else {
			blocks = append(blocks, [3]uint64{start, end, minBlocksPerThread})
		}
	}
	// We sort the block ranges in descending order of their endBlocks to avoid duplicate reexecution of blocks
	sort.Slice(blocks, func(i, j int) bool {
		return blocks[i][1] > blocks[j][1]
	})
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
	}
	return blocksReExecutor, nil
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
	if s.pathMode {
		return s.launchHistoricChunk(ctx, startBlock, currentBlock, minBlocksPerThread)
	}
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
		var orcaObserver *orcanitrofeed.SweepObserver
		if s.orcaSink != nil {
			orcaObserver = orcanitrofeed.NewSweepObserver(s.orcaSink, s.orcaRanges)
		}
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

// launchHistoricChunk — path archive용 chunk worker. (start, currentBlock] 블록들을
// HistoricReader 기반 state로 각각 독립 실행한다 (state 전진·release 관리 불필요).
func (s *BlocksReExecutor) launchHistoricChunk(ctx context.Context, startBlock, currentBlock, minBlocksPerThread uint64) uint64 {
	start := arbmath.SaturatingUSub(currentBlock, minBlocksPerThread)
	if start < startBlock {
		start = startBlock
	}
	s.LaunchThread(func(ctx context.Context) {
		defer func() { s.done <- struct{}{} }()
		var orcaObserver *orcanitrofeed.SweepObserver
		if s.orcaSink != nil {
			orcaObserver = orcanitrofeed.NewSweepObserver(s.orcaSink, s.orcaRanges)
		}
		log.Info("Starting historic reexecution of blocks", "startBlock", start+1, "endBlock", currentBlock)
		// statedb는 연속 블록에 걸쳐 재사용한다 — hash 경로의 AdvanceStateByBlock과
		// 동일 원리 (touch되지 않은 계정은 pinned reader가, 변경분은 in-memory가 답한다).
		// 블록마다 historic reader를 새로 여는 것이 path 모드의 지배적 비용이었다.
		var statedb *state.StateDB
		for n := start + 1; n <= currentBlock; n++ {
			if ctx.Err() != nil {
				return
			}
			// dirty set 누적을 막기 위해 주기적으로 재오픈 (비용은 1/reopenInterval로 상각)
			if statedb == nil || (n-start-1)%historicReopenInterval == 0 {
				statedb = nil
			}
			next, err := s.reExecuteHistoricBlock(n, statedb, orcaObserver)
			if err != nil {
				if ctx.Err() == nil {
					s.reportFatalErr(fmt.Errorf("blocksReExecutor historic reexecution failed at block %d: %w", n, err))
				}
				return
			}
			statedb = next
		}
		log.Info("Successfully reexecuted historic blocks", "startBlock", start+1, "endBlock", currentBlock)
		if orcaObserver != nil {
			orcaObserver.OnRangeDone(start+1, currentBlock)
		}
	})
	return start
}

// reExecuteHistoricBlock — 블록 하나를 재실행하고 receipts root·gas 정합을 검증한다.
// statedb가 nil이 아니면 (직전 블록의 post-state) 재사용하고, nil이면 부모 root에서 연다.
// 반환값은 다음 블록에 물려줄 post-state.
func (s *BlocksReExecutor) reExecuteHistoricBlock(blockNum uint64, statedb *state.StateDB, orcaObserver *orcanitrofeed.SweepObserver) (*state.StateDB, error) {
	block := s.blockchain.GetBlockByNumber(blockNum)
	if block == nil {
		return nil, fmt.Errorf("block %d not found", blockNum)
	}
	if statedb == nil {
		parent := s.blockchain.GetHeader(block.ParentHash(), blockNum-1)
		if parent == nil {
			return nil, fmt.Errorf("parent header of block %d not found", blockNum)
		}
		// 최근 블록은 live pathdb(diff layer·persistent)에서, 오래된 블록은 state history
		// freezer(HistoricReader)에서 읽는다.
		var err error
		statedb, err = s.blockchain.StateAt(parent.Root)
		if err != nil {
			statedb, err = state.New(parent.Root, s.historicDB)
		}
		if err != nil {
			return nil, fmt.Errorf("historic state at block %d (root %v): %w", blockNum-1, parent.Root, err)
		}
	}
	vmConfig := vm.Config{ExposeMultiGas: s.config.ValidateMultiGas}
	if orcaObserver != nil {
		vmConfig.Tracer = orcaObserver.Hooks()
	}
	result, err := s.blockchain.Processor().Process(block, statedb, vmConfig)
	if err != nil {
		return nil, fmt.Errorf("processing block %d: %w", blockNum, err)
	}
	receipts := result.Receipts
	if got := types.DeriveSha(receipts, trie.NewStackTrie(nil)); got != block.ReceiptHash() {
		return nil, fmt.Errorf("receipts root mismatch at block %d: got %v want %v", blockNum, got, block.ReceiptHash())
	}
	if result.GasUsed != block.GasUsed() {
		return nil, fmt.Errorf("gas used mismatch at block %d: got %d want %d", blockNum, result.GasUsed, block.GasUsed())
	}
	if orcaObserver != nil {
		orcaObserver.OnBlockExecuted(block, receipts, statedb)
	}
	return statedb, nil
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
		if s.success != nil && !s.fatalReported.Load() && ctx.Err() == nil {
			close(s.success)
		}
	})
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
