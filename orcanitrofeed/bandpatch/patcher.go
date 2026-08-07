package bandpatch

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/ethereum/go-ethereum/trie"

	"github.com/offchainlabs/nitro/orcanitrofeed"
)

// Patcher — 블록 목록을 외부 데이터로 복원해 기존 FEED consumer에 dispatch한다.
// Data는 블록·receipts·code·헤더(로컬 archive 권장), Trace는 debug tracer 지원
// 엔드포인트(Alchemy). 둘이 같은 클라이언트여도 된다.
type Patcher struct {
	Data     *rpc.Client
	Trace    *rpc.Client
	Sink     orcanitrofeed.Sink
	Lookback uint64

	limiter     *time.Ticker
	headerTimes map[uint64]uint64
	stats       PatchStats
}

// PatchStats — 실행 요약.
type PatchStats struct {
	Blocks       int
	Receipts     int
	DegradedTxs  int // post balance 검증 실패로 강등된 tx 수
	LogFallbacks int // 재구성 로그 수 불일치로 receipt 로그 폴백한 tx 수
}

// NewPatcher — rps는 RPC 호출 rate 상한 (Data·Trace 합산).
func NewPatcher(data, trace *rpc.Client, sink orcanitrofeed.Sink, lookback uint64, rps int) *Patcher {
	if rps <= 0 {
		rps = 8
	}
	return &Patcher{
		Data:        data,
		Trace:       trace,
		Sink:        sink,
		Lookback:    lookback,
		limiter:     time.NewTicker(time.Second / time.Duration(rps)),
		headerTimes: make(map[uint64]uint64),
		stats:       PatchStats{},
	}
}

func (p *Patcher) call(ctx context.Context, c *rpc.Client, result any, method string, args ...any) error {
	select {
	case <-p.limiter.C:
	case <-ctx.Done():
		return ctx.Err()
	}
	return c.CallContext(ctx, result, method, args...)
}

// traceResult — debug_traceBlockByNumber 항목.
type traceResult struct {
	TxHash common.Hash     `json:"txHash"`
	Result json.RawMessage `json:"result"`
	Error  string          `json:"error"`
}

// rpcTxEnvelope — full-tx 블록 응답에서 tx 본문과 from을 함께 취한다
// (ethclient는 from을 버리고 signer 재복원을 요구하므로 raw로 받는다).
type rpcTxEnvelope struct {
	raw  json.RawMessage
	tx   *types.Transaction
	from common.Address
}

func (p *Patcher) fetchHeader(ctx context.Context, n uint64) (*types.Header, error) {
	var head *types.Header
	if err := p.call(ctx, p.Data, &head, "eth_getBlockByNumber", hexutil.EncodeUint64(n), false); err != nil {
		return nil, err
	}
	if head == nil {
		return nil, fmt.Errorf("block %d not found", n)
	}
	p.headerTimes[n] = head.Time
	return head, nil
}

func (p *Patcher) headerTime(ctx context.Context) orcanitrofeed.HeaderTimeFunc {
	return func(n uint64) (uint64, bool) {
		if ts, ok := p.headerTimes[n]; ok {
			return ts, true
		}
		head, err := p.fetchHeader(ctx, n)
		if err != nil {
			return 0, false
		}
		return head.Time, true
	}
}

func (p *Patcher) fetchTxs(ctx context.Context, n uint64) ([]rpcTxEnvelope, error) {
	var raw struct {
		Transactions []json.RawMessage `json:"transactions"`
	}
	if err := p.call(ctx, p.Data, &raw, "eth_getBlockByNumber", hexutil.EncodeUint64(n), true); err != nil {
		return nil, err
	}
	envs := make([]rpcTxEnvelope, 0, len(raw.Transactions))
	for i, rawTx := range raw.Transactions {
		tx := new(types.Transaction)
		if err := tx.UnmarshalJSON(rawTx); err != nil {
			return nil, fmt.Errorf("block %d tx %d decode: %w", n, i, err)
		}
		var meta struct {
			From common.Address `json:"from"`
		}
		if err := json.Unmarshal(rawTx, &meta); err != nil {
			return nil, fmt.Errorf("block %d tx %d from: %w", n, i, err)
		}
		envs = append(envs, rpcTxEnvelope{raw: rawTx, tx: tx, from: meta.From})
	}
	return envs, nil
}

// accountKindFor — eth_getCode 기반 AccountKind (블록별 캐시).
func (p *Patcher) accountKindFor(ctx context.Context, n uint64, cache map[common.Address]uint8) func(common.Address) uint8 {
	blockArg := hexutil.EncodeUint64(n)
	return func(addr common.Address) uint8 {
		if v, ok := cache[addr]; ok {
			return v
		}
		var code hexutil.Bytes
		kind := orcanitrofeed.AccountKindEmpty
		if err := p.call(ctx, p.Data, &code, "eth_getCode", addr, blockArg); err != nil {
			log.Warn("bandpatch: eth_getCode failed, assuming empty", "addr", addr, "block", n, "err", err)
		} else if len(code) == 23 {
			if _, ok := types.ParseDelegation(code); ok {
				kind = orcanitrofeed.AccountKindEip7702
			} else {
				kind = orcanitrofeed.AccountKindContract
			}
		} else if len(code) > 0 {
			kind = orcanitrofeed.AccountKindContract
		}
		cache[addr] = kind
		return kind
	}
}

// PatchBlocks — 정렬·중복 제거 후 블록별 복원·dispatch. 연속 구간마다 RangeDone.
func (p *Patcher) PatchBlocks(ctx context.Context, nums []uint64) (PatchStats, error) {
	nums = slices.Clone(nums)
	slices.Sort(nums)
	nums = slices.Compact(nums)
	rangeStart := uint64(0)
	prev := uint64(0)
	flushRange := func() {
		if rangeStart != 0 {
			p.Sink.Enqueue(orcanitrofeed.MsgRangeDone, &orcanitrofeed.RangeDoneMsg{Seq: 0, StartBlock: rangeStart, EndBlock: prev})
		}
	}
	for i, n := range nums {
		if err := p.patchBlock(ctx, n); err != nil {
			flushRange()
			return p.stats, fmt.Errorf("patching block %d: %w", n, err)
		}
		if rangeStart == 0 || n != prev+1 {
			flushRange()
			rangeStart = n
		}
		prev = n
		p.stats.Blocks++
		if (i+1)%100 == 0 {
			log.Info("bandpatch progress", "done", i+1, "total", len(nums), "receipts", p.stats.Receipts)
		}
	}
	flushRange()
	return p.stats, nil
}

func (p *Patcher) patchBlock(ctx context.Context, n uint64) error {
	head, err := p.fetchHeader(ctx, n)
	if err != nil {
		return err
	}
	txs, err := p.fetchTxs(ctx, n)
	if err != nil {
		return err
	}
	var receipts types.Receipts
	if err := p.call(ctx, p.Data, &receipts, "eth_getBlockReceipts", hexutil.EncodeUint64(n)); err != nil {
		return fmt.Errorf("receipts: %w", err)
	}
	if len(receipts) != len(txs) {
		return fmt.Errorf("receipts/txs count mismatch: %d vs %d", len(receipts), len(txs))
	}
	// canonical 무결성: JSON 왕복 손실이 있으면 여기서 경고가 뜬다 (진행은 계속)
	if got := types.DeriveSha(receipts, trie.NewStackTrie(nil)); got != head.ReceiptHash {
		log.Warn("bandpatch: fetched receipts do not re-derive header receiptsRoot (json round-trip loss?)",
			"block", n, "got", got, "want", head.ReceiptHash)
	}
	var traces []traceResult
	if err := p.call(ctx, p.Trace, &traces, "debug_traceBlockByNumber", hexutil.EncodeUint64(n),
		map[string]any{"tracer": "callTracer", "tracerConfig": map[string]any{"withLog": true}}); err != nil {
		return fmt.Errorf("callTracer: %w", err)
	}
	var prestates []traceResult
	if err := p.call(ctx, p.Trace, &prestates, "debug_traceBlockByNumber", hexutil.EncodeUint64(n),
		map[string]any{"tracer": "prestateTracer", "tracerConfig": map[string]any{"diffMode": true}}); err != nil {
		return fmt.Errorf("prestateTracer: %w", err)
	}
	if len(traces) != len(txs) || len(prestates) != len(txs) {
		return fmt.Errorf("trace count mismatch: call=%d prestate=%d txs=%d", len(traces), len(prestates), len(txs))
	}

	extra := types.DeserializeHeaderExtraInformation(head)
	sameTsIdx := orcanitrofeed.WalkSameTimestampIndex(n, head.Time, p.Lookback, p.headerTime(ctx))
	kindCache := make(map[common.Address]uint8)
	accountKind := p.accountKindFor(ctx, n, kindCache)

	var receiptCount uint32
	for i, env := range txs {
		if env.tx.Type() == types.ArbitrumInternalTxType {
			continue
		}
		var frame CallFrame
		if traces[i].Error != "" {
			return fmt.Errorf("tx %d trace error: %s", i, traces[i].Error)
		}
		if err := json.Unmarshal(traces[i].Result, &frame); err != nil {
			return fmt.Errorf("tx %d callTracer decode: %w", i, err)
		}
		var diff PrestateDiff
		if prestates[i].Error == "" && len(prestates[i].Result) > 0 {
			if err := json.Unmarshal(prestates[i].Result, &diff); err != nil {
				return fmt.Errorf("tx %d prestate decode: %w", i, err)
			}
		}
		transfers, logs, calls, degraded := ReconstructTx(&frame, &diff, env.from, head.Coinbase)
		if degraded {
			p.stats.DegradedTxs++
			log.Warn("bandpatch: post-balance validation failed, balances degraded to nil", "block", n, "tx", i)
		}
		if len(logs) != len(receipts[i].Logs) {
			p.stats.LogFallbacks++
		}
		msg := orcanitrofeed.NewReceiptMsg(n, head.Time, extra.L1BlockNumber, sameTsIdx, i,
			env.tx, env.from, receipts[i], transfers, logs, calls, accountKind)
		p.Sink.Enqueue(orcanitrofeed.MsgReceipt, msg)
		receiptCount++
		p.stats.Receipts++
	}
	// #nosec G115
	p.Sink.Enqueue(orcanitrofeed.MsgBlockSeal, &orcanitrofeed.BlockSealMsg{
		Seq:                0,
		BlockNumber:        n,
		TxCount:            uint32(len(txs)),
		SameTimestampIndex: sameTsIdx,
		ReceiptCount:       receiptCount,
		L1BlockNumber:      extra.L1BlockNumber,
	})
	return nil
}
