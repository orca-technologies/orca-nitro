package orcanitrofeed

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/types"
)

// SweepObserver — blocks_reexecutor의 과거 블록 재실행 경로용 observer.
// worker goroutine마다 독립 인스턴스를 만든다 (공유 상태 없음, sink만 공유).
//
// 실행 흐름: geth Process가 tx마다 OnTxStart/OnTxEnd를 발화 → OnTxEnd 콜백에서
// (tx, sender, transfers)를 블록 로컬로 축적 → 블록 실행 완료 시 OnBlockExecuted가
// receipts와 짝지어 ReceiptMsg를 조립·dispatch한다 (out-of-order 허용).
type SweepObserver struct {
	sink      Sink
	collector *Collector
	tracker   *SameTimestampTracker
	// dispatch 대상 범위 (config에서 받은 원본 [start, end] 목록).
	// 재실행은 pre-state 확보를 위해 range 이전 블록도 실행하므로 범위 밖은 dispatch하지 않는다.
	ranges [][2]uint64

	// 현재 블록의 tx별 수집분 (블록마다 clear)
	txCaps map[common.Hash]*txCapture
}

type txCapture struct {
	from         common.Address
	transfers    []TransferRecord
	logs         []LogRecord
	calls        []WhitelistedCallRecord
	revertOutput []byte
}

func NewSweepObserver(sink Sink, ranges [][2]uint64, lookback uint64, headerTime HeaderTimeFunc) *SweepObserver {
	o := &SweepObserver{
		sink:      sink,
		collector: NewCollector(),
		tracker:   NewSameTimestampTracker(lookback, headerTime),
		ranges:    ranges,
		txCaps:    make(map[common.Hash]*txCapture),
	}
	o.collector.SetTxEndCallback(func(tx *types.Transaction, from common.Address, _ *types.Receipt, transfers []TransferRecord) {
		if tx == nil {
			return
		}
		cp := &txCapture{from: from, transfers: nil, logs: nil, calls: nil, revertOutput: nil}
		if len(transfers) > 0 {
			cp.transfers = make([]TransferRecord, len(transfers))
			copy(cp.transfers, transfers)
		}
		if ls := o.collector.DrainLogs(); len(ls) > 0 {
			cp.logs = make([]LogRecord, len(ls))
			copy(cp.logs, ls)
		}
		if cs := o.collector.DrainCalls(); len(cs) > 0 {
			cp.calls = make([]WhitelistedCallRecord, len(cs))
			copy(cp.calls, cs)
		}
		cp.revertOutput = o.collector.DrainRevertOutput()
		o.txCaps[tx.Hash()] = cp
	})
	return o
}

// Hooks — 재실행 vm.Config.Tracer에 연결 (Process가 hooked statedb 래핑까지 처리).
func (o *SweepObserver) Hooks() *tracing.Hooks { return o.collector.Hooks() }

// OnBlockExecuted — AdvanceStateByBlock 성공 직후 호출. statedb는 블록 실행 후 상태.
func (o *SweepObserver) OnBlockExecuted(block *types.Block, receipts types.Receipts, statedb *state.StateDB) {
	defer clear(o.txCaps)
	blockNumber := block.NumberU64()
	index := o.tracker.Advance(blockNumber, block.Time())
	if !o.inRange(blockNumber) {
		return
	}
	codeCache := make(map[common.Address]uint8)
	l1BlockNumber := types.DeserializeHeaderExtraInformation(block.Header()).L1BlockNumber
	var receiptCount uint32
	for i, tx := range block.Transactions() {
		if tx.Type() == types.ArbitrumInternalTxType {
			continue
		}
		if i >= len(receipts) {
			break
		}
		var sender common.Address
		var transfers []TransferRecord
		var logs []LogRecord
		var calls []WhitelistedCallRecord
		var revertOutput []byte
		if cp, ok := o.txCaps[tx.Hash()]; ok {
			sender = cp.from
			transfers = cp.transfers
			logs = cp.logs
			calls = cp.calls
			revertOutput = cp.revertOutput
		}
		msg := NewReceiptMsg(blockNumber, block.Time(), l1BlockNumber, index, i, tx, sender, receipts[i], transfers, logs, calls, revertOutput,
			func(addr common.Address) uint8 { return accountKindCached(statedb, codeCache, addr) })
		o.sink.Enqueue(MsgReceipt, msg)
		receiptCount++
	}
	// #nosec G115
	txCount := uint32(len(block.Transactions()))
	o.sink.Enqueue(MsgBlockSeal, &BlockSealMsg{
		Seq:                0,
		BlockNumber:        blockNumber,
		TxCount:            txCount,
		SameTimestampIndex: index,
		ReceiptCount:       receiptCount,
		L1BlockNumber:      l1BlockNumber,
		L2Timestamp:        block.Time(),
	})
}

// OnBlockSkipped — mismatch로 dispatch를 생략한 블록 (skip-on-mismatch).
// SameTimestampIndex 연속성만 유지하고 수집분은 폐기한다 — 다음 블록 오염 방지.
func (o *SweepObserver) OnBlockSkipped(block *types.Block) {
	o.tracker.Advance(block.NumberU64(), block.Time())
	clear(o.txCaps)
}

// OnRangeDone — worker chunk [start, end] 재실행 완료 마커.
func (o *SweepObserver) OnRangeDone(start, end uint64) {
	o.sink.Enqueue(MsgRangeDone, &RangeDoneMsg{Seq: 0, StartBlock: start, EndBlock: end})
}

func (o *SweepObserver) inRange(block uint64) bool {
	for _, r := range o.ranges {
		if block >= r[0] && block <= r[1] {
			return true
		}
	}
	return false
}
