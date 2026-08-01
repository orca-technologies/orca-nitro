package orcanitrofeed

import (
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
)

// Sink는 dispatch 대상 — 프로덕션은 orcasock.Dispatcher, 테스트는 메모리 sink.
// dispatcher는 net 의존이라 하위 패키지로 분리돼 있다 (wasm replay 빌드 오염 방지).
type Sink interface {
	Enqueue(typ MsgType, msg SeqSetter)
}

// BlockObserver는 한 블록 생성 lifecycle 동안 arbos.ProduceBlockAdvanced가 호출하는
// 훅 묶음이다. 블록 생성은 단일 goroutine이므로 동시성 없음. nil observer = 무변경 경로.
type BlockObserver struct {
	sink      Sink
	mode      string // "tx" | "block"
	collector *Collector

	blockNumber   uint64
	l2Timestamp   uint64
	l1BlockNumber uint64

	// PERF:MEM-GROW
	//   cost: mem=O(N_addrs)/block, N~1e0..1e2 → 블록마다 clear
	//   note: AccountKind 캐시 (GetCodeSize·GetCode 중복 회피)
	codeCache map[common.Address]uint8

	// block 모드: seal까지 버퍼링
	pendingMsgs []*ReceiptMsg
}

func NewBlockObserver(sink Sink, mode string) *BlockObserver {
	return &BlockObserver{
		sink:          sink,
		mode:          mode,
		collector:     NewCollector(),
		blockNumber:   0,
		l2Timestamp:   0,
		l1BlockNumber: 0,
		codeCache:     make(map[common.Address]uint8),
		pendingMsgs:   nil,
	}
}

// BeginBlock — ProduceBlockAdvanced 초입에서 호출.
// l1BlockNumber는 이 L2 블록을 만든 L1 incoming message의 block number.
func (o *BlockObserver) BeginBlock(blockNumber uint64, l2Timestamp uint64, l1BlockNumber uint64) {
	o.blockNumber = blockNumber
	o.l2Timestamp = l2Timestamp
	o.l1BlockNumber = l1BlockNumber
	clear(o.codeCache)
	o.pendingMsgs = o.pendingMsgs[:0]
	o.collector.Reset()
}

// EVMHooks — per-tx EVM의 vm.Config.Tracer 및 hooked statedb에 연결할 훅.
func (o *BlockObserver) EVMHooks() *tracing.Hooks {
	return o.collector.Hooks()
}

// OnTxAccepted — tx가 블록에 확정 수록된 직후 (receipts append 직후) 호출.
// internal tx(ArbitrumInternalTxType)는 호출자가 걸러서 호출하지 않는다.
func (o *BlockObserver) OnTxAccepted(tx *types.Transaction, sender common.Address, receipt *types.Receipt, statedb *state.StateDB, txIndex int) {
	msg := o.buildReceiptMsg(tx, sender, receipt, statedb, txIndex)
	if o.mode == "tx" {
		o.sink.Enqueue(MsgReceipt, msg)
	} else {
		o.pendingMsgs = append(o.pendingMsgs, msg)
	}
	o.collector.Reset()
}

// OnBlockSealed — 블록 seal 직후 (ProduceBlockAdvanced 반환 직전) 호출.
func (o *BlockObserver) OnBlockSealed(block *types.Block) {
	if o.mode == "tx" {
		seal := &BlockSealMsg{
			Seq:         0,
			BlockNumber: block.NumberU64(),
			// #nosec G115
			TxCount: uint32(len(block.Transactions())),
		}
		o.sink.Enqueue(MsgBlockSeal, seal)
		return
	}
	for _, msg := range o.pendingMsgs {
		o.sink.Enqueue(MsgReceipt, msg)
	}
	o.pendingMsgs = o.pendingMsgs[:0]
}

// OnAppendFailed — appendBlock(DB commit) 실패 시 invalidation 통지.
func (o *BlockObserver) OnAppendFailed(blockNumber uint64) {
	o.sink.Enqueue(MsgInvalidation, &InvalidationMsg{Seq: 0, BlockNumber: blockNumber})
}

func (o *BlockObserver) buildReceiptMsg(tx *types.Transaction, sender common.Address, receipt *types.Receipt, statedb *state.StateDB, txIndex int) *ReceiptMsg {
	var transfers []TransferRecord
	if recs := o.collector.Drain(); len(recs) > 0 {
		// Drain 버퍼는 다음 tx에서 재사용되므로 복사 (내부 []byte는 이벤트별 신규 할당이라 공유 안전)
		transfers = make([]TransferRecord, len(recs))
		copy(transfers, recs)
	}
	var logs []LogRecord
	if ls := o.collector.DrainLogs(); len(ls) > 0 {
		logs = make([]LogRecord, len(ls))
		copy(logs, ls)
	}
	var calls []CallRecord
	if cs := o.collector.DrainCalls(); len(cs) > 0 {
		calls = make([]CallRecord, len(cs))
		copy(calls, cs)
	}
	return newReceiptMsg(o.blockNumber, o.l2Timestamp, o.l1BlockNumber, txIndex, tx, sender, receipt, transfers, logs, calls,
		func(addr common.Address) uint8 { return accountKindCached(statedb, o.codeCache, addr) })
}

// newReceiptMsg — live(BlockObserver)·sweep(SweepObserver) 공용 메시지 조립.
// transfers/calls는 호출자가 소유권을 넘긴 슬라이스여야 한다 (재사용 버퍼 금지).
func newReceiptMsg(blockNumber, l2Timestamp, l1BlockNumber uint64, txIndex int, tx *types.Transaction, sender common.Address, receipt *types.Receipt, transfers []TransferRecord, logs []LogRecord, calls []CallRecord, accountKind func(common.Address) uint8) *ReceiptMsg {
	// PERF:ALLOC
	//   cost: mem=O(1)·struct + O(N_logs+N_transfers+N_calls) 슬라이스/tx, N~1e0..1e2 → N 불확실
	//   note: msg는 writer가 비동기 직렬화하므로 tx-scope 버퍼 재사용 불가
	msg := &ReceiptMsg{
		Seq:         0,
		BlockNumber: blockNumber,
		// #nosec G115
		TxIndex:           uint32(txIndex),
		L2Timestamp:       l2Timestamp,
		TxType:            tx.Type(),
		From:              sender,
		To:                [20]byte{},
		FromAccountKind:   accountKind(sender),
		ToAccountKind:     AccountKindEmpty,
		ContractAddress:   receipt.ContractAddress,
		Nonce:             tx.Nonce(),
		Gas:               tx.Gas(),
		EffectiveGasPrice: nil,
		Value:             tx.Value().Bytes(),
		Calldata:          tx.Data(),
		Status:            receipt.Status,
		GasUsed:           receipt.GasUsed,
		CumulativeGasUsed: receipt.CumulativeGasUsed,
		GasUsedForL1:      receipt.GasUsedForL1,
		L1BlockNumber:     l1BlockNumber,
		Logs:              nil,
		Transfers:         transfers,
		Calls:             calls,
		// #nosec G115
		EmittedAtNs: uint64(time.Now().UnixNano()),
	}
	if to := tx.To(); to != nil {
		msg.To = *to
		msg.ToAccountKind = accountKind(*to)
	}
	if receipt.EffectiveGasPrice != nil {
		msg.EffectiveGasPrice = receipt.EffectiveGasPrice.Bytes()
	}
	// collector가 관측한 로그를 쓴다 (inner_index 보유). revert 필터링까지 끝난
	// 상태라 receipt.Logs와 개수가 같아야 한다 — 어긋나면 관측 누락이므로
	// 경고하고 receipt.Logs로 폴백한다 (순서 정보는 잃되 데이터는 지킨다).
	if len(logs) == len(receipt.Logs) {
		msg.Logs = logs
	} else {
		log.Warn("orca-nitro-feed: collector log count mismatch, falling back to receipt logs",
			"collector", len(logs), "receipt", len(receipt.Logs), "tx", tx.Hash())
		if len(receipt.Logs) > 0 {
			msg.Logs = make([]LogRecord, len(receipt.Logs))
			for i, l := range receipt.Logs {
				topics := make([][32]byte, len(l.Topics))
				for j, topic := range l.Topics {
					topics[j] = topic
				}
				msg.Logs[i] = LogRecord{Address: l.Address, Topics: topics, Data: l.Data, InnerIndex: 0}
			}
		}
	}
	return msg
}

// accountKindCached — Empty / Eip7702 / Contract. GetCode는 size==23일 때만.
func accountKindCached(statedb *state.StateDB, cache map[common.Address]uint8, addr common.Address) uint8 {
	if v, ok := cache[addr]; ok {
		return v
	}
	size := statedb.GetCodeSize(addr)
	var kind uint8
	switch {
	case size == 0:
		kind = AccountKindEmpty
	case size == 23:
		if _, ok := types.ParseDelegation(statedb.GetCode(addr)); ok {
			kind = AccountKindEip7702
		} else {
			kind = AccountKindContract
		}
	default:
		kind = AccountKindContract
	}
	cache[addr] = kind
	return kind
}
