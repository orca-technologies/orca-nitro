package orcafeed

import (
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/types"
)

// Collector는 한 tx 실행 동안 발화하는 tracing 훅에서 TransferRecord를 수집한다.
// OnOpcode를 등록하지 않으므로 EVM 인터프리터 핫루프에는 영향이 없다.
// 동시성 없음 — 실행 goroutine 전용 (블록 생성은 단일 goroutine).
type Collector struct {
	hooks *tracing.Hooks

	// 현재 tx 수집 상태
	// PERF:MEM-GROW
	//   cost: mem=O(N_transfers)·~120B, N~1e0..1e2/tx → 재사용으로 상쇄
	//   note: records는 Drain 후 재사용 (capacity 유지)
	records []TransferRecord
	frames  []frameMark // open frame 스택
	pending *pendingSub // 쌍 대기 중인 Transfer sub 이벤트
	txFrom  common.Address
	tx      *types.Transaction

	// sweep 경로: OnTxEnd(receipt) 시점 콜백 — receipt와 수집분이 정렬돼 전달된다
	onTxEnd func(tx *types.Transaction, from common.Address, receipt *types.Receipt, transfers []TransferRecord)
}

type frameMark struct {
	depth    uint16
	startIdx int // 이 frame 진입 시점의 records 길이
}

type pendingSub struct {
	addr common.Address
	// value = prev-new (양수)
	value       *big.Int
	postBalance *big.Int
}

// 수집 대상 reason. 여기 없는 reason(gas·fee·refund·bookkeeping)은 무시한다.
// 정책 조정은 이 테이블만 수정한다.
var includedReasons = map[tracing.BalanceChangeReason]bool{
	tracing.BalanceChangeTransfer:           true,
	tracing.BalanceIncreaseSelfdestruct:     true,
	tracing.BalanceDecreaseSelfdestruct:     true,
	tracing.BalanceDecreaseSelfdestructBurn: true,
	tracing.BalanceChangeDuringEVMExecution: true,
	tracing.BalanceIncreaseDeposit:          true,
	tracing.BalanceDecreaseWithdrawToL1:     true,
	tracing.BalanceChangeEscrowTransfer:     true,
	tracing.BalanceIncreaseMintNativeToken:  true,
	tracing.BalanceDecreaseBurnNativeToken:  true,
}

func NewCollector() *Collector {
	c := &Collector{
		hooks:   nil,
		records: nil,
		frames:  nil,
		pending: nil,
		txFrom:  common.Address{},
		tx:      nil,
		onTxEnd: nil,
	}
	c.hooks = &tracing.Hooks{
		OnTxStart:       c.onTxStartHook,
		OnTxEnd:         c.onTxEndHook,
		OnEnter:         c.onEnter,
		OnExit:          c.onExit,
		OnBalanceChange: c.onBalanceChange,
	}
	return c
}

// SetTxEndCallback은 sweep 경로에서 tx별 (receipt, transfers) 정렬 콜백을 등록한다.
// transfers 슬라이스는 콜백 리턴 후 재사용된다 — 보관하려면 복사할 것.
func (c *Collector) SetTxEndCallback(cb func(tx *types.Transaction, from common.Address, receipt *types.Receipt, transfers []TransferRecord)) {
	c.onTxEnd = cb
}

func (c *Collector) Hooks() *tracing.Hooks { return c.hooks }

// Drain은 현재 tx 수집분을 반환하고 내부 버퍼를 재사용 모드로 되돌린다.
// 반환 슬라이스는 다음 tx 수집 시작(OnTxStart/Reset) 전까지만 유효 — 이후 재사용된다.
func (c *Collector) Drain() []TransferRecord {
	c.flushPending()
	return c.records
}

// TxSender는 OnTxStart에서 캡처한 sender를 반환한다 (signer 복원 재계산 회피).
func (c *Collector) TxSender() common.Address { return c.txFrom }

func (c *Collector) Reset() {
	c.records = c.records[:0]
	c.frames = c.frames[:0]
	c.pending = nil
}

func (c *Collector) onTxStartHook(_ *tracing.VMContext, tx *types.Transaction, from common.Address) {
	c.Reset()
	c.tx = tx
	c.txFrom = from
}

func (c *Collector) onTxEndHook(receipt *types.Receipt, err error) {
	if c.onTxEnd == nil {
		return
	}
	if err != nil || receipt == nil {
		c.Reset()
		return
	}
	c.flushPending()
	c.onTxEnd(c.tx, c.txFrom, receipt, c.records)
	c.Reset()
}

func (c *Collector) onEnter(depth int, _ byte, _ common.Address, _ common.Address, _ []byte, _ uint64, _ *big.Int) {
	c.flushPending()
	// #nosec G115
	c.frames = append(c.frames, frameMark{depth: uint16(depth), startIdx: len(c.records)})
}

func (c *Collector) onExit(_ int, _ []byte, _ uint64, _ error, reverted bool) {
	c.flushPending()
	if len(c.frames) == 0 {
		return
	}
	frame := c.frames[len(c.frames)-1]
	c.frames = c.frames[:len(c.frames)-1]
	if reverted {
		// 이 frame span 내 모든 record (더 깊은 frame 포함) revert 마킹
		for i := frame.startIdx; i < len(c.records); i++ {
			c.records[i].Reverted = true
		}
	}
}

func (c *Collector) currentDepth() uint16 {
	if len(c.frames) == 0 {
		return 0
	}
	return c.frames[len(c.frames)-1].depth
}

func (c *Collector) onBalanceChange(addr common.Address, prev, new *big.Int, reason tracing.BalanceChangeReason) {
	if !includedReasons[reason] {
		c.flushPending()
		return
	}
	diff := big.NewInt(0).Sub(new, prev)
	decrease := diff.Sign() < 0
	if decrease {
		diff.Neg(diff)
	}

	if reason == tracing.BalanceChangeTransfer {
		if decrease {
			// sub — 다음 add와 쌍 병합 대기
			c.flushPending()
			c.pending = &pendingSub{addr: addr, value: diff, postBalance: big.NewInt(0).Set(new)}
			return
		}
		if c.pending != nil && c.pending.value.Cmp(diff) == 0 {
			// sub→add 쌍 병합
			p := c.pending
			c.pending = nil
			c.append(TransferRecord{
				Reason:          uint8(reason),
				From:            p.addr,
				To:              addr,
				Value:           diff.Bytes(),
				PostBalanceFrom: p.postBalance.Bytes(),
				PostBalanceTo:   new.Bytes(),
				Depth:           c.currentDepth(),
				Reverted:        false,
			})
			return
		}
		c.flushPending()
	} else {
		c.flushPending()
	}

	// 단독 이벤트 (mint/burn/selfdestruct/짝 없는 transfer add)
	rec := TransferRecord{
		Reason:          uint8(reason),
		From:            common.Address{},
		To:              common.Address{},
		Value:           diff.Bytes(),
		PostBalanceFrom: nil,
		PostBalanceTo:   nil,
		Depth:           c.currentDepth(),
		Reverted:        false,
	}
	if decrease {
		rec.From = addr
		rec.PostBalanceFrom = new.Bytes()
	} else {
		rec.To = addr
		rec.PostBalanceTo = new.Bytes()
	}
	c.append(rec)
}

// flushPending — 쌍이 안 맞은 Transfer sub를 단독 record로 확정
func (c *Collector) flushPending() {
	if c.pending == nil {
		return
	}
	p := c.pending
	c.pending = nil
	c.append(TransferRecord{
		Reason:          uint8(tracing.BalanceChangeTransfer),
		From:            p.addr,
		To:              common.Address{},
		Value:           p.value.Bytes(),
		PostBalanceFrom: p.postBalance.Bytes(),
		PostBalanceTo:   nil,
		Depth:           c.currentDepth(),
		Reverted:        false,
	})
}

func (c *Collector) append(r TransferRecord) {
	c.records = append(c.records, r)
}
