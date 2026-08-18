package orcanitrofeed

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
	//   cost: mem=O(N_transfers+N_logs+N_calls)·~120B, N~1e0..1e2/tx → 재사용으로 상쇄
	//   note: records·logs·calls는 Drain 후 재사용 (capacity 유지)
	records []TransferRecord
	logs    []LogRecord
	calls   []WhitelistedCallRecord
	// tx 안 방출 순번 — 로그·transfer·WhitelistedCallRecord가 공유한다. revert된 항목도
	// 번호를 소비하므로 살아남은 항목의 상대 순서가 보존된다.
	innerNext uint16
	frames    []frameMark // open frame 스택
	pending   *pendingSub // 쌍 대기 중인 Transfer sub 이벤트
	txFrom    common.Address
	tx        *types.Transaction
	// top-level frame revert return data (tx 실패 시), revertDataCap 캡.
	revertOutput []byte

	// sweep 경로: OnTxEnd(receipt) 시점 콜백 — receipt와 수집분이 정렬돼 전달된다
	onTxEnd func(tx *types.Transaction, from common.Address, receipt *types.Receipt, transfers []TransferRecord)
}

type frameMark struct {
	depth        uint16
	startIdx     int // 이 frame 진입 시점의 records 길이
	logStartIdx  int // 이 frame 진입 시점의 logs 길이
	callStartIdx int // 이 frame 진입 시점의 calls 길이
	selfCallIdx  int // 이 frame 자신이 남긴 call record 인덱스, 없으면 -1
}

// revert return data 캡 — 공격자 제어 무한 바이트 방지 (Error(string)·custom
// error 판별에는 수십 바이트면 충분).
const revertDataCap = 512

// CapRevertData — revert return data를 revertDataCap으로 잘라 소유 슬라이스로 복사한다
// (collector·bandpatch 공용).
func CapRevertData(output []byte) []byte {
	if len(output) == 0 {
		return nil
	}
	n := min(len(output), revertDataCap)
	out := make([]byte, n)
	copy(out, output[:n])
	return out
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
		hooks:        nil,
		records:      nil,
		logs:         nil,
		calls:        nil,
		innerNext:    0,
		frames:       nil,
		pending:      nil,
		txFrom:       common.Address{},
		tx:           nil,
		revertOutput: nil,
		onTxEnd:      nil,
	}
	c.hooks = &tracing.Hooks{
		OnTxStart:       c.onTxStartHook,
		OnTxEnd:         c.onTxEndHook,
		OnEnter:         c.onEnter,
		OnExit:          c.onExit,
		OnBalanceChange: c.onBalanceChange,
		OnLog:           c.onLog,
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
	c.logs = c.logs[:0]
	c.calls = c.calls[:0]
	c.innerNext = 0
	c.frames = c.frames[:0]
	c.pending = nil
	c.revertOutput = nil
}

// DrainLogs — revert되지 않은 로그를 방출 순서대로 반환한다.
// 반환 슬라이스는 다음 tx 수집 시작(Reset) 전까지만 유효 — 이후 재사용된다.
func (c *Collector) DrainLogs() []LogRecord {
	return c.logs
}

// DrainCalls — TARGET WhitelistedCallRecord (revert 플래그 포함)를 방출 순서대로 반환한다.
// 반환 슬라이스는 다음 tx 수집 시작(Reset) 전까지만 유효 — 이후 재사용된다.
func (c *Collector) DrainCalls() []WhitelistedCallRecord {
	return c.calls
}

// DrainRevertOutput — top-level frame revert return data (tx 실패 시), 없으면 nil.
// CapRevertData가 복사한 소유 슬라이스라 Reset 이후에도 안전하다.
func (c *Collector) DrainRevertOutput() []byte {
	return c.revertOutput
}

func (c *Collector) onLog(log *types.Log) {
	c.flushPending()
	topics := make([][32]byte, len(log.Topics))
	for i, t := range log.Topics {
		topics[i] = t
	}
	c.logs = append(c.logs, LogRecord{
		Address:    log.Address,
		Topics:     topics,
		Data:       log.Data,
		InnerIndex: c.nextInner(),
	})
}

// nextInner — 로그·transfer가 공유하는 tx-local 시퀀스. saturate로 wrap을 막는다.
func (c *Collector) nextInner() uint16 {
	i := c.innerNext
	if c.innerNext < ^uint16(0) {
		c.innerNext++
	}
	return i
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

func (c *Collector) onEnter(depth int, _ byte, _ common.Address, to common.Address, input []byte, _ uint64, value *big.Int) {
	c.flushPending()
	// #nosec G115
	c.frames = append(c.frames, frameMark{
		depth:        uint16(depth),
		startIdx:     len(c.records),
		logStartIdx:  len(c.logs),
		callStartIdx: len(c.calls),
		selfCallIdx:  -1,
	})
	if sel, ok := isTarget(to, input); ok {
		var valueBytes []byte
		if value != nil && value.Sign() != 0 {
			valueBytes = value.Bytes()
		}
		// PERF:ALLOC
		//   cost: mem=O(len(input))/TARGET hit, N_hits~0..few/tx → N 불확실
		//   note: TARGET input 복사 — tracing 버퍼 재사용 방지
		inputCopy := make([]byte, len(input))
		copy(inputCopy, input)
		c.frames[len(c.frames)-1].selfCallIdx = len(c.calls)
		// #nosec G115
		c.calls = append(c.calls, WhitelistedCallRecord{
			To:           to,
			Selector:     sel,
			Input:        inputCopy,
			Value:        valueBytes,
			Depth:        uint16(depth),
			Reverted:     false,
			InnerIndex:   c.nextInner(),
			RevertReason: nil,
		})
	}
}

func (c *Collector) onExit(_ int, output []byte, _ uint64, _ error, reverted bool) {
	c.flushPending()
	if len(c.frames) == 0 {
		return
	}
	frame := c.frames[len(c.frames)-1]
	c.frames = c.frames[:len(c.frames)-1]
	if frame.selfCallIdx >= 0 {
		// exit 시점의 inner 시퀀스 = exclusive 상한. revert 여부 무관 — 시도
		// frame의 span도 하위 Call 귀속 판정에 쓰인다.
		c.calls[frame.selfCallIdx].ExitInnerIndex = c.innerNext
	}
	if reverted {
		// transfer·WhitelistedCallRecord는 "시도됐다 무효화됨"이 신호가 되므로 플래그만 세운다.
		for i := frame.startIdx; i < len(c.records); i++ {
			c.records[i].Reverted = true
		}
		for i := frame.callStartIdx; i < len(c.calls); i++ {
			c.calls[i].Reverted = true
		}
		// 로그는 receipt.Logs에 남지 않으므로 버린다 — 시퀀스 번호는 이미 소비됐다.
		c.logs = c.logs[:frame.logStartIdx]
		// revert reason은 revert한 frame **자신**의 record에만 싣는다 (하위 전파
		// 금지 — 자식 record는 자기 onExit에서 자기 output을 받는다).
		if frame.selfCallIdx >= 0 {
			c.calls[frame.selfCallIdx].RevertReason = CapRevertData(output)
		}
		// 최상위 frame revert = tx 실패 — ReceiptMsg.RevertOutput.
		if len(c.frames) == 0 {
			c.revertOutput = CapRevertData(output)
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
	r.InnerIndex = c.nextInner()
	c.records = append(c.records, r)
}
