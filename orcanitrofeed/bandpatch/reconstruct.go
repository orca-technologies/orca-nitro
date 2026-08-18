// Package bandpatch — 재실행이 불가능한 블록(예: receipts-root mismatch 대역)의
// FEED 레코드를 외부 trace(Alchemy debug_traceBlockByNumber)로 재구성한다.
//
// receipts·logs는 아카이브에 저장된 canonical 데이터를 그대로 쓰고, 재실행이
// 만들어내던 tracer 산출물(transfers·whitelisted calls)만 callTracer 결과에서
// 복원한다. net 의존이 있어 orcanitrofeed 본체와 분리한다 (wasm 빌드 오염 방지).
//
// 충실도 한계 (재실행 대비):
//   - InnerIndex는 순서 보존 합성값이다 — 실제 실행의 시퀀스 번호와 절대값이
//     다를 수 있다 (revert로 소비된 번호를 재현하지 않음).
//   - post balance는 prestate diff 기반 running 계산값이며, gas/fee 흐름이 섞인
//     주소(tx sender·coinbase 등)는 nil로 남긴다. per-tx 검증 실패 시 해당 tx의
//     post balance 전체를 nil로 강등한다.
package bandpatch

import (
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/tracing"

	"github.com/offchainlabs/nitro/orcanitrofeed"
)

// CallFrame — geth callTracer(withLog) 결과. Arbitrum 플레이버는 top-level frame에
// before/afterEVMTransfers(EVM 밖 잔액 이동: deposit·escrow 등)를 추가한다.
type CallFrame struct {
	Type               string          `json:"type"`
	From               common.Address  `json:"from"`
	To                 *common.Address `json:"to"`
	Value              *hexutil.Big    `json:"value"`
	Input              hexutil.Bytes   `json:"input"`
	// revert된 frame이면 revert return data (callTracer `output`).
	Output             hexutil.Bytes `json:"output"`
	Error              string        `json:"error"`
	Calls              []CallFrame   `json:"calls"`
	Logs               []FrameLog    `json:"logs"`
	BeforeEVMTransfers []ArbTransfer `json:"beforeEVMTransfers"`
	AfterEVMTransfers  []ArbTransfer `json:"afterEVMTransfers"`
}

// FrameLog — position은 "이 로그보다 앞서 실행된 subcall 수".
type FrameLog struct {
	Address  common.Address `json:"address"`
	Topics   []common.Hash  `json:"topics"`
	Data     hexutil.Bytes  `json:"data"`
	Position hexutil.Uint   `json:"position"`
}

// ArbTransfer — Arbitrum tracer의 EVM 밖 잔액 이동.
type ArbTransfer struct {
	Purpose string          `json:"purpose"`
	From    *common.Address `json:"from"`
	To      *common.Address `json:"to"`
	Value   hexutil.Big     `json:"value"`
}

// PrestateAccount — prestateTracer diffMode 계정 상태 (잔액만 사용).
type PrestateAccount struct {
	Balance *hexutil.Big `json:"balance"`
}

// PrestateDiff — prestateTracer diffMode 결과.
type PrestateDiff struct {
	Pre  map[common.Address]PrestateAccount `json:"pre"`
	Post map[common.Address]PrestateAccount `json:"post"`
}

// arbPurposeReason — collector includedReasons에 대응하는 purpose만 레코드로 만든다.
// gas·fee 계열 purpose는 의도적으로 제외 (collector 정책과 동일).
var arbPurposeReason = map[string]tracing.BalanceChangeReason{
	"deposit":  tracing.BalanceIncreaseDeposit,
	"escrow":   tracing.BalanceChangeEscrowTransfer,
	"unescrow": tracing.BalanceChangeEscrowTransfer,
	"withdraw": tracing.BalanceDecreaseWithdrawToL1,
	"mint":     tracing.BalanceIncreaseMintNativeToken,
	"burn":     tracing.BalanceDecreaseBurnNativeToken,
}

type balanceUndo struct {
	addr common.Address
	old  *big.Int // nil = 미상이었음
}

type txReconstructor struct {
	seq      uint16
	balances map[common.Address]*big.Int
	tainted  map[common.Address]bool // gas/fee 등 미추적 흐름이 섞인 주소
	// 우리가 transfer를 방출한 주소만 post 검증 대상이다 — fee 수취 등 미추적
	// 변화만 있는 주소(pre에 존재)까지 대조하면 모든 tx가 강등된다.
	touched   map[common.Address]bool
	journal   []balanceUndo
	transfers []orcanitrofeed.TransferRecord
	logs      []orcanitrofeed.LogRecord
	calls     []orcanitrofeed.WhitelistedCallRecord
}

// ReconstructTx — 한 tx의 callTracer frame + prestate diff에서 FEED 레코드를 복원한다.
// degraded=true면 post balance 검증이 실패해 이 tx의 post balance를 모두 비웠다.
// revertOutput은 top-level frame revert return data (tx 실패 시), 없으면 nil.
func ReconstructTx(frame *CallFrame, diff *PrestateDiff, txFrom, coinbase common.Address) (
	transfers []orcanitrofeed.TransferRecord, logs []orcanitrofeed.LogRecord,
	calls []orcanitrofeed.WhitelistedCallRecord, revertOutput []byte, degraded bool,
) {
	r := &txReconstructor{
		balances: make(map[common.Address]*big.Int),
		tainted:  map[common.Address]bool{txFrom: true, coinbase: true},
		touched:  make(map[common.Address]bool),
	}
	if diff != nil {
		for addr, acct := range diff.Pre {
			if acct.Balance != nil {
				r.balances[addr] = new(big.Int).Set(acct.Balance.ToInt())
			}
		}
	}
	for _, t := range frame.BeforeEVMTransfers {
		r.emitArbTransfer(t)
	}
	r.walkFrame(frame, 0)
	for _, t := range frame.AfterEVMTransfers {
		r.emitArbTransfer(t)
	}
	if frame.Error != "" {
		revertOutput = orcanitrofeed.CapRevertData(frame.Output)
	}
	degraded = r.validateAgainstPost(diff)
	return r.transfers, r.logs, r.calls, revertOutput, degraded
}

func (r *txReconstructor) next() uint16 {
	i := r.seq
	if r.seq < ^uint16(0) {
		r.seq++
	}
	return i
}

// apply — 잔액 변경 + undo journal. 미상(nil) 주소는 미상으로 유지된다.
func (r *txReconstructor) apply(addr common.Address, delta *big.Int) {
	r.touched[addr] = true
	old := r.balances[addr]
	r.journal = append(r.journal, balanceUndo{addr: addr, old: old})
	if old == nil {
		return
	}
	r.balances[addr] = new(big.Int).Add(old, delta)
}

func (r *txReconstructor) rollback(to int) {
	for i := len(r.journal) - 1; i >= to; i-- {
		u := r.journal[i]
		if u.old == nil {
			delete(r.balances, u.addr)
		} else {
			r.balances[u.addr] = u.old
		}
	}
	r.journal = r.journal[:to]
}

// balAfter — 방출 시점의 post balance. 오염됐거나 미상이면 nil.
func (r *txReconstructor) balAfter(addr common.Address) []byte {
	if r.tainted[addr] {
		return nil
	}
	b := r.balances[addr]
	if b == nil {
		return nil
	}
	return b.Bytes()
}

// emitArbTransfer — EVM 밖 이동. collector는 사이드별 OnBalanceChange를 각각
// standalone 레코드로 남기므로 여기서도 from/to를 분리 방출한다.
func (r *txReconstructor) emitArbTransfer(t ArbTransfer) {
	reason, ok := arbPurposeReason[t.Purpose]
	v := t.Value.ToInt()
	if !ok || v.Sign() == 0 {
		// 미추적 purpose(gas/fee 등): 관련 주소의 running 잔액은 신뢰 불가
		if t.From != nil {
			r.tainted[*t.From] = true
		}
		if t.To != nil {
			r.tainted[*t.To] = true
		}
		return
	}
	if t.From != nil {
		r.apply(*t.From, new(big.Int).Neg(v))
		r.transfers = append(r.transfers, orcanitrofeed.TransferRecord{
			Reason: uint8(reason), From: *t.From, To: common.Address{},
			Value: v.Bytes(), PostBalanceFrom: r.balAfter(*t.From), PostBalanceTo: nil,
			Depth: 0, Reverted: false, InnerIndex: r.next(),
		})
	}
	if t.To != nil {
		r.apply(*t.To, v)
		r.transfers = append(r.transfers, orcanitrofeed.TransferRecord{
			Reason: uint8(reason), From: common.Address{}, To: *t.To,
			Value: v.Bytes(), PostBalanceFrom: nil, PostBalanceTo: r.balAfter(*t.To),
			Depth: 0, Reverted: false, InnerIndex: r.next(),
		})
	}
}

func hasEntryValueTransfer(typ string) bool {
	switch typ {
	case "CALL", "CALLCODE", "CREATE", "CREATE2":
		return true
	}
	return false
}

// walkFrame — collector의 onEnter/onExit/onBalanceChange 순서를 모사한다:
// TARGET call record → entry value transfer → (position 인터리브로) 로그·자식 →
// revert 시 subtree 레코드 Reverted 마킹 + 로그 폐기 + 잔액 롤백.
func (r *txReconstructor) walkFrame(f *CallFrame, depth uint16) {
	tStart, cStart, lStart, jStart := len(r.transfers), len(r.calls), len(r.logs), len(r.journal)

	selfCallIdx := -1
	if f.To != nil {
		if sel, ok := orcanitrofeed.TargetCall(*f.To, f.Input); ok {
			var valueBytes []byte
			if f.Value != nil && f.Value.ToInt().Sign() != 0 {
				valueBytes = f.Value.ToInt().Bytes()
			}
			input := make([]byte, len(f.Input))
			copy(input, f.Input)
			selfCallIdx = len(r.calls)
			r.calls = append(r.calls, orcanitrofeed.WhitelistedCallRecord{
				To: *f.To, Selector: sel, Input: input, Value: valueBytes,
				Depth: depth, Reverted: false, InnerIndex: r.next(),
			})
		}
	}

	if v := frameValue(f); v != nil {
		switch {
		case f.Type == "SELFDESTRUCT":
			r.emitSelfdestruct(f, v, depth)
		case hasEntryValueTransfer(f.Type) && f.To != nil:
			r.apply(f.From, new(big.Int).Neg(v))
			postFrom := r.balAfter(f.From)
			r.apply(*f.To, v)
			r.transfers = append(r.transfers, orcanitrofeed.TransferRecord{
				Reason: uint8(tracing.BalanceChangeTransfer), From: f.From, To: *f.To,
				Value: v.Bytes(), PostBalanceFrom: postFrom, PostBalanceTo: r.balAfter(*f.To),
				Depth: depth, Reverted: false, InnerIndex: r.next(),
			})
		}
	}

	// position 인터리브: pos==i 로그는 i번째 자식 이전에 방출됐다
	logsByPos := make(map[int][]FrameLog, len(f.Logs))
	for _, l := range f.Logs {
		logsByPos[int(l.Position)] = append(logsByPos[int(l.Position)], l)
	}
	for i := 0; i <= len(f.Calls); i++ {
		for _, l := range logsByPos[i] {
			topics := make([][32]byte, len(l.Topics))
			for j, t := range l.Topics {
				topics[j] = t
			}
			r.logs = append(r.logs, orcanitrofeed.LogRecord{
				Address: l.Address, Topics: topics, Data: l.Data, InnerIndex: r.next(),
			})
		}
		if i < len(f.Calls) {
			r.walkFrame(&f.Calls[i], depth+1)
		}
	}

	if f.Error != "" {
		for i := tStart; i < len(r.transfers); i++ {
			r.transfers[i].Reverted = true
		}
		for i := cStart; i < len(r.calls); i++ {
			r.calls[i].Reverted = true
		}
		r.logs = r.logs[:lStart]
		// collector.onExit와 동일 — revert reason은 이 frame 자신의 record에만.
		if selfCallIdx >= 0 {
			r.calls[selfCallIdx].RevertReason = orcanitrofeed.CapRevertData(f.Output)
		}
		r.rollback(jStart)
	}
}

func frameValue(f *CallFrame) *big.Int {
	if f.Value == nil {
		return nil
	}
	v := f.Value.ToInt()
	if v.Sign() == 0 {
		return nil
	}
	return v
}

// emitSelfdestruct — collector는 사이드별 standalone 레코드를 남긴다.
// beneficiary == self는 burn 단일 레코드.
func (r *txReconstructor) emitSelfdestruct(f *CallFrame, v *big.Int, depth uint16) {
	if f.To != nil && *f.To == f.From {
		r.apply(f.From, new(big.Int).Neg(v))
		r.transfers = append(r.transfers, orcanitrofeed.TransferRecord{
			Reason: uint8(tracing.BalanceDecreaseSelfdestructBurn), From: f.From, To: common.Address{},
			Value: v.Bytes(), PostBalanceFrom: r.balAfter(f.From), PostBalanceTo: nil,
			Depth: depth, Reverted: false, InnerIndex: r.next(),
		})
		return
	}
	r.apply(f.From, new(big.Int).Neg(v))
	r.transfers = append(r.transfers, orcanitrofeed.TransferRecord{
		Reason: uint8(tracing.BalanceDecreaseSelfdestruct), From: f.From, To: common.Address{},
		Value: v.Bytes(), PostBalanceFrom: r.balAfter(f.From), PostBalanceTo: nil,
		Depth: depth, Reverted: false, InnerIndex: r.next(),
	})
	if f.To != nil {
		r.apply(*f.To, v)
		r.transfers = append(r.transfers, orcanitrofeed.TransferRecord{
			Reason: uint8(tracing.BalanceIncreaseSelfdestruct), From: common.Address{}, To: *f.To,
			Value: v.Bytes(), PostBalanceFrom: nil, PostBalanceTo: r.balAfter(*f.To),
			Depth: depth, Reverted: false, InnerIndex: r.next(),
		})
	}
}

// validateAgainstPost — transfer를 방출한 미오염·기지 주소의 running 결과를
// diff.Post와 대조. 하나라도 어긋나면 이 tx의 post balance 전부를 nil로 강등한다.
func (r *txReconstructor) validateAgainstPost(diff *PrestateDiff) bool {
	if diff == nil {
		return false
	}
	for addr := range r.touched {
		if r.tainted[addr] {
			continue
		}
		got := r.balances[addr]
		if got == nil {
			continue
		}
		want, ok := diff.Post[addr]
		if !ok || want.Balance == nil {
			continue
		}
		if got.Cmp(want.Balance.ToInt()) != 0 {
			for i := range r.transfers {
				r.transfers[i].PostBalanceFrom = nil
				r.transfers[i].PostBalanceTo = nil
			}
			return true
		}
	}
	return false
}
