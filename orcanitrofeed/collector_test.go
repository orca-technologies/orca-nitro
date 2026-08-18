package orcanitrofeed

import (
	"bytes"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
)

var (
	addrA = common.HexToAddress("0x0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a")
	addrB = common.HexToAddress("0x0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b")
)

func bal(c *Collector, addr common.Address, prev, next int64, reason tracing.BalanceChangeReason) {
	c.Hooks().OnBalanceChange(addr, big.NewInt(prev), big.NewInt(next), reason)
}

func TestCollectorTransferPairMerge(t *testing.T) {
	c := NewCollector()
	bal(c, addrA, 100, 70, tracing.BalanceChangeTransfer)
	bal(c, addrB, 0, 30, tracing.BalanceChangeTransfer)
	recs := c.Drain()
	if len(recs) != 1 {
		t.Fatalf("record 수: %d", len(recs))
	}
	r := recs[0]
	if r.From != addrA || r.To != addrB || big.NewInt(0).SetBytes(r.Value).Int64() != 30 {
		t.Fatalf("병합 실패: %+v", r)
	}
	if big.NewInt(0).SetBytes(r.PostBalanceFrom).Int64() != 70 || big.NewInt(0).SetBytes(r.PostBalanceTo).Int64() != 30 {
		t.Fatalf("post balance: %+v", r)
	}
	if r.Depth != 0 || r.Reverted {
		t.Fatalf("depth/reverted: %+v", r)
	}
}

func TestCollectorInternalDepthAndRevert(t *testing.T) {
	c := NewCollector()
	h := c.Hooks()

	// 성공한 depth-1 call
	h.OnEnter(1, byte(vm.CALL), addrA, addrB, nil, 0, big.NewInt(5))
	bal(c, addrA, 100, 95, tracing.BalanceChangeTransfer)
	bal(c, addrB, 0, 5, tracing.BalanceChangeTransfer)
	h.OnExit(1, nil, 0, nil, false)

	// revert된 depth-1 call (내부에 depth-2 성공 call 포함 — 조상 revert 전파)
	h.OnEnter(1, byte(vm.CALL), addrA, addrB, nil, 0, big.NewInt(7))
	bal(c, addrA, 95, 88, tracing.BalanceChangeTransfer)
	bal(c, addrB, 5, 12, tracing.BalanceChangeTransfer)
	h.OnEnter(2, byte(vm.CALL), addrB, addrA, nil, 0, big.NewInt(1))
	bal(c, addrB, 12, 11, tracing.BalanceChangeTransfer)
	bal(c, addrA, 88, 89, tracing.BalanceChangeTransfer)
	h.OnExit(2, nil, 0, nil, false)
	h.OnExit(1, nil, 0, nil, true)

	recs := c.Drain()
	if len(recs) != 3 {
		t.Fatalf("record 수: %d (%+v)", len(recs), recs)
	}
	if recs[0].Depth != 1 || recs[0].Reverted {
		t.Fatalf("성공 call: %+v", recs[0])
	}
	if recs[1].Depth != 1 || !recs[1].Reverted {
		t.Fatalf("revert call: %+v", recs[1])
	}
	if recs[2].Depth != 2 || !recs[2].Reverted {
		t.Fatalf("조상 revert 전파: %+v", recs[2])
	}
}

func TestCollectorMintBurnStandalone(t *testing.T) {
	c := NewCollector()
	bal(c, addrA, 0, 50, tracing.BalanceIncreaseDeposit)
	bal(c, addrA, 50, 20, tracing.BalanceDecreaseBurnNativeToken)
	recs := c.Drain()
	if len(recs) != 2 {
		t.Fatalf("record 수: %d", len(recs))
	}
	mint, burn := recs[0], recs[1]
	if mint.From != (common.Address{}) || mint.To != addrA || big.NewInt(0).SetBytes(mint.Value).Int64() != 50 {
		t.Fatalf("mint: %+v", mint)
	}
	if mint.PostBalanceFrom != nil || big.NewInt(0).SetBytes(mint.PostBalanceTo).Int64() != 50 {
		t.Fatalf("mint post balance: %+v", mint)
	}
	if burn.From != addrA || burn.To != (common.Address{}) || big.NewInt(0).SetBytes(burn.Value).Int64() != 30 {
		t.Fatalf("burn: %+v", burn)
	}
}

func TestCollectorIgnoresFeeReasons(t *testing.T) {
	c := NewCollector()
	bal(c, addrA, 100, 90, tracing.BalanceDecreaseGasBuy)
	bal(c, addrA, 90, 92, tracing.BalanceIncreaseGasReturn)
	bal(c, addrB, 0, 8, tracing.BalanceIncreaseNetworkFee)
	bal(c, addrA, 92, 91, tracing.BalanceIncreaseL1PosterFee)
	if recs := c.Drain(); len(recs) != 0 {
		t.Fatalf("fee reason이 수집됨: %+v", recs)
	}
}

func TestCollectorUnpairedTransferFlush(t *testing.T) {
	c := NewCollector()
	bal(c, addrA, 100, 70, tracing.BalanceChangeTransfer)
	bal(c, addrB, 0, 8, tracing.BalanceIncreaseNetworkFee) // 쌍이 아닌 이벤트가 끼어듦
	recs := c.Drain()
	if len(recs) != 1 {
		t.Fatalf("record 수: %d", len(recs))
	}
	if recs[0].From != addrA || recs[0].To != (common.Address{}) {
		t.Fatalf("단독 sub flush: %+v", recs[0])
	}
}

func TestCollectorTxBoundaryReset(t *testing.T) {
	c := NewCollector()
	h := c.Hooks()
	bal(c, addrA, 100, 70, tracing.BalanceChangeTransfer)
	bal(c, addrB, 0, 30, tracing.BalanceChangeTransfer)
	h.OnTxStart(nil, nil, addrA) // 새 tx 시작 → 이전 잔량 리셋
	if recs := c.Drain(); len(recs) != 0 {
		t.Fatalf("tx 경계 리셋 실패: %+v", recs)
	}
}

func TestCollectorSelfdestructPair(t *testing.T) {
	c := NewCollector()
	bal(c, addrB, 10, 50, tracing.BalanceIncreaseSelfdestruct)
	bal(c, addrA, 40, 0, tracing.BalanceDecreaseSelfdestruct)
	recs := c.Drain()
	if len(recs) != 2 {
		t.Fatalf("selfdestruct는 단독 record 2건: %+v", recs)
	}
	if recs[0].To != addrB || recs[1].From != addrA {
		t.Fatalf("방향: %+v", recs)
	}
	if recs[0].Reason != uint8(tracing.BalanceIncreaseSelfdestruct) {
		t.Fatalf("reason 보존: %+v", recs[0])
	}
}

func TestCollectorInterleavesLogsAndTransfers(t *testing.T) {
	c := NewCollector()
	h := c.Hooks()

	// tx 진입 시 top-level transfer → inner 0
	bal(c, addrA, 100, 70, tracing.BalanceChangeTransfer)
	bal(c, addrB, 0, 30, tracing.BalanceChangeTransfer)
	// 로그 하나 → inner 1
	h.OnLog(&types.Log{Address: addrB, Topics: []common.Hash{{0x11}}, Data: []byte{0x01}})
	// internal call 안에서 transfer → inner 2, 로그 → inner 3
	h.OnEnter(1, byte(vm.CALL), addrB, addrA, nil, 0, big.NewInt(5))
	bal(c, addrB, 30, 25, tracing.BalanceChangeTransfer)
	bal(c, addrA, 70, 75, tracing.BalanceChangeTransfer)
	h.OnLog(&types.Log{Address: addrA, Topics: []common.Hash{{0x22}}, Data: nil})
	h.OnExit(1, nil, 0, nil, false)

	transfers := c.Drain()
	logs := c.DrainLogs()
	if len(transfers) != 2 || len(logs) != 2 {
		t.Fatalf("수집 개수: transfers=%d logs=%d", len(transfers), len(logs))
	}
	if transfers[0].InnerIndex != 0 || logs[0].InnerIndex != 1 ||
		transfers[1].InnerIndex != 2 || logs[1].InnerIndex != 3 {
		t.Fatalf("interleave 순서: t0=%d l0=%d t1=%d l1=%d",
			transfers[0].InnerIndex, logs[0].InnerIndex,
			transfers[1].InnerIndex, logs[1].InnerIndex)
	}
	if logs[0].Address != addrB || logs[1].Address != addrA {
		t.Fatalf("로그 내용: %+v", logs)
	}
}

func TestCollectorDropsRevertedLogs(t *testing.T) {
	c := NewCollector()
	h := c.Hooks()

	h.OnLog(&types.Log{Address: addrA, Topics: nil, Data: nil}) // 살아남음
	h.OnEnter(1, byte(vm.CALL), addrA, addrB, nil, 0, big.NewInt(0))
	h.OnLog(&types.Log{Address: addrB, Topics: nil, Data: nil}) // revert됨
	h.OnExit(1, nil, 0, nil, true)

	logs := c.DrainLogs()
	if len(logs) != 1 || logs[0].Address != addrA {
		t.Fatalf("revert된 로그가 남음: %+v", logs)
	}
	// revert된 항목도 시퀀스 번호는 소비한다 — 살아남은 로그의 inner_index는 0
	if logs[0].InnerIndex != 0 {
		t.Fatalf("inner_index: %d", logs[0].InnerIndex)
	}
}

func TestCollectorResetClearsLogs(t *testing.T) {
	c := NewCollector()
	c.Hooks().OnLog(&types.Log{Address: addrA})
	c.Reset()
	if len(c.DrainLogs()) != 0 {
		t.Fatal("Reset 후 로그가 남음")
	}
	// 새 tx의 첫 이벤트는 다시 0
	c.Hooks().OnLog(&types.Log{Address: addrB})
	if l := c.DrainLogs(); len(l) != 1 || l[0].InnerIndex != 0 {
		t.Fatalf("inner_index 리셋 실패: %+v", l)
	}
}

func TestCollectorEmitsTargetCallRecord(t *testing.T) {
	c := NewCollector()
	h := c.Hooks()
	airlock := common.HexToAddress("0xeb7C034704eF8Dcd2D32324c1545f62fB4aD0862")
	input := []byte{0x88, 0x2d, 0xb7, 0x07, 0xde, 0xad}
	h.OnEnter(1, byte(vm.CALL), addrA, airlock, input, 0, big.NewInt(0))
	h.OnExit(1, nil, 0, nil, false)

	calls := c.DrainCalls()
	if len(calls) != 1 {
		t.Fatalf("WhitelistedCallRecord 수: %d", len(calls))
	}
	r := calls[0]
	if r.To != airlock || r.Selector != [4]byte{0x88, 0x2d, 0xb7, 0x07} {
		t.Fatalf("to/sel: %+v", r)
	}
	if string(r.Input) != string(input) || r.Depth != 1 || r.Reverted || r.InnerIndex != 0 {
		t.Fatalf("fields: %+v", r)
	}
}

func TestCollectorIgnoresNonTargetCalls(t *testing.T) {
	c := NewCollector()
	h := c.Hooks()
	h.OnEnter(1, byte(vm.CALL), addrA, addrB, []byte{0xde, 0xad, 0xbe, 0xef}, 0, nil)
	h.OnExit(1, nil, 0, nil, false)
	if calls := c.DrainCalls(); len(calls) != 0 {
		t.Fatalf("비타겟 WhitelistedCallRecord: %+v", calls)
	}
}

func TestCollectorEmitsPoolsTradeMulticallAndInnerCall(t *testing.T) {
	c := NewCollector()
	h := c.Hooks()
	launcher := common.HexToAddress("0x0000FffFBE8efE702c8703aE3477FF5dE3d319C0")
	outer := []byte{0xac, 0x96, 0x50, 0xd8, 0x11} // multicall(bytes[])
	inner := []byte{0xb6, 0x98, 0x2b, 0x48, 0x22} // distributeToken(...)

	// EOA가 launcher를 직접 호출 (depth 0) → multicall이 자기 자신에게 delegatecall (depth 1).
	// delegatecall의 to는 실행되는 코드 주소이므로 둘 다 launcher다.
	h.OnEnter(0, byte(vm.CALL), addrA, launcher, outer, 0, big.NewInt(0))
	h.OnEnter(1, byte(vm.DELEGATECALL), launcher, launcher, inner, 0, nil)
	h.OnExit(1, nil, 0, nil, false)
	h.OnExit(0, nil, 0, nil, false)

	calls := c.DrainCalls()
	if len(calls) != 2 {
		t.Fatalf("래퍼+inner 2건이어야 함: %d (%+v)", len(calls), calls)
	}
	if calls[0].To != launcher || calls[0].Selector != [4]byte{0xac, 0x96, 0x50, 0xd8} || calls[0].Depth != 0 {
		t.Fatalf("multicall 래퍼: %+v", calls[0])
	}
	if calls[1].To != launcher || calls[1].Selector != [4]byte{0xb6, 0x98, 0x2b, 0x48} || calls[1].Depth != 1 {
		t.Fatalf("delegatecall inner distributeToken: %+v", calls[1])
	}
	if string(calls[1].Input) != string(inner) {
		t.Fatalf("inner input 보존: %+v", calls[1])
	}
	if calls[0].InnerIndex != 0 || calls[1].InnerIndex != 1 {
		t.Fatalf("inner_index 순서: %d %d", calls[0].InnerIndex, calls[1].InnerIndex)
	}
}

func TestCollectorPoolsTradeDistributeWithNativeSkipsG1(t *testing.T) {
	c := NewCollector()
	h := c.Hooks()
	sel := []byte{0x0e, 0xf8, 0x47, 0xb6} // distributeWithNative(...)

	// G1 launcher에는 이 함수가 없다 — 등록되지 않아야 한다
	g1 := common.HexToAddress("0x00004c4ccc709Ef590F7C81102C0689F0263D4e9")
	h.OnEnter(1, byte(vm.CALL), addrA, g1, sel, 0, nil)
	h.OnExit(1, nil, 0, nil, false)
	if calls := c.DrainCalls(); len(calls) != 0 {
		t.Fatalf("G1 distributeWithNative는 TARGET이 아니다: %+v", calls)
	}

	// G1.5에는 있다
	c.Reset()
	g15 := common.HexToAddress("0x7A6C474b4DcD35b72203D2B569EAfE4C9b5C768e")
	h.OnEnter(1, byte(vm.CALL), addrA, g15, sel, 0, nil)
	h.OnExit(1, nil, 0, nil, false)
	if calls := c.DrainCalls(); len(calls) != 1 {
		t.Fatalf("G1.5 distributeWithNative: %+v", calls)
	}
}

func TestCollectorEmitsPoolsTradeRouterEntry(t *testing.T) {
	c := NewCollector()
	h := c.Hooks()
	router := common.HexToAddress("0xa0177CF584E06f4E7876d7bf0b2D5016e0d8a1fa")
	launcher := common.HexToAddress("0x7A6C474b4DcD35b72203D2B569EAfE4C9b5C768e")
	launch := []byte{0x27, 0xa1, 0x09, 0x8d, 0x33} // launch(...)
	create := []byte{0xde, 0xc1, 0x4b, 0xe1, 0x44} // createToken(...)

	// 서드파티 라우터 진입 → 라우터가 launcher를 호출
	h.OnEnter(0, byte(vm.CALL), addrA, router, launch, 0, big.NewInt(9))
	h.OnEnter(1, byte(vm.CALL), router, launcher, create, 0, nil)
	h.OnExit(1, nil, 0, nil, false)
	h.OnExit(0, nil, 0, nil, false)

	calls := c.DrainCalls()
	if len(calls) != 2 {
		t.Fatalf("라우터+launcher 2건이어야 함: %d (%+v)", len(calls), calls)
	}
	if calls[0].To != router || calls[0].Selector != [4]byte{0x27, 0xa1, 0x09, 0x8d} {
		t.Fatalf("라우터 launch: %+v", calls[0])
	}
	if big.NewInt(0).SetBytes(calls[0].Value).Int64() != 9 {
		t.Fatalf("value 보존: %+v", calls[0])
	}
	if calls[1].To != launcher || calls[1].Selector != [4]byte{0xde, 0xc1, 0x4b, 0xe1} || calls[1].Depth != 1 {
		t.Fatalf("launcher createToken: %+v", calls[1])
	}
}

func TestCollectorMarksRevertedCallRecord(t *testing.T) {
	c := NewCollector()
	h := c.Hooks()
	pons := common.HexToAddress("0xA5aAb3F0c6EeadF30Ef1D3Eb997108E976351feB")
	input := []byte{0x68, 0x63, 0x99, 0xcb, 0x01}
	h.OnEnter(1, byte(vm.CALL), addrA, pons, input, 0, big.NewInt(1))
	h.OnExit(1, nil, 0, nil, true)
	calls := c.DrainCalls()
	if len(calls) != 1 || !calls[0].Reverted {
		t.Fatalf("revert WhitelistedCallRecord: %+v", calls)
	}
}

// RevertReason은 revert한 frame **자신**의 record에만 실린다. 하위 트리 전파로
// Reverted만 켜진 record는 nil로 남고, 최상위 frame revert는 tx-level
// RevertOutput으로 잡힌다.
func TestCollectorRevertReasonOnSelfRecordOnly(t *testing.T) {
	c := NewCollector()
	h := c.Hooks()
	ur := common.HexToAddress("0x8876789976dEcBfCbBbe364623C63652db8C0904")
	pons := common.HexToAddress("0xA5aAb3F0c6EeadF30Ef1D3Eb997108E976351feB")
	urInput := []byte{0x35, 0x93, 0x56, 0x4c, 0x01}
	ponsInput := []byte{0x68, 0x63, 0x99, 0xcb, 0x01}
	innerReason := []byte{0x08, 0xc3, 0x79, 0xa0, 0x11}
	topReason := []byte{0x08, 0xc3, 0x79, 0xa0, 0x22}

	// depth 0: UR TARGET frame — 안에서 pons TARGET frame이 자기 이유로 revert,
	// 이어서 최상위도 revert.
	h.OnEnter(0, byte(vm.CALL), addrA, ur, urInput, 0, nil)
	h.OnEnter(1, byte(vm.CALL), ur, pons, ponsInput, 0, nil)
	h.OnExit(1, innerReason, 0, nil, true)
	h.OnExit(0, topReason, 0, nil, true)

	calls := c.DrainCalls()
	if len(calls) != 2 {
		t.Fatalf("expected 2 records, got %+v", calls)
	}
	if !bytes.Equal(calls[0].RevertReason, topReason) {
		t.Fatalf("UR frame must carry its own revert output: %+v", calls[0])
	}
	if !bytes.Equal(calls[1].RevertReason, innerReason) {
		t.Fatalf("inner frame must carry its own revert output: %+v", calls[1])
	}
	if !calls[0].Reverted || !calls[1].Reverted {
		t.Fatalf("both records must be marked reverted: %+v", calls)
	}
	if !bytes.Equal(c.DrainRevertOutput(), topReason) {
		t.Fatalf("tx-level revert output must be the top frame's: %x", c.DrainRevertOutput())
	}
}

// 캡 초과 revert data는 revertDataCap으로 잘린다.
func TestCollectorRevertReasonCapped(t *testing.T) {
	c := NewCollector()
	h := c.Hooks()
	ur := common.HexToAddress("0x8876789976dEcBfCbBbe364623C63652db8C0904")
	input := []byte{0x35, 0x93, 0x56, 0x4c, 0x01}
	big := make([]byte, revertDataCap*2)
	for i := range big {
		big[i] = byte(i)
	}
	h.OnEnter(0, byte(vm.CALL), addrA, ur, input, 0, nil)
	h.OnExit(0, big, 0, nil, true)
	calls := c.DrainCalls()
	if len(calls) != 1 || len(calls[0].RevertReason) != revertDataCap {
		t.Fatalf("revert reason must be capped at %d: got %d", revertDataCap, len(calls[0].RevertReason))
	}
	if !bytes.Equal(calls[0].RevertReason, big[:revertDataCap]) {
		t.Fatalf("capped bytes must be a prefix")
	}
}

// v4: ExitInnerIndex — 중첩 Call·로그가 공유 시퀀스 위에서 exact 구간을
// 이룬다: 프레임 소속 항목은 InnerIndex < i < ExitInnerIndex.
func TestCollectorStampsExitInnerIndex(t *testing.T) {
	c := NewCollector()
	h := c.Hooks()
	airlock := common.HexToAddress("0xeb7C034704eF8Dcd2D32324c1545f62fB4aD0862")
	swapRouter := common.HexToAddress("0xCaf681a66D020601342297493863E78C959E5cb2")

	// outer TARGET frame (inner=0) → log(1) → inner TARGET frame(2) → log(3)
	// → inner exit → log(4) → outer exit
	h.OnEnter(0, byte(vm.CALL), addrA, airlock, []byte{0x88, 0x2d, 0xb7, 0x07, 0x01}, 0, nil)
	h.OnLog(&types.Log{Address: addrB})
	h.OnEnter(1, byte(vm.CALL), airlock, swapRouter, []byte{0x42, 0x71, 0x2a, 0x67, 0x02}, 0, nil)
	h.OnLog(&types.Log{Address: addrB})
	h.OnExit(1, nil, 0, nil, false)
	h.OnLog(&types.Log{Address: addrB})
	h.OnExit(0, nil, 0, nil, false)

	calls := c.DrainCalls()
	if len(calls) != 2 {
		t.Fatalf("WhitelistedCallRecord 수: %d", len(calls))
	}
	outer, inner := calls[0], calls[1]
	if outer.InnerIndex != 0 || outer.ExitInnerIndex != 5 {
		t.Fatalf("outer span: [%d, %d)", outer.InnerIndex, outer.ExitInnerIndex)
	}
	if inner.InnerIndex != 2 || inner.ExitInnerIndex != 4 {
		t.Fatalf("inner span: [%d, %d)", inner.InnerIndex, inner.ExitInnerIndex)
	}
	// inner frame이 닫힌 뒤의 로그(4)는 outer 구간에만 든다.
	if !(outer.InnerIndex < 4 && 4 < outer.ExitInnerIndex) || inner.ExitInnerIndex <= 3 {
		t.Fatalf("containment: outer=[%d,%d) inner=[%d,%d)",
			outer.InnerIndex, outer.ExitInnerIndex, inner.InnerIndex, inner.ExitInnerIndex)
	}
}

// revert된 frame도 span은 남는다 (시도 axis + 하위 Call 귀속).
func TestCollectorExitIndexOnRevertedFrame(t *testing.T) {
	c := NewCollector()
	h := c.Hooks()
	swapRouter := common.HexToAddress("0xCaf681a66D020601342297493863E78C959E5cb2")
	h.OnEnter(0, byte(vm.CALL), addrA, swapRouter, []byte{0x42, 0x71, 0x2a, 0x67, 0x02}, 0, nil)
	h.OnLog(&types.Log{Address: addrB}) // revert로 폐기되지만 시퀀스는 소비됨
	h.OnExit(0, []byte{0x08, 0xc3, 0x79, 0xa0}, 0, nil, true)

	calls := c.DrainCalls()
	if len(calls) != 1 {
		t.Fatalf("WhitelistedCallRecord 수: %d", len(calls))
	}
	r := calls[0]
	if !r.Reverted || r.InnerIndex != 0 || r.ExitInnerIndex != 2 {
		t.Fatalf("reverted span: %+v", r)
	}
}
