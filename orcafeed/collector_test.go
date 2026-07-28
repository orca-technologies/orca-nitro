package orcafeed

import (
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/tracing"
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
