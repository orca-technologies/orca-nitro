package bandpatch

import (
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/tracing"
)

var (
	addrA = common.HexToAddress("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	addrB = common.HexToAddress("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	addrC = common.HexToAddress("0xcccccccccccccccccccccccccccccccccccccccc")
	addrD = common.HexToAddress("0xdddddddddddddddddddddddddddddddddddddddd")
	coin  = common.HexToAddress("0x1111111111111111111111111111111111111111")
)

func hb(v int64) *hexutil.Big { return (*hexutil.Big)(big.NewInt(v)) }

// A -(5)-> B; B -(2)-> C 성공; B -(1)-> D revert(내부 로그 포함); 루트 로그 1개.
func fixtureFrame() *CallFrame {
	return &CallFrame{
		Type: "CALL", From: addrA, To: &addrB, Value: hb(5),
		Logs: []FrameLog{{Address: addrB, Topics: []common.Hash{{0x01}}, Data: []byte{0xaa}, Position: 1}},
		Calls: []CallFrame{
			{Type: "CALL", From: addrB, To: &addrC, Value: hb(2)},
			{
				Type: "CALL", From: addrB, To: &addrD, Value: hb(1), Error: "execution reverted",
				Logs: []FrameLog{{Address: addrD, Topics: nil, Data: []byte{0xdd}, Position: 0}},
			},
		},
	}
}

func fixtureDiff() *PrestateDiff {
	return &PrestateDiff{
		Pre: map[common.Address]PrestateAccount{
			addrA: {Balance: hb(100)},
			addrB: {Balance: hb(10)},
			addrC: {Balance: hb(0)},
			addrD: {Balance: hb(7)},
		},
		// revert 롤백 후 기대값: B=10+5-2=13, C=2, D=7 (A는 tainted — 검증 제외)
		Post: map[common.Address]PrestateAccount{
			addrB: {Balance: hb(13)},
			addrC: {Balance: hb(2)},
			addrD: {Balance: hb(7)},
		},
	}
}

func TestReconstructTxTransfersAndRevert(t *testing.T) {
	transfers, logs, _, degraded := ReconstructTx(fixtureFrame(), fixtureDiff(), addrA, coin)
	if degraded {
		t.Fatal("post-balance validation should pass")
	}
	if len(transfers) != 3 {
		t.Fatalf("expected 3 transfers, got %d: %+v", len(transfers), transfers)
	}

	root := transfers[0]
	if root.From != addrA || root.To != addrB || new(big.Int).SetBytes(root.Value).Int64() != 5 {
		t.Fatalf("root transfer wrong: %+v", root)
	}
	if root.PostBalanceFrom != nil {
		t.Fatal("sender(A)는 gas 오염으로 post balance nil이어야 함")
	}
	if new(big.Int).SetBytes(root.PostBalanceTo).Int64() != 15 {
		t.Fatalf("B post after root should be 15, got %v", new(big.Int).SetBytes(root.PostBalanceTo))
	}
	if root.Depth != 0 || root.Reverted {
		t.Fatalf("root transfer depth/revert wrong: %+v", root)
	}

	bc := transfers[1]
	if bc.From != addrB || bc.To != addrC || bc.Reverted || bc.Depth != 1 {
		t.Fatalf("B->C wrong: %+v", bc)
	}
	if new(big.Int).SetBytes(bc.PostBalanceFrom).Int64() != 13 || new(big.Int).SetBytes(bc.PostBalanceTo).Int64() != 2 {
		t.Fatalf("B->C post balances wrong: %+v", bc)
	}

	bd := transfers[2]
	if bd.From != addrB || bd.To != addrD || !bd.Reverted {
		t.Fatalf("B->D should be reverted transfer: %+v", bd)
	}
	// revert 이전 관측값이 기록된다 (collector 동작과 동일)
	if new(big.Int).SetBytes(bd.PostBalanceFrom).Int64() != 12 || new(big.Int).SetBytes(bd.PostBalanceTo).Int64() != 8 {
		t.Fatalf("B->D post balances (pre-revert observation) wrong: %+v", bd)
	}

	// revert된 frame의 로그는 폐기, 루트 로그만 남는다
	if len(logs) != 1 || logs[0].Address != addrB {
		t.Fatalf("expected only root log, got: %+v", logs)
	}

	// InnerIndex 순서: root transfer(0) < B->C(1) < 루트로그(2) < B->D(3)
	if !(root.InnerIndex < bc.InnerIndex && bc.InnerIndex < logs[0].InnerIndex && logs[0].InnerIndex < bd.InnerIndex) {
		t.Fatalf("inner index ordering wrong: root=%d bc=%d log=%d bd=%d",
			root.InnerIndex, bc.InnerIndex, logs[0].InnerIndex, bd.InnerIndex)
	}
}

func TestReconstructTxValidationDegrades(t *testing.T) {
	diff := fixtureDiff()
	diff.Post[addrC] = PrestateAccount{Balance: hb(999)} // 고의 불일치
	transfers, _, _, degraded := ReconstructTx(fixtureFrame(), diff, addrA, coin)
	if !degraded {
		t.Fatal("expected degraded=true on post mismatch")
	}
	for i, tr := range transfers {
		if tr.PostBalanceFrom != nil || tr.PostBalanceTo != nil {
			t.Fatalf("transfer %d post balances should be nil after degrade: %+v", i, tr)
		}
	}
}

func TestReconstructTxArbTransfers(t *testing.T) {
	frame := &CallFrame{
		Type: "CALL", From: addrA, To: &addrB,
		BeforeEVMTransfers: []ArbTransfer{
			{Purpose: "deposit", To: &addrB, Value: *hb(3)},
			{Purpose: "feePayment", From: &addrB, To: &coin, Value: *hb(1)}, // 미추적 → 레코드 없음 + taint
		},
	}
	diff := &PrestateDiff{
		Pre:  map[common.Address]PrestateAccount{addrB: {Balance: hb(10)}},
		Post: map[common.Address]PrestateAccount{addrB: {Balance: hb(12)}},
	}
	transfers, _, _, degraded := ReconstructTx(frame, diff, addrA, coin)
	if len(transfers) != 1 {
		t.Fatalf("expected 1 transfer (deposit only), got: %+v", transfers)
	}
	dep := transfers[0]
	if dep.Reason != uint8(tracing.BalanceIncreaseDeposit) || dep.To != addrB {
		t.Fatalf("deposit record wrong: %+v", dep)
	}
	// 방출 시점(taint 이전)의 관측값이 남는다 — collector와 동일
	if new(big.Int).SetBytes(dep.PostBalanceTo).Int64() != 13 {
		t.Fatalf("deposit post는 방출 시점 값 13이어야 함: %+v", dep)
	}
	// 이후 feePayment가 B를 taint → 검증에서 제외되어 degrade 없음
	if degraded {
		t.Fatal("tainted 주소는 검증 대상이 아니므로 degrade되면 안 됨")
	}
}
