package orcafeed

import (
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/holiman/uint256"
)

type sinkEntry struct {
	typ MsgType
	msg SeqSetter
}

type memSink struct {
	msgs []sinkEntry
}

func (m *memSink) Enqueue(typ MsgType, msg SeqSetter) {
	m.msgs = append(m.msgs, sinkEntry{typ: typ, msg: msg})
}

func newTestStateDB(t *testing.T) *state.StateDB {
	t.Helper()
	sdb, err := state.New(types.EmptyRootHash, state.NewDatabaseForTesting())
	if err != nil {
		t.Fatal(err)
	}
	return sdb
}

func makeTxAndReceipt(to common.Address, value int64, status uint64) (*types.Transaction, *types.Receipt) {
	tx := types.NewTx(&types.DynamicFeeTx{
		ChainID: big.NewInt(1), Nonce: 7, GasFeeCap: big.NewInt(100), GasTipCap: big.NewInt(1),
		Gas: 21000, To: &to, Value: big.NewInt(value), Data: []byte{0xde, 0xad},
	})
	receipt := &types.Receipt{
		Type: tx.Type(), Status: status, GasUsed: 21000, CumulativeGasUsed: 42000,
		TxHash: tx.Hash(), EffectiveGasPrice: big.NewInt(100),
		Logs: []*types.Log{{Address: to, Topics: []common.Hash{{0x01}}, Data: []byte{0x02}}},
	}
	return tx, receipt
}

func TestObserverTxModeDispatch(t *testing.T) {
	sink := &memSink{}
	obs := NewBlockObserver(sink, "tx")
	sdb := newTestStateDB(t)

	to := common.HexToAddress("0x1111111111111111111111111111111111111111")
	sdb.SetCode(to, []byte{0x60, 0x00}, tracing.CodeChangeUnspecified)
	sender := common.HexToAddress("0x2222222222222222222222222222222222222222")

	obs.BeginBlock(100, 1753689600)

	// collector에 transfer 하나 주입 (실행 중 훅 발화 시뮬레이션)
	obs.EVMHooks().OnBalanceChange(sender, big.NewInt(50), big.NewInt(20), tracing.BalanceChangeTransfer)
	obs.EVMHooks().OnBalanceChange(to, big.NewInt(0), big.NewInt(30), tracing.BalanceChangeTransfer)

	tx, receipt := makeTxAndReceipt(to, 30, 1)
	obs.OnTxAccepted(tx, sender, receipt, sdb, 1)

	if len(sink.msgs) != 1 || sink.msgs[0].typ != MsgReceipt {
		t.Fatalf("tx 모드에서 즉시 dispatch돼야 함: %+v", sink.msgs)
	}
	msg := sink.msgs[0].msg.(*ReceiptMsg)
	if msg.BlockNumber != 100 || msg.L2Timestamp != 1753689600 || msg.TxIndex != 1 {
		t.Fatalf("블록 컨텍스트: %+v", msg)
	}
	if msg.BlockHash != ([32]byte{}) {
		t.Fatalf("tx 모드는 blockHash zero: %+v", msg.BlockHash)
	}
	if msg.From != sender || msg.To != to || !msg.ToIsContract {
		t.Fatalf("from/to/contract 플래그: %+v", msg)
	}
	if big.NewInt(0).SetBytes(msg.Value).Int64() != 30 || len(msg.Calldata) != 2 || msg.Nonce != 7 {
		t.Fatalf("tx 필드: %+v", msg)
	}
	if len(msg.Logs) != 1 || msg.Logs[0].Address != to || len(msg.Logs[0].Topics) != 1 {
		t.Fatalf("logs: %+v", msg.Logs)
	}
	if len(msg.Transfers) != 1 || msg.Transfers[0].From != sender {
		t.Fatalf("transfers: %+v", msg.Transfers)
	}

	// seal → BlockSealMsg
	header := &types.Header{Number: big.NewInt(100), Time: 1753689600}
	block := types.NewBlock(header, &types.Body{Transactions: types.Transactions{tx}}, types.Receipts{receipt}, dummyHasher{})
	obs.OnBlockSealed(block)
	if len(sink.msgs) != 2 || sink.msgs[1].typ != MsgBlockSeal {
		t.Fatalf("BlockSealMsg 없음: %+v", sink.msgs)
	}
	seal := sink.msgs[1].msg.(*BlockSealMsg)
	if seal.BlockNumber != 100 || seal.TxCount != 1 || seal.BlockHash == ([32]byte{}) {
		t.Fatalf("seal: %+v", seal)
	}
}

func TestObserverBlockModeDispatch(t *testing.T) {
	sink := &memSink{}
	obs := NewBlockObserver(sink, "block")
	sdb := newTestStateDB(t)
	to := common.HexToAddress("0x3333333333333333333333333333333333333333")
	sender := common.HexToAddress("0x2222222222222222222222222222222222222222")

	obs.BeginBlock(200, 1753689601)
	tx, receipt := makeTxAndReceipt(to, 5, 1)
	obs.OnTxAccepted(tx, sender, receipt, sdb, 0)

	if len(sink.msgs) != 0 {
		t.Fatalf("block 모드는 seal 전 dispatch 금지: %+v", sink.msgs)
	}

	header := &types.Header{Number: big.NewInt(200), Time: 1753689601}
	block := types.NewBlock(header, &types.Body{Transactions: types.Transactions{tx}}, types.Receipts{receipt}, dummyHasher{})
	obs.OnBlockSealed(block)

	if len(sink.msgs) != 1 || sink.msgs[0].typ != MsgReceipt {
		t.Fatalf("seal 후 일괄 dispatch: %+v", sink.msgs)
	}
	msg := sink.msgs[0].msg.(*ReceiptMsg)
	if msg.BlockHash == ([32]byte{}) {
		t.Fatalf("block 모드는 blockHash 채움: %+v", msg)
	}
	if msg.ToIsContract {
		t.Fatalf("EOA인데 contract 플래그: %+v", msg)
	}
}

func TestObserverAppendFailed(t *testing.T) {
	sink := &memSink{}
	obs := NewBlockObserver(sink, "tx")
	obs.BeginBlock(300, 0)
	obs.OnAppendFailed(300)
	if len(sink.msgs) != 1 || sink.msgs[0].typ != MsgInvalidation {
		t.Fatalf("invalidation: %+v", sink.msgs)
	}
	if sink.msgs[0].msg.(*InvalidationMsg).BlockNumber != 300 {
		t.Fatalf("블록 번호: %+v", sink.msgs[0].msg)
	}
}

// types.NewBlock hasher — 테스트용 (실제 trie hash 불필요)
type dummyHasher struct{}

func (dummyHasher) Reset()                   {}
func (dummyHasher) Update(_, _ []byte) error { return nil }
func (dummyHasher) Hash() common.Hash        { return common.Hash{0xff} }

var _ = uint256.NewInt // keep import if unused later

func TestObserverEmitsCollectorLogsWithInnerIndex(t *testing.T) {
	sink := &memSink{}
	obs := NewBlockObserver(sink, "tx")
	sdb := newTestStateDB(t)
	to := common.HexToAddress("0x4444444444444444444444444444444444444444")
	sender := common.HexToAddress("0x5555555555555555555555555555555555555555")

	obs.BeginBlock(300, 1753689600)
	h := obs.EVMHooks()
	// transfer(inner 0) → log(inner 1)
	h.OnBalanceChange(sender, big.NewInt(50), big.NewInt(20), tracing.BalanceChangeTransfer)
	h.OnBalanceChange(to, big.NewInt(0), big.NewInt(30), tracing.BalanceChangeTransfer)
	h.OnLog(&types.Log{Address: to, Topics: []common.Hash{{0x01}}, Data: []byte{0x02}})

	tx, receipt := makeTxAndReceipt(to, 30, 1)
	obs.OnTxAccepted(tx, sender, receipt, sdb, 0)

	msg := sink.msgs[0].msg.(*ReceiptMsg)
	if len(msg.Logs) != 1 || msg.Logs[0].InnerIndex != 1 {
		t.Fatalf("로그 inner_index: %+v", msg.Logs)
	}
	if len(msg.Transfers) != 1 || msg.Transfers[0].InnerIndex != 0 {
		t.Fatalf("transfer inner_index: %+v", msg.Transfers)
	}
}

func TestObserverFallsBackWhenLogCountMismatches(t *testing.T) {
	sink := &memSink{}
	obs := NewBlockObserver(sink, "tx")
	sdb := newTestStateDB(t)
	to := common.HexToAddress("0x6666666666666666666666666666666666666666")
	sender := common.HexToAddress("0x7777777777777777777777777777777777777777")

	obs.BeginBlock(301, 0)
	// collector에 로그를 넣지 않는다 — receipt.Logs(1건)와 불일치
	tx, receipt := makeTxAndReceipt(to, 1, 1)
	obs.OnTxAccepted(tx, sender, receipt, sdb, 0)

	msg := sink.msgs[0].msg.(*ReceiptMsg)
	// 폴백: receipt.Logs가 그대로 실린다
	if len(msg.Logs) != 1 || msg.Logs[0].Address != to {
		t.Fatalf("폴백 실패: %+v", msg.Logs)
	}
}
