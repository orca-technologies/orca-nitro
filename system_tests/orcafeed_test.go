// Copyright 2026 Orca Technologies.
// orcafeed live/sweep dispatch 통합 테스트.
package arbtest

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/vm"

	blocksreexecutor "github.com/offchainlabs/nitro/blocks_reexecutor"
	"github.com/offchainlabs/nitro/orcafeed"
	"github.com/offchainlabs/nitro/solgen/go/localgen"
	"github.com/offchainlabs/nitro/util/arbmath"
	"github.com/offchainlabs/nitro/util/testhelpers"
)

// orcaMsgStore — 소켓/sink에서 수신한 메시지 집합 (goroutine-safe)
type orcaMsgStore struct {
	mu       sync.Mutex
	hello    *orcafeed.Hello
	receipts map[common.Hash]*orcafeed.ReceiptMsg
	seals    map[uint64]*orcafeed.BlockSealMsg
	ranges   []*orcafeed.RangeDoneMsg
	seqs     []uint64
}

func newOrcaMsgStore() *orcaMsgStore {
	return &orcaMsgStore{
		mu:       sync.Mutex{},
		hello:    nil,
		receipts: make(map[common.Hash]*orcafeed.ReceiptMsg),
		seals:    make(map[uint64]*orcafeed.BlockSealMsg),
		ranges:   nil,
		seqs:     nil,
	}
}

func (s *orcaMsgStore) Enqueue(typ orcafeed.MsgType, msg orcafeed.SeqSetter) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch m := msg.(type) {
	case *orcafeed.ReceiptMsg:
		s.receipts[common.Hash(m.TxHash)] = m
	case *orcafeed.BlockSealMsg:
		s.seals[m.BlockNumber] = m
	case *orcafeed.RangeDoneMsg:
		s.ranges = append(s.ranges, m)
	}
}

func (s *orcaMsgStore) receipt(txHash common.Hash) *orcafeed.ReceiptMsg {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.receipts[txHash]
}

func (s *orcaMsgStore) waitReceipt(t *testing.T, txHash common.Hash) *orcafeed.ReceiptMsg {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if msg := s.receipt(txHash); msg != nil {
			return msg
		}
		time.Sleep(20 * time.Millisecond)
	}
	Fatal(t, "orcafeed receipt 미수신: ", txHash)
	return nil
}

func (s *orcaMsgStore) readSocket(t *testing.T, conn net.Conn) {
	header := make([]byte, 5)
	for {
		if _, err := io.ReadFull(conn, header); err != nil {
			return // 연결 종료
		}
		payload := make([]byte, binary.LittleEndian.Uint32(header[:4]))
		if _, err := io.ReadFull(conn, payload); err != nil {
			return
		}
		typ := orcafeed.MsgType(header[4])
		s.mu.Lock()
		switch typ {
		case orcafeed.MsgHello:
			var m orcafeed.Hello
			if _, err := m.UnmarshalMsg(payload); err == nil {
				s.hello = &m
			}
		case orcafeed.MsgReceipt:
			var m orcafeed.ReceiptMsg
			if _, err := m.UnmarshalMsg(payload); err == nil {
				s.receipts[common.Hash(m.TxHash)] = &m
				s.seqs = append(s.seqs, m.Seq)
			}
		case orcafeed.MsgBlockSeal:
			var m orcafeed.BlockSealMsg
			if _, err := m.UnmarshalMsg(payload); err == nil {
				s.seals[m.BlockNumber] = &m
				s.seqs = append(s.seqs, m.Seq)
			}
		case orcafeed.MsgRangeDone:
			var m orcafeed.RangeDoneMsg
			if _, err := m.UnmarshalMsg(payload); err == nil {
				s.ranges = append(s.ranges, &m)
			}
		}
		s.mu.Unlock()
	}
}

// argsForMulticallEmit — argsForMulticall과 같되 kind에 emit 플래그(0x8)를 세워
// MultiCallTest가 Called 로그를 방출하게 한다. 같은 tx에서 native transfer와 로그가
// 함께 나오므로 inner_index interleave를 실체인에서 검증할 수 있다.
func argsForMulticallEmit(address common.Address, value *big.Int, calldata []byte) []byte {
	args := []byte{0x01}
	length := 21 + len(calldata) + 32
	// #nosec G115
	args = append(args, arbmath.Uint32ToBytes(uint32(length))...)
	args = append(args, 0x08) // CALL(0x0) | emit(0x8)
	if value == nil {
		value = common.Big0
	}
	args = append(args, common.BigToHash(value).Bytes()...)
	args = append(args, address.Bytes()...)
	args = append(args, calldata...)
	return args
}

func orcaSocketPath(t *testing.T) string {
	t.Helper()
	// t.TempDir()는 macOS unix socket 경로 한계(104B)를 넘을 수 있어 짧은 경로 사용
	dir, err := os.MkdirTemp("", "orca")
	Require(t, err)
	t.Cleanup(func() { os.RemoveAll(dir) })
	return filepath.Join(dir, "o.sock")
}

func TestOrcaFeedLiveDispatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// sequencer 노드
	builderSeq := NewNodeBuilder(ctx).DefaultConfig(t, false).DontParalellise()
	builderSeq.nodeConfig.Feed.Output = *newBroadcasterConfigTest()
	cleanupSeq := builderSeq.Build(t)
	defer cleanupSeq()
	seqInfo, seqNode, seqClient := builderSeq.L2Info, builderSeq.L2.ConsensusNode, builderSeq.L2.Client

	// follower 노드 — feed 수신 + orca dispatch (tx 모드)
	sock := orcaSocketPath(t)
	port := testhelpers.AddrTCPPort(seqNode.BroadcastServer.ListenerAddr(), t)
	builder := NewNodeBuilder(ctx).DefaultConfig(t, false).DontParalellise()
	builder.nodeConfig.Feed.Input = *newBroadcastClientConfigTest(port)
	builder.takeOwnership = false
	builder.execConfig.OrcaFeed = orcafeed.DefaultConfig
	builder.execConfig.OrcaFeed.Enable = true
	builder.execConfig.OrcaFeed.SocketPath = sock
	cleanup := builder.Build(t)
	defer cleanup()
	followerClient := builder.L2.Client

	conn, err := net.Dial("unix", sock)
	Require(t, err)
	defer conn.Close()
	store := newOrcaMsgStore()
	go store.readSocket(t, conn)

	// (1) 단순 value transfer
	seqInfo.GenerateAccount("User2")
	user2 := seqInfo.GetAddress("User2")
	transferValue := big.NewInt(1e12)
	tx1 := seqInfo.PrepareTx("Owner", "User2", seqInfo.TransferGas, transferValue, nil)
	Require(t, seqClient.SendTransaction(ctx, tx1))
	_, err = EnsureTxSucceeded(ctx, seqClient, tx1)
	Require(t, err)

	// (2) MultiCallTest 배포 → (3) internal transfer 유발
	auth := seqInfo.GetDefaultTransactOpts("Owner", ctx)
	multiAddr, txDeploy, _, err := localgen.DeployMultiCallTest(&auth, seqClient)
	Require(t, err)
	_, err = EnsureTxSucceeded(ctx, seqClient, txDeploy)
	Require(t, err)

	innerValue := big.NewInt(400)
	outerValue := big.NewInt(1000)
	callData := argsForMulticallEmit(user2, innerValue, nil)
	tx3 := seqInfo.PrepareTxTo("Owner", &multiAddr, 1e9, outerValue, callData)
	Require(t, seqClient.SendTransaction(ctx, tx3))
	_, err = EnsureTxSucceeded(ctx, seqClient, tx3)
	Require(t, err)

	// follower가 feed로 따라올 때까지 대기
	_, err = WaitForTx(ctx, followerClient, tx3.Hash(), time.Second*15)
	Require(t, err)

	// hello 검증
	store.mu.Lock()
	hello := store.hello
	store.mu.Unlock()
	if hello == nil || hello.SchemaVersion != orcafeed.SchemaVersion || hello.Mode != orcafeed.ModeLiveTx {
		Fatal(t, "hello frame 불일치: ", hello)
	}

	// (1) 검증: tx 필드 + depth-0 transfer + post balance
	msg1 := store.waitReceipt(t, tx1.Hash())
	if common.Address(msg1.To) != user2 || msg1.Status != 1 || msg1.ToIsContract {
		Fatal(t, "tx1 필드 불일치: ", msg1)
	}
	if new(big.Int).SetBytes(msg1.Value).Cmp(transferValue) != 0 {
		Fatal(t, "tx1 value 불일치")
	}
	if msg1.BlockHash != ([32]byte{}) {
		Fatal(t, "tx 모드는 blockHash가 비어야 함")
	}
	var found1 bool
	for _, tr := range msg1.Transfers {
		if common.Address(tr.To) == user2 && new(big.Int).SetBytes(tr.Value).Cmp(transferValue) == 0 && tr.Depth == 0 && !tr.Reverted {
			found1 = true
			if len(tr.PostBalanceTo) == 0 {
				Fatal(t, "post balance 누락: ", tr)
			}
		}
	}
	if !found1 {
		Fatal(t, "tx1 depth-0 transfer 미발견: ", msg1.Transfers)
	}

	// tx1 블록의 BlockSeal 확인 (blockHash 보완 + TxCount)
	store.mu.Lock()
	seal := store.seals[msg1.BlockNumber]
	store.mu.Unlock()
	if seal == nil || seal.BlockHash == ([32]byte{}) || seal.TxCount < 2 {
		Fatal(t, "BlockSeal 불일치: ", seal)
	}

	// (3) 검증: internal transfer (depth>=1) + ToIsContract
	msg3 := store.waitReceipt(t, tx3.Hash())
	if !msg3.ToIsContract || common.Address(msg3.To) != multiAddr {
		Fatal(t, "tx3 contract 플래그 불일치: ", msg3)
	}
	var foundOuter, foundInner bool
	for _, tr := range msg3.Transfers {
		if common.Address(tr.From) == multiAddr && common.Address(tr.To) == user2 &&
			new(big.Int).SetBytes(tr.Value).Cmp(innerValue) == 0 && tr.Depth >= 1 && !tr.Reverted {
			foundInner = true
		}
		if common.Address(tr.To) == multiAddr && new(big.Int).SetBytes(tr.Value).Cmp(outerValue) == 0 && tr.Depth == 0 {
			foundOuter = true
		}
	}
	if !foundOuter || !foundInner {
		Fatal(t, "tx3 transfer 검증 실패 (outer:", foundOuter, " inner:", foundInner, "): ", msg3.Transfers)
	}

	// (4) 로그·transfer가 하나의 실행 순서 시퀀스를 공유하는지
	if len(msg3.Logs) == 0 {
		Fatal(t, "tx3에 로그가 없음 — MultiCallTest는 로그를 방출해야 함")
	}
	seen := map[uint16]bool{}
	maxInner := uint16(0)
	for _, l := range msg3.Logs {
		if seen[l.InnerIndex] {
			Fatal(t, "inner_index 중복(로그): ", l.InnerIndex)
		}
		seen[l.InnerIndex] = true
		if l.InnerIndex > maxInner {
			maxInner = l.InnerIndex
		}
	}
	for _, tr := range msg3.Transfers {
		if seen[tr.InnerIndex] {
			Fatal(t, "inner_index 중복(transfer/로그 간): ", tr.InnerIndex)
		}
		seen[tr.InnerIndex] = true
		if tr.InnerIndex > maxInner {
			maxInner = tr.InnerIndex
		}
	}
	// revert 없는 tx이므로 시퀀스는 0부터 촘촘해야 한다
	if int(maxInner)+1 != len(seen) {
		Fatal(t, "inner_index 시퀀스에 구멍: max=", maxInner, " count=", len(seen))
	}
	// top-level value transfer가 첫 이벤트
	var topLevel *orcafeed.TransferRecord
	for i := range msg3.Transfers {
		if msg3.Transfers[i].Depth == 0 {
			topLevel = &msg3.Transfers[i]
		}
	}
	if topLevel == nil || topLevel.InnerIndex != 0 {
		Fatal(t, "top-level transfer가 inner_index 0이 아님: ", topLevel)
	}

	// seq 단조증가 (drop 없음 전제)
	store.mu.Lock()
	defer store.mu.Unlock()
	for i := 1; i < len(store.seqs); i++ {
		if store.seqs[i] != store.seqs[i-1]+1 {
			Fatal(t, "seq 불연속: ", store.seqs)
		}
	}
}

// hash 스킴: state 전진(advanceStateUpToBlock) 경로
func TestOrcaFeedSweep(t *testing.T) {
	testOrcaFeedSweep(t, rawdb.HashScheme)
}

// path archive 스킴 (Titan 프로필): HistoricReader 블록별 직접 실행 경로
func TestOrcaFeedSweepPathArchive(t *testing.T) {
	testOrcaFeedSweep(t, rawdb.PathScheme)
}

func testOrcaFeedSweep(t *testing.T, scheme string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	builder := NewNodeBuilder(ctx).DefaultConfig(t, false)
	builder.RequireScheme(t, scheme)
	if scheme == rawdb.PathScheme {
		// Titan archive 노드 구성과 동일: path + archive (state history 전체 보존)
		builder.execConfig.Caching.Archive = true
		builder.execConfig.Caching.StateHistory = 0
	}
	cleanup := builder.Build(t)
	defer cleanup()

	l2info, client := builder.L2Info, builder.L2.Client
	l2info.GenerateAccount("User2")
	user2 := l2info.GetAddress("User2")

	// 블록 몇 개 생성: 단순 transfer + internal transfer
	tx1 := l2info.PrepareTx("Owner", "User2", l2info.TransferGas, big.NewInt(1e12), nil)
	Require(t, client.SendTransaction(ctx, tx1))
	_, err := EnsureTxSucceeded(ctx, client, tx1)
	Require(t, err)

	auth := l2info.GetDefaultTransactOpts("Owner", ctx)
	multiAddr, txDeploy, _, err := localgen.DeployMultiCallTest(&auth, client)
	Require(t, err)
	_, err = EnsureTxSucceeded(ctx, client, txDeploy)
	Require(t, err)

	innerValue := big.NewInt(400)
	callData := argsForMulticall(vm.CALL, user2, innerValue, nil)
	tx3 := l2info.PrepareTxTo("Owner", &multiAddr, 1e9, big.NewInt(1000), callData)
	Require(t, client.SendTransaction(ctx, tx3))
	receipt3, err := EnsureTxSucceeded(ctx, client, tx3)
	Require(t, err)

	latest, err := client.BlockNumber(ctx)
	Require(t, err)

	// 재실행 + sink dispatch
	store := newOrcaMsgStore()
	c := blocksreexecutor.TestConfig
	c.Blocks = fmt.Sprintf(`[[1, %d]]`, latest)
	c.MinBlocksPerThread = 10
	Require(t, c.Validate())
	blockchain := builder.L2.ExecNode.Backend.ArbInterface().BlockChain()
	executor, err := blocksreexecutor.New(&c, blockchain, builder.L2.ExecNode.ExecutionDB)
	Require(t, err)
	executor.SetOrcaSink(store)
	executor.Start(ctx)
	Require(t, executor.WaitForReExecution(ctx))

	// live와 동일 스키마 + blockHash 채워짐
	msg1 := store.receipt(tx1.Hash())
	if msg1 == nil || msg1.BlockHash == ([32]byte{}) {
		Fatal(t, "sweep tx1 receipt 불일치: ", msg1)
	}
	if common.Address(msg1.To) != user2 || new(big.Int).SetBytes(msg1.Value).Cmp(big.NewInt(1e12)) != 0 {
		Fatal(t, "sweep tx1 필드 불일치: ", msg1)
	}

	msg3 := store.receipt(tx3.Hash())
	if msg3 == nil || msg3.BlockNumber != receipt3.BlockNumber.Uint64() || !msg3.ToIsContract {
		Fatal(t, "sweep tx3 불일치: ", msg3)
	}
	var foundInner bool
	for _, tr := range msg3.Transfers {
		if common.Address(tr.From) == multiAddr && common.Address(tr.To) == user2 &&
			new(big.Int).SetBytes(tr.Value).Cmp(innerValue) == 0 && tr.Depth >= 1 {
			foundInner = true
		}
	}
	if !foundInner {
		Fatal(t, "sweep internal transfer 미발견: ", msg3.Transfers)
	}

	// 로그·transfer가 같은 시퀀스를 공유 (충돌 없음)
	for _, l := range msg3.Logs {
		for _, tr := range msg3.Transfers {
			if l.InnerIndex == tr.InnerIndex {
				Fatal(t, "sweep: 로그·transfer inner_index 충돌: ", l.InnerIndex)
			}
		}
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.ranges) == 0 {
		Fatal(t, "RangeDone 마커 미수신")
	}
}
