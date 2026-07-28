# orcafeed inner_index — 로그·transfer 실행 순서 관측 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** orcafeed가 로그 방출과 native transfer의 **실제 실행 순서**를 tx-local `inner_index`로 관측해 wire에 실어 보낸다.

**Architecture:** collector가 이미 등록한 tracing 훅에 `OnLog`를 추가해, 로그와 transfer를 **하나의 방출 시퀀스**로 기록한다. tx마다 0부터 증가하는 `inner_index`를 부여하고, revert된 frame의 항목은 기존 transfer와 동일하게 마킹한다. 로그는 revert되지 않은 것만 내보내며, 그 결과가 `receipt.Logs`와 개수·내용이 일치하는지 테스트로 고정한다.

**Tech Stack:** Go, geth v1.16 `core/tracing.Hooks`, `tinylib/msgp` (msgpack array-mode 코드젠)

## Global Constraints

- 작업 경로: `/Users/como/workspace/Orca-Technologies/worktrees/orca-nitro/como_feat_receipt-dispatch` (브랜치 `como/feat/receipt-dispatch`)
- `SCHEMA_VERSION`은 **1로 고정** — 프로덕션 배포 전까지 bump 금지. 필드 변경 시 golden fixture만 재생성한다.
- `OnOpcode` 훅 등록 금지 (EVM 인터프리터 핫루프 오염).
- 핫패스 무할당 지향 — collector 버퍼는 `Reset()` 후 재사용 (capacity 유지).
- msgpack **array-mode**(`//msgp:tuple`) — 필드 **순서가 계약**이다. 추가는 반드시 struct 끝에.
- 테스트 실행은 compile과 분리: `go test --no-run` 없이 Go는 `go build` 후 `go test`. 단위 테스트는 수 초, system_tests는 단건 실행(`-run TestOrcaFeed -v`, ~2분).
- 커밋 메시지 끝: `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`

## 조사로 확정된 사실 (재확인 불요)

- `OnLog` 시그니처: `LogHook = func(log *types.Log)` (`go-ethereum/core/tracing/hooks.go:194-195`), 필드명 `OnLog` (`:234`).
- 발화 지점: `go-ethereum/core/state/statedb_hooked.go:276-277` — **hooked statedb를 거칠 때만** 발화한다. `arbos/block_processor.go`가 observer 활성 시 `state.NewHookedState(...)`로 감싸므로 이미 조건 충족.
- `OnLog`은 `AddLog` 시점에 발화하므로 **나중에 revert되는 로그도 발화**한다. 따라서 revert 마킹 후 필터링이 필요하다.
- collector 현재 상태: `orcafeed/collector.go` — `OnTxStart/OnTxEnd/OnEnter/OnExit/OnBalanceChange` 등록, `records []TransferRecord`, `frames []frameMark{depth, startIdx}`, `pending *pendingSub`.
- observer 현재 상태: `orcafeed/observer.go` `newReceiptMsg(...)`가 `receipt.Logs`를 그대로 `LogRecord`로 변환한다.

---

### Task 1: 스키마에 inner_index 추가

**Files:**
- Modify: `orcafeed/schema.go` (`TransferRecord`, `LogRecord`)
- Regenerate: `orcafeed/schema_gen.go` (`go generate ./orcafeed`)
- Modify: `orcafeed/golden_test.go` (고정 샘플에 값 추가)
- Regenerate: `orcafeed/testdata/golden_v1_*.bin`

**Interfaces:**
- Produces: `TransferRecord.InnerIndex uint16`, `LogRecord.InnerIndex uint16` — tx 안에서 이벤트가 방출된 순번 (0부터). Task 2·3이 채운다.

- [ ] **Step 1: 스키마에 필드 추가**

`orcafeed/schema.go`에서 두 struct 끝에 필드를 추가한다 (array-mode라 순서가 계약 — 반드시 끝에).

```go
//msgp:tuple TransferRecord
type TransferRecord struct {
	Reason          uint8
	From            [20]byte
	To              [20]byte
	Value           []byte
	PostBalanceFrom []byte
	PostBalanceTo   []byte
	Depth           uint16
	Reverted        bool
	// tx 안에서 이 이벤트가 방출된 순번 (0부터, 로그와 공유하는 단일 시퀀스).
	// 체인이 매긴 번호가 아니라 우리가 실행 중 관측한 순서다.
	InnerIndex uint16
}

//msgp:tuple LogRecord
type LogRecord struct {
	Address [20]byte
	Topics  [][32]byte
	Data    []byte
	// TransferRecord.InnerIndex와 같은 시퀀스 — 로그와 transfer의 실제 interleave.
	InnerIndex uint16
}
```

- [ ] **Step 2: 코드젠 재실행**

```bash
cd orcafeed && PATH="$(go env GOPATH)/bin:$PATH" go generate . && cd .. && go build ./orcafeed/
```
Expected: 에러 없음. `msgp`가 없으면 `go install github.com/tinylib/msgp` 선행.

- [ ] **Step 3: golden 샘플에 값 넣기**

`orcafeed/golden_test.go`의 `goldenMessages()`에서 receipt 샘플을 수정한다.

```go
			Logs: []LogRecord{{Address: addr(0x03), Topics: [][32]byte{hash(0xcc)}, Data: []byte{0x01}, InnerIndex: 1}},
			Transfers: []TransferRecord{{
				Reason: 10, From: addr(0x01), To: addr(0x02),
				Value:           []byte{0x0d, 0xe0, 0xb6, 0xb3, 0xa7, 0x64, 0x00, 0x00},
				PostBalanceFrom: []byte{0x01}, PostBalanceTo: []byte{0x02},
				Depth: 0, Reverted: false, InnerIndex: 0,
			}},
```

- [ ] **Step 4: golden fixture 재생성 후 통과 확인**

```bash
go test ./orcafeed/ -run TestGolden -update && go test ./orcafeed/ -run TestGolden -v
```
Expected: PASS (5개 서브테스트)

- [ ] **Step 5: 커밋**

```bash
git add orcafeed
git commit -m "feat(orcafeed): TransferRecord·LogRecord에 inner_index 필드

tx 안 방출 순번 — 로그와 transfer가 공유하는 단일 시퀀스.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 2: collector가 로그를 순서와 함께 수집

**Files:**
- Modify: `orcafeed/collector.go`
- Test: `orcafeed/collector_test.go`

**Interfaces:**
- Consumes: `TransferRecord.InnerIndex`, `LogRecord.InnerIndex` (Task 1)
- Produces:
  - `func (c *Collector) DrainLogs() []LogRecord` — revert되지 않은 로그만, 방출 순서대로
  - `Collector.Drain()` 반환 `TransferRecord`에 `InnerIndex` 채워짐
  - 두 슬라이스 모두 다음 tx 수집 시작(`Reset`) 전까지만 유효 (재사용 버퍼)

- [ ] **Step 1: 실패하는 테스트 작성**

`orcafeed/collector_test.go` 끝에 추가한다.

```go
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
```

- [ ] **Step 2: 실패 확인**

```bash
go test ./orcafeed/ -run TestCollector 2>&1 | tail -5
```
Expected: FAIL — `c.DrainLogs undefined`, `h.OnLog` nil 역참조

- [ ] **Step 3: collector 구현**

`orcafeed/collector.go`를 수정한다.

struct에 로그 시퀀스 상태를 추가:

```go
type Collector struct {
	hooks *tracing.Hooks

	// PERF:MEM-GROW
	//   cost: mem=O(N_transfers+N_logs)/tx, N~1e0..1e2 → Drain 후 재사용
	//   note: records·logs는 Reset으로 capacity 유지
	records []TransferRecord
	logs    []LogRecord
	// tx 안 방출 순번 — 로그와 transfer가 공유한다. revert된 항목도 번호를 소비하므로
	// 살아남은 항목의 상대 순서가 보존된다.
	innerNext uint16
	frames    []frameMark
	pending   *pendingSub
	txFrom    common.Address
	tx        *types.Transaction

	onTxEnd func(tx *types.Transaction, from common.Address, receipt *types.Receipt, transfers []TransferRecord)
}
```

`frameMark`에 로그 시작 인덱스 추가:

```go
type frameMark struct {
	depth         uint16
	startIdx      int // 이 frame 진입 시점의 records 길이
	logStartIdx   int // 이 frame 진입 시점의 logs 길이
}
```

훅 등록에 `OnLog` 추가 (`NewCollector` 안):

```go
	c.hooks = &tracing.Hooks{
		OnTxStart:       c.onTxStartHook,
		OnTxEnd:         c.onTxEndHook,
		OnEnter:         c.onEnter,
		OnExit:          c.onExit,
		OnBalanceChange: c.onBalanceChange,
		OnLog:           c.onLog,
	}
```

`onLog` 구현 (파일 끝 `append` 근처에 추가):

```go
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

// DrainLogs — revert되지 않은 로그를 방출 순서대로 반환한다.
// 반환 슬라이스는 다음 tx 수집 시작 전까지만 유효 (재사용 버퍼).
func (c *Collector) DrainLogs() []LogRecord {
	return c.logs
}
```

`append`가 `InnerIndex`를 채우도록 수정:

```go
func (c *Collector) append(r TransferRecord) {
	r.InnerIndex = c.nextInner()
	c.records = append(c.records, r)
}
```

`Reset`이 로그·카운터도 비우도록 수정:

```go
func (c *Collector) Reset() {
	c.records = c.records[:0]
	c.logs = c.logs[:0]
	c.innerNext = 0
	c.frames = c.frames[:0]
	c.pending = nil
}
```

`onEnter`가 로그 시작 인덱스도 기록:

```go
func (c *Collector) onEnter(depth int, _ byte, _ common.Address, _ common.Address, _ []byte, _ uint64, _ *big.Int) {
	c.flushPending()
	// #nosec G115
	c.frames = append(c.frames, frameMark{
		depth:       uint16(depth),
		startIdx:    len(c.records),
		logStartIdx: len(c.logs),
	})
}
```

`onExit`가 revert 시 로그를 **잘라낸다** (transfer는 플래그만, 로그는 receipt에 남지 않으므로 제거):

```go
func (c *Collector) onExit(_ int, _ []byte, _ uint64, _ error, reverted bool) {
	c.flushPending()
	if len(c.frames) == 0 {
		return
	}
	frame := c.frames[len(c.frames)-1]
	c.frames = c.frames[:len(c.frames)-1]
	if reverted {
		// transfer는 "시도됐다 무효화됨"이 신호가 되므로 플래그만 세운다.
		for i := frame.startIdx; i < len(c.records); i++ {
			c.records[i].Reverted = true
		}
		// 로그는 receipt.Logs에 남지 않으므로 버린다 — 시퀀스 번호는 이미 소비됐다.
		c.logs = c.logs[:frame.logStartIdx]
	}
}
```

- [ ] **Step 4: 통과 확인**

```bash
go test ./orcafeed/ -run TestCollector -v 2>&1 | tail -12
```
Expected: 기존 7개 + 신규 3개 전부 PASS

- [ ] **Step 5: 커밋**

```bash
git add orcafeed
git commit -m "feat(orcafeed): collector가 OnLog로 로그·transfer 실행 순서 관측

로그와 transfer를 하나의 tx-local 시퀀스(inner_index)로 기록한다. revert된
frame의 로그는 receipt에 남지 않으므로 버리고, transfer는 플래그만 세운다.
번호는 revert된 항목도 소비해 살아남은 항목의 상대 순서를 보존한다.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 3: observer가 collector 로그를 내보내고 receipt와 대조

**Files:**
- Modify: `orcafeed/observer.go` (`buildReceiptMsg`, `newReceiptMsg`)
- Test: `orcafeed/observer_test.go`

**Interfaces:**
- Consumes: `Collector.Drain()`, `Collector.DrainLogs()` (Task 2)
- Produces: `ReceiptMsg.Logs`가 collector 시퀀스에서 오며 `InnerIndex`를 담는다. 개수·주소가 `receipt.Logs`와 일치하지 않으면 로그 경고 후 `receipt.Logs`로 폴백한다 (데이터 유실 방지).

- [ ] **Step 1: 실패하는 테스트 작성**

`orcafeed/observer_test.go` 끝에 추가한다.

```go
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
	// 폴백: receipt.Logs가 그대로 실린다 (inner_index는 0)
	if len(msg.Logs) != 1 || msg.Logs[0].Address != to {
		t.Fatalf("폴백 실패: %+v", msg.Logs)
	}
}
```

- [ ] **Step 2: 실패 확인**

```bash
go test ./orcafeed/ -run TestObserver 2>&1 | tail -5
```
Expected: FAIL — `msg.Logs[0].InnerIndex`가 0 (collector 로그를 안 쓰고 있음)

- [ ] **Step 3: observer 구현**

`orcafeed/observer.go`의 `buildReceiptMsg`가 collector 로그를 넘기도록 수정한다.

```go
func (o *BlockObserver) buildReceiptMsg(tx *types.Transaction, sender common.Address, receipt *types.Receipt, statedb *state.StateDB, txIndex int) *ReceiptMsg {
	var transfers []TransferRecord
	if recs := o.collector.Drain(); len(recs) > 0 {
		// Drain 버퍼는 다음 tx에서 재사용되므로 복사
		transfers = make([]TransferRecord, len(recs))
		copy(transfers, recs)
	}
	var logs []LogRecord
	if ls := o.collector.DrainLogs(); len(ls) > 0 {
		logs = make([]LogRecord, len(ls))
		copy(logs, ls)
	}
	return newReceiptMsg(o.blockNumber, o.l2Timestamp, txIndex, tx, sender, receipt, transfers, logs,
		func(addr common.Address) bool { return isContractCached(statedb, o.codeCache, addr) })
}
```

`newReceiptMsg` 시그니처에 `logs []LogRecord`를 추가하고, 기존 `receipt.Logs` 변환 블록을 대조·폴백으로 교체한다.

```go
func newReceiptMsg(blockNumber, l2Timestamp uint64, txIndex int, tx *types.Transaction, sender common.Address, receipt *types.Receipt, transfers []TransferRecord, logs []LogRecord, isContract func(common.Address) bool) *ReceiptMsg {
```

기존의
```go
	if len(receipt.Logs) > 0 {
		msg.Logs = make([]LogRecord, len(receipt.Logs))
		...
	}
```
를 아래로 교체:

```go
	// collector가 관측한 로그를 쓴다 (inner_index 보유). revert 필터링까지 끝난
	// 상태라 receipt.Logs와 개수가 같아야 한다 — 어긋나면 관측 누락이므로
	// 경고하고 receipt.Logs로 폴백한다 (순서 정보는 잃되 데이터는 지킨다).
	if len(logs) == len(receipt.Logs) {
		msg.Logs = logs
	} else {
		log.Warn("orcafeed: collector log count mismatch, falling back to receipt logs",
			"collector", len(logs), "receipt", len(receipt.Logs), "tx", tx.Hash())
		if len(receipt.Logs) > 0 {
			msg.Logs = make([]LogRecord, len(receipt.Logs))
			for i, l := range receipt.Logs {
				topics := make([][32]byte, len(l.Topics))
				for j, topic := range l.Topics {
					topics[j] = topic
				}
				msg.Logs[i] = LogRecord{Address: l.Address, Topics: topics, Data: l.Data}
			}
		}
	}
```

`orcafeed/observer.go` import에 `"github.com/ethereum/go-ethereum/log"`를 추가한다.

`orcafeed/sweep_observer.go`의 `newReceiptMsg` 호출도 새 시그니처에 맞춘다 — collector 로그를 `txCapture`에 함께 담는다.

```go
type txCapture struct {
	from      common.Address
	transfers []TransferRecord
	logs      []LogRecord
}
```

`NewSweepObserver`의 `SetTxEndCallback` 안에서:

```go
		cp := &txCapture{from: from, transfers: nil, logs: nil}
		if len(transfers) > 0 {
			cp.transfers = make([]TransferRecord, len(transfers))
			copy(cp.transfers, transfers)
		}
		if ls := o.collector.DrainLogs(); len(ls) > 0 {
			cp.logs = make([]LogRecord, len(ls))
			copy(cp.logs, ls)
		}
		o.txCaps[tx.Hash()] = cp
```

`OnBlockExecuted` 안의 호출:

```go
		var sender common.Address
		var transfers []TransferRecord
		var logs []LogRecord
		if cp, ok := o.txCaps[tx.Hash()]; ok {
			sender = cp.from
			transfers = cp.transfers
			logs = cp.logs
		}
		msg := newReceiptMsg(blockNumber, block.Time(), i, tx, sender, receipts[i], transfers, logs,
			func(addr common.Address) bool { return isContractCached(statedb, codeCache, addr) })
```

- [ ] **Step 4: 통과 확인**

```bash
go build ./orcafeed/ && go test ./orcafeed/... -v 2>&1 | grep -E "^--- |^(ok|FAIL)" | tail -20
```
Expected: 전부 PASS

- [ ] **Step 5: 커밋**

```bash
git add orcafeed
git commit -m "feat(orcafeed): ReceiptMsg 로그를 collector 시퀀스에서 (inner_index 보유)

receipt.Logs와 개수가 어긋나면 경고 후 폴백해 데이터는 지킨다.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 4: 실제 체인에서 interleave 검증 (system test)

**Files:**
- Modify: `system_tests/orcafeed_test.go`

**Interfaces:**
- Consumes: Task 1–3 전부

- [ ] **Step 1: 실패하는 테스트 작성**

`system_tests/orcafeed_test.go`의 `TestOrcaFeedLiveDispatch` 안, `msg3` 검증 뒤에 추가한다.

```go
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
	// 시퀀스는 0부터 촘촘해야 한다 (revert 없는 tx이므로 구멍이 없다)
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
```

- [ ] **Step 2: 컴파일·실행해서 실패 확인**

```bash
go test -buildvcs=false -tags ckzg -c -o /tmp/systest ./system_tests/
cd system_tests && /tmp/systest -test.run TestOrcaFeedLiveDispatch -test.v 2>&1 | grep -E "^--- |FAIL"
```
Expected: 이 시점엔 Task 1–3이 이미 되어 있으므로 PASS. 만약 FAIL이면 inner_index 배선 누락이므로 Task 2·3을 점검한다.

- [ ] **Step 3: sweep 경로도 같은 불변식 확인**

`testOrcaFeedSweep`의 `msg3` 검증 뒤에 추가한다.

```go
	if len(msg3.Logs) > 0 {
		for _, l := range msg3.Logs {
			for _, tr := range msg3.Transfers {
				if l.InnerIndex == tr.InnerIndex {
					Fatal(t, "sweep: 로그·transfer inner_index 충돌: ", l.InnerIndex)
				}
			}
		}
	}
```

- [ ] **Step 4: 통합 테스트 3종 전부 실행**

```bash
go test -buildvcs=false -tags ckzg -c -o /tmp/systest ./system_tests/
cd system_tests && /tmp/systest -test.run TestOrcaFeed -test.v 2>&1 | grep -E "^--- |^(PASS|FAIL)"
```
Expected: `TestOrcaFeedLiveDispatch`, `TestOrcaFeedSweep`, `TestOrcaFeedSweepPathArchive` 전부 PASS

- [ ] **Step 5: 커밋**

```bash
git add system_tests/orcafeed_test.go
git commit -m "test(orcafeed): 로그·transfer inner_index 시퀀스 불변식 검증

실제 체인에서 중복 없이 0부터 촘촘하고, top-level transfer가 0인지 확인.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 5: Rust 디코더 동기화

**Files:**
- Modify: `dora-shared/como_feat_evm-platform/orcafeed-types/src/schema.rs`
- Copy: `orcafeed/testdata/golden_v1_*.bin` → `dora-shared/como_feat_evm-platform/orcafeed-types/tests/golden/`
- Modify: `dora-shared/como_feat_evm-platform/orcafeed-types/tests/golden_tests.rs`

**Interfaces:**
- Consumes: Task 1의 wire 스키마
- Produces: Rust 쪽 `TransferRecord.inner_index: u16`, `LogRecord.inner_index: u16`

- [ ] **Step 1: Rust 스키마에 필드 추가**

`orcafeed-types/src/schema.rs`의 두 struct 끝에 추가한다 (순서 = Go와 동일해야 함).

```rust
pub struct TransferRecord {
    pub reason: u8,
    pub from: ByteArray<20>,
    pub to: ByteArray<20>,
    pub value: ByteBuf,
    pub post_balance_from: Option<ByteBuf>,
    pub post_balance_to: Option<ByteBuf>,
    pub depth: u16,
    pub reverted: bool,
    /// tx 안에서 이 이벤트가 방출된 순번 (0부터, 로그와 공유하는 단일 시퀀스).
    pub inner_index: u16,
}

pub struct LogRecord {
    pub address: ByteArray<20>,
    pub topics: Vec<ByteArray<32>>,
    pub data: Option<ByteBuf>,
    pub inner_index: u16,
}
```

- [ ] **Step 2: golden fixture 동기화**

```bash
cp /Users/como/workspace/Orca-Technologies/worktrees/orca-nitro/como_feat_receipt-dispatch/orcafeed/testdata/golden_v1_*.bin \
   /Users/como/workspace/Orca-Technologies/worktrees/dora-shared/como_feat_evm-platform/orcafeed-types/tests/golden/
```

- [ ] **Step 3: 기대값 추가**

`orcafeed-types/tests/golden_tests.rs`의 `decodes_receipt_full`에서 로그·transfer 검증에 추가한다.

```rust
    assert_eq!(logs[0].inner_index, 1);
    ...
    assert_eq!(t.inner_index, 0);
```

- [ ] **Step 4: 테스트 통과 확인**

```bash
cd /Users/como/workspace/Orca-Technologies/worktrees/dora-shared/como_feat_evm-platform
cargo test --no-run -p orcafeed-types && cargo test -p orcafeed-types 2>&1 | grep "test result"
```
Expected: 4 passed

- [ ] **Step 5: 커밋**

```bash
cd /Users/como/workspace/Orca-Technologies/worktrees/dora-shared/como_feat_evm-platform
git add orcafeed-types
git commit -m "feat(orcafeed-types): inner_index 필드 + golden 동기화

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
git push
```

---

## Self-Review 결과

**1. 스펙 커버리지** — 이 계획은 설계 문서 §5의 `inner_index` 생산 부분만 다룬다 (서브프로젝트 A). `ordinal` 패킹·`chain_ts`/`observed_ts` 필드·taxonomy 경로·파생 디코더·소비자 마이그레이션은 서브프로젝트 B–E의 별도 계획으로 간다. 의도된 범위 분할이다.

**2. placeholder 스캔** — 없음. 모든 코드 단계에 실제 코드가 있다.

**3. 타입 일관성** — `InnerIndex uint16`(Go) ↔ `inner_index: u16`(Rust), `DrainLogs()` 이름이 Task 2 정의와 Task 3 사용에서 일치. `newReceiptMsg` 시그니처 변경이 observer·sweep_observer 양쪽 호출부에 반영됨.
