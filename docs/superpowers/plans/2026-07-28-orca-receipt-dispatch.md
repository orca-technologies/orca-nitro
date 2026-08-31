# orca-nitro receipt dispatch 구현 계획

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** feed tx 실행 즉시 enriched receipt(+transfer)를 unix socket으로 dispatch하는 live 경로와, 동일 파이프라인으로 과거 block range를 병렬 재실행하는 sweep 경로를 구현한다.

**Architecture:** 신규 패키지 `orca-nitro-feed`(schema·collector·observer·dispatcher)가 코어. live는 `arbos.ProduceBlockAdvanced`에 observer 파라미터를 추가해 per-tx 훅, sweep은 `blocks_reexecutor`의 `vm.Config`에 tracer만 주입. geth fork 변경 없음.

**Tech Stack:** Go (nitro v3.11.2 베이스), `tinylib/msgp` (msgpack array-mode 코드젠), geth v1.16 `core/tracing.Hooks`, unix domain socket.

## Global Constraints

- 베이스: `orca/main` (= v3.11.2). go-ethereum submodule 변경 금지.
- 핫패스(실행 goroutine)에서 소켓 write·직렬화 금지 — enqueue만. 할당은 `sync.Pool` 재사용.
- `OnOpcode` 훅 등록 금지.
- msgpack **array-mode** (`//msgp:tuple`). wire 하위호환 불요, `SchemaVersion` 상수로 lockstep 검증.
- 기존 기능 코드 삭제 금지. 모든 신규 동작은 config로 opt-in (기본 off — upstream 동작 무변경).
- 커밋 메시지 끝: `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`
- 테스트: `go test ./orcanitrofeed/...`는 수 초 내. system_tests는 단건 실행 (`go test ./system_tests/ -run TestOrcaNitroFeed -v`, ~2분 허용).

## 조사로 확정된 배선 사실 (구현 시 재확인 불요)

- `arbos/block_processor.go:294 ProduceBlock` / `:319 ProduceBlockAdvanced`. 호출처 7곳:
  block_processor.go:313(내부), executionengine.go:765,998(sequencer),1045(follower),
  block_recorder.go:161, cmd/replay/main.go:388, cmd/unified-replay/main.go:141.
- per-tx EVM 생성: block_processor.go:521 `vm.NewEVM(blockContext, buildState.statedb, chainConfig, vm.Config{ExposeMultiGas:...})`.
- receipt 확정: block_processor.go `buildState.receipts = append(buildState.receipts, receipt)` (complete append 직후).
- blockHash 확정: ProduceBlockAdvanced 말미 "Touch up the block hashes" 루프.
- OnBalanceChange는 `state.NewHookedState(statedb, hooks)` 래퍼를 EVM에 줄 때만 발화
  (upstream 패턴: go-ethereum/core/state_processor.go:82). `vm.NewEVM`은 `vm.StateDB`
  인터페이스를 받으므로 래퍼 전달 가능. `ApplyTransactionWithResultFilter`는 concrete
  statedb를 병행으로 받음 (state_processor.go:232) — 동일 객체이므로 정합.
- ArbOS 이체: `arbos/util/transfer.go TransferBalance` — EVM 밖은
  `CaptureArbitrumTransfer(from,to,value,before,reason)` (from/to nil = mint/burn),
  EVM 안은 `MockCall`이 합성 OnEnter/OnExit 발화.
- reason enum: `tracing.BalanceChangeTransfer=10`, `BalanceIncrease/DecreaseSelfdestruct=12/13`,
  `BalanceDecreaseSelfdestructBurn=14`, `BalanceChangeRevert=15`, gas류 5·6·7. Arbitrum 확장 128+.
- sweep: cmd/nitro/nitro.go:474 — `--blocks-reexecutor.enable` + `--init.then-quit`으로
  "DB open → 재실행 → 종료"가 이미 배선됨. 실행은 blocks_reexecutor
  `advanceStateUpToBlock` → `arbitrum.AdvanceStateByBlock(ctx, bc, state, n, prevHash, nil, vmConfig)`
  가 블록별 receipts 반환. vmConfig에 Tracer만 넣으면 hooked state는 내부 Process가 래핑.
  pathdb 거부·hash 전용 체크 기존재 (`New()`).
- prefetch 실행(`createBlockFromNextMessage(msg, true, ...)`)과 sequencer 경로에는 observer를
  절대 전달하지 않는다 (중복 dispatch 방지).

---

### Task 1: orca-nitro-feed schema + golden vector

**Files:**
- Create: `orcanitrofeed/schema.go`, `orcanitrofeed/golden_test.go`, `orcanitrofeed/testdata/` (fixture)
- Modify: `go.mod` (+`github.com/tinylib/msgp`)
- Generate: `orcanitrofeed/schema_gen.go` (`go:generate msgp`)

**Interfaces (Produces):**

```go
package orcanitrofeed

const SchemaVersion uint32 = 1

// 프레임: [u32 LE payload len][u8 MsgType][msgpack payload]
type MsgType byte
const (
    MsgHello        MsgType = 1 // Hello
    MsgReceipt      MsgType = 2 // ReceiptMsg
    MsgBlockSeal    MsgType = 3 // BlockSealMsg (tx 모드에서 blockHash 보완)
    MsgInvalidation MsgType = 4 // InvalidationMsg (commit 실패)
    MsgRangeDone    MsgType = 5 // RangeDoneMsg (sweep chunk 완료)
)

//msgp:tuple Hello
type Hello struct { SchemaVersion uint32; Mode string } // Mode: "live-tx"|"live-block"|"sweep"

//msgp:tuple TransferRecord
type TransferRecord struct {
    Reason         uint8   // tracing.BalanceChangeReason
    From, To       [20]byte // mint/burn은 zero addr
    Value          []byte  // big.Int bytes (BE)
    PostBalanceFrom, PostBalanceTo []byte // 미상(-)이면 nil
    Depth          uint16  // 0=top-level
    Reverted       bool
}

//msgp:tuple ReceiptMsg
type ReceiptMsg struct {
    Seq         uint64
    BlockNumber uint64
    BlockHash   [32]byte // tx 모드: zero, block/sweep 모드: 채움
    TxIndex     uint32
    L2Timestamp uint64
    TxHash      [32]byte
    TxType      uint8
    From, To    [20]byte // contract creation: To=zero
    ToIsContract bool
    ContractAddress [20]byte
    Nonce       uint64
    Gas         uint64
    EffectiveGasPrice []byte
    Value       []byte
    Calldata    []byte
    Status      uint64
    GasUsed     uint64
    CumulativeGasUsed uint64
    Logs        []LogRecord
    Transfers   []TransferRecord
}

//msgp:tuple LogRecord
type LogRecord struct { Address [20]byte; Topics [][32]byte; Data []byte }

//msgp:tuple BlockSealMsg
type BlockSealMsg struct { Seq uint64; BlockNumber uint64; BlockHash [32]byte; TxCount uint32 }

//msgp:tuple InvalidationMsg
type InvalidationMsg struct { Seq uint64; BlockNumber uint64 }

//msgp:tuple RangeDoneMsg
type RangeDoneMsg struct { Seq uint64; StartBlock, EndBlock uint64 }
```

**Steps:**
- [ ] `go get github.com/tinylib/msgp && go install github.com/tinylib/msgp` — 버전은 go.mod 최신
- [ ] schema.go 작성 (`//go:generate msgp -tests=false`), `go generate ./orca-nitro-feed`
- [ ] golden_test: 고정 값으로 각 메시지 인코딩 → `testdata/golden_v1_<type>.bin`과 바이트 비교
      (fixture 최초 생성은 `-update` 플래그 패턴). 디코딩 왕복 검증 포함
- [ ] `go test ./orcanitrofeed/ -run TestGolden -v` PASS 확인 → commit

### Task 2: dispatcher (unix socket + ring buffer)

**Files:**
- Create: `orcanitrofeed/dispatcher.go`, `orcanitrofeed/dispatcher_test.go`, `orcanitrofeed/config.go`

**Interfaces (Produces):**

```go
type Config struct {
    Enable     bool   `koanf:"enable"`
    SocketPath string `koanf:"socket-path"`
    Mode       string `koanf:"mode"`        // "tx" | "block"
    BufferSize int    `koanf:"buffer-size"` // ring 슬롯 수, default 4096
}
func ConfigAddOptions(prefix string, f *pflag.FlagSet)
var DefaultConfig = Config{Enable: false, SocketPath: "", Mode: "tx", BufferSize: 4096}

type Dispatcher struct { ... }
func NewDispatcher(cfg *Config, mode string) (*Dispatcher, error) // listen 시작
func (d *Dispatcher) Enqueue(t MsgType, m msgp.Marshaler)  // 논블로킹, drop-oldest, seq 부여
func (d *Dispatcher) Close()
```

**동작 요구:**
- listener goroutine: accept → 연결마다 Hello frame 즉시 write → 구독 목록 등록
- writer goroutine 1개: ring에서 꺼내 marshal(→ pooled buf) → 모든 연결에 write.
  write 실패/블록 연결은 즉시 끊음 (로컬 클라이언트 전제 — per-conn 버퍼 없음)
- Enqueue: mutex ring, 가득 차면 oldest drop + drop 카운터 로그(1s 스로틀).
  seq는 enqueue 시점 단조증가 — drop돼도 gap으로 관측됨
- `// PERF:` 태그: Enqueue(LOCK), marshal buf(ALLOC — pool)

**Steps:**
- [ ] dispatcher_test 작성: (1) hello frame 수신·SchemaVersion 일치 (2) Enqueue한
      ReceiptMsg 프레임 왕복 (3) buffer overflow 시 drop-oldest + seq gap (4) 클라이언트
      없이 Enqueue해도 무블로킹. 실소켓은 `t.TempDir()` 경로 사용
- [ ] FAIL 확인 → 구현 → PASS → commit

### Task 3: collector (tracing.Hooks → TransferRecord)

**Files:**
- Create: `orcanitrofeed/collector.go`, `orcanitrofeed/collector_test.go`

**Interfaces (Produces):**

```go
// Collector는 한 tx 실행 동안의 transfer를 수집한다. 재사용(Reset) 가능.
type Collector struct { ... }
func NewCollector() *Collector
func (c *Collector) Hooks() *tracing.Hooks // OnEnter/OnExit/OnBalanceChange/CaptureArbitrumTransfer만 등록
func (c *Collector) Reset()
func (c *Collector) Drain() []TransferRecord // 수집분 반환 (내부 버퍼 재사용)
```

**수집 규칙:**
- OnEnter/OnExit로 depth·frame span 추적. ExitHook(reverted=true) 시 해당 frame span 내
  기록된 record에 `Reverted=true` 마킹 (조상 revert 전파 포함)
- OnBalanceChange: reason=BalanceChangeTransfer의 Sub(from)→Add(to) 인접 쌍을 하나의
  TransferRecord로 병합 (PostBalanceFrom=sub.new, PostBalanceTo=add.new). 쌍이 안 맞는
  단독 이벤트(mint/burn/selfdestruct 12·13·14)는 한쪽 zero addr로 단독 record.
  gas류 reason(5,6,7)·BalanceChangeRevert(15)·TouchAccount(11)는 무시
- CaptureArbitrumTransfer: EVM 밖 ArbOS 이체 → record (post balance는 이 시점 미상 → nil,
  Task 4의 observer가 tx 종료 시 statedb로 보충)
- 버퍼는 append-후-Reset 재사용, `// PERF:MEM-GROW` 태그

**Steps:**
- [ ] collector_test: 훅을 직접 호출하는 시나리오 테이블 — (1) 단순 transfer 쌍 병합
      (2) internal call depth 기록 (3) revert된 subtree 마킹 (4) mint/burn 단독
      (5) gas reason 무시. EVM 불필요 (순수 훅 호출)
- [ ] FAIL → 구현 → PASS → commit

### Task 4: BlockObserver + ProduceBlock 훅

**Files:**
- Create: `orcanitrofeed/observer.go`, `orcanitrofeed/observer_test.go`
- Modify: `arbos/block_processor.go` (ProduceBlock/ProduceBlockAdvanced 시그니처 + 3지점),
  호출처 7곳에 `nil` 또는 observer 전달

**Interfaces (Produces):**

```go
// BlockObserver: 한 블록 생성 lifecycle. nil observer = 완전 무변경 경로.
type BlockObserver struct { ... }
func NewBlockObserver(d *Dispatcher, mode string) *BlockObserver
// block_processor가 호출:
func (o *BlockObserver) BeginBlock(blockNumber uint64, l2Timestamp uint64)
func (o *BlockObserver) EVMHooks() *tracing.Hooks           // collector 훅
func (o *BlockObserver) OnTxAccepted(tx *types.Transaction, receipt *types.Receipt,
    statedb *state.StateDB, txIndex int)                    // receipts append 직후
func (o *BlockObserver) OnBlockSealed(block *types.Block)   // blockHash touch-up 직후
```

**block_processor 수정 (3지점, observer nil 가드):**
1. 시그니처: `ProduceBlock(..., exposeMultiGas bool, observer *orcanitrofeed.BlockObserver)` /
   `ProduceBlockAdvanced(..., addressChecker state.AddressChecker, observer *orcanitrofeed.BlockObserver)`.
   observer != nil이면 초입에서 `BeginBlock(header.Number.Uint64(), header.Time)`
2. EVM 생성부(:521): observer != nil이면
   `vm.NewEVM(blockContext, state.NewHookedState(buildState.statedb, observer.EVMHooks()), chainConfig, vm.Config{Tracer: observer.EVMHooks(), ExposeMultiGas: ...})`
3. `buildState.receipts = append` 직후: observer != nil && `tx.Type() != types.ArbitrumInternalTxType`
   이면 `observer.OnTxAccepted(tx, receipt, buildState.statedb, len(buildState.receipts)-1)`
4. blockHash touch-up 루프 직후: `observer.OnBlockSealed(tmpBlock)`

**OnTxAccepted 동작 (tx 모드):**
- collector Drain → ArbOS record의 nil post balance를 `statedb.GetBalance`로 보충
- `ToIsContract`: `statedb.GetCodeSize(to) > 0`, 블록 로컬 map 캐시 (BeginBlock에서 clear)
- ReceiptMsg 조립(BlockHash zero) → `d.Enqueue(MsgReceipt, msg)` → collector Reset
- block 모드에서는 조립만 하고 버퍼에 쌓음 (dispatch는 OnBlockSealed에서 일괄, BlockHash 채워서)
- OnBlockSealed(tx 모드): `BlockSealMsg` enqueue
- tx 실행 실패·rollback 경로(에러 continue)에서는 collector Reset만 (dispatch 없음) —
  group rollback(`rollbackToGroupCheckpoint`) 시 이미 dispatch된 redeem record는 tx 모드
  한계로 수용 (invalidation 불필요 — receipt 자체가 블록에 안 들어가므로 BlockSeal의
  TxCount와 대조해 클라이언트가 판별 가능하도록 TxCount 포함)

**Steps:**
- [ ] observer_test: 가짜 dispatcher(메모리 sink)로 OnTxAccepted→ReceiptMsg 필드 검증
      (from/to/value/calldata/logs/transfers/ToIsContract), tx·block 모드 분기
- [ ] FAIL → observer 구현 → PASS
- [ ] block_processor.go 수정 + 호출처 7곳 `nil` 전달, `go build ./...` 통과
- [ ] commit

### Task 5: 노드 config·engine 배선 (live 경로)

**Files:**
- Modify: `execution/gethexec/node.go` (Config에 `OrcaNitroFeed orcanitrofeed.Config koanf:"orca-nitro-feed"` +
  AddOptions + 기본값), `execution/gethexec/executionengine.go`

**배선:**
- gethexec `CreateExecutionNode`(node.go): cfg.OrcaNitroFeed.Enable이면 `NewDispatcher` 생성,
  `NewBlockObserver` → ExecutionEngine 필드 `orcaObserver *orcanitrofeed.BlockObserver`로 주입.
  Close는 node 종료 deferFuncs
- executionengine.go:1045 (`createBlockFromNextMessage` 내 ProduceBlock 호출):
  `isMsgForPrefetch == false`일 때만 observer 전달, prefetch·sequencer(:765,:998)·
  block_recorder는 nil 유지
- block 모드 dispatch는 observer가 OnBlockSealed에서 수행하므로 engine 추가 분기 불요.
  invalidation: `digestMessageWithBlockMutex`에서 `appendBlock` 에러 시
  `observer.OnAppendFailed(block.NumberU64())` → `InvalidationMsg` enqueue 후 기존 에러 반환
- 플래그 확인: `--execution.orca-nitro-feed.enable`, `--execution.orca-nitro-feed.socket-path`,
  `--execution.orca-nitro-feed.mode`

**Steps:**
- [ ] config 배선 + `go build ./...`
- [ ] `go run ./cmd/nitro --help | grep orca-nitro-feed`로 플래그 노출 확인
- [ ] commit

### Task 6: live 통합 테스트 (system_tests)

**Files:**
- Create: `system_tests/orcanitrofeed_test.go`

**시나리오 (`TestOrcaNitroFeedLiveDispatch`):**
- system_tests 빌더 패턴(기존 `common_test.go`의 `NewNodeBuilder` 사용례 모방)으로 L2 노드
  기동, `execConfig.OrcaNitroFeed = {Enable: true, SocketPath: t.TempDir()+"/orca.sock", Mode: "tx"}`
- 테스트 goroutine이 소켓 connect → hello 검증
- (1) 단순 value transfer tx 전송 → ReceiptMsg: from/to/value/status=1, Transfers 1건
  (depth 0, post balance = 체인 조회값과 일치), BlockSealMsg 수신
- (2) mocksgen 테스트 컨트랙트로 internal transfer 유발 (기존 system_tests에서 value
  전달 컨트랙트 검색해 재사용; 없으면 CREATE+CALL 조합 raw tx) → Transfers에 depth≥1 record
- (3) revert하는 호출 → Reverted=true record 또는 transfer 부재 확인
- seq 단조증가 검증

**Steps:**
- [ ] 테스트 작성 → `go test ./system_tests/ -run TestOrcaNitroFeedLiveDispatch -v` PASS
- [ ] commit

### Task 7: sweep 경로

**Files:**
- Modify: `blocks_reexecutor/blocks_reexecutor.go` (Config에 `OrcaNitroFeed orcanitrofeed.Config` 재사용
  불가 — dispatcher는 cmd에서 공유하므로 `SetObserverFactory` 주입 방식),
  `cmd/nitro/nitro.go:474` 부근
- Create: `orcanitrofeed/sweep_observer.go` (블록 단위: receipts+블록에서 ReceiptMsg 조립)

**배선:**
- `BlocksReExecutor`에 optional `tracerFactory func() (*tracing.Hooks, func(block *types.Block, receipts types.Receipts, statedb *state.StateDB))`
  주입 메서드 추가. `advanceStateUpToBlock`의 vmConfig에 worker별 hooks 설정,
  `AdvanceStateByBlock` 반환 직후 target range 내 블록이면 콜백 → ReceiptMsg 조립
  (BlockHash 포함, per-tx transfer는 collector가 OnTxStart/OnTxEnd로 tx 경계 분할 —
  collector에 `OnTxStart/OnTxEnd` 훅 추가 등록) → Enqueue
- 주의: advanceState는 "가용 state → target"까지 전진하며 range 밖 블록도 실행 —
  dispatch는 config range 내 블록만
- chunk 완료 시 `RangeDoneMsg{start, end}` enqueue
- cmd/nitro: BlocksReExecutor.Enable && OrcaNitroFeed.Enable이면 dispatcher(mode "sweep") 생성해
  주입. 기존 `--init.then-quit` 종료 경로에서 dispatcher Close(flush 대기)
- DB read-only: 기존 실행 경로 그대로 (reexecutor는 commit-state-to-disk=false면 무커밋) —
  별도 작업 불요

**Steps:**
- [ ] collector에 tx 경계 훅 추가 + 단위 테스트 갱신 → PASS
- [ ] reexecutor 주입 + cmd 배선, `go build ./...`
- [ ] 통합 테스트 `TestOrcaNitroFeedSweep`: system_tests로 N(~20)블록 생성 후 같은 datadir에서
      reexecutor 직접 구성(cmd 경유 없이 blocksreexecutor.New + observer) → 소켓 수신
      메시지가 live에서 수집한 것과 대응(blockHash 채움 차이만) 확인
- [ ] commit

### Task 8: CI 워크플로

**Files:**
- Create: `.github/workflows/orca-build.yml`, `.github/workflows/orca-check.yml`

**orca-check.yml** (pull_request → orca/main, push orca/main):
- ubuntu-24.04, submodule recursive checkout, Go 1.25.5 설치(actions/setup-go),
  `go vet ./orcanitrofeed/... ./execution/... ./arbos/...` + `go build ./...` +
  `go test ./orcanitrofeed/...`. brotli 등 cgo 의존으로 build 실패 시 범위를
  `./orcanitrofeed/...`로 축소하고 사유 주석

**orca-build.yml** (push tags `orca-v*`):
- ubuntu-24.04, dora-master `build_nitro.rs` 레시피 이식: apt deps → Go 1.25.5 tarball →
  rustup + cbindgen 0.29.2 → foundry 1.2.3 → emsdk 3.1.7 → node24+yarn →
  `make CBROTLI_WASM_BUILD_ARGS="" NITRO_VERSION=${{ github.ref_name }} target/bin/nitro`
- `sha256sum target/bin/nitro > nitro.sha256` → `gh release create ${{ github.ref_name }}
  target/bin/nitro nitro.sha256` (softprops/action-gh-release 또는 gh CLI)
- actions/cache: `~/go/pkg/mod`, `~/.cargo`, emsdk

**Steps:**
- [ ] 두 워크플로 작성 → commit → push → orca-check 실행 결과 확인 (gh run watch)
- [ ] 테스트 태그 `orca-v3.11.2-r0`로 orca-build 1회 검증 → release asset·sha256 확인 →
      필요시 수정 후 태그 재발행

### Task 9: 문서·마무리

- [ ] `docs/orca/README.md`: 플래그, 소켓 프로토콜(프레임·메시지 표), live/sweep 실행 예,
      Rust 클라이언트 디코딩 주석 (rmp-serde tuple struct 예시)
- [ ] `rg '@ai-draft'`로 PERF 태그 검수 상태 확인
- [ ] 전체 빌드·orca-nitro-feed 테스트 재실행 → PR 준비 (pull-request 규칙: pr_info.sh →
      한국어 본문 → self review). reviewer는 사용자에게 확인 필요하므로 PR 생성 전 보고

## Self-Review 결과

- 스펙 커버리지: 노드 구성(config opt-in)·tx/block 모드·전송계층·스키마·sweep·CI 모두 task 존재.
  titan EC2 실측은 코드 완료 후 별도 ops 단계(스펙의 "검증 계획")로 이 plan 밖.
- placeholder 없음. Task 간 타입 일치 확인 (TransferRecord/ReceiptMsg/Dispatcher 시그니처).
