# orca-nitro: 저지연 receipt dispatch

feed tx가 실행되는 즉시 enriched receipt(+native transfer 전체)를 로컬 unix socket으로
전달한다. 설계·결정 배경: `docs/superpowers/specs/2026-07-28-orca-receipt-dispatch-design.md`

## 실행

### Live (follower 노드)

```bash
nitro \
  --execution.orca-feed.enable \
  --execution.orca-feed.socket-path /run/orca/feed.sock \
  --execution.orca-feed.mode tx \
  ... # 기존 노드 플래그
```

| 플래그 | 기본 | 설명 |
|---|---|---|
| `--execution.orca-feed.enable` | false | dispatch 활성화 |
| `--execution.orca-feed.socket-path` | — | unix socket 경로 (필수) |
| `--execution.orca-feed.mode` | `tx` | `tx`: tx 실행 완료마다 즉시 (blockHash 없음, BlockSeal로 보완) / `block`: 블록 완성 직후 (blockHash 포함). 둘 다 DB commit **전** |
| `--execution.orca-feed.buffer-size` | 4096 | ring 슬롯 수 — 가득 차면 oldest drop |

### Sweep (과거 block range 재실행)

titan archive 스냅샷(hash 스킴) 복원 노드에서:

```bash
nitro \
  --blocks-reexecutor.enable \
  --blocks-reexecutor.mode full \
  --blocks-reexecutor.blocks '[[15078295, 21107733]]' \
  --blocks-reexecutor.room 16 \
  --init.then-quit \
  --execution.orca-feed.enable \
  --execution.orca-feed.socket-path /run/orca/sweep.sock \
  ... # datadir 등
```

- range를 chunk로 쪼개 `room`개 스레드가 병렬 재실행 → **out-of-order** dispatch
  (메시지의 BlockNumber로 consumer가 정렬·적재)
- chunk 완료마다 `RangeDoneMsg{start, end}` — 전체 완료는 chunk들의 합집합으로 판단
- 재실행 완료 후 프로세스 종료 (`--init.then-quit` 필수)

## Wire 프로토콜

frame = `[u32 LE payload len][u8 MsgType][msgpack payload]`

msgpack은 **array-mode**(필드명 없음, 위치 기반). 스키마 변경 시 `SchemaVersion` bump +
양쪽(lockstep) 동시 배포가 계약이다. 하위호환 없음.

| MsgType | 값 | 페이로드 | 발생 |
|---|---|---|---|
| Hello | 1 | `[schemaVersion u32, mode str]` | 연결 직후 1회. version 불일치 시 클라이언트는 즉시 종료해야 함 |
| Receipt | 2 | `ReceiptMsg` (아래) | tx당 1개 |
| BlockSeal | 3 | `[seq, blockNumber, blockHash, txCount]` | tx 모드에서 블록 seal 직후 |
| Invalidation | 4 | `[seq, blockNumber]` | DB commit 실패 (극히 드묾) — 해당 블록 receipt 무효 |
| RangeDone | 5 | `[seq, startBlock, endBlock]` | sweep chunk 완료 |

`seq`: dispatcher 단위 단조증가. gap = ring overflow drop 발생.

### ReceiptMsg 필드 (array 순서 = 스키마 순서)

`orcafeed/schema.go`가 SoT. 순서대로:
`seq, blockNumber, blockHash(32B, tx모드 zero), txIndex, l2Timestamp, txHash(32B),
txType, from(20B), to(20B), toIsContract, contractAddress(20B), nonce, gas,
effectiveGasPrice(BE bytes), value(BE bytes), calldata, status, gasUsed,
cumulativeGasUsed, logs[], transfers[]`

`LogRecord`: `[address(20B), topics[](32B), data]`

`TransferRecord`: `[reason u8, from(20B), to(20B), value(BE), postBalanceFrom(BE|nil),
postBalanceTo(BE|nil), depth u16, reverted bool]`

- top-level·internal transfer 통합. `depth`: 0 = top-level, 1+ = internal call depth
- `reason`: geth `tracing.BalanceChangeReason` — transfer(10), selfdestruct(12/13/14),
  deposit(129), withdrawToL1(130) 등. gas/fee/refund류는 수집하지 않음
  (정책: `orcafeed/collector.go` `includedReasons`)
- mint/burn은 한쪽 주소가 zero
- `reverted=true`: revert된 subtree 내 transfer (실제 반영 안 됨 — 신호로만 사용)
- `postBalance*`: transfer 적용 직후 잔액

### Rust 클라이언트 디코딩

array-mode = rmp-serde 기본 동작. tuple struct 그대로:

```rust
#[derive(Deserialize)]
struct TransferRecord {
    reason: u8,
    #[serde(with = "serde_bytes")] from: [u8; 20],
    #[serde(with = "serde_bytes")] to: [u8; 20],
    value: serde_bytes::ByteBuf,
    post_balance_from: Option<serde_bytes::ByteBuf>,
    post_balance_to: Option<serde_bytes::ByteBuf>,
    depth: u16,
    reverted: bool,
}
```

golden fixture: `orcafeed/testdata/golden_v1_*.bin` — Rust 쪽 디코더 검증에 사용
(dora-master 배포 시점 대조).

## 구현 지도

| 파일 | 역할 |
|---|---|
| `orcafeed/schema.go` | wire 스키마 (msgp 코드젠) + SchemaVersion |
| `orcafeed/collector.go` | tracing.Hooks → TransferRecord (pairing, depth, revert 전파) |
| `orcafeed/observer.go` | live 블록 lifecycle (BeginBlock/OnTxAccepted/OnBlockSealed) |
| `orcafeed/sweep_observer.go` | sweep 블록 단위 조립 |
| `orcafeed/orcasock/dispatcher.go` | unix socket listener + drop-oldest ring + writer (net 의존 분리 — wasm replay 오염 방지) |
| `arbos/block_processor.go` | ProduceBlockAdvanced 훅 지점 (observer nil이면 무변경) |
| `execution/gethexec/node.go` | config·dispatcher 생성 |
| `blocks_reexecutor/` | sweep tracer·dispatch 주입 |

성능 원칙: 실행 goroutine은 `Enqueue`(mutex push)만 수행, 직렬화·write는 전용
goroutine. `OnOpcode` 훅 미등록 — EVM 인터프리터 핫루프 무변경. tracer 오버헤드는
call frame·balance change 훅뿐 (실행 시간의 수 % 이내).

## 브랜치·릴리즈

- `master`: upstream 미러. `orca/main`: 기본 브랜치 (v3.11.2 베이스 + orca 패치)
- 릴리즈: `orca-v3.11.2-rN` 태그 push → `.github/workflows/orca-build.yml`이
  ubuntu-24.04 바이너리 + sha256을 GitHub Release asset으로 업로드
- upstream 업그레이드: 새 공식 태그에 패치 rebase (`orca/rebase/vX.Y.Z`) 후 orca/main 갱신
