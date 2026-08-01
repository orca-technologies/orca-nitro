// Copyright 2026 Orca Technologies.
// orca-nitro-feed wire schema — msgpack array-mode (msgp tuple), lockstep 배포 전제.
// 필드 추가·변경 시 SchemaVersion을 반드시 bump하고 Rust 클라이언트와 함께 배포한다.
package orcanitrofeed

//go:generate msgp -tests=false

// SchemaVersion은 hello frame으로 전달되어 클라이언트와의 lockstep 배포를 검증한다.
// 프로덕션 배포 전까지는 v1으로 고정한다 — 개발 중 필드 변경은 양쪽을 함께 고치고
// golden fixture만 재생성한다 (bump는 배포된 컨슈머가 생긴 뒤부터).
const SchemaVersion uint32 = 1

// 프레임 형식: [u32 LE payload length][u8 MsgType][msgpack payload]
type MsgType byte

const (
	MsgHello        MsgType = 1 // Hello
	MsgReceipt      MsgType = 2 // ReceiptMsg
	MsgBlockSeal    MsgType = 3 // BlockSealMsg — tx 모드 블록 경계·tx_count
	MsgInvalidation MsgType = 4 // InvalidationMsg — block commit 실패 통지
	MsgRangeDone    MsgType = 5 // RangeDoneMsg — sweep chunk 완료 마커
)

// 디스패치 모드 문자열 (Hello.Mode)
const (
	ModeLiveTx    = "live-tx"
	ModeLiveBlock = "live-block"
	ModeSweep     = "sweep"
)

// AccountKind — GetCodeSize(+선택적 GetCode)로 판별한 계정 종류.
// wire는 u8. Rust `types-evm-orca-nitro::schema::AccountKind`와 lockstep.
const (
	AccountKindEmpty    uint8 = 0 // GetCodeSize == 0
	AccountKindEip7702  uint8 = 1 // size==23 && ParseDelegation ok (0xef0100‖addr)
	AccountKindContract uint8 = 2 // 그 외 code 보유
)

//msgp:tuple Hello
type Hello struct {
	SchemaVersion uint32
	Mode          string
}

//msgp:tuple TransferRecord
type TransferRecord struct {
	Reason          uint8 // tracing.BalanceChangeReason
	From            [20]byte
	To              [20]byte // mint/burn은 zero addr
	Value           []byte   // big.Int BE bytes, 빈 슬라이스 = 0
	PostBalanceFrom []byte   // transfer 직후 from 잔액. nil = 미상
	PostBalanceTo   []byte   // transfer 직후 to 잔액. nil = 미상
	Depth           uint16   // 0 = top-level
	Reverted        bool     // revert된 subtree 내 transfer
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

//msgp:tuple CallRecord
// CallRecord — TARGET `(to, selector)` hit at OnEnter (TOKEN_METADATA D3).
// Not a full call dump: allowlist only. Shares InnerIndex with logs/transfers.
type CallRecord struct {
	To         [20]byte
	Selector   [4]byte
	Input      []byte // full call input (selector ‖ args)
	Value      []byte // call value, big.Int BE; empty = 0
	Depth      uint16
	Reverted   bool
	InnerIndex uint16
}

//msgp:tuple ReceiptMsg
type ReceiptMsg struct {
	Seq               uint64
	BlockNumber       uint64
	TxIndex           uint32
	L2Timestamp       uint64
	TxType            uint8
	From              [20]byte
	To                [20]byte // contract creation: zero
	FromAccountKind   uint8    // AccountKind*
	ToAccountKind     uint8    // AccountKind*; creation(to=zero)이면 Empty
	ContractAddress   [20]byte // creation이 아니면 zero
	Nonce             uint64
	Gas               uint64
	EffectiveGasPrice []byte
	Value             []byte
	Calldata          []byte
	Status            uint64
	GasUsed           uint64
	CumulativeGasUsed uint64
	// Arbitrum: L1 calldata posting에 쓰인 gas 분량 (receipt.GasUsedForL1).
	GasUsedForL1 uint64
	// Arbitrum: 이 L2 블록이 시퀀싱된 L1 block number.
	// live는 BeginBlock 시점 l1Info, sweep는 finalized header extra.
	L1BlockNumber uint64
	Logs          []LogRecord
	Transfers     []TransferRecord
	Calls         []CallRecord // TARGET allowlist hits (may be empty)
	// 노드가 이 receipt를 방출한 시각 (unix ns). retention replay 시에도 원래
	// 방출 시각이 보존된다 — reader의 live feed ts 기준. sweep 재실행에선
	// 재실행 시각이므로 의미 없음 (reader가 l2 timestamp 합성 사용).
	EmittedAtNs uint64
}

//msgp:tuple BlockSealMsg
type BlockSealMsg struct {
	Seq         uint64
	BlockNumber uint64
	TxCount     uint32 // 블록에 최종 포함된 tx 수 (internal tx 포함) — tx 모드 정합 확인용
}

//msgp:tuple InvalidationMsg
type InvalidationMsg struct {
	Seq         uint64
	BlockNumber uint64
}

//msgp:tuple RangeDoneMsg
type RangeDoneMsg struct {
	Seq        uint64
	StartBlock uint64
	EndBlock   uint64
}
