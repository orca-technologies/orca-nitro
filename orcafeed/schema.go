// Copyright 2026 Orca Technologies.
// orcafeed wire schema — msgpack array-mode (msgp tuple), lockstep 배포 전제.
// 필드 추가·변경 시 SchemaVersion을 반드시 bump하고 Rust 클라이언트와 함께 배포한다.
package orcafeed

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
	MsgBlockSeal    MsgType = 3 // BlockSealMsg — tx 모드에서 blockHash 보완
	MsgInvalidation MsgType = 4 // InvalidationMsg — block commit 실패 통지
	MsgRangeDone    MsgType = 5 // RangeDoneMsg — sweep chunk 완료 마커
)

// 디스패치 모드 문자열 (Hello.Mode)
const (
	ModeLiveTx    = "live-tx"
	ModeLiveBlock = "live-block"
	ModeSweep     = "sweep"
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
}

//msgp:tuple LogRecord
type LogRecord struct {
	Address [20]byte
	Topics  [][32]byte
	Data    []byte
}

//msgp:tuple ReceiptMsg
type ReceiptMsg struct {
	Seq               uint64
	BlockNumber       uint64
	BlockHash         [32]byte // tx 모드: zero (BlockSealMsg로 보완), block/sweep 모드: 채움
	TxIndex           uint32
	L2Timestamp       uint64
	TxHash            [32]byte
	TxType            uint8
	From              [20]byte
	To                [20]byte // contract creation: zero
	ToIsContract      bool
	ContractAddress   [20]byte // creation이 아니면 zero
	Nonce             uint64
	Gas               uint64
	EffectiveGasPrice []byte
	Value             []byte
	Calldata          []byte
	Status            uint64
	GasUsed           uint64
	CumulativeGasUsed uint64
	Logs              []LogRecord
	Transfers         []TransferRecord
	// 노드가 이 receipt를 방출한 시각 (unix ns). retention replay 시에도 원래
	// 방출 시각이 보존된다 — reader의 live feed ts 기준. sweep 재실행에선
	// 재실행 시각이므로 의미 없음 (reader가 l2 timestamp 합성 사용).
	EmittedAtNs uint64
}

//msgp:tuple BlockSealMsg
type BlockSealMsg struct {
	Seq         uint64
	BlockNumber uint64
	BlockHash   [32]byte
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
