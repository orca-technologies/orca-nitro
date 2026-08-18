package orcanitrofeed

import (
	"bytes"
	"flag"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/tinylib/msgp/msgp"
)

var updateGolden = flag.Bool("update", false, "update golden fixtures")

// 고정 샘플 — 값 변경 금지 (Rust 클라이언트 fixture와 dora-master 배포 시 대조)
func goldenMessages() map[string]msgp.Marshaler {
	addr := func(b byte) (a [20]byte) { a[0], a[19] = b, b; return }
	hash := func(b byte) (h [32]byte) { h[0], h[31] = b, b; return }
	return map[string]msgp.Marshaler{
		"hello": &Hello{SchemaVersion: SchemaVersion, Mode: ModeLiveTx},
		"receipt": &ReceiptMsg{
			Seq: 7, BlockNumber: 123456, TxIndex: 2,
			L2Timestamp: 1753689600, TxType: 2,
			From: addr(0x01), To: addr(0x02),
			FromAccountKind: AccountKindEmpty, ToAccountKind: AccountKindContract,
			Nonce: 9, Gas: 21000, EffectiveGasPrice: []byte{0x3b, 0x9a, 0xca, 0x00},
			Value:    []byte{0x0d, 0xe0, 0xb6, 0xb3, 0xa7, 0x64, 0x00, 0x00},
			Calldata: []byte{0xde, 0xad, 0xbe, 0xef}, Status: 1,
			GasUsed: 21000, CumulativeGasUsed: 42000,
			GasUsedForL1: 66, L1BlockNumber: 25_000_000,
			Logs: []LogRecord{{Address: addr(0x03), Topics: [][32]byte{hash(0xcc)}, Data: []byte{0x01}, InnerIndex: 1}},
			Transfers: []TransferRecord{{
				Reason: 10, From: addr(0x01), To: addr(0x02),
				Value:           []byte{0x0d, 0xe0, 0xb6, 0xb3, 0xa7, 0x64, 0x00, 0x00},
				PostBalanceFrom: []byte{0x01}, PostBalanceTo: []byte{0x02},
				Depth: 0, Reverted: false, InnerIndex: 0,
			}},
			Calls: []WhitelistedCallRecord{{
				To: addr(0xeb), Selector: [4]byte{0x88, 0x2d, 0xb7, 0x07},
				Input: []byte{0x88, 0x2d, 0xb7, 0x07, 0xca}, Value: nil,
				Depth: 1, Reverted: false, InnerIndex: 2,
				// v4: frame span exclusive 상한 커버리지.
				ExitInnerIndex: 5,
			}, {
				// v3: revert된 시도 — RevertReason 커버리지.
				To: addr(0x88), Selector: [4]byte{0x35, 0x93, 0x56, 0x4c},
				Input: []byte{0x35, 0x93, 0x56, 0x4c, 0x01}, Value: []byte{0x64},
				Depth: 0, Reverted: true, InnerIndex: 3,
				RevertReason:   []byte{0x08, 0xc3, 0x79, 0xa0, 0xaa},
				ExitInnerIndex: 4,
			}},
			EmittedAtNs:        1_753_689_600_123_456_789,
			SameTimestampIndex: 2,
			// v3: top-level revert output 커버리지 (Status=1 샘플이지만 wire 형상 고정 목적).
			RevertOutput: []byte{0x08, 0xc3, 0x79, 0xa0, 0xbb},
		},
		"blockseal": &BlockSealMsg{
			Seq: 8, BlockNumber: 123456, TxCount: 3,
			SameTimestampIndex: 2, ReceiptCount: 2,
			L1BlockNumber: 25_000_000, L2Timestamp: 1_786_000_000,
		},
		"invalidation": &InvalidationMsg{Seq: 9, BlockNumber: 123456},
		"rangedone":    &RangeDoneMsg{Seq: 10, StartBlock: 100, EndBlock: 200},
	}
}

func TestGolden(t *testing.T) {
	for name, msg := range goldenMessages() {
		t.Run(name, func(t *testing.T) {
			enc, err := msg.MarshalMsg(nil)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			path := filepath.Join("testdata", "golden_v1_"+name+".bin")
			if *updateGolden {
				if err := os.MkdirAll("testdata", 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, enc, 0o644); err != nil {
					t.Fatal(err)
				}
			}
			want, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("fixture 읽기 실패 (최초 생성은 -update): %v", err)
			}
			if !bytes.Equal(enc, want) {
				t.Fatalf("인코딩이 golden fixture와 다름 — 스키마 변경 시 SchemaVersion bump + -update 필요\n got: %x\nwant: %x", enc, want)
			}

			decoded := reflect.New(reflect.TypeOf(msg).Elem()).Interface().(msgp.Unmarshaler)
			rest, err := decoded.UnmarshalMsg(enc)
			if err != nil || len(rest) != 0 {
				t.Fatalf("unmarshal: err=%v rest=%d", err, len(rest))
			}
			if !reflect.DeepEqual(msg, decoded) {
				t.Fatalf("왕복 불일치\n got: %+v\nwant: %+v", decoded, msg)
			}
		})
	}
}
