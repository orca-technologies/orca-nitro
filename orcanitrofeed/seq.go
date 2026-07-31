package orcanitrofeed

import "github.com/tinylib/msgp/msgp"

// SeqSetter — Dispatcher.Enqueue가 부여하는 단조증가 seq를 받는 메시지.
// Hello는 seq가 없다 (연결별 직접 write).
type SeqSetter interface {
	msgp.Marshaler
	SetSeq(uint64)
}

func (m *ReceiptMsg) SetSeq(s uint64)      { m.Seq = s }
func (m *BlockSealMsg) SetSeq(s uint64)    { m.Seq = s }
func (m *InvalidationMsg) SetSeq(s uint64) { m.Seq = s }
func (m *RangeDoneMsg) SetSeq(s uint64)    { m.Seq = s }
