package orcasock

import (
	"github.com/offchainlabs/nitro/orcafeed"

	"encoding/binary"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func readFrame(t *testing.T, conn net.Conn) (orcafeed.MsgType, []byte) {
	t.Helper()
	header := make([]byte, 5)
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(conn, header); err != nil {
		t.Fatalf("frame header: %v", err)
	}
	payload := make([]byte, binary.LittleEndian.Uint32(header[:4]))
	if _, err := io.ReadFull(conn, payload); err != nil {
		t.Fatalf("frame payload: %v", err)
	}
	return orcafeed.MsgType(header[4]), payload
}

func newTestDispatcher(t *testing.T, bufferSize int) (*Dispatcher, string) {
	t.Helper()
	// t.TempDir()는 테스트명이 들어가 macOS unix socket 경로 한계(104B)를 넘는다
	dir, err := os.MkdirTemp("", "orca")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	sock := filepath.Join(dir, "orca.sock")
	cfg := orcafeed.DefaultConfig
	cfg.Enable = true
	cfg.SocketPath = sock
	cfg.BufferSize = bufferSize
	d, err := NewDispatcher(&cfg, orcafeed.ModeLiveTx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(d.Close)
	return d, sock
}

func TestDispatcherHelloAndRoundtrip(t *testing.T) {
	d, sock := newTestDispatcher(t, 16)

	conn, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	typ, payload := readFrame(t, conn)
	if typ != orcafeed.MsgHello {
		t.Fatalf("첫 프레임이 hello가 아님: %d", typ)
	}
	var hello orcafeed.Hello
	if _, err := hello.UnmarshalMsg(payload); err != nil {
		t.Fatal(err)
	}
	if hello.SchemaVersion != orcafeed.SchemaVersion || hello.Mode != orcafeed.ModeLiveTx {
		t.Fatalf("hello 불일치: %+v", hello)
	}

	sent := &orcafeed.ReceiptMsg{BlockNumber: 42, TxIndex: 1, Status: 1}
	d.Enqueue(orcafeed.MsgReceipt, sent)

	typ, payload = readFrame(t, conn)
	if typ != orcafeed.MsgReceipt {
		t.Fatalf("receipt 프레임 아님: %d", typ)
	}
	var got orcafeed.ReceiptMsg
	if _, err := got.UnmarshalMsg(payload); err != nil {
		t.Fatal(err)
	}
	if got.BlockNumber != 42 || got.Seq != 0 {
		t.Fatalf("receipt 불일치 (첫 seq는 0): %+v", got)
	}

	d.Enqueue(orcafeed.MsgBlockSeal, &orcafeed.BlockSealMsg{BlockNumber: 42})
	typ, payload = readFrame(t, conn)
	var seal orcafeed.BlockSealMsg
	if _, err := seal.UnmarshalMsg(payload); err != nil {
		t.Fatal(err)
	}
	if typ != orcafeed.MsgBlockSeal || seal.Seq != 1 {
		t.Fatalf("seq 단조증가 실패: typ=%d seq=%d", typ, seal.Seq)
	}
}

func TestDispatcherEnqueueWithoutClientDoesNotBlock(t *testing.T) {
	d, _ := newTestDispatcher(t, 8)
	done := make(chan struct{})
	go func() {
		for i := 0; i < 1000; i++ {
			d.Enqueue(orcafeed.MsgReceipt, &orcafeed.ReceiptMsg{BlockNumber: uint64(i)})
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("클라이언트 없이 Enqueue가 블로킹됨")
	}
}

func TestRingDropOldest(t *testing.T) {
	r := newMsgRing(4)
	for i := 0; i < 6; i++ {
		r.push(queuedMsg{seq: uint64(i)})
	}
	var seqs []uint64
	for {
		m, ok := r.pop()
		if !ok {
			break
		}
		seqs = append(seqs, m.seq)
	}
	// 6개 push, 용량 4 → oldest 2개(0,1) drop, 2..5 유지
	want := []uint64{2, 3, 4, 5}
	if len(seqs) != len(want) {
		t.Fatalf("남은 개수: got %v want %v", seqs, want)
	}
	for i := range want {
		if seqs[i] != want[i] {
			t.Fatalf("drop-oldest 순서: got %v want %v", seqs, want)
		}
	}
	if r.dropped != 2 {
		t.Fatalf("drop 카운터: got %d want 2", r.dropped)
	}
}
