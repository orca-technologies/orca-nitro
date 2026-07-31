package orcasock

import (
	"github.com/offchainlabs/nitro/orcanitrofeed"

	"encoding/binary"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func readFrame(t *testing.T, conn net.Conn) (orcanitrofeed.MsgType, []byte) {
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
	return orcanitrofeed.MsgType(header[4]), payload
}

// t.TempDir()는 테스트명이 들어가 macOS unix socket 경로 한계(104B)를 넘는다
func mustTempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "orca")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

func newTestDispatcher(t *testing.T, bufferSize int) (*Dispatcher, string) {
	t.Helper()
	sock := filepath.Join(mustTempDir(t), "orca.sock")
	cfg := orcanitrofeed.DefaultConfig
	cfg.Enable = true
	cfg.SocketPath = sock
	cfg.BufferSize = bufferSize
	d, err := NewDispatcher(&cfg, orcanitrofeed.ModeLiveTx)
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
	if typ != orcanitrofeed.MsgHello {
		t.Fatalf("첫 프레임이 hello가 아님: %d", typ)
	}
	var hello orcanitrofeed.Hello
	if _, err := hello.UnmarshalMsg(payload); err != nil {
		t.Fatal(err)
	}
	if hello.SchemaVersion != orcanitrofeed.SchemaVersion || hello.Mode != orcanitrofeed.ModeLiveTx {
		t.Fatalf("hello 불일치: %+v", hello)
	}

	sent := &orcanitrofeed.ReceiptMsg{BlockNumber: 42, TxIndex: 1, Status: 1}
	d.Enqueue(orcanitrofeed.MsgReceipt, sent)

	typ, payload = readFrame(t, conn)
	if typ != orcanitrofeed.MsgReceipt {
		t.Fatalf("receipt 프레임 아님: %d", typ)
	}
	var got orcanitrofeed.ReceiptMsg
	if _, err := got.UnmarshalMsg(payload); err != nil {
		t.Fatal(err)
	}
	if got.BlockNumber != 42 || got.Seq != 0 {
		t.Fatalf("receipt 불일치 (첫 seq는 0): %+v", got)
	}

	d.Enqueue(orcanitrofeed.MsgBlockSeal, &orcanitrofeed.BlockSealMsg{BlockNumber: 42})
	typ, payload = readFrame(t, conn)
	var seal orcanitrofeed.BlockSealMsg
	if _, err := seal.UnmarshalMsg(payload); err != nil {
		t.Fatal(err)
	}
	if typ != orcanitrofeed.MsgBlockSeal || seal.Seq != 1 {
		t.Fatalf("seq 단조증가 실패: typ=%d seq=%d", typ, seal.Seq)
	}
}

func TestDispatcherEnqueueWithoutClientDoesNotBlock(t *testing.T) {
	d, _ := newTestDispatcher(t, 8)
	done := make(chan struct{})
	go func() {
		for i := 0; i < 1000; i++ {
			d.Enqueue(orcanitrofeed.MsgReceipt, &orcanitrofeed.ReceiptMsg{BlockNumber: uint64(i)})
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

// 컨슈머 다운타임 동안 보관 → 재접속 시 backlog 전체 replay
func TestDispatcherRetentionReplay(t *testing.T) {
	d, sock := newTestDispatcher(t, 4096)

	for i := 0; i < 100; i++ {
		d.Enqueue(orcanitrofeed.MsgReceipt, &orcanitrofeed.ReceiptMsg{BlockNumber: uint64(i)})
	}
	time.Sleep(200 * time.Millisecond) // writer가 staging → retention 옮길 시간

	conn, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	typ, _ := readFrame(t, conn)
	if typ != orcanitrofeed.MsgHello {
		t.Fatalf("hello 먼저: %d", typ)
	}
	for i := 0; i < 100; i++ {
		typ, payload := readFrame(t, conn)
		if typ != orcanitrofeed.MsgReceipt {
			t.Fatalf("receipt 아님: %d", typ)
		}
		var m orcanitrofeed.ReceiptMsg
		if _, err := m.UnmarshalMsg(payload); err != nil {
			t.Fatal(err)
		}
		if m.Seq != uint64(i) || m.BlockNumber != uint64(i) {
			t.Fatalf("replay 순서/내용 불일치: seq=%d blk=%d want %d", m.Seq, m.BlockNumber, i)
		}
	}

	// 이후 live 이어붙임
	d.Enqueue(orcanitrofeed.MsgReceipt, &orcanitrofeed.ReceiptMsg{BlockNumber: 100})
	_, payload := readFrame(t, conn)
	var m orcanitrofeed.ReceiptMsg
	if _, err := m.UnmarshalMsg(payload); err != nil {
		t.Fatal(err)
	}
	if m.Seq != 100 {
		t.Fatalf("live 이어붙임 실패: %+v", m)
	}
}

// 바이트 예산 초과 시 oldest 제거 → 재접속 클라이언트는 seq gap으로 감지
func TestDispatcherByteBudgetEviction(t *testing.T) {
	sock := filepath.Join(mustTempDir(t), "orca.sock")
	cfg := orcanitrofeed.DefaultConfig
	cfg.Enable = true
	cfg.SocketPath = sock
	cfg.BufferSize = 4096
	cfg.BufferBytes = 4096 // 아주 작은 예산 — 대부분 evict
	cfg.BufferAge = 0      // age 끔 — 바이트 예산만 검증
	d, err := NewDispatcher(&cfg, orcanitrofeed.ModeLiveTx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(d.Close)

	payload := make([]byte, 200)
	for i := 0; i < 200; i++ {
		d.Enqueue(orcanitrofeed.MsgReceipt, &orcanitrofeed.ReceiptMsg{BlockNumber: uint64(i), Calldata: payload})
	}
	time.Sleep(300 * time.Millisecond)

	conn, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	typ, _ := readFrame(t, conn)
	if typ != orcanitrofeed.MsgHello {
		t.Fatalf("hello 먼저: %d", typ)
	}
	_, p := readFrame(t, conn)
	var first orcanitrofeed.ReceiptMsg
	if _, err := first.UnmarshalMsg(p); err != nil {
		t.Fatal(err)
	}
	if first.Seq == 0 {
		t.Fatal("예산 초과인데 oldest가 남아있음 (evict 실패)")
	}
	// 남은 프레임 전부 소진 — 마지막이 199여야 함
	last := first
	for {
		if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
			t.Fatal(err)
		}
		header := make([]byte, 5)
		if _, err := io.ReadFull(conn, header); err != nil {
			break // timeout = 소진
		}
		pl := make([]byte, binary.LittleEndian.Uint32(header[:4]))
		if _, err := io.ReadFull(conn, pl); err != nil {
			t.Fatal(err)
		}
		if _, err := last.UnmarshalMsg(pl); err != nil {
			t.Fatal(err)
		}
	}
	if last.Seq != 199 {
		t.Fatalf("최신이 보존돼야 함: last seq=%d", last.Seq)
	}
}

// age 초과 시 oldest 제거 — 재접속 클라이언트는 seq gap으로 감지
func TestDispatcherAgeEviction(t *testing.T) {
	sock := filepath.Join(mustTempDir(t), "orca.sock")
	cfg := orcanitrofeed.DefaultConfig
	cfg.Enable = true
	cfg.SocketPath = sock
	cfg.BufferSize = 4096
	cfg.BufferBytes = 1 << 20
	cfg.BufferAge = 80 * time.Millisecond
	d, err := NewDispatcher(&cfg, orcanitrofeed.ModeLiveTx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(d.Close)

	for i := 0; i < 10; i++ {
		d.Enqueue(orcanitrofeed.MsgReceipt, &orcanitrofeed.ReceiptMsg{BlockNumber: uint64(i)})
	}
	time.Sleep(200 * time.Millisecond) // age 초과

	// 새 메시지 push로 trim이 돌고, 이후 접속 시 오래된 backlog는 없어야 함
	d.Enqueue(orcanitrofeed.MsgReceipt, &orcanitrofeed.ReceiptMsg{BlockNumber: 10})
	time.Sleep(50 * time.Millisecond)

	conn, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	typ, _ := readFrame(t, conn)
	if typ != orcanitrofeed.MsgHello {
		t.Fatalf("hello 먼저: %d", typ)
	}
	_, p := readFrame(t, conn)
	var first orcanitrofeed.ReceiptMsg
	if _, err := first.UnmarshalMsg(p); err != nil {
		t.Fatal(err)
	}
	if first.Seq < 10 {
		t.Fatalf("age 초과 backlog가 남아있음: first seq=%d", first.Seq)
	}
	if first.BlockNumber != 10 {
		t.Fatalf("최신만 남아야 함: %+v", first)
	}
}
