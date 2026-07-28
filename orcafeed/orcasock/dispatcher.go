package orcasock

import (
	"github.com/offchainlabs/nitro/orcafeed"
)

import (
	"encoding/binary"
	"net"
	"os"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/tinylib/msgp/msgp"
)

const (
	frameHeaderSize  = 5 // u32 LE payload len + u8 orcafeed.MsgType
	connWriteTimeout = time.Second
	closeFlushCap    = 5 * time.Second
	dropLogThrottle  = time.Second
)

type queuedMsg struct {
	typ orcafeed.MsgType
	seq uint64
	msg msgp.Marshaler
}

// msgRing은 고정 용량 원형 큐 — 가득 차면 oldest를 버린다. 락은 호출자 몫.
type msgRing struct {
	buf     []queuedMsg
	head    int // 다음 pop 위치
	size    int
	dropped uint64
}

func newMsgRing(capacity int) *msgRing {
	return &msgRing{buf: make([]queuedMsg, capacity), head: 0, size: 0, dropped: 0}
}

func (r *msgRing) push(m queuedMsg) {
	if r.size == len(r.buf) {
		r.head = (r.head + 1) % len(r.buf)
		r.size--
		r.dropped++
	}
	r.buf[(r.head+r.size)%len(r.buf)] = m
	r.size++
}

func (r *msgRing) pop() (queuedMsg, bool) {
	if r.size == 0 {
		return queuedMsg{}, false
	}
	m := r.buf[r.head]
	r.buf[r.head] = queuedMsg{}
	r.head = (r.head + 1) % len(r.buf)
	r.size--
	return m, true
}

// Dispatcher는 unix socket 리스너 + drop-oldest ring + 단일 writer goroutine.
// Enqueue는 실행 핫패스에서 호출된다 — 직렬화·소켓 write는 writer가 전담한다.
type Dispatcher struct {
	mode       string
	socketPath string

	mu          sync.Mutex
	cond        *sync.Cond
	ring        *msgRing
	nextSeq     uint64
	closed      bool
	lastDropLog time.Time

	connMu sync.Mutex
	conns  map[net.Conn]struct{}

	listener net.Listener
	wg       sync.WaitGroup

	// PERF:ALLOC
	//   cost: mem=O(frame_size)·재사용, N~1e0/msg → pool로 상쇄
	//   note: writer의 marshal 프레임 버퍼
	bufPool sync.Pool
}

func NewDispatcher(cfg *orcafeed.Config, mode string) (*Dispatcher, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	// 이전 프로세스가 남긴 socket 파일 제거 (stale bind 방지)
	if err := os.Remove(cfg.SocketPath); err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	listener, err := net.Listen("unix", cfg.SocketPath)
	if err != nil {
		return nil, err
	}
	d := &Dispatcher{
		mode:        mode,
		socketPath:  cfg.SocketPath,
		ring:        newMsgRing(cfg.BufferSize),
		conns:       make(map[net.Conn]struct{}),
		listener:    listener,
		bufPool:     sync.Pool{New: func() any { return make([]byte, 0, 4096) }},
		mu:          sync.Mutex{},
		cond:        nil,
		nextSeq:     0,
		closed:      false,
		lastDropLog: time.Time{},
		connMu:      sync.Mutex{},
		wg:          sync.WaitGroup{},
	}
	d.cond = sync.NewCond(&d.mu)
	d.wg.Add(2)
	go d.acceptLoop()
	go d.writeLoop()
	return d, nil
}

// Enqueue는 seq를 부여해 ring에 넣는다. 논블로킹 — 가득 차면 oldest drop.
// msg는 enqueue 이후 수정하면 안 된다 (writer가 비동기로 직렬화).
func (d *Dispatcher) Enqueue(typ orcafeed.MsgType, msg orcafeed.SeqSetter) {
	// PERF:LOCK
	//   cost: hold~ns (push + signal)
	//   note: 실행 핫패스 유일한 동기화 지점
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return
	}
	seq := d.nextSeq
	d.nextSeq++
	msg.SetSeq(seq)
	prevDropped := d.ring.dropped
	d.ring.push(queuedMsg{typ: typ, seq: seq, msg: msg})
	dropped := d.ring.dropped
	shouldLog := dropped > prevDropped && time.Since(d.lastDropLog) > dropLogThrottle
	if shouldLog {
		d.lastDropLog = time.Now()
	}
	d.mu.Unlock()
	d.cond.Signal()
	if shouldLog {
		log.Warn("orcafeed: ring overflow, dropping oldest", "totalDropped", dropped)
	}
}

func (d *Dispatcher) acceptLoop() {
	defer d.wg.Done()
	for {
		conn, err := d.listener.Accept()
		if err != nil {
			return // listener closed
		}
		hello := orcafeed.Hello{SchemaVersion: orcafeed.SchemaVersion, Mode: d.mode}
		frame := d.encodeFrame(orcafeed.MsgHello, &hello)
		_ = conn.SetWriteDeadline(time.Now().Add(connWriteTimeout))
		if _, err := conn.Write(frame); err != nil {
			conn.Close()
			d.releaseBuf(frame)
			continue
		}
		d.releaseBuf(frame)
		d.connMu.Lock()
		d.conns[conn] = struct{}{}
		d.connMu.Unlock()
	}
}

func (d *Dispatcher) writeLoop() {
	defer d.wg.Done()
	for {
		d.mu.Lock()
		for d.ring.size == 0 && !d.closed {
			d.cond.Wait()
		}
		m, ok := d.ring.pop()
		closed := d.closed
		d.mu.Unlock()
		if !ok {
			if closed {
				return
			}
			continue
		}
		frame := d.encodeFrame(m.typ, m.msg)
		d.broadcast(frame)
		d.releaseBuf(frame)
	}
}

// encodeFrame: [u32 LE len][u8 type][payload]. 반환 버퍼는 releaseBuf로 반납.
func (d *Dispatcher) encodeFrame(typ orcafeed.MsgType, msg msgp.Marshaler) []byte {
	buf := d.bufPool.Get().([]byte)[:0]
	buf = append(buf, 0, 0, 0, 0, byte(typ))
	buf, err := msg.MarshalMsg(buf)
	if err != nil {
		// msgp 생성 코드는 인코딩 실패 경로가 사실상 없음 — 발생 시 스키마 버그
		log.Error("orcafeed: marshal failed", "err", err)
		return buf[:frameHeaderSize]
	}
	// #nosec G115
	binary.LittleEndian.PutUint32(buf[:4], uint32(len(buf)-frameHeaderSize))
	return buf
}

func (d *Dispatcher) releaseBuf(buf []byte) {
	d.bufPool.Put(buf[:0]) //nolint:staticcheck
}

func (d *Dispatcher) broadcast(frame []byte) {
	d.connMu.Lock()
	defer d.connMu.Unlock()
	for conn := range d.conns {
		_ = conn.SetWriteDeadline(time.Now().Add(connWriteTimeout))
		if _, err := conn.Write(frame); err != nil {
			// 로컬 클라이언트 전제 — 느리거나 죽은 연결은 즉시 정리 (per-conn 버퍼 없음)
			log.Warn("orcafeed: dropping slow/dead client", "err", err)
			conn.Close()
			delete(d.conns, conn)
		}
	}
}

// Close는 ring 잔량 flush를 closeFlushCap까지 기다린 뒤 정리한다 (sweep 종료 경로).
func (d *Dispatcher) Close() {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return
	}
	deadline := time.Now().Add(closeFlushCap)
	for d.ring.size > 0 && time.Now().Before(deadline) {
		d.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		d.mu.Lock()
	}
	d.closed = true
	d.mu.Unlock()
	d.cond.Broadcast()
	d.listener.Close()
	d.wg.Wait()
	d.connMu.Lock()
	for conn := range d.conns {
		conn.Close()
		delete(d.conns, conn)
	}
	d.connMu.Unlock()
	_ = os.Remove(d.socketPath)
}
