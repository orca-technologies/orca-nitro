package orcasock

import (
	"encoding/binary"
	"net"
	"os"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/tinylib/msgp/msgp"

	"github.com/offchainlabs/nitro/orcafeed"
)

const (
	frameHeaderSize  = 5 // u32 LE payload len + u8 MsgType
	connWriteTimeout = 5 * time.Second
	closeFlushCap    = 5 * time.Second
	dropLogThrottle  = time.Second
)

type queuedMsg struct {
	typ orcafeed.MsgType
	seq uint64
	msg orcafeed.SeqSetter
}

// msgRing — Enqueue(핫패스)와 writer 사이의 staging 원형 큐. 가득 차면 oldest를
// 버린다 (writer가 인코딩을 못 따라가는 극단 상황의 방어선). 락은 호출자 몫.
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

// frameLog — 인코딩된 프레임의 바이트 예산 retention log. 클라이언트가 없어도
// 보관하고, 예산 초과 시 oldest부터 버린다 (컨슈머 다운타임 무손실 재접속용).
// PERF:MEM-GROW
//
//	cost: mem=O(budget), budget 기본 1GiB (config)
//	note: 컨슈머 backlog 보관 — drop-oldest로 상한 고정
type frameLog struct {
	frames  [][]byte
	start   uint64 // frames[0]의 전역 인덱스
	bytes   int
	budget  int
	dropped uint64
}

func (l *frameLog) end() uint64 {
	// #nosec G115
	return l.start + uint64(len(l.frames))
}

func (l *frameLog) push(f []byte) {
	l.frames = append(l.frames, f)
	l.bytes += len(f)
	for l.bytes > l.budget && len(l.frames) > 1 {
		l.bytes -= len(l.frames[0])
		l.frames[0] = nil
		l.frames = l.frames[1:]
		l.start++
		l.dropped++
	}
	// 앞부분을 slice-off해도 backing array는 유지되므로 주기적으로 압축
	if cap(l.frames) > 4096 && len(l.frames)*2 < cap(l.frames) {
		l.frames = append(make([][]byte, 0, len(l.frames)*2), l.frames...)
	}
}

// Dispatcher — unix socket 리스너 + staging ring + retention log.
// 실행 핫패스는 Enqueue(mutex push)만 수행하고, 인코딩·보관·소켓 write는
// writer/sender goroutine이 전담한다. 연결마다 sender가 log 커서를 따라가며
// 백로그 replay 후 live로 이어붙는다.
type Dispatcher struct {
	mode       string
	socketPath string

	mu          sync.Mutex
	stagingCond *sync.Cond // staging에 새 메시지 or closed
	logCond     *sync.Cond // retention log append or closed
	staging     *msgRing
	retained    *frameLog
	nextSeq     uint64
	closed      bool
	lastDropLog time.Time

	connMu sync.Mutex
	conns  map[net.Conn]struct{}

	listener net.Listener
	wg       sync.WaitGroup // accept + writer
	connWg   sync.WaitGroup // per-conn sender
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
	// 다계정 컨슈머 허용: unix socket connect는 write 권한이 필요하다.
	// 노드 계정 분리(기록기=ubuntu, executor=rudy) 전제 — 접근 통제는 호스트
	// 계정/SG가 담당하므로 0666으로 연다.
	if err := os.Chmod(cfg.SocketPath, 0o666); err != nil {
		listener.Close()
		return nil, err
	}
	d := &Dispatcher{
		mode:        mode,
		socketPath:  cfg.SocketPath,
		staging:     newMsgRing(cfg.BufferSize),
		retained:    &frameLog{frames: nil, start: 0, bytes: 0, budget: cfg.BufferBytes, dropped: 0},
		conns:       make(map[net.Conn]struct{}),
		listener:    listener,
		mu:          sync.Mutex{},
		stagingCond: nil,
		logCond:     nil,
		nextSeq:     0,
		closed:      false,
		lastDropLog: time.Time{},
		connMu:      sync.Mutex{},
		wg:          sync.WaitGroup{},
		connWg:      sync.WaitGroup{},
	}
	d.stagingCond = sync.NewCond(&d.mu)
	d.logCond = sync.NewCond(&d.mu)
	d.wg.Add(2)
	go d.acceptLoop()
	go d.writeLoop()
	return d, nil
}

// Enqueue는 seq를 부여해 staging에 넣는다. 논블로킹 — 가득 차면 oldest drop.
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
	prevDropped := d.staging.dropped
	d.staging.push(queuedMsg{typ: typ, seq: seq, msg: msg})
	dropped := d.staging.dropped
	shouldLog := dropped > prevDropped && time.Since(d.lastDropLog) > dropLogThrottle
	if shouldLog {
		d.lastDropLog = time.Now()
	}
	d.mu.Unlock()
	d.stagingCond.Signal()
	if shouldLog {
		log.Warn("orcafeed: staging ring overflow, dropping oldest", "totalDropped", dropped)
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
		frame := encodeFrame(orcafeed.MsgHello, &hello)
		_ = conn.SetWriteDeadline(time.Now().Add(connWriteTimeout))
		if _, err := conn.Write(frame); err != nil {
			conn.Close()
			continue
		}
		d.connMu.Lock()
		d.conns[conn] = struct{}{}
		d.connMu.Unlock()
		d.connWg.Add(1)
		go d.sendLoop(conn)
	}
}

// writeLoop — staging에서 꺼내 인코딩하고 retention log에 보관한다.
func (d *Dispatcher) writeLoop() {
	defer d.wg.Done()
	for {
		d.mu.Lock()
		for d.staging.size == 0 && !d.closed {
			d.stagingCond.Wait()
		}
		m, ok := d.staging.pop()
		if !ok && d.closed {
			d.mu.Unlock()
			d.logCond.Broadcast()
			return
		}
		d.mu.Unlock()
		if !ok {
			continue
		}
		// PERF:ALLOC
		//   cost: mem=O(frame_size)/msg, N~1e1..1e3/s → retention 보관용이라 pool 불가
		//   note: 프레임 버퍼는 frameLog가 소유 (drop-oldest로 수명 관리)
		frame := encodeFrame(m.typ, m.msg)
		d.mu.Lock()
		d.retained.push(frame)
		d.mu.Unlock()
		d.logCond.Broadcast()
	}
}

// sendLoop — 연결별 sender. retention log 커서를 따라가며 backlog replay 후
// live tail. 예산 초과로 밀린 구간은 건너뛴다 (클라이언트는 seq gap으로 감지).
func (d *Dispatcher) sendLoop(conn net.Conn) {
	defer d.connWg.Done()
	defer func() {
		conn.Close()
		d.connMu.Lock()
		delete(d.conns, conn)
		d.connMu.Unlock()
	}()

	d.mu.Lock()
	cursor := d.retained.start // 보관 중인 가장 오래된 것부터 replay
	d.mu.Unlock()

	batch := make([][]byte, 0, 256)
	for {
		d.mu.Lock()
		for cursor >= d.retained.end() && !d.closed {
			d.logCond.Wait()
		}
		if cursor >= d.retained.end() && d.closed {
			d.mu.Unlock()
			return
		}
		if cursor < d.retained.start {
			cursor = d.retained.start // 예산 초과로 밀림 — gap
		}
		batch = batch[:0]
		batch = append(batch, d.retained.frames[cursor-d.retained.start:]...)
		// #nosec G115
		cursor += uint64(len(batch))
		d.mu.Unlock()

		for _, f := range batch {
			_ = conn.SetWriteDeadline(time.Now().Add(connWriteTimeout))
			if _, err := conn.Write(f); err != nil {
				// 로컬 클라이언트 전제 — 죽었거나 심하게 느린 연결은 정리 (재접속 시 replay)
				log.Warn("orcafeed: dropping slow/dead client", "err", err)
				return
			}
		}
	}
}

// encodeFrame: [u32 LE len][u8 type][payload]. 반환 버퍼는 retention log가 소유.
func encodeFrame(typ orcafeed.MsgType, msg msgp.Marshaler) []byte {
	buf := make([]byte, frameHeaderSize, frameHeaderSize+256)
	buf[4] = byte(typ)
	buf, err := msg.MarshalMsg(buf)
	if err != nil {
		// msgp 생성 코드는 인코딩 실패 경로가 사실상 없음 — 발생 시 스키마 버그
		log.Error("orcafeed: marshal failed", "err", err)
		buf = buf[:frameHeaderSize]
	}
	// #nosec G115
	binary.LittleEndian.PutUint32(buf[:4], uint32(len(buf)-frameHeaderSize))
	return buf
}

// Close는 staging 잔량의 인코딩·보관과 연결된 클라이언트의 catch-up을
// closeFlushCap까지 기다린 뒤 정리한다.
func (d *Dispatcher) Close() {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return
	}
	deadline := time.Now().Add(closeFlushCap)
	for d.staging.size > 0 && time.Now().Before(deadline) {
		d.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		d.mu.Lock()
	}
	d.closed = true
	d.mu.Unlock()
	d.stagingCond.Broadcast()
	d.logCond.Broadcast()
	d.listener.Close()
	d.wg.Wait()

	// sender들의 catch-up 대기 (cap 초과 시 강제 종료)
	done := make(chan struct{})
	go func() { d.connWg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(closeFlushCap):
		d.connMu.Lock()
		for conn := range d.conns {
			conn.Close()
		}
		d.connMu.Unlock()
		<-done
	}
	_ = os.Remove(d.socketPath)
}
