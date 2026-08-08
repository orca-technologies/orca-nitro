package orcasock

import (
	"encoding/binary"
	"net"
	"os"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/tinylib/msgp/msgp"

	"github.com/offchainlabs/nitro/orcanitrofeed"
)

const (
	frameHeaderSize  = 5 // u32 LE payload len + u8 MsgType
	connWriteTimeout = 5 * time.Second
	closeFlushCap    = 5 * time.Second
	// sweep(무손실) 모드 flush 상한 — 잔여 backlog(≤ buffer-bytes)를 로컬 소켓으로
	// 밀어내는 시간이면 충분히 크다.
	closeFlushCapNoDrop = 10 * time.Minute
	dropLogThrottle     = time.Second
)

type queuedMsg struct {
	typ orcanitrofeed.MsgType
	seq uint64
	msg orcanitrofeed.SeqSetter
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

type retainedFrame struct {
	data []byte
	at   time.Time // retention 시각 (wall clock) — age eviction용
}

// frameLog — 인코딩된 프레임 retention log. 클라이언트가 없어도 보관하고,
// 바이트 예산·최대 age 중 먼저 닿는 쪽부터 oldest를 버린다.
// PERF:MEM-GROW
//
//	cost: mem=O(min(budget, throughput×age)), budget 기본 1GiB / age 기본 30m
//	note: 컨슈머 backlog 보관 — drop-oldest로 상한 고정
type frameLog struct {
	frames []retainedFrame
	start  uint64 // frames[0]의 전역 인덱스
	bytes  int
	budget int
	maxAge time.Duration // 0 = age 제한 없음
	// noDrop(sweep): age·budget 기반 drop 금지 — trim은 소비-trim(trimConsumed)만.
	// budget은 writeLoop의 역압 임계로만 쓰인다.
	noDrop  bool
	dropped uint64
}

func (l *frameLog) end() uint64 {
	// #nosec G115
	return l.start + uint64(len(l.frames))
}

// popFront — 맨 앞 프레임 제거 (유실 카운트 없음 — 소비 완료 trim용).
func (l *frameLog) popFront() {
	l.bytes -= len(l.frames[0].data)
	l.frames[0] = retainedFrame{}
	l.frames = l.frames[1:]
	l.start++
}

func (l *frameLog) dropOldest() {
	l.popFront()
	l.dropped++
}

// trim — 바이트 예산·age 초과분 drop. age는 마지막 프레임까지 비울 수 있고,
// 바이트 예산은 최소 1프레임은 남긴다 (단일 거대 프레임 허용).
// noDrop 모드에서는 유실성 trim을 하지 않는다 (compaction만).
func (l *frameLog) trim(now time.Time) {
	for len(l.frames) > 0 && !l.noDrop {
		if l.maxAge > 0 && now.Sub(l.frames[0].at) > l.maxAge {
			l.dropOldest()
			continue
		}
		if l.bytes > l.budget && len(l.frames) > 1 {
			l.dropOldest()
			continue
		}
		break
	}
	if cap(l.frames) > 4096 && len(l.frames)*2 < cap(l.frames) {
		l.frames = append(make([]retainedFrame, 0, len(l.frames)*2), l.frames...)
	}
}

func (l *frameLog) push(f []byte, now time.Time) {
	l.frames = append(l.frames, retainedFrame{data: f, at: now})
	l.bytes += len(f)
	l.trim(now)
}

// Dispatcher — unix socket 리스너 + staging ring + retention log.
// 실행 핫패스는 Enqueue(mutex push)만 수행하고, 인코딩·보관·소켓 write는
// writer/sender goroutine이 전담한다. 연결마다 sender가 log 커서를 따라가며
// 백로그 replay 후 live로 이어붙는다.
//
// 모드별 정책:
//   - live: 완전 비블로킹 — staging·retention 모두 drop-oldest (지연 < 완전성).
//   - sweep(noDrop): 무손실 — 모든 연결이 소비한 프레임만 trim하고, 예산 초과 시
//     writer→Enqueue로 역압이 걸려 재실행 자체가 스로틀된다. 컨슈머가 없으면
//     예산까지 보관 후 정지한다 (컨슈머 접속 시 재개).
type Dispatcher struct {
	mode       string
	noDrop     bool
	socketPath string

	mu          sync.Mutex
	stagingCond *sync.Cond // staging에 새 메시지 or closed
	logCond     *sync.Cond // retention log append or closed
	flowCond    *sync.Cond // noDrop: staging pop·cursor 전진 (역압 해제 신호)
	staging     *msgRing
	retained    *frameLog
	cursors     map[net.Conn]uint64 // sender별 전송 커서 (noDrop trim 기준)
	nextSeq     uint64
	closed      bool
	lastDropLog time.Time

	connMu sync.Mutex
	conns  map[net.Conn]struct{}

	listener net.Listener
	wg       sync.WaitGroup // accept + writer
	connWg   sync.WaitGroup // per-conn sender
}

// minCursor — 모든 활성 sender가 이미 보낸 프레임의 하한. 연결이 없으면 (0,false).
// 호출자는 d.mu를 잡고 있어야 한다.
func (d *Dispatcher) minCursor() (uint64, bool) {
	if len(d.cursors) == 0 {
		return 0, false
	}
	min := ^uint64(0)
	for _, c := range d.cursors {
		if c < min {
			min = c
		}
	}
	return min, true
}

// trimConsumed — noDrop: 모든 연결이 소비한 프레임만 버린다. 연결이 없으면 보존.
// 호출자는 d.mu를 잡고 있어야 한다.
func (d *Dispatcher) trimConsumed() {
	min, ok := d.minCursor()
	if !ok {
		return
	}
	for len(d.retained.frames) > 0 && d.retained.start < min {
		d.retained.popFront()
	}
}

func NewDispatcher(cfg *orcanitrofeed.Config, mode string) (*Dispatcher, error) {
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
		mode:       mode,
		noDrop:     mode == orcanitrofeed.ModeSweep,
		socketPath: cfg.SocketPath,
		staging:    newMsgRing(cfg.BufferSize),
		retained: &frameLog{
			frames: nil, start: 0, bytes: 0,
			budget: cfg.BufferBytes, maxAge: cfg.BufferAge,
			noDrop: mode == orcanitrofeed.ModeSweep, dropped: 0,
		},
		cursors:     make(map[net.Conn]uint64),
		conns:       make(map[net.Conn]struct{}),
		listener:    listener,
		mu:          sync.Mutex{},
		stagingCond: nil,
		logCond:     nil,
		flowCond:    nil,
		nextSeq:     0,
		closed:      false,
		lastDropLog: time.Time{},
		connMu:      sync.Mutex{},
		wg:          sync.WaitGroup{},
		connWg:      sync.WaitGroup{},
	}
	d.stagingCond = sync.NewCond(&d.mu)
	d.logCond = sync.NewCond(&d.mu)
	d.flowCond = sync.NewCond(&d.mu)
	d.wg.Add(2)
	go d.acceptLoop()
	go d.writeLoop()
	return d, nil
}

// Enqueue는 seq를 부여해 staging에 넣는다.
// live: 논블로킹 — 가득 차면 oldest drop. sweep(noDrop): 가득 차면 블로킹 —
// writer→컨슈머 체인의 역압이 재실행을 스로틀한다 (유실 없음).
// msg는 enqueue 이후 수정하면 안 된다 (writer가 비동기로 직렬화).
func (d *Dispatcher) Enqueue(typ orcanitrofeed.MsgType, msg orcanitrofeed.SeqSetter) {
	// PERF:LOCK
	//   cost: hold~ns (push + signal); noDrop 포화 시 flowCond 대기
	//   note: 실행 핫패스 유일한 동기화 지점 — sweep 역압 지점이기도 하다
	d.mu.Lock()
	if d.noDrop {
		for d.staging.size == len(d.staging.buf) && !d.closed {
			d.flowCond.Wait()
		}
	}
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
		log.Warn("orca-nitro-feed: staging ring overflow, dropping oldest", "totalDropped", dropped)
	}
}

func (d *Dispatcher) acceptLoop() {
	defer d.wg.Done()
	for {
		conn, err := d.listener.Accept()
		if err != nil {
			return // listener closed
		}
		hello := orcanitrofeed.Hello{SchemaVersion: orcanitrofeed.SchemaVersion, Mode: d.mode}
		frame := encodeFrame(orcanitrofeed.MsgHello, &hello)
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
// noDrop: 예산 초과 시 push 전에 소비-trim이 자리를 낼 때까지 대기 (역압 전파).
func (d *Dispatcher) writeLoop() {
	defer d.wg.Done()
	for {
		d.mu.Lock()
		for d.staging.size == 0 && !d.closed {
			d.stagingCond.Wait()
		}
		m, ok := d.staging.pop()
		if ok && d.noDrop {
			d.flowCond.Broadcast() // staging에 자리 → Enqueue 재개
		}
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
		if d.noDrop {
			d.trimConsumed()
			for d.retained.bytes > d.retained.budget && !d.closed {
				d.flowCond.Wait() // sender 전진(trimConsumed)이 예산을 비울 때까지
				d.trimConsumed()
			}
		}
		d.retained.push(frame, time.Now())
		d.mu.Unlock()
		d.logCond.Broadcast()
	}
}

// sendLoop — 연결별 sender. retention log 커서를 따라가며 backlog replay 후
// live tail. live: 예산·age 초과로 밀린 구간은 건너뛴다 (클라이언트는 seq gap으로
// 감지). noDrop: 커서를 cursors에 등록해 소비-trim·역압의 기준이 된다.
func (d *Dispatcher) sendLoop(conn net.Conn) {
	defer d.connWg.Done()
	defer func() {
		conn.Close()
		d.connMu.Lock()
		delete(d.conns, conn)
		d.connMu.Unlock()
		d.mu.Lock()
		delete(d.cursors, conn)
		d.mu.Unlock()
		d.flowCond.Broadcast() // 죽은 연결이 min cursor를 붙잡지 않도록
	}()

	d.mu.Lock()
	d.retained.trim(time.Now())
	cursor := d.retained.start // 보관 중인 가장 오래된 것부터 replay
	d.cursors[conn] = cursor
	d.mu.Unlock()

	batch := make([][]byte, 0, 256)
	for {
		d.mu.Lock()
		d.retained.trim(time.Now())
		for cursor >= d.retained.end() && !d.closed {
			d.logCond.Wait()
			d.retained.trim(time.Now())
		}
		if cursor >= d.retained.end() && d.closed {
			d.mu.Unlock()
			return
		}
		if cursor < d.retained.start {
			cursor = d.retained.start // 예산·age 초과로 밀림 — gap (live 전용)
		}
		batch = batch[:0]
		for _, rf := range d.retained.frames[cursor-d.retained.start:] {
			batch = append(batch, rf.data)
		}
		// #nosec G115
		cursor += uint64(len(batch))
		d.mu.Unlock()

		for _, f := range batch {
			_ = conn.SetWriteDeadline(time.Now().Add(connWriteTimeout))
			if _, err := conn.Write(f); err != nil {
				// 로컬 클라이언트 전제 — 죽었거나 심하게 느린 연결은 정리 (재접속 시 replay)
				log.Warn("orca-nitro-feed: dropping slow/dead client", "err", err)
				return
			}
		}
		d.mu.Lock()
		d.cursors[conn] = cursor
		if d.noDrop {
			d.trimConsumed()
		}
		d.mu.Unlock()
		d.flowCond.Broadcast() // 커서 전진 → writeLoop 역압 해제 기회
	}
}

// encodeFrame: [u32 LE len][u8 type][payload]. 반환 버퍼는 retention log가 소유.
func encodeFrame(typ orcanitrofeed.MsgType, msg msgp.Marshaler) []byte {
	buf := make([]byte, frameHeaderSize, frameHeaderSize+256)
	buf[4] = byte(typ)
	buf, err := msg.MarshalMsg(buf)
	if err != nil {
		// msgp 생성 코드는 인코딩 실패 경로가 사실상 없음 — 발생 시 스키마 버그
		log.Error("orca-nitro-feed: marshal failed", "err", err)
		buf = buf[:frameHeaderSize]
	}
	// #nosec G115
	binary.LittleEndian.PutUint32(buf[:4], uint32(len(buf)-frameHeaderSize))
	return buf
}

// Close는 staging 잔량의 인코딩·보관과 연결된 클라이언트의 catch-up을
// 기다린 뒤 정리한다. noDrop(sweep)은 완전 전달까지 대기(cap 10m) —
// 미전달 잔량이 남으면 유실로 간주하고 크게 로깅한다.
func (d *Dispatcher) Close() {
	flushCap := closeFlushCap
	if d.noDrop {
		flushCap = closeFlushCapNoDrop
	}
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return
	}
	deadline := time.Now().Add(flushCap)
	flushed := func() bool {
		if d.staging.size > 0 {
			return false
		}
		if !d.noDrop {
			return true
		}
		min, ok := d.minCursor()
		// 컨슈머가 하나도 없으면 전달 완료를 기다릴 수 없다 — deadline까지 대기
		return ok && min >= d.retained.end()
	}
	for !flushed() && time.Now().Before(deadline) {
		d.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		d.mu.Lock()
	}
	stagingLeft := d.staging.size
	stagingDropped := d.staging.dropped
	retainedDropped := d.retained.dropped
	var undelivered uint64
	if min, ok := d.minCursor(); ok && d.retained.end() > min {
		undelivered = d.retained.end() - min
	} else if !ok {
		// #nosec G115
		undelivered = uint64(len(d.retained.frames))
	}
	d.closed = true
	d.mu.Unlock()
	if d.noDrop && (stagingLeft > 0 || stagingDropped > 0 || retainedDropped > 0 || undelivered > 0) {
		log.Error("orca-nitro-feed: sweep dispatcher closing with LOSS — feed is incomplete",
			"stagingLeft", stagingLeft, "stagingDropped", stagingDropped,
			"retainedDropped", retainedDropped, "undelivered", undelivered)
	} else if stagingDropped > 0 || retainedDropped > 0 {
		log.Warn("orca-nitro-feed: dispatcher dropped frames during run",
			"stagingDropped", stagingDropped, "retainedDropped", retainedDropped)
	}
	d.stagingCond.Broadcast()
	d.logCond.Broadcast()
	d.flowCond.Broadcast()
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
