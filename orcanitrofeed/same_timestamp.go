package orcanitrofeed

// DefaultSameTimestampLookback — SameTimestampIndex warm-up 시 헤더를 뒤로
// 걷는 최대 칸 수. feed order block_rel은 5 bit(cap 31)이므로 기본 32.
const DefaultSameTimestampLookback uint64 = 32

// HeaderTimeFunc — block number → L2 header unix sec. 없으면 ok=false.
type HeaderTimeFunc func(blockNum uint64) (ts uint64, ok bool)

// WalkSameTimestampIndex — startNum 블록이 같은 startTime을 가진 연속 그룹에서
// 몇 번째(0-based)인지 헤더만 보고 계산한다. lookback 칸까지만 뒤로 걷고,
// time이 달라지면 즉시 멈춘다.
func WalkSameTimestampIndex(startNum, startTime, lookback uint64, headerTime HeaderTimeFunc) uint32 {
	if startNum == 0 || lookback == 0 || headerTime == nil {
		return 0
	}
	var index uint32
	lowest := uint64(0)
	if startNum > lookback {
		lowest = startNum - lookback
	}
	for n := startNum; n > lowest; {
		n--
		ts, ok := headerTime(n)
		if !ok || ts != startTime {
			break
		}
		index++
	}
	return index
}

// SameTimestampTracker — 연속 블록 재실행/생성 경로에서 SameTimestampIndex를
// 유지한다. 첫 블록(또는 prev 미설정)은 header walk으로 초기화한다.
type SameTimestampTracker struct {
	lookback   uint64
	headerTime HeaderTimeFunc
	hasPrev    bool
	prevTime   uint64
	index      uint32
}

func NewSameTimestampTracker(lookback uint64, headerTime HeaderTimeFunc) *SameTimestampTracker {
	if lookback == 0 {
		lookback = DefaultSameTimestampLookback
	}
	return &SameTimestampTracker{lookback: lookback, headerTime: headerTime}
}

// Advance — blockNumber의 l2Timestamp에 대한 index를 반환하고 상태를 갱신한다.
func (t *SameTimestampTracker) Advance(blockNumber, l2Timestamp uint64) uint32 {
	if t == nil {
		return 0
	}
	if !t.hasPrev {
		t.index = WalkSameTimestampIndex(blockNumber, l2Timestamp, t.lookback, t.headerTime)
		t.prevTime = l2Timestamp
		t.hasPrev = true
		return t.index
	}
	if l2Timestamp == t.prevTime {
		t.index++
	} else {
		t.index = 0
		t.prevTime = l2Timestamp
	}
	return t.index
}

// Index — 마지막으로 Advance한 블록의 index.
func (t *SameTimestampTracker) Index() uint32 {
	if t == nil {
		return 0
	}
	return t.index
}
