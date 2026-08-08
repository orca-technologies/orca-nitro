package bandpatch

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// blockBundle — 블록 하나를 복원하는 데 필요한 외부 데이터 전부.
// RPC 응답을 원본 JSON 그대로 보관한다 — Alchemy fetch는 비용이 들고 sweep은
// 몇 번이고 재생성되므로, 번들은 내구 저장소에 두고 무한 재사용한다.
type blockBundle struct {
	Schema int    `json:"schema"`
	Block  uint64 `json:"block"`
	// eth_getBlockByNumber(n, true) 원본 — header 필드 + full tx objects
	BlockJSON json.RawMessage `json:"block_json"`
	// eth_getBlockReceipts 원본
	ReceiptsJSON json.RawMessage `json:"receipts_json"`
	// debug_traceBlockByNumber callTracer(withLog) 원본
	CallTracesJSON json.RawMessage `json:"call_traces_json"`
	// debug_traceBlockByNumber prestateTracer(diffMode) 원본
	PrestatesJSON json.RawMessage `json:"prestates_json"`
	// tx sender·to의 AccountKind (fetch 시점에 전량 선계산 — 재사용 시 RPC 불필요)
	AccountKinds map[string]uint8 `json:"account_kinds"`
	// fetch 시점에 header walk으로 계산 (결정적 — 재사용 가능)
	SameTimestampIndex uint32 `json:"same_timestamp_index"`
}

const bundleSchema = 1

// FetchCache — 블록 번들의 파일 캐시. <dir>/<block/10000>/<block>.json.gz
type FetchCache struct {
	Dir string
	// Refetch: 캐시가 있어도 다시 받는다 (캐시는 갱신).
	Refetch bool
	// Only: 캐시 미스 시 RPC 대신 에러 — 의도치 않은 Alchemy 지출 방지.
	Only bool
}

func (c *FetchCache) enabled() bool { return c != nil && c.Dir != "" }

func (c *FetchCache) path(block uint64) string {
	return filepath.Join(c.Dir, fmt.Sprintf("%d", block/10000), fmt.Sprintf("%d.json.gz", block))
}

// Load — 캐시된 번들. 없거나 손상이면 (nil, nil)·(nil, err) — 호출자가 fetch로 폴백.
func (c *FetchCache) Load(block uint64) (*blockBundle, error) {
	if !c.enabled() || c.Refetch {
		return nil, nil
	}
	f, err := os.Open(c.path(block))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	zr, err := gzip.NewReader(f)
	if err != nil {
		return nil, fmt.Errorf("cache %d gzip: %w", block, err)
	}
	defer zr.Close()
	body, err := io.ReadAll(zr)
	if err != nil {
		return nil, fmt.Errorf("cache %d read: %w", block, err)
	}
	var b blockBundle
	if err := json.Unmarshal(body, &b); err != nil {
		return nil, fmt.Errorf("cache %d decode: %w", block, err)
	}
	if b.Schema != bundleSchema || b.Block != block {
		return nil, fmt.Errorf("cache %d schema/block mismatch (schema=%d block=%d)", block, b.Schema, b.Block)
	}
	return &b, nil
}

// Store — 원자적 저장 (tmp + rename). 최초 저장 시 디렉터리에 README를 남긴다.
func (c *FetchCache) Store(b *blockBundle) error {
	if !c.enabled() {
		return nil
	}
	path := c.path(b.Block)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	c.writeReadmeOnce()
	body, err := json.Marshal(b)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	zw := gzip.NewWriter(f)
	if _, err := zw.Write(body); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := zw.Close(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}

func (c *FetchCache) writeReadmeOnce() {
	readme := filepath.Join(c.Dir, "README.md")
	if _, err := os.Stat(readme); err == nil {
		return
	}
	_ = os.WriteFile(readme, []byte(`# orca-band-patch fetch cache

블록별 복원 번들 (schema v1, gzip JSON): eth_getBlockByNumber(full)·
eth_getBlockReceipts·debug_traceBlockByNumber(callTracer withLog /
prestateTracer diffMode) 원본 응답 + account kinds + same-timestamp index.

- Alchemy fetch 비용 절약용 영구 캐시 — sweep/FEED가 몇 번이고 재생성돼도
  이 번들로 orca-band-patch를 재실행하면 외부 RPC 없이 복원된다.
- 레이아웃: <block/10000>/<block>.json.gz
- 삭제 금지. 재수집이 필요하면 orca-band-patch --refetch.
`), 0o644)
}
