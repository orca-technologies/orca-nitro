package bandpatch

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestFetchCacheRoundTrip(t *testing.T) {
	dir := t.TempDir()
	c := &FetchCache{Dir: dir}

	if b, err := c.Load(24483478); err != nil || b != nil {
		t.Fatalf("미스는 (nil,nil)이어야 함: %v %v", b, err)
	}

	in := &blockBundle{
		Schema:             bundleSchema,
		Block:              24483478,
		BlockJSON:          json.RawMessage(`{"number":"0x1759696","transactions":[]}`),
		ReceiptsJSON:       json.RawMessage(`[]`),
		CallTracesJSON:     json.RawMessage(`[]`),
		PrestatesJSON:      json.RawMessage(`[]`),
		AccountKinds:       map[string]uint8{"0xAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAa": 2},
		SameTimestampIndex: 3,
	}
	if err := c.Store(in); err != nil {
		t.Fatal(err)
	}

	out, err := c.Load(24483478)
	if err != nil || out == nil {
		t.Fatalf("로드 실패: %v %v", out, err)
	}
	if out.Block != in.Block || out.SameTimestampIndex != 3 ||
		string(out.BlockJSON) != string(in.BlockJSON) ||
		out.AccountKinds["0xAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAa"] != 2 {
		t.Fatalf("round-trip 불일치: %+v", out)
	}

	// 샤딩 레이아웃 + README
	if _, err := os.Stat(filepath.Join(dir, "2448", "24483478.json.gz")); err != nil {
		t.Fatalf("샤딩 경로 없음: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "README.md")); err != nil {
		t.Fatalf("README 없음: %v", err)
	}

	// Refetch는 캐시를 무시한다
	c.Refetch = true
	if b, err := c.Load(24483478); err != nil || b != nil {
		t.Fatalf("refetch면 (nil,nil): %v %v", b, err)
	}

	// nil cache 안전성
	var nilCache *FetchCache
	if b, err := nilCache.Load(1); err != nil || b != nil {
		t.Fatal("nil cache Load는 no-op이어야 함")
	}
	if err := nilCache.Store(in); err != nil {
		t.Fatal("nil cache Store는 no-op이어야 함")
	}
}
