package orcanitrofeed

import (
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

// selector-only TARGET(Pons v2 curve buy/sell)은 to 무관하게 매치된다.
func TestSelectorOnlyTargetMatchesAnyAddress(t *testing.T) {
	randomCurve := common.HexToAddress("0x1234567890abcdef1234567890abcdef12345678")
	buyInput := []byte{0x59, 0xa8, 0x7b, 0xc1, 0x01}
	sellInput := []byte{0xd0, 0x4c, 0x69, 0x83, 0x01}
	if _, ok := isTarget(randomCurve, buyInput); !ok {
		t.Fatal("curve buy selector must match any address")
	}
	if _, ok := isTarget(randomCurve, sellInput); !ok {
		t.Fatal("curve sell selector must match any address")
	}
	// Settler execute도 selector-only다 (배포 로테이션).
	if _, ok := isTarget(randomCurve, []byte{0x1f, 0xff, 0x99, 0x1f, 0x00}); !ok {
		t.Fatal("settler execute selector must match any address")
	}
	// exact (to, selector) 매치는 그대로 동작한다.
	swapRouter := common.HexToAddress("0xCaf681a66D020601342297493863E78C959E5cb2")
	if _, ok := isTarget(swapRouter, []byte{0x42, 0x71, 0x2a, 0x67, 0x00}); !ok {
		t.Fatal("swapTokensForExactTokens on SwapRouter02 must match")
	}
	if _, ok := isTarget(randomCurve, []byte{0x42, 0x71, 0x2a, 0x67, 0x00}); ok {
		t.Fatal("address-gated selector must not match a random address")
	}
}

// 2라운드 독립 라우터 TARGET — 라우터별 대표 selector가 매치되고, 같은
// selector라도 무관 주소는 매치되지 않는다.
func TestStandaloneRouterTargets(t *testing.T) {
	random := common.HexToAddress("0x1234567890abcdef1234567890abcdef12345678")
	cases := []struct {
		name string
		to   common.Address
		sel  []byte
	}{
		{"v2router02 swapExactETHForTokens", common.HexToAddress("0x89e5db8b5aa49aa85ac63f691524311aeb649eba"), []byte{0x7f, 0xf3, 0x6a, 0xb5}},
		{"v2router02 swapTokensForExactTokens", common.HexToAddress("0x89e5db8b5aa49aa85ac63f691524311aeb649eba"), []byte{0x88, 0x03, 0xdb, 0xee}},
		{"rh router swap", common.HexToAddress("0x65050a9b7e5075a2ba5ced7b1b64ee66262c40dc"), []byte{0x4d, 0x81, 0x9a, 0x2a}},
		{"okx dagSwapTo", common.HexToAddress("0xe58b3089df6667fbf99b75595a1671baf6797d6d"), []byte{0x0c, 0x30, 0x7f, 0x76}},
		{"okx unxswapByOrderId", common.HexToAddress("0xe58b3089df6667fbf99b75595a1671baf6797d6d"), []byte{0x98, 0x71, 0xef, 0xa4}},
		{"kyber swap", common.HexToAddress("0x6131b5fae19ea4f9d964eac0408e4408b66337b5"), []byte{0xe2, 0x1f, 0xd0, 0xe9}},
		{"universal router 2 execute", common.HexToAddress("0x248a454ac3584c2a48d1fcb28d3910a6b6ea00af"), []byte{0x35, 0x93, 0x56, 0x4c}},
		{"rh router 2 swap", common.HexToAddress("0xe492912f37c2a4eca45d42dc67548f4c6cd7ce2b"), []byte{0x4d, 0x81, 0x9a, 0x2a}},
		{"oneinch v6 swap", common.HexToAddress("0x5a705de8982235a7fa45bb83dcacf03a211389c7"), []byte{0x07, 0xed, 0x23, 0x79}},
	}
	for _, c := range cases {
		input := append(append([]byte{}, c.sel...), 0x00)
		if _, ok := isTarget(c.to, input); !ok {
			t.Fatalf("%s must match", c.name)
		}
		if _, ok := isTarget(random, input); ok {
			t.Fatalf("%s must not match a random address", c.name)
		}
	}
}
