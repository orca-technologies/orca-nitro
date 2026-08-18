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
	// exact (to, selector) 매치는 그대로 동작한다.
	swapRouter := common.HexToAddress("0xCaf681a66D020601342297493863E78C959E5cb2")
	if _, ok := isTarget(swapRouter, []byte{0x42, 0x71, 0x2a, 0x67, 0x00}); !ok {
		t.Fatal("swapTokensForExactTokens on SwapRouter02 must match")
	}
	if _, ok := isTarget(randomCurve, []byte{0x42, 0x71, 0x2a, 0x67, 0x00}); ok {
		t.Fatal("address-gated selector must not match a random address")
	}
}
