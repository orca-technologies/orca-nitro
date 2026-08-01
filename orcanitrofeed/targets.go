package orcanitrofeed

import "github.com/ethereum/go-ethereum/common"

// TARGETS — TOKEN_METADATA §1 `(to, selector)` allowlist.
// Flap Portal create selector is not measured yet — omit until fixed.

type targetKey struct {
	to  common.Address
	sel [4]byte
}

var targets = map[targetKey]struct{}{}

func init() {
	add := func(toHex string, sel [4]byte) {
		targets[targetKey{to: common.HexToAddress(toHex), sel: sel}] = struct{}{}
	}
	// Airlock.create(CreateParams)
	add("0xeb7C034704eF8Dcd2D32324c1545f62fB4aD0862", [4]byte{0x88, 0x2d, 0xb7, 0x07})
	// NOXA / Pons launchToken (shared ABI)
	selLaunchToken := [4]byte{0x68, 0x63, 0x99, 0xcb}
	add("0xD9eC2db5f3D1b236843925949fe5bd8a3836FCcB", selLaunchToken) // NOXA
	add("0x0c37a24F5D23A486FA692d1500881d698B1F77a4", selLaunchToken) // Pons v1
	add("0xA5aAb3F0c6EeadF30Ef1D3Eb997108E976351feB", selLaunchToken) // Pons v2
	// Other pads (nitro emit only — feeder NamedEvent decode may lag)
	add("0x1B2A2ee9E66862e6323B0D43b26f60235214660A", [4]byte{0xda, 0xcc, 0x2f, 0x97}) // MetaLaunch
	add("0x985DFae571A0c5c90aC997F08687056D2cE1E46f", [4]byte{0x76, 0x90, 0x30, 0xbf}) // Coinbarrel
	add("0x9eab33527fBfb6bfFb918F7E18E797FE303a77d9", [4]byte{0x21, 0xb6, 0xf8, 0xa2}) // Furnace
	add("0xC25c1e209313856e3A66FDd3aFd98aBe90B047F6", [4]byte{0xc2, 0xf0, 0xcd, 0x4b}) // RobinPad
	add("0x9634AA5EB176064D9D04d6282E3D4a0A2456F01c", [4]byte{0x94, 0xae, 0xd7, 0xd0}) // Runner
	add("0xD69A9fDee44a42c8E614128FEda486128cB27222", [4]byte{0x34, 0xfb, 0x85, 0x89}) // RobinFun
}

func isTarget(to common.Address, input []byte) (sel [4]byte, ok bool) {
	if len(input) < 4 {
		return sel, false
	}
	copy(sel[:], input[:4])
	_, ok = targets[targetKey{to: to, sel: sel}]
	return sel, ok
}
