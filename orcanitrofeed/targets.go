package orcanitrofeed

import "github.com/ethereum/go-ethereum/common"

// TARGETS — TOKEN_METADATA §1 `(to, selector)` allowlist.
// Flap: multi-step (commit/stage/newTokenV2–V7); meta decode on newTokenV*.

type targetKey struct {
	to  common.Address
	sel [4]byte
}

var targets = map[targetKey]struct{}{}

func init() {
	add := func(toHex string, sel [4]byte) {
		targets[targetKey{to: common.HexToAddress(toHex), sel: sel}] = struct{}{}
	}
	// Airlock.create(CreateParams) — two deployments on RHC
	add("0xeb7C034704eF8Dcd2D32324c1545f62fB4aD0862", [4]byte{0x88, 0x2d, 0xb7, 0x07})
	add("0x22e99278308b393ea1260859b181ad7e78f5eeed", [4]byte{0x88, 0x2d, 0xb7, 0x07})
	// NOXA / Pons V3-direct launchToken (shared ABI, selector 0x686399cb)
	selLaunchToken := [4]byte{0x68, 0x63, 0x99, 0xcb}
	add("0xD9eC2db5f3D1b236843925949fe5bd8a3836FCcB", selLaunchToken) // NOXA
	add("0x0c37a24F5D23A486FA692d1500881d698B1F77a4", selLaunchToken) // Pons v1
	add("0xA5aAb3F0c6EeadF30Ef1D3Eb997108E976351feB", selLaunchToken) // Pons V3-direct factory (Dune "v2"; not protocol-v2)
	// Pons protocol-v2 (bonding curve → Uniswap V4) — distinct from V3-direct 0xA5aA… / 0x686399cb
	ponsV2Factory := "0x7eD598BcEf8bd9Edd8C97A195C6d13f40801EC7e"
	add(ponsV2Factory, [4]byte{0xf3, 0x5a, 0xbb, 0xcf}) // launchToken(TokenParams,uint256,address)
	add(ponsV2Factory, [4]byte{0xa7, 0x21, 0x01, 0xaf}) // launchToken(TokenParams,uint256,address,address[])
	add(ponsV2Factory, [4]byte{0xd6, 0xa0, 0xee, 0xf5}) // launchTokenFor(…)
	add("0xe33E9E479dF8802cb0866d5d05258bEc4cF62948", [4]byte{0xf8, 0x5f, 0x8e, 0x41}) // LaunchAndBuy.launchAndBuy
	// Other pads (nitro emit only — feeder NamedEvent decode may lag)
	add("0x1B2A2ee9E66862e6323B0D43b26f60235214660A", [4]byte{0xda, 0xcc, 0x2f, 0x97}) // MetaLaunch
	add("0x985DFae571A0c5c90aC997F08687056D2cE1E46f", [4]byte{0x76, 0x90, 0x30, 0xbf}) // Coinbarrel
	add("0x9eab33527fBfb6bfFb918F7E18E797FE303a77d9", [4]byte{0x21, 0xb6, 0xf8, 0xa2}) // Furnace
	add("0xC25c1e209313856e3A66FDd3aFd98aBe90B047F6", [4]byte{0xc2, 0xf0, 0xcd, 0x4b}) // RobinPad
	add("0x9634AA5EB176064D9D04d6282E3D4a0A2456F01c", [4]byte{0x94, 0xae, 0xd7, 0xd0}) // Runner
	add("0xD69A9fDee44a42c8E614128FEda486128cB27222", [4]byte{0x34, 0xfb, 0x85, 0x89}) // RobinFun
	// Flap Portal proxy — newTokenV2–V7 + commit/stage V5 (emit; feeder decodes V2–V7 meta)
	flapPortal := "0x26605f322f7fF986f381bB9A6e3f5DAb0bEaEb09"
	add(flapPortal, [4]byte{0x0b, 0xa6, 0x32, 0x4e}) // newTokenV2
	add(flapPortal, [4]byte{0x7e, 0x15, 0x67, 0x6e}) // newTokenV3
	add(flapPortal, [4]byte{0x3b, 0xa6, 0xf2, 0x6a}) // newTokenV4
	add(flapPortal, [4]byte{0x2e, 0x2f, 0xdb, 0xd9}) // newTokenV5
	add(flapPortal, [4]byte{0x8c, 0xb5, 0x77, 0x2c}) // newTokenV6
	add(flapPortal, [4]byte{0x87, 0xef, 0x5b, 0x30}) // newTokenV7
	add(flapPortal, [4]byte{0x5d, 0x29, 0xf9, 0xf2}) // commitNewTokenV5
	add(flapPortal, [4]byte{0x9d, 0x55, 0xbd, 0xe4}) // stageNewTokenV5
}

func isTarget(to common.Address, input []byte) (sel [4]byte, ok bool) {
	if len(input) < 4 {
		return sel, false
	}
	copy(sel[:], input[:4])
	_, ok = targets[targetKey{to: to, sel: sel}]
	return sel, ok
}
