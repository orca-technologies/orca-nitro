package orcanitrofeed

import "github.com/ethereum/go-ethereum/common"

// TARGETS — TOKEN_METADATA §1 `(to, selector)` allowlist.
// Flap: multi-step (commit/stage/newTokenV2–V7); meta decode on newTokenV*.
// pools.trade: multicall delegatecalls back into the launcher, so one launch
// emits both the wrapper and its inner calls — consumers must dedupe by tx.

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
	// pools.trade LiquidityLauncher — G2 is current, but G1/G1.5 still take launches
	ptG2 := "0x0000FffFBE8efE702c8703aE3477FF5dE3d319C0"
	ptG15 := "0x7A6C474b4DcD35b72203D2B569EAfE4C9b5C768e"
	ptG1 := "0x00004c4ccc709Ef590F7C81102C0689F0263D4e9"
	for _, launcher := range []string{ptG2, ptG15, ptG1} {
		// multicall(bytes[]) is RECORD-ONLY, like the third-party router below:
		// the sniping decoder (`named_events_from_calls`) matches only the three
		// launch selectors, so the wrapper record never becomes a NamedEvent.
		// Nothing is lost — the wrapper delegatecalls back into the launcher and
		// each inner frame is its own TARGET hit carrying the full launch
		// metadata. At depth 0 (all observed entries) the wrapper Input also
		// duplicates ReceiptMsg.Calldata, so the record is pure wire cost there;
		// what it uniquely captures is the wrapper frame itself when the
		// launcher is entered by an inner call. If wire volume matters more,
		// delete this line — no decoder depends on it.
		add(launcher, [4]byte{0xac, 0x96, 0x50, 0xd8}) // multicall(bytes[])
		add(launcher, [4]byte{0xb6, 0x98, 0x2b, 0x48}) // distributeToken(address,(address,uint128,bytes),bytes32)
		add(launcher, [4]byte{0xde, 0xc1, 0x4b, 0xe1}) // createToken(address,string,string,uint8,uint128,address,bytes) — meta is calldata-only
	}
	// distributeWithNative(address,bytes,bytes32,uint256) — G2/G1.5 only, absent from the G1 ABI
	add(ptG2, [4]byte{0x0e, 0xf8, 0x47, 0xb6})
	add(ptG15, [4]byte{0x0e, 0xf8, 0x47, 0xb6})
	// pools.trade third-party router — separate launch entry path (unverified on explorer; sig from on-chain observation)
	//
	// RECORD-ONLY, on purpose: rhc-decode has no calldata decoder for this
	// selector, so the frame reaches the wire and `named_events_from_calls`
	// drops it. Nothing is lost by that — the router CALLs into the launcher and
	// those inner frames match the launcher targets above on their own (TARGET
	// matching is per-frame), so a router launch still yields createToken and
	// distributeToken records with the same metadata.
	//
	// It is kept for the one case ReceiptMsg does not cover: the router being
	// entered by an inner call from another contract — unobserved so far (the
	// router is 26.3% of launches, all of it top-level). For top-level entries
	// ReceiptMsg already attributes the router (To is the tx recipient) and
	// carries its entry-level salt and metadata blob (Calldata is the full
	// tx.Data(), see NewReceiptMsg in observer.go), so there this record only
	// duplicates them. If wire volume matters more, delete this line — no
	// decoder depends on it.
	add("0xa0177CF584E06f4E7876d7bf0b2D5016e0d8a1fa", [4]byte{0x27, 0xa1, 0x09, 0x8d}) // launch(string,string,(string,string,string,uint256),uint256,bytes32)
	// Buy entrypoints (schema v3) — 매수가 최종적으로 무조건 도달하는 프로토콜
	// 진입점만 등록한다. 어떤 앞단(1inch·OKX·Settler·RH router·AA·multicall)을
	// 거치든 이 진입점 frame이 자기 TARGET hit로 잡히므로 (matching is
	// per-frame), 진입점에서만 기록하면 경로와 무관하게 체결당 정확히 한 번
	// 세진다. 앞단 라우터를 추가로 등록하면 같은 체결이 두 곳에서 잡혀 dedup
	// 부담만 생긴다 — 등록하지 않는다.
	//
	// UniversalRouter는 체인 전체 스왑 라우터다 (5.5M txs vs 10k on the
	// launcher). depth 0 hit의 Input은 ReceiptMsg.Calldata와 중복이라 순수 wire
	// 비용인데, 선언 하한(minAmountOut)의 소유자를 depth로 가르는 축이 필요해
	// 일단 전 depth를 기록한다. smoke sweep 물량 계측 후 depth>0 필터를 붙일지
	// 결정한다 (그 경우 depth 0 축은 ReceiptMsg.Calldata 디코드로 복원 가능).
	universalRouter := "0x8876789976dEcBfCbBbe364623C63652db8C0904"
	add(universalRouter, [4]byte{0x35, 0x93, 0x56, 0x4c}) // execute(bytes,bytes[],uint256)
	add(universalRouter, [4]byte{0x24, 0x85, 0x6b, 0xc3}) // execute(bytes,bytes[])
	// SwapRouter02 — V3 pad 스왑 진입점.
	swapRouter02 := "0xCaf681a66D020601342297493863E78C959E5cb2"
	add(swapRouter02, [4]byte{0x04, 0xe4, 0x5a, 0xaf}) // exactInputSingle(ExactInputSingleParams)
	add(swapRouter02, [4]byte{0xb8, 0x58, 0x18, 0x3f}) // exactInput(ExactInputParams)
	// Flap Portal — swapExactInput이 curve·졸업 양 단계의 유일한 live 경로
	// (legacy buy/sell은 FeatureDisabled revert).
	add(flapPortal, [4]byte{0xef, 0x7e, 0xc2, 0xe7}) // swapExactInput(ExactInputParams)
	// Pons v2 curve buy(uint256,uint256,address) 0x59a87bc1은 아직 등록하지
	// 못한다 — 타깃이 launch마다 뜨는 per-token CREATE2 curve라 exact (to,
	// selector) 매칭으로는 잡을 수 없다. selector-only 매치 모드(충돌은 Rust
	// 디코더의 drop으로 방어) 또는 codehash 매치가 필요하다 — 별도 결정.
}

// TargetCall — allowlist 매칭 공개 래퍼 (bandpatch 등 재구성 경로용).
func TargetCall(to common.Address, input []byte) (sel [4]byte, ok bool) {
	return isTarget(to, input)
}

func isTarget(to common.Address, input []byte) (sel [4]byte, ok bool) {
	if len(input) < 4 {
		return sel, false
	}
	copy(sel[:], input[:4])
	_, ok = targets[targetKey{to: to, sel: sel}]
	return sel, ok
}
