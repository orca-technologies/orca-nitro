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

// selector-only allowlist — 타깃 주소가 launch마다 뜨는 CREATE2 계열
// (Pons v2 curve)용. 주소 무관 매치라 무관 컨트랙트의 동일 selector 호출도
// record로 남는다 (Rust 디코더/다운스트림이 거른다).
var selectorOnlyTargets = map[[4]byte]struct{}{}

func init() {
	add := func(toHex string, sel [4]byte) {
		targets[targetKey{to: common.HexToAddress(toHex), sel: sel}] = struct{}{}
	}
	addSelectorOnly := func(sel [4]byte) {
		selectorOnlyTargets[sel] = struct{}{}
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
	// Swap TARGETs (schema v3+) — 두 층이다.
	//
	// 1) 프로토콜 진입점 (UniversalRouter / SwapRouter02 / Flap Portal /
	//    Pons v2 curve): 그 프로토콜의 공식 프론트 경로.
	// 2) 독립 라우터 (아래 V2Router02 / RH router / OKX / Kyber / Settler):
	//    진입점을 거치지 않고 pool·PoolManager를 **직접** 치는 것이 실측된
	//    라우터들. 진입점 등록만으로는 이들의 스왑이 어떤 depth에서도 잡히지
	//    않는다 (v3 smoke: fill tx의 32%가 미커버였고 전수 (to,selector)
	//    식별로 이 목록이 나왔다).
	//
	// 겹침 걱정은 없다 — 독립 라우터는 정의상 진입점 frame을 만들지 않으므로
	// 같은 체결이 두 TARGET에서 잡히지 않는다. multicall 래퍼(Relay 등)는
	// 여전히 미등록: 안쪽 스왑 frame(예: Relay→Kyber)이 자기 TARGET hit로
	// 잡힌다. 봇의 custom 컨트랙트는 선언 한계 개념이 없어 TARGET이 아니다.
	//
	// UniversalRouter는 체인 전체 스왑 라우터다 (5.5M txs vs 10k on the
	// launcher). depth 0 hit의 Input은 ReceiptMsg.Calldata와 중복이라 순수 wire
	// 비용인데, 선언 하한(minAmountOut)의 소유자를 depth로 가르는 축이 필요해
	// 일단 전 depth를 기록한다. smoke sweep 물량 계측 후 depth>0 필터를 붙일지
	// 결정한다 (그 경우 depth 0 축은 ReceiptMsg.Calldata 디코드로 복원 가능).
	universalRouter := "0x8876789976dEcBfCbBbe364623C63652db8C0904"
	add(universalRouter, [4]byte{0x35, 0x93, 0x56, 0x4c}) // execute(bytes,bytes[],uint256)
	add(universalRouter, [4]byte{0x24, 0x85, 0x6b, 0xc3}) // execute(bytes,bytes[])
	// SwapRouter02 — V2·V3 pad 스왑 진입점. 스왑 한계를 싣는 함수 6종 전부
	// (multicall 래퍼는 넣지 않는다 — 안쪽 스왑 frame이 자기 TARGET hit로
	// 잡힌다. 나머지 함수는 정산·승인·LP라 스왑이 아니다).
	swapRouter02 := "0xCaf681a66D020601342297493863E78C959E5cb2"
	add(swapRouter02, [4]byte{0x04, 0xe4, 0x5a, 0xaf}) // exactInputSingle(ExactInputSingleParams)
	add(swapRouter02, [4]byte{0xb8, 0x58, 0x18, 0x3f}) // exactInput(ExactInputParams)
	add(swapRouter02, [4]byte{0x50, 0x23, 0xb4, 0xdf}) // exactOutputSingle(ExactOutputSingleParams)
	add(swapRouter02, [4]byte{0x09, 0xb8, 0x13, 0x46}) // exactOutput(ExactOutputParams)
	add(swapRouter02, [4]byte{0x47, 0x2b, 0x43, 0xf3}) // swapExactTokensForTokens(uint256,uint256,address[],address)
	add(swapRouter02, [4]byte{0x42, 0x71, 0x2a, 0x67}) // swapTokensForExactTokens(uint256,uint256,address[],address)
	// Flap Portal — swapExactInput이 curve·졸업 양 단계의 유일한 live 경로
	// (legacy buy/sell은 FeatureDisabled revert). 매수·매도는 토큰 leg 방향.
	add(flapPortal, [4]byte{0xef, 0x7e, 0xc2, 0xe7}) // swapExactInput(ExactInputParams)
	// Pons v2 curve buy/sell — 타깃이 launch마다 뜨는 per-token CREATE2
	// curve라 exact (to, selector) 매칭이 불가능해 **selector-only** 로
	// 잡는다. 같은 selector의 무관 컨트랙트 호출이 섞일 수 있다 — record는
	// 넓게 남고, curve 여부 판별은 Rust 쪽(chain state curve→token 매핑)이
	// 한다. 물량은 smoke sweep에서 계측한다.
	addSelectorOnly([4]byte{0x59, 0xa8, 0x7b, 0xc1}) // buy(uint256,uint256,address)
	addSelectorOnly([4]byte{0xd0, 0x4c, 0x69, 0x83}) // sell(uint256,uint256,address)
	// UniswapV2Router02 (독립 배포, verified) — RHC V2 스왑의 주경로 (v3
	// smoke: V2 venue fill의 95%가 SwapRouter02가 아니라 이 라우터). pair를
	// 직접 치므로 진입점 등록으로는 안 잡힌다. swap 함수 9종 전부 — exact in
	// 6종(FeeOnTransfer 변형 포함) + exact out 3종. addLiquidity* 계열은 스왑
	// 선언이 아니라 제외.
	v2Router02 := "0x89e5db8b5aa49aa85ac63f691524311aeb649eba"
	add(v2Router02, [4]byte{0x7f, 0xf3, 0x6a, 0xb5}) // swapExactETHForTokens
	add(v2Router02, [4]byte{0x18, 0xcb, 0xaf, 0xe5}) // swapExactTokensForETH
	add(v2Router02, [4]byte{0x38, 0xed, 0x17, 0x39}) // swapExactTokensForTokens
	add(v2Router02, [4]byte{0xb6, 0xf9, 0xde, 0x95}) // swapExactETHForTokensSupportingFeeOnTransferTokens
	add(v2Router02, [4]byte{0x79, 0x1a, 0xc9, 0x47}) // swapExactTokensForETHSupportingFeeOnTransferTokens
	add(v2Router02, [4]byte{0x5c, 0x11, 0xd7, 0x95}) // swapExactTokensForTokensSupportingFeeOnTransferTokens
	add(v2Router02, [4]byte{0xfb, 0x3b, 0xdb, 0x41}) // swapETHForExactTokens
	add(v2Router02, [4]byte{0x4a, 0x25, 0xd9, 0x4a}) // swapTokensForExactETH
	add(v2Router02, [4]byte{0x88, 0x03, 0xdb, 0xee}) // swapTokensForExactTokens
	// RH in-app router (TransparentUpgradeableProxy, impl 미검증) — 최대 소매
	// 스왑 소스 (v3 smoke 미커버 1위: 16,403 tx / 399 ETH). V3 pool·V4
	// PoolManager를 직접 친다 (실측 tx 0x487e389d… / 0x1b014c81…). proxy
	// 업그레이드로 calldata 레이아웃이 바뀔 수 있다 — record는 그대로 남고
	// Rust 디코더가 구조 게이트로 거른다.
	add("0x65050a9b7e5075a2ba5ced7b1b64ee66262c40dc", [4]byte{0x4d, 0x81, 0x9a, 0x2a}) // swap(Step[],address,uint256,uint256,uint256)
	// OKX DexRouter (verified) — 자체 adapter로 pool 직행. dagSwap 2종 +
	// unxswap 2종 (선언 축은 BaseRequest/minReturn에 완비).
	okxDexRouter := "0xe58b3089df6667fbf99b75595a1671baf6797d6d"
	add(okxDexRouter, [4]byte{0x0c, 0x30, 0x7f, 0x76}) // dagSwapTo
	add(okxDexRouter, [4]byte{0xf2, 0xc4, 0x26, 0x96}) // dagSwapByOrderId
	add(okxDexRouter, [4]byte{0x08, 0x29, 0x8b, 0x5a}) // unxswapTo
	add(okxDexRouter, [4]byte{0x98, 0x71, 0xef, 0xa4}) // unxswapByOrderId
	// Kyber MetaAggregationRouterV2 (verified) — 자체 executor로 pool 직행.
	// Relay(RelayApprovalProxyV3)의 multicall이 이 라우터를 내부 호출하는
	// 것이 실측되어, 이 TARGET이 Relay 몫도 depth>0으로 잡는다.
	add("0x6131b5fae19ea4f9d964eac0408e4408b66337b5", [4]byte{0xe2, 0x1f, 0xd0, 0xe9}) // swap(SwapExecutionParams)
	// 0x RobinHoodSettler execute — Settler는 배포 로테이션을 한다 (2026-07
	// 0x1d4B… → 2026-08 0x39b38686…, AllowanceHolder exec의 target). 주소
	// 고정 TARGET은 썩으므로 selector-only. AllowanceHolder(0x…1ff3) 겉
	// envelope은 미등록 — 안쪽 execute frame이 슬리피지 tuple까지 들고 있다.
	addSelectorOnly([4]byte{0x1f, 0xff, 0x99, 0x1f}) // execute((address,address,uint256),bytes[],bytes32)
	// 3라운드 (v4 smoke 잔여 실측) — 2호기·별도 배포들.
	// verified UniversalRouter 2호기 (623 tx).
	universalRouter2 := "0x248a454ac3584c2a48d1fcb28d3910a6b6ea00af"
	add(universalRouter2, [4]byte{0x35, 0x93, 0x56, 0x4c}) // execute(bytes,bytes[],uint256)
	add(universalRouter2, [4]byte{0x24, 0x85, 0x6b, 0xc3}) // execute(bytes,bytes[])
	// RH router 동일 selector 2호기 (미검증, 1,153 tx / 27.8 ETH) — 레이아웃이
	// 다르면 Rust 구조 게이트가 드랍한다.
	add("0xe492912f37c2a4eca45d42dc67548f4c6cd7ce2b", [4]byte{0x4d, 0x81, 0x9a, 0x2a})
	// 1inch AggregationRouterV6 — canonical + verified 별도 배포 (477 tx).
	// unoswap 계열은 output 레그 복원 불가라 미등록.
	add("0x111111125421ca6dc452d289314280a0f8842a65", [4]byte{0x07, 0xed, 0x23, 0x79}) // swap(address,SwapDescription,bytes)
	add("0x5a705de8982235a7fa45bb83dcacf03a211389c7", [4]byte{0x07, 0xed, 0x23, 0x79})
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
	if _, ok = targets[targetKey{to: to, sel: sel}]; ok {
		return sel, true
	}
	_, ok = selectorOnlyTargets[sel]
	return sel, ok
}
