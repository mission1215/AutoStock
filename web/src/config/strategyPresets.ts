/**
 * AutoStock 전략 프리셋 (보수 / 보통 / 적극)
 *
 * - 일반적으로 쓰는 **분산·리스크·진입 엄격도** 기준의 초기값 묶음입니다.
 * - **수익을 보장하지 않으며**, 백테스트·실거래에 맞게 사용자가 조정하는 것이 전제입니다.
 * - **보통**이 기본 권장(기존 “모의/실전 권장값”과 동일 계열).
 */

export type StrategyTier = "conservative" | "balanced" | "aggressive";

/** 전략 설정 폼과 동일 필드 (프리셋 주입용) */
export type StrategyFormFields = {
  k: string;
  ma: string;
  stopPct: string;
  maxPct: string;
  dailyPct: string;
  idxGatePct: string;
  minScoreKr: string;
  krWl: string;
  usWl: string;
  aiCount: number;
  partialTpEn: boolean;
  partialTpTrig: string;
  partialTpSell: string;
  partialTpTight: boolean;
  slipMockPct: string;
  slipLivePct: string;
  avgDownEn: boolean;
  avgDownTrig: string;
  avgDownMax: string;
  avgDownQty: string;
  avgDownGapH: string;
  /** 동적 KR AI 유니버스 시 KIS 현재가 기반 2차 제외(관리·유의 등) */
  aiKrQualityGates: boolean;
  /** 동적 유니버스 시 시총 하한(억원), "0" = 미사용 */
  aiKrMinCapEok: string;
  /** 섹터당 최대 보유·추천 분산(전략·AI 공통, 정수 문자열) */
  maxSector: string;
};

/** 국내 동적 풀 고급 기본 — 프리셋 전 tier 동일 (시총 미저장 서버와 동일 1000억) */
const UNIVERSE_FORM_DEFAULTS = {
  aiKrQualityGates: true,
  aiKrMinCapEok: "1000",
  maxSector: "2",
} as const;

export const STRATEGY_TIER_LABELS: Record<StrategyTier, { title: string; blurb: string }> = {
  conservative: {
    title: "보수",
    blurb: "한 종목 비중·추격 폭을 낮추고, 진입 점수·지수 게이트를 더 빡뜻게 잡은 편.",
  },
  balanced: {
    title: "보통(기본)",
    blurb: "모의/실전에서 많이 쓰던 권장값과 비슷한 균형. 별다른 사유가 없으면 여기서 시작.",
  },
  aggressive: {
    title: "적극",
    blurb: "비중·슬리퍼(추격) 여유, 진입 완화·물타기 쪽으로 더 켜 둔 편. 변동·손실 폭이 커질 수 있음.",
  },
};

const WATCH_KR = "005930,000660,035420,035720,051910";
const WATCH_US = "AAPL,NVDA,TSLA,MSFT,GOOGL";

function base(tier: StrategyTier, isMock: boolean): StrategyFormFields {
  const c = tier === "conservative";
  const a = tier === "aggressive";
  const b = tier === "balanced";

  if (isMock) {
    if (c) {
      return {
        k: "0.32",
        ma: "5",
        stopPct: "2.2",
        maxPct: "12",
        dailyPct: "0.8",
        idxGatePct: "1.2",
        minScoreKr: "45",
        krWl: WATCH_KR,
        usWl: WATCH_US,
        aiCount: 5,
        partialTpEn: true,
        partialTpTrig: "2.0",
        partialTpSell: "25",
        partialTpTight: true,
        slipMockPct: "3.0",
        slipLivePct: "2.5",
        avgDownEn: true,
        avgDownTrig: "5.0",
        avgDownMax: "1",
        avgDownQty: "25",
        avgDownGapH: "24",
        ...UNIVERSE_FORM_DEFAULTS,
      };
    }
    if (a) {
      return {
        k: "0.45",
        ma: "5",
        stopPct: "3.2",
        maxPct: "28",
        dailyPct: "1.3",
        idxGatePct: "0.5",
        minScoreKr: "22",
        krWl: WATCH_KR,
        usWl: WATCH_US,
        aiCount: 5,
        partialTpEn: true,
        partialTpTrig: "2.0",
        partialTpSell: "35",
        partialTpTight: true,
        slipMockPct: "6.0",
        slipLivePct: "4.0",
        avgDownEn: true,
        avgDownTrig: "3.0",
        avgDownMax: "3",
        avgDownQty: "40",
        avgDownGapH: "12",
        ...UNIVERSE_FORM_DEFAULTS,
      };
    }
    // balanced
    if (b) {
      return {
        k: "0.3",
        ma: "5",
        stopPct: "3.0",
        maxPct: "20",
        dailyPct: "1.0",
        idxGatePct: "0",
        minScoreKr: "25",
        krWl: WATCH_KR,
        usWl: WATCH_US,
        aiCount: 5,
        partialTpEn: true,
        partialTpTrig: "2.0",
        partialTpSell: "30",
        partialTpTight: true,
        slipMockPct: "5.0",
        slipLivePct: "3.0",
        avgDownEn: true,
        avgDownTrig: "4.0",
        avgDownMax: "2",
        avgDownQty: "35",
        avgDownGapH: "20",
        ...UNIVERSE_FORM_DEFAULTS,
      };
    }
  }

  // live
  if (c) {
    return {
      k: "0.42",
      ma: "5",
      stopPct: "1.6",
      maxPct: "6",
      dailyPct: "1.0",
      idxGatePct: "2.0",
      minScoreKr: "50",
      krWl: WATCH_KR,
      usWl: WATCH_US,
      aiCount: 5,
      partialTpEn: true,
      partialTpTrig: "2.5",
      partialTpSell: "25",
      partialTpTight: true,
      slipMockPct: "5.0",
      slipLivePct: "2.0",
      avgDownEn: false,
      avgDownTrig: "4.0",
      avgDownMax: "1",
      avgDownQty: "25",
      avgDownGapH: "24",
      ...UNIVERSE_FORM_DEFAULTS,
    };
  }
  if (a) {
    return {
      k: "0.58",
      ma: "5",
      stopPct: "2.5",
      maxPct: "15",
      dailyPct: "2.5",
      idxGatePct: "0.8",
      minScoreKr: "32",
      krWl: WATCH_KR,
      usWl: WATCH_US,
      aiCount: 5,
      partialTpEn: true,
      partialTpTrig: "3.5",
      partialTpSell: "35",
      partialTpTight: true,
      slipMockPct: "5.0",
      slipLivePct: "4.0",
      avgDownEn: true,
      avgDownTrig: "3.0",
      avgDownMax: "3",
      avgDownQty: "40",
      avgDownGapH: "12",
      ...UNIVERSE_FORM_DEFAULTS,
    };
  }
  // balanced live
  return {
    k: "0.5",
    ma: "5",
    stopPct: "2.0",
    maxPct: "10",
    dailyPct: "2.0",
    idxGatePct: "1.5",
    minScoreKr: "40",
    krWl: WATCH_KR,
    usWl: WATCH_US,
    aiCount: 5,
    partialTpEn: true,
    partialTpTrig: "3.0",
    partialTpSell: "30",
    partialTpTight: true,
    slipMockPct: "5.0",
    slipLivePct: "3.0",
    avgDownEn: false,
    avgDownTrig: "4.0",
    avgDownMax: "2",
    avgDownQty: "35",
    avgDownGapH: "20",
    ...UNIVERSE_FORM_DEFAULTS,
  };
}

export function getStrategyPreset(
  tier: StrategyTier,
  isMock: boolean,
  keepWatchlists: { krWl: string; usWl: string },
): StrategyFormFields {
  const s = base(tier, isMock);
  return {
    ...s,
    krWl: keepWatchlists.krWl || s.krWl,
    usWl: keepWatchlists.usWl || s.usWl,
  };
}

const PRESET_FORM_KEYS: (keyof StrategyFormFields)[] = [
  "k",
  "ma",
  "stopPct",
  "maxPct",
  "dailyPct",
  "idxGatePct",
  "minScoreKr",
  "krWl",
  "usWl",
  "aiCount",
  "partialTpEn",
  "partialTpTrig",
  "partialTpSell",
  "partialTpTight",
  "slipMockPct",
  "slipLivePct",
  "avgDownEn",
  "avgDownTrig",
  "avgDownMax",
  "avgDownQty",
  "avgDownGapH",
  "aiKrQualityGates",
  "aiKrMinCapEok",
  "maxSector",
];

function formFieldsEqual(
  a: StrategyFormFields,
  b: StrategyFormFields,
): boolean {
  for (const k of PRESET_FORM_KEYS) {
    const va = a[k];
    const vb = b[k];
    if (typeof va === "boolean" && typeof vb === "boolean") {
      if (va !== vb) return false;
    } else {
      if (String(va ?? "").trim() !== String(vb ?? "").trim()) return false;
    }
  }
  return true;
}

/**
 * 현재 폼이 어느 프리셋과 정확히 일치하는지 (감시종목 포함). 없으면 `custom`.
 */
export function inferStrategyTier(
  form: StrategyFormFields,
  isMock: boolean,
): StrategyTier | "custom" {
  for (const t of (["conservative", "balanced", "aggressive"] as const)) {
    const p = getStrategyPreset(t, isMock, { krWl: form.krWl, usWl: form.usWl });
    if (formFieldsEqual(p, form)) return t;
  }
  return "custom";
}

export function strategyTierLabel(
  tier: StrategyTier | "custom",
): { title: string; sub: string } {
  if (tier === "custom") {
    return { title: "사용자 지정", sub: "프리셋과 다른 수치" };
  }
  const x = STRATEGY_TIER_LABELS[tier];
  return { title: x.title, sub: x.blurb };
}
