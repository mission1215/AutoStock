import { useEffect, useMemo, useState } from "react";
import { apiFetch } from "../api/client";
import type { AppConfig, ModeProfiles } from "../types";
import {
  getStrategyPreset,
  STRATEGY_TIER_LABELS,
  inferStrategyTier,
  strategyTierLabel,
  type StrategyFormFields,
  type StrategyTier,
} from "../config/strategyPresets";
import {
  type MarketScope,
  normalizeMarketScope,
} from "../utils/marketScope";

type StrategyFormState = StrategyFormFields;

/** 권장 기본(보통) — 모의/실전 프리셋 */
const WATCH_DEFAULTS = {
  krWl: "005930,000660,035420,035720,051910",
  usWl: "AAPL,NVDA,TSLA,MSFT,GOOGL",
};

const PRESET_MOCK: StrategyFormState = {
  ...getStrategyPreset("balanced", true, WATCH_DEFAULTS),
};
const PRESET_LIVE: StrategyFormState = {
  ...getStrategyPreset("balanced", false, WATCH_DEFAULTS),
};

function configToForm(config: Partial<AppConfig> | undefined): StrategyFormState {
  if (!config) return { ...PRESET_MOCK };
  const isMock = config.is_mock !== false;
  const base = isMock ? PRESET_MOCK : PRESET_LIVE;
  return {
    k: String(config.k_factor ?? base.k),
    ma: String(config.ma_period ?? base.ma),
    stopPct: config.stop_loss_ratio != null ? (config.stop_loss_ratio * 100).toFixed(1) : base.stopPct,
    maxPct: config.max_position_ratio != null ? String(Math.round(config.max_position_ratio * 100)) : base.maxPct,
    dailyPct: config.daily_profit_target != null ? (config.daily_profit_target * 100).toFixed(1) : base.dailyPct,
    idxGatePct: config.kr_index_drop_limit_pct != null ? String(config.kr_index_drop_limit_pct) : base.idxGatePct,
    minScoreKr: config.min_score_kr != null ? String(config.min_score_kr) : base.minScoreKr,
    krWl: Array.isArray(config.kr_watchlist) ? config.kr_watchlist.join(",") : String(config.kr_watchlist || base.krWl),
    usWl: Array.isArray(config.us_watchlist) ? config.us_watchlist.join(",") : String(config.us_watchlist || base.usWl),
    aiCount: (() => {
      const n = Number(config.ai_stock_count);
      return Number.isFinite(n) && n >= 3 && n <= 5 ? Math.round(n) : base.aiCount;
    })(),
    partialTpEn: config.partial_tp_enabled !== false,
    partialTpTrig: config.partial_tp_trigger_pct != null ? (config.partial_tp_trigger_pct * 100).toFixed(1) : base.partialTpTrig,
    partialTpSell: config.partial_tp_sell_ratio != null ? String(Math.round(config.partial_tp_sell_ratio * 100)) : base.partialTpSell,
    partialTpTight: config.partial_tp_tighten_stop !== false,
    slipMockPct: config.max_entry_slip_pct_mock != null ? (config.max_entry_slip_pct_mock * 100).toFixed(1) : base.slipMockPct,
    slipLivePct: config.max_entry_slip_pct_live != null ? (config.max_entry_slip_pct_live * 100).toFixed(1) : base.slipLivePct,
    avgDownEn: config.avg_down_enabled ?? base.avgDownEn,
    avgDownTrig: config.avg_down_trigger_pct != null ? (config.avg_down_trigger_pct * 100).toFixed(1) : base.avgDownTrig,
    avgDownMax: config.avg_down_max_times != null ? String(config.avg_down_max_times) : base.avgDownMax,
    avgDownQty: config.avg_down_qty_ratio != null ? String(Math.round(config.avg_down_qty_ratio * 100)) : base.avgDownQty,
    avgDownGapH: config.avg_down_min_interval_hours != null ? String(config.avg_down_min_interval_hours) : base.avgDownGapH,
    aiKrQualityGates: config.ai_universe_kr_quality_gates !== false,
    aiKrMinCapEok:
      config.ai_universe_kr_min_cap_eok != null &&
      Number.isFinite(Number(config.ai_universe_kr_min_cap_eok))
        ? String(Math.max(0, Number(config.ai_universe_kr_min_cap_eok)))
        : base.aiKrMinCapEok,
    maxSector:
      config.max_positions_per_sector != null &&
      Number.isFinite(Number(config.max_positions_per_sector))
        ? String(Math.max(1, Math.min(10, Math.round(Number(config.max_positions_per_sector)))))
        : base.maxSector,
  };
}

function inp(cls = "") {
  return `w-full rounded-xl border border-white/10 bg-white/5 px-3 py-2 text-sm text-white ${cls}`;
}

export function StrategySettings({
  idToken,
  config,
  profiles,
  onSaved,
}: {
  idToken: string;
  config: AppConfig | undefined;
  profiles?: ModeProfiles;
  onSaved: () => void | Promise<void>;
}) {
  const [mockForm, setMockForm] = useState<StrategyFormState>(() => ({ ...PRESET_MOCK }));
  const [liveForm, setLiveForm] = useState<StrategyFormState>(() => ({ ...PRESET_LIVE }));
  const [isMock, setIsMock] = useState(true);
  const [dirty, setDirty] = useState(false);
  const [msg, setMsg] = useState("");
  const [saving, setSaving] = useState(false);
  const [tierHint, setTierHint] = useState<string | null>(null);
  const [marketScope, setMarketScope] = useState<MarketScope>("kr");
  /** KR 스케줄 AI 입력 유니버스 — US 전략에는 영향 없음 */
  const [aiUniverseMode, setAiUniverseMode] = useState<"legacy" | "dynamic">("legacy");

  const profilesStr = useMemo(() => (profiles ? JSON.stringify(profiles) : ""), [profiles]);

  function normalizeAiUniverseMode(raw: unknown): "legacy" | "dynamic" {
    if (typeof raw !== "string") return "legacy";
    const x = raw.trim().toLowerCase();
    if (x === "dynamic" || x === "kis" || x === "rank" || x === "auto") return "dynamic";
    return "legacy";
  }

  useEffect(() => {
    if (dirty) return;
    if (profiles?.mock != null && profiles?.live != null) {
      setMockForm(configToForm({ ...profiles.mock, is_mock: true }));
      setLiveForm(configToForm({ ...profiles.live, is_mock: false }));
      if (config) {
        setIsMock(config.is_mock !== false);
        setMarketScope(normalizeMarketScope(config.market_scope));
        setAiUniverseMode(normalizeAiUniverseMode(config.ai_universe_mode));
      }
      return;
    }
    if (config) {
      const f = configToForm(config);
      setMockForm(f);
      setLiveForm({ ...f, ...PRESET_LIVE, krWl: f.krWl, usWl: f.usWl });
      setIsMock(config.is_mock !== false);
      setMarketScope(normalizeMarketScope(config.market_scope));
      setAiUniverseMode(normalizeAiUniverseMode(config.ai_universe_mode));
    }
  }, [profilesStr, config, dirty]);

  const form = isMock ? mockForm : liveForm;

  const currentProfile = (isMock ? profiles?.mock : profiles?.live) as AppConfig | undefined;
  const savedStrategyTier = currentProfile?.strategy_tier;
  const inferredStrategyTier = useMemo(
    () => inferStrategyTier(form, isMock),
    [isMock, mockForm, liveForm],
  );

  function patch(p: Partial<StrategyFormState>) {
    setDirty(true);
    if (isMock) setMockForm((prev) => ({ ...prev, ...p }));
    else setLiveForm((prev) => ({ ...prev, ...p }));
  }

  /** 이전 "모의/실전 권장값" = 보통(균형) 프리셋 + 현재 감시종목 유지 */
  function applyModeDefaults() {
    setDirty(true);
    const current = isMock ? mockForm : liveForm;
    const next = getStrategyPreset("balanced", isMock, {
      krWl: current.krWl,
      usWl: current.usWl,
    });
    if (isMock) setMockForm({ ...next });
    else setLiveForm({ ...next });
    setTierHint(STRATEGY_TIER_LABELS.balanced.blurb);
  }

  function applyStrategyTier(tier: StrategyTier) {
    setDirty(true);
    const current = isMock ? mockForm : liveForm;
    const next = getStrategyPreset(tier, isMock, {
      krWl: current.krWl,
      usWl: current.usWl,
    });
    if (isMock) setMockForm({ ...next });
    else setLiveForm({ ...next });
    setTierHint(STRATEGY_TIER_LABELS[tier].blurb);
  }

  async function save() {
    if (!isMock && !window.confirm("⚠️ 실전투자 모드\n실제 자산 거래됩니다. 정말 변경하시겠습니까?")) return;
    const maxPosRaw = parseFloat(form.maxPct);
    if (isNaN(maxPosRaw) || maxPosRaw <= 0 || maxPosRaw > 100) {
      setMsg("❌ 최대 비중은 1~100 사이로 입력해 주세요.");
      return;
    }
    const capEok = parseFloat(form.aiKrMinCapEok);
    if (Number.isNaN(capEok) || capEok < 0) {
      setMsg("❌ 시총 하한(억)은 0 이상 숫자로 입력해 주세요. (0 = 미사용)");
      return;
    }
    const maxSec = parseInt(form.maxSector, 10);
    if (Number.isNaN(maxSec) || maxSec < 1 || maxSec > 10) {
      setMsg("❌ 섹터당 최대 종목 수는 1~10 사이로 입력해 주세요.");
      return;
    }
    setSaving(true);
    setMsg("저장 중…");
    try {
      const payload = {
        k_factor: parseFloat(form.k) || 0.5,
        ma_period: parseInt(form.ma, 10) || 5,
        stop_loss_ratio: (parseFloat(form.stopPct) || 0) / 100,
        max_position_ratio: maxPosRaw / 100,
        daily_profit_target: (parseFloat(form.dailyPct) || 0) / 100,
        kr_index_drop_limit_pct: parseFloat(form.idxGatePct) || 0,
        min_score_kr: parseInt(form.minScoreKr, 10) || 40,
        kr_watchlist: form.krWl.split(",").map((s) => s.trim()).filter(Boolean),
        us_watchlist: form.usWl.split(",").map((s) => s.trim().toUpperCase()).filter(Boolean),
        is_mock: isMock,
        ai_stock_count: Math.min(5, Math.max(3, Math.round(Number(form.aiCount)) || 5)),
        partial_tp_enabled: form.partialTpEn,
        partial_tp_trigger_pct: (parseFloat(form.partialTpTrig) || 5) / 100,
        partial_tp_sell_ratio: (parseFloat(form.partialTpSell) || 30) / 100,
        partial_tp_tighten_stop: form.partialTpTight,
        max_entry_slip_pct_mock: (parseFloat(form.slipMockPct) || 5) / 100,
        max_entry_slip_pct_live: (parseFloat(form.slipLivePct) || 3) / 100,
        max_entry_slip_pct: isMock
          ? (parseFloat(form.slipMockPct) || 5) / 100
          : (parseFloat(form.slipLivePct) || 3) / 100,
        avg_down_enabled: form.avgDownEn,
        avg_down_trigger_pct: (parseFloat(form.avgDownTrig) || 4) / 100,
        avg_down_max_times: parseInt(form.avgDownMax, 10) || 2,
        avg_down_qty_ratio: (parseFloat(form.avgDownQty) || 35) / 100,
        avg_down_min_interval_hours: parseFloat(form.avgDownGapH) || 20,
        strategy_tier: (() => {
          const inf = inferStrategyTier(form, isMock);
          return inf === "custom" ? null : inf;
        })(),
        market_scope: marketScope,
        ai_universe_mode: aiUniverseMode,
        ai_universe_kr_quality_gates: form.aiKrQualityGates,
        ai_universe_kr_min_cap_eok: Math.max(0, capEok),
        max_positions_per_sector: maxSec,
      };
      const data = await apiFetch<{ ok: boolean; error?: string }>("/api/config", {
        method: "POST",
        idToken,
        body: JSON.stringify(payload),
      });
      if (data.ok) {
        setMsg("✅ 저장 완료");
        await Promise.resolve(onSaved());
        setDirty(false);
        setTimeout(() => setMsg(""), 2500);
      } else {
        setMsg("❌ " + (data.error || "오류"));
      }
    } catch (e: unknown) {
      setMsg("❌ " + (e instanceof Error ? e.message : "오류"));
    } finally {
      setSaving(false);
    }
  }

  return (
    <details className="mt-8 glass rounded-2xl overflow-hidden group">
      <summary className="cursor-pointer list-none px-4 sm:px-5 py-4 flex items-center justify-between border-b border-white/5">
        <span className="text-sm font-semibold text-white flex items-center gap-2">
          <span className="w-1 h-4 rounded-full bg-purple-500 inline-block" />
          전략 설정
        </span>
        <span className="text-slate-500 text-xs group-open:rotate-180 transition-transform">▼</span>
      </summary>

      <div className="p-4 sm:p-5 space-y-6">
        {/* 모드 선택 + 기본값 버튼 */}
        <div
          className="rounded-xl border border-cyan-500/20 bg-cyan-500/5 px-3 py-3 space-y-2"
          role="group"
          aria-label="자동매매 시장 범위"
        >
          <p className="text-xs text-slate-400">
            <span className="text-cyan-300 font-medium">시장 범위</span> — 스케줄 자동매매·스케줄 AI·장마감
            자동 청산이 대상으로 할 시장입니다. (수동 주문·대시보드 조회는 제한 없음)
          </p>
          <div className="flex flex-wrap gap-2">
            {(
              [
                { id: "both" as const, label: "🇰🇷+🇺🇸 국내+미국" },
                { id: "kr" as const, label: "🇰🇷 국내만" },
                { id: "us" as const, label: "🇺🇸 미국만" },
              ] as const
            ).map(({ id, label }) => (
              <button
                key={id}
                type="button"
                onClick={() => {
                  setMarketScope(id);
                  setDirty(true);
                }}
                className={`px-3 py-2 rounded-lg text-xs font-medium border transition-all ${
                  marketScope === id
                    ? "border-cyan-400/70 bg-cyan-600/30 text-cyan-100"
                    : "border-white/10 bg-white/5 text-slate-300 hover:border-cyan-500/40"
                }`}
              >
                {label}
              </button>
            ))}
          </div>
          <p className="text-[10px] text-slate-500 leading-relaxed">
            국내 매도 대금이 당일 미국 주식 주문에 바로 쓰이지 않을 수 있습니다(결제
            T+2 등). 미국 가용 달러는 KIS·예수금에서 확인하세요.
          </p>
          <p className="text-xs text-slate-400 pt-2 border-t border-cyan-500/15">
            <span className="text-cyan-300 font-medium">🇰🇷 AI 입력 종목 풀</span> — 스케줄 AI가
            시세를 모을 국내 종목 범위입니다. (미국 AI·수동 주문은 그대로)
          </p>
          <div className="flex flex-wrap gap-2">
            {(
              [
                { id: "legacy" as const, label: "고정 풀 (기본)" },
                { id: "dynamic" as const, label: "동적 (KIS 거래량·거래대금 순위)" },
              ] as const
            ).map(({ id, label }) => (
              <button
                key={id}
                type="button"
                onClick={() => {
                  setAiUniverseMode(id);
                  setDirty(true);
                }}
                className={`px-3 py-2 rounded-lg text-xs font-medium border transition-all ${
                  aiUniverseMode === id
                    ? "border-cyan-400/70 bg-cyan-600/30 text-cyan-100"
                    : "border-white/10 bg-white/5 text-slate-300 hover:border-cyan-500/40"
                }`}
              >
                {label}
              </button>
            ))}
          </div>
          <p className="text-[10px] text-slate-500 leading-relaxed">
            동적 모드는 한국투자 Open API 순위(거래량·거래대금)로 최대 20종을 구성하고,
            감시 종목은 그대로 우선 포함합니다. API 오류·데이터 부족 시 자동으로 고정 풀로
            돌아갑니다.
          </p>
          <div
            className="mt-3 rounded-lg border border-cyan-500/20 bg-slate-950/40 px-3 py-3 space-y-3"
            role="group"
            aria-label="국내 동적 풀 고급 설정"
          >
            <p className="text-[11px] text-slate-400">
              <span className="text-cyan-300/90 font-medium">동적 모드 품질 필터</span> — KIS 현재가 기준
              (모의/실전 현재 프로필에 저장됩니다. 레거시 풀만 쓸 때는 무시됩니다.)
            </p>
            <label className="flex items-center gap-2 text-xs text-slate-200 cursor-pointer">
              <input
                type="checkbox"
                className="rounded border-white/20"
                checked={form.aiKrQualityGates}
                onChange={(e) => {
                  patch({ aiKrQualityGates: e.target.checked });
                }}
              />
              KIS 2차 필터 (투자유의·관리·정리매매·임시정지·시장경고 등 제외)
            </label>
            <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
              <div>
                <label className="text-xs text-slate-500 block mb-1">
                  시총 하한 (억 원) <span className="text-slate-600 text-[10px]">0 = 사용 안 함</span>
                </label>
                <input
                  type="number"
                  min={0}
                  step={50}
                  className={inp()}
                  value={form.aiKrMinCapEok}
                  onChange={(e) => patch({ aiKrMinCapEok: e.target.value })}
                />
              </div>
              <div>
                <label className="text-xs text-slate-500 block mb-1">
                  섹터당 최대 종목 수{" "}
                  <span className="text-slate-600 text-[10px]">(전략·AI·추천 분산)</span>
                </label>
                <input
                  type="number"
                  min={1}
                  max={10}
                  className={inp()}
                  value={form.maxSector}
                  onChange={(e) => patch({ maxSector: e.target.value })}
                />
              </div>
            </div>
            <p className="text-[10px] text-slate-500 leading-relaxed">
              시총은 상장주수 × 현재가 근사값입니다. 초소형 제거에 쓰려면 예: 300 (300억 이상).
            </p>
          </div>
        </div>

        <div className="flex flex-wrap items-center gap-3">
          <select
            className="rounded-xl border border-white/10 bg-white/5 px-3 py-2 text-sm text-white"
            value={isMock ? "mock" : "real"}
            onChange={(e) => { setIsMock(e.target.value === "mock"); setDirty(true); }}
          >
            <option value="mock">🧪 모의투자</option>
            <option value="real">💰 실전투자 ⚠️</option>
          </select>
          <button
            type="button"
            onClick={applyModeDefaults}
            className="px-3 py-2 rounded-xl text-xs border border-white/10 text-slate-400 hover:text-white hover:border-white/30 transition-all"
          >
            {isMock ? "모의: 보통 권장값" : "실전: 보통 권장값"}
          </button>
          <span className="text-[11px] text-slate-600">모의·실전 각각 별도 저장</span>
        </div>

        <div
          className="rounded-xl border border-indigo-500/25 bg-indigo-500/5 px-3 py-3 space-y-2"
          role="group"
          aria-label="성향 프리셋"
        >
          <p className="text-xs text-slate-400">
            <span className="text-indigo-300 font-medium">성향 프리셋</span> — 흔히 쓰는 위험·진입
            민감도 묶음입니다. 수익을 보장하지 않으며,{" "}
            <span className="text-slate-500">저장</span>해야 서버에 반영됩니다.
          </p>
          <div className="flex flex-wrap gap-2" role="radiogroup" aria-label="사전 정의된 성향">
            {(["conservative", "balanced", "aggressive"] as const).map((t) => {
              const isActive = inferredStrategyTier === t;
              return (
                <button
                  key={t}
                  type="button"
                  role="radio"
                  aria-checked={isActive}
                  aria-label={STRATEGY_TIER_LABELS[t].title}
                  onClick={() => applyStrategyTier(t)}
                  className={`px-3 py-2 rounded-lg text-xs font-medium border transition-all focus:outline-none focus-visible:ring-2 focus-visible:ring-indigo-500/60 ${
                    isActive
                      ? "border-indigo-400/80 bg-indigo-600/35 text-indigo-100 shadow-[0_0_0_1px_rgba(129,140,248,.35)]"
                      : "border-white/10 bg-white/5 text-slate-200 hover:border-indigo-500/50 hover:bg-indigo-500/10"
                  }`}
                >
                  {STRATEGY_TIER_LABELS[t].title}
                </button>
              );
            })}
          </div>
          <div className="rounded-lg bg-slate-950/40 border border-white/[0.06] px-2.5 py-2 space-y-1.5 text-[11px] leading-relaxed text-slate-400">
            <p>
              <span className="text-slate-500">이 모드(모의/실전)에 마지막으로 저장됨</span>{" "}
              {savedStrategyTier && savedStrategyTier in STRATEGY_TIER_LABELS ? (
                <span className="font-semibold text-emerald-200/90">
                  {STRATEGY_TIER_LABELS[savedStrategyTier as StrategyTier].title}
                </span>
              ) : (
                <span className="text-slate-500">— (아직 없음, 저장 시 기록)</span>
              )}
              {dirty && (
                <span className="text-amber-300/80 ml-1.5">· 미저장 변경 있음</span>
              )}
            </p>
            <p>
              <span className="text-slate-500">현재 입력·수치가 맞는 프리셋</span>{" "}
              {inferredStrategyTier === "custom" ? (
                <span className="font-medium text-amber-200/80">사용자 지정 (프리셋과 정확히 일치 않음)</span>
              ) : (
                <span className="font-semibold text-indigo-200/90">
                  {STRATEGY_TIER_LABELS[inferredStrategyTier].title}
                </span>
              )}
            </p>
            {savedStrategyTier &&
            ["conservative", "balanced", "aggressive"].includes(savedStrategyTier) &&
            inferredStrategyTier !== "custom" &&
            savedStrategyTier !== inferredStrategyTier && (
              <p className="text-amber-200/80">
                ↑ 저장 기록({strategyTierLabel(savedStrategyTier as StrategyTier).title})과
                지금 수치(
                {strategyTierLabel(inferredStrategyTier as StrategyTier).title})이 다릅니다.
                {dirty ? " 저장하면 아래에 맞춰 갱신됩니다." : " 숫자를 손댄 뒤 저장하거나, 프리셋을 다시 누르세요."}
              </p>
            )}
          </div>
          {tierHint && (
            <p className="text-[11px] text-slate-500 leading-relaxed border-t border-white/5 pt-2">{tierHint}</p>
          )}
        </div>

        {/* 기본 설정 */}
        <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-4">
          <div>
            <label className="text-xs text-slate-500 block mb-1">K 팩터 <span className="text-slate-600 text-[10px]" title="시가 + K × 전일범위 = 목표가">(?)</span></label>
            <input type="number" step="0.1" min="0.1" max="1.5" className={inp()} value={form.k} onChange={(e) => patch({ k: e.target.value })} />
          </div>
          <div>
            <label className="text-xs text-slate-500 block mb-1">이동평균 기간</label>
            <input type="number" className={inp()} value={form.ma} onChange={(e) => patch({ ma: e.target.value })} />
          </div>
          <div>
            <label className="text-xs text-slate-500 block mb-1">손절 비율 (%)</label>
            <input type="number" step="0.1" className={inp()} value={form.stopPct} onChange={(e) => patch({ stopPct: e.target.value })} />
          </div>
          <div>
            <label className="text-xs text-slate-500 block mb-1">최대 비중 (%)</label>
            <input type="number" min={1} max={100} className={inp()} value={form.maxPct} onChange={(e) => patch({ maxPct: e.target.value })} />
          </div>
          <div>
            <label className="text-xs text-slate-500 block mb-1">일일 목표 수익 (%)</label>
            <input type="number" step="0.1" className={inp()} value={form.dailyPct} onChange={(e) => patch({ dailyPct: e.target.value })} />
          </div>
          <div>
            <label className="text-xs text-slate-500 block mb-1">지수 급락 게이트 (%) <span className="text-slate-600 text-[10px]" title="KOSPI 이 % 이상 하락 시 매수 차단. 0 = 비활성">(?)</span></label>
            <input type="number" step="0.1" min="0" max="10" className={inp()} value={form.idxGatePct} onChange={(e) => patch({ idxGatePct: e.target.value })} />
          </div>
          <div>
            <label className="text-xs text-slate-500 block mb-1">최소 점수 <span className="text-slate-600 text-[10px]" title="100점 만점. 낮을수록 더 많은 종목 매수">(?)</span></label>
            <input type="number" min="0" max="100" className={inp()} value={form.minScoreKr} onChange={(e) => patch({ minScoreKr: e.target.value })} />
          </div>
          <div>
            <label className="text-xs text-slate-500 block mb-1">AI 추천 종목 수</label>
            <div className="flex gap-2">
              {[3, 4, 5].map((n) => (
                <button
                  key={n}
                  type="button"
                  onClick={() => patch({ aiCount: n })}
                  className={`flex-1 py-2 rounded-lg text-sm font-bold transition-all ${form.aiCount === n ? "bg-indigo-600 text-white" : "bg-white/5 text-slate-500 hover:bg-white/10"}`}
                >
                  {n}
                </button>
              ))}
            </div>
          </div>
          <div className="col-span-2">
            <label className="text-xs text-slate-500 block mb-1">한국 감시 종목 (쉼표)</label>
            <input className={inp()} value={form.krWl} onChange={(e) => patch({ krWl: e.target.value })} placeholder="005930,000660" />
          </div>
          <div className="col-span-2">
            <label className="text-xs text-slate-500 block mb-1">미국 감시 종목 (쉼표)</label>
            <input className={inp()} value={form.usWl} onChange={(e) => patch({ usWl: e.target.value })} placeholder="AAPL,NVDA" />
          </div>
        </div>

        {/* 고급: 추격 허용 + 분할익절 */}
        <div className="border-t border-white/10 pt-4">
          <p className="text-xs font-medium text-slate-400 mb-3">고급 · 추격 허용 / 분할익절 / 물타기</p>
          <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-4">
            <div>
              <label className="text-xs text-slate-500 block mb-1">추격 허용 — 모의 (%)</label>
              <input type="number" step="0.1" className={inp()} value={form.slipMockPct} onChange={(e) => patch({ slipMockPct: e.target.value })} />
            </div>
            <div>
              <label className="text-xs text-slate-500 block mb-1">추격 허용 — 실전 (%)</label>
              <input type="number" step="0.1" className={inp()} value={form.slipLivePct} onChange={(e) => patch({ slipLivePct: e.target.value })} />
            </div>
            <label className="flex items-center gap-2 text-xs text-slate-300 cursor-pointer">
              <input type="checkbox" className="rounded border-white/20" checked={form.partialTpEn} onChange={(e) => patch({ partialTpEn: e.target.checked })} />
              분할익절 사용
            </label>
            <div>
              <label className="text-xs text-slate-500 block mb-1">분할익절 시작 (%)</label>
              <input type="number" step="0.1" min={0.5} className={inp()} value={form.partialTpTrig} onChange={(e) => patch({ partialTpTrig: e.target.value })} />
            </div>
            <div>
              <label className="text-xs text-slate-500 block mb-1">분할 매도 비중 (%)</label>
              <input type="number" step="1" min={10} max={90} className={inp()} value={form.partialTpSell} onChange={(e) => patch({ partialTpSell: e.target.value })} />
            </div>
            <label className="flex items-center gap-2 text-xs text-slate-300 cursor-pointer">
              <input type="checkbox" className="rounded border-white/20" checked={form.partialTpTight} onChange={(e) => patch({ partialTpTight: e.target.checked })} />
              익절 후 손절선 본전 상향
            </label>
            <label className="flex items-center gap-2 text-xs text-slate-300 cursor-pointer">
              <input type="checkbox" className="rounded border-white/20" checked={form.avgDownEn} onChange={(e) => patch({ avgDownEn: e.target.checked })} />
              물타기 사용
            </label>
            <div>
              <label className="text-xs text-slate-500 block mb-1">물타기 하락폭 (%)</label>
              <input type="number" step="0.1" min={1} className={inp()} value={form.avgDownTrig} onChange={(e) => patch({ avgDownTrig: e.target.value })} />
            </div>
            <div>
              <label className="text-xs text-slate-500 block mb-1">물타기 최대 횟수</label>
              <input type="number" min={0} max={5} className={inp()} value={form.avgDownMax} onChange={(e) => patch({ avgDownMax: e.target.value })} />
            </div>
            <div>
              <label className="text-xs text-slate-500 block mb-1">추가 매수 비율 (%)</label>
              <input type="number" min={10} max={80} className={inp()} value={form.avgDownQty} onChange={(e) => patch({ avgDownQty: e.target.value })} />
            </div>
            <div>
              <label className="text-xs text-slate-500 block mb-1">물타기 최소 간격 (h)</label>
              <input type="number" min={1} className={inp()} value={form.avgDownGapH} onChange={(e) => patch({ avgDownGapH: e.target.value })} />
            </div>
          </div>
        </div>

        {/* 저장 버튼 */}
        <div className="flex flex-wrap items-center gap-3 pt-2">
          <button
            type="button"
            disabled={saving}
            onClick={save}
            className="px-5 py-2.5 rounded-xl text-sm font-semibold bg-gradient-to-r from-blue-700 to-indigo-600 text-white disabled:opacity-50"
          >
            설정 저장
          </button>
          {msg && <span className="text-xs text-slate-400">{msg}</span>}
        </div>
      </div>
    </details>
  );
}
