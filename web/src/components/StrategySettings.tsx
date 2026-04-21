import { useEffect, useMemo, useState } from "react";
import { apiFetch } from "../api/client";
import type { AppConfig, ModeProfiles } from "../types";

type StrategyFormState = {
  k: string;
  ma: string;
  stopPct: string;
  maxPct: string;
  dailyPct: string;
  krWl: string;
  usWl: string;
  aiCount: number;
  partialTpEn: boolean;
  partialTpTrig: string;
  partialTpSell: string;
  partialTpTight: boolean;
  slipMockPct: string;
  slipLivePct: string;
};

function emptyForm(): StrategyFormState {
  return {
    k: "",
    ma: "",
    stopPct: "",
    maxPct: "",
    dailyPct: "",
    krWl: "",
    usWl: "",
    aiCount: 3,
    partialTpEn: true,
    partialTpTrig: "5",
    partialTpSell: "30",
    partialTpTight: true,
    slipMockPct: "5",
    slipLivePct: "3",
  };
}

function configToForm(config: Partial<AppConfig> | undefined): StrategyFormState {
  if (!config) return emptyForm();
  return {
    k: String(config.k_factor ?? 0.5),
    ma: String(config.ma_period ?? 5),
    stopPct:
      config.stop_loss_ratio != null
        ? (config.stop_loss_ratio * 100).toFixed(1)
        : "",
    maxPct:
      config.max_position_ratio != null
        ? String(Math.round(config.max_position_ratio * 100))
        : "",
    dailyPct:
      config.daily_profit_target != null
        ? (config.daily_profit_target * 100).toFixed(1)
        : "",
    krWl: Array.isArray(config.kr_watchlist)
      ? config.kr_watchlist.join(",")
      : String(config.kr_watchlist || ""),
    usWl: Array.isArray(config.us_watchlist)
      ? config.us_watchlist.join(",")
      : String(config.us_watchlist || ""),
    aiCount: parseInt(String(config.ai_stock_count), 10) || 3,
    partialTpEn: config.partial_tp_enabled !== false,
    partialTpTrig:
      config.partial_tp_trigger_pct != null
        ? (config.partial_tp_trigger_pct * 100).toFixed(1)
        : "5",
    partialTpSell:
      config.partial_tp_sell_ratio != null
        ? String(Math.round(config.partial_tp_sell_ratio * 100))
        : "30",
    partialTpTight: config.partial_tp_tighten_stop !== false,
    slipMockPct:
      config.max_entry_slip_pct_mock != null
        ? (config.max_entry_slip_pct_mock * 100).toFixed(1)
        : "5",
    slipLivePct:
      config.max_entry_slip_pct_live != null
        ? (config.max_entry_slip_pct_live * 100).toFixed(1)
        : "3",
  };
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
  onSaved: () => void;
}) {
  const [mockForm, setMockForm] = useState<StrategyFormState>(() =>
    emptyForm(),
  );
  const [liveForm, setLiveForm] = useState<StrategyFormState>(() =>
    emptyForm(),
  );
  const [isMock, setIsMock] = useState(true);
  const [dirty, setDirty] = useState(false);
  const [msg, setMsg] = useState("");
  const [saving, setSaving] = useState(false);

  const profilesStr = useMemo(
    () => (profiles ? JSON.stringify(profiles) : ""),
    [profiles],
  );

  useEffect(() => {
    if (dirty) return;
    if (profiles?.mock != null && profiles?.live != null) {
      setMockForm(configToForm(profiles.mock));
      setLiveForm(configToForm(profiles.live));
      if (config) setIsMock(config.is_mock !== false);
      return;
    }
    if (config) {
      const f = configToForm(config);
      setMockForm(f);
      setLiveForm(f);
      setIsMock(config.is_mock !== false);
    }
  }, [profilesStr, config, dirty]);

  const form = isMock ? mockForm : liveForm;

  function patchActive(patch: Partial<StrategyFormState>) {
    setDirty(true);
    if (isMock) {
      setMockForm((prev) => ({ ...prev, ...patch }));
    } else {
      setLiveForm((prev) => ({ ...prev, ...patch }));
    }
  }

  async function save() {
    const f = form;
    if (!isMock) {
      if (
        !window.confirm(
          "⚠️ 실전투자 모드\n실제 자산 거래됩니다. 정말 변경하시겠습니까?",
        )
      )
        return;
    }
    const maxPosRaw = parseFloat(f.maxPct);
    if (isNaN(maxPosRaw) || maxPosRaw <= 0 || maxPosRaw > 100) {
      setMsg("❌ 최대 비중은 1~100 사이로 입력해 주세요.");
      return;
    }
    setSaving(true);
    setMsg("저장 중…");
    try {
      const payload = {
        k_factor: parseFloat(f.k) || 0.5,
        ma_period: parseInt(f.ma, 10) || 5,
        stop_loss_ratio: (parseFloat(f.stopPct) || 0) / 100,
        max_position_ratio: maxPosRaw / 100,
        daily_profit_target: (parseFloat(f.dailyPct) || 0) / 100,
        kr_watchlist: f.krWl
          .split(",")
          .map((s) => s.trim())
          .filter(Boolean),
        us_watchlist: f.usWl
          .split(",")
          .map((s) => s.trim().toUpperCase())
          .filter(Boolean),
        is_mock: isMock,
        ai_stock_count: f.aiCount,
        partial_tp_enabled: f.partialTpEn,
        partial_tp_trigger_pct: (parseFloat(f.partialTpTrig) || 5) / 100,
        partial_tp_sell_ratio: (parseFloat(f.partialTpSell) || 30) / 100,
        partial_tp_tighten_stop: f.partialTpTight,
        max_entry_slip_pct_mock: (parseFloat(f.slipMockPct) || 5) / 100,
        max_entry_slip_pct_live: (parseFloat(f.slipLivePct) || 3) / 100,
      };
      const data = await apiFetch<{ ok: boolean; error?: string }>(
        "/api/config",
        {
          method: "POST",
          idToken,
          body: JSON.stringify(payload),
        },
      );
      if (data.ok) {
        setDirty(false);
        setMsg("✅ 저장 완료");
        onSaved();
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
        <span className="text-slate-500 text-xs group-open:rotate-180 transition-transform">
          ▼
        </span>
      </summary>
      <div className="p-4 sm:p-5 border-t border-white/5">
        <p className="text-[11px] text-slate-500 mb-3">
          모의·실전 각각 별도 저장됩니다. 투자 모드를 바꾸면 해당 모드에 저장된 값이
          표시됩니다.
        </p>
        <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-4 mb-4">
          <div>
            <label className="text-xs text-slate-500 block mb-1">K 팩터</label>
            <input
              type="number"
              step="0.1"
              className="w-full rounded-xl border border-white/10 bg-white/5 px-3 py-2 text-sm text-white"
              value={form.k}
              onChange={(e) => patchActive({ k: e.target.value })}
            />
          </div>
          <div>
            <label className="text-xs text-slate-500 block mb-1">이동평균 기간</label>
            <input
              type="number"
              className="w-full rounded-xl border border-white/10 bg-white/5 px-3 py-2 text-sm text-white"
              value={form.ma}
              onChange={(e) => patchActive({ ma: e.target.value })}
            />
          </div>
          <div>
            <label className="text-xs text-slate-500 block mb-1">손절 비율 (%)</label>
            <input
              type="number"
              step="0.1"
              className="w-full rounded-xl border border-white/10 bg-white/5 px-3 py-2 text-sm text-white"
              value={form.stopPct}
              onChange={(e) => patchActive({ stopPct: e.target.value })}
            />
          </div>
          <div>
            <label className="text-xs text-slate-500 block mb-1">최대 비중 (%)</label>
            <input
              type="number"
              min={1}
              max={100}
              className="w-full rounded-xl border border-white/10 bg-white/5 px-3 py-2 text-sm text-white"
              value={form.maxPct}
              onChange={(e) => patchActive({ maxPct: e.target.value })}
            />
          </div>
          <div>
            <label className="text-xs text-slate-500 block mb-1">일일 목표 수익 (%)</label>
            <input
              type="number"
              step="0.1"
              className="w-full rounded-xl border border-white/10 bg-white/5 px-3 py-2 text-sm text-white"
              value={form.dailyPct}
              onChange={(e) => patchActive({ dailyPct: e.target.value })}
            />
          </div>
          <div className="md:col-span-2">
            <label className="text-xs text-slate-500 block mb-1">
              한국 감시 종목 (쉼표)
            </label>
            <input
              className="w-full rounded-xl border border-white/10 bg-white/5 px-3 py-2 text-sm text-white"
              value={form.krWl}
              onChange={(e) => patchActive({ krWl: e.target.value })}
              placeholder="005930,000660"
            />
          </div>
          <div className="md:col-span-2">
            <label className="text-xs text-slate-500 block mb-1">
              미국 감시 종목 (쉼표)
            </label>
            <input
              className="w-full rounded-xl border border-white/10 bg-white/5 px-3 py-2 text-sm text-white"
              value={form.usWl}
              onChange={(e) => patchActive({ usWl: e.target.value })}
              placeholder="AAPL,NVDA"
            />
          </div>
          <div>
            <label className="text-xs text-slate-500 block mb-1">투자 모드</label>
            <select
              className="w-full rounded-xl border border-white/10 bg-white/5 px-3 py-2 text-sm text-white"
              value={isMock ? "mock" : "real"}
              onChange={(e) => {
                setIsMock(e.target.value === "mock");
                setDirty(true);
              }}
            >
              <option value="mock">모의투자</option>
              <option value="real">실전투자 ⚠️</option>
            </select>
          </div>
          <div>
            <label className="text-xs text-slate-500 block mb-1">AI 추천 종목 수</label>
            <div className="flex gap-2">
              {[3, 4, 5].map((n) => (
                <button
                  key={n}
                  type="button"
                  onClick={() => patchActive({ aiCount: n })}
                  className={`flex-1 py-2 rounded-lg text-sm font-bold ${
                    form.aiCount === n
                      ? "bg-indigo-600 text-white"
                      : "bg-white/5 text-slate-500"
                  }`}
                >
                  {n}
                </button>
              ))}
            </div>
          </div>
        </div>

        <div className="mt-6 pt-5 border-t border-white/10">
          <p className="text-xs font-medium text-slate-400 mb-3">
            분할 익절 · 돌파 후 추격 방지 (슬리피지)
          </p>
          <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-4 mb-2">
            <label className="flex items-center gap-2 text-xs text-slate-300 col-span-2 md:col-span-1 cursor-pointer">
              <input
                type="checkbox"
                className="rounded border-white/20"
                checked={form.partialTpEn}
                onChange={(e) => patchActive({ partialTpEn: e.target.checked })}
              />
              분할익절 사용
            </label>
            <div>
              <label className="text-xs text-slate-500 block mb-1">
                분할익절 시작 (%)
              </label>
              <input
                type="number"
                step="0.1"
                min={0.5}
                title="평단 대비"
                className="w-full rounded-xl border border-white/10 bg-white/5 px-3 py-2 text-sm text-white"
                value={form.partialTpTrig}
                onChange={(e) => patchActive({ partialTpTrig: e.target.value })}
              />
            </div>
            <div>
              <label className="text-xs text-slate-500 block mb-1">
                분할 매도 비중 (%)
              </label>
              <input
                type="number"
                step="1"
                min={10}
                max={90}
                title="보유 수량 중"
                className="w-full rounded-xl border border-white/10 bg-white/5 px-3 py-2 text-sm text-white"
                value={form.partialTpSell}
                onChange={(e) => patchActive({ partialTpSell: e.target.value })}
              />
            </div>
            <label className="flex items-center gap-2 text-xs text-slate-300 md:col-span-2 cursor-pointer">
              <input
                type="checkbox"
                className="rounded border-white/20"
                checked={form.partialTpTight}
                onChange={(e) => patchActive({ partialTpTight: e.target.checked })}
              />
              익절 후 손절선 본전 상향
            </label>
            <div>
              <label className="text-xs text-slate-500 block mb-1">
                추격 허용 (모의 %)
              </label>
              <input
                type="number"
                step="0.1"
                className="w-full rounded-xl border border-white/10 bg-white/5 px-3 py-2 text-sm text-white"
                value={form.slipMockPct}
                onChange={(e) => patchActive({ slipMockPct: e.target.value })}
              />
            </div>
            <div>
              <label className="text-xs text-slate-500 block mb-1">
                추격 허용 (실전 %)
              </label>
              <input
                type="number"
                step="0.1"
                className="w-full rounded-xl border border-white/10 bg-white/5 px-3 py-2 text-sm text-white"
                value={form.slipLivePct}
                onChange={(e) => patchActive({ slipLivePct: e.target.value })}
              />
            </div>
          </div>
          <p className="text-[10px] text-slate-600 mt-2">
            미저장 시 표시 기본값: 익절 +5% / 매도 30% / 모의 추격 5%·실전 3% (백엔드 기본과 동일)
          </p>
        </div>

        <div className="flex flex-wrap items-center gap-3 mt-4">
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
