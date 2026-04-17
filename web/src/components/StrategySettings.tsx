import { useEffect, useState } from "react";
import { apiFetch } from "../api/client";
import type { AppConfig } from "../types";

export function StrategySettings({
  idToken,
  config,
  onSaved,
}: {
  idToken: string;
  config: AppConfig | undefined;
  onSaved: () => void;
}) {
  const [k, setK] = useState("");
  const [ma, setMa] = useState("");
  const [stopPct, setStopPct] = useState("");
  const [maxPct, setMaxPct] = useState("");
  const [dailyPct, setDailyPct] = useState("");
  const [krWl, setKrWl] = useState("");
  const [usWl, setUsWl] = useState("");
  const [isMock, setIsMock] = useState(true);
  const [aiCount, setAiCount] = useState(3);
  const [msg, setMsg] = useState("");
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    if (!config) return;
    setK(String(config.k_factor ?? 0.5));
    setMa(String(config.ma_period ?? 5));
    setStopPct(
      config.stop_loss_ratio != null
        ? (config.stop_loss_ratio * 100).toFixed(1)
        : "",
    );
    setMaxPct(
      config.max_position_ratio != null
        ? String(Math.round(config.max_position_ratio * 100))
        : "",
    );
    setDailyPct(
      config.daily_profit_target != null
        ? (config.daily_profit_target * 100).toFixed(1)
        : "",
    );
    setKrWl(
      Array.isArray(config.kr_watchlist)
        ? config.kr_watchlist.join(",")
        : String(config.kr_watchlist || ""),
    );
    setUsWl(
      Array.isArray(config.us_watchlist)
        ? config.us_watchlist.join(",")
        : String(config.us_watchlist || ""),
    );
    setIsMock(config.is_mock !== false);
    setAiCount(parseInt(String(config.ai_stock_count), 10) || 3);
  }, [config]);

  async function save() {
    if (!isMock) {
      if (
        !window.confirm(
          "⚠️ 실전투자 모드\n실제 자산 거래됩니다. 정말 변경하시겠습니까?",
        )
      )
        return;
    }
    const maxPosRaw = parseFloat(maxPct);
    if (isNaN(maxPosRaw) || maxPosRaw <= 0 || maxPosRaw > 100) {
      setMsg("❌ 최대 비중은 1~100 사이로 입력해 주세요.");
      return;
    }
    setSaving(true);
    setMsg("저장 중…");
    try {
      const payload = {
        k_factor: parseFloat(k) || 0.5,
        ma_period: parseInt(ma, 10) || 5,
        stop_loss_ratio: (parseFloat(stopPct) || 0) / 100,
        max_position_ratio: maxPosRaw / 100,
        daily_profit_target: (parseFloat(dailyPct) || 0) / 100,
        kr_watchlist: krWl
          .split(",")
          .map((s) => s.trim())
          .filter(Boolean),
        us_watchlist: usWl
          .split(",")
          .map((s) => s.trim().toUpperCase())
          .filter(Boolean),
        is_mock: isMock,
        ai_stock_count: aiCount,
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
        <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-4 mb-4">
          <div>
            <label className="text-xs text-slate-500 block mb-1">K 팩터</label>
            <input
              type="number"
              step="0.1"
              className="w-full rounded-xl border border-white/10 bg-white/5 px-3 py-2 text-sm text-white"
              value={k}
              onChange={(e) => setK(e.target.value)}
            />
          </div>
          <div>
            <label className="text-xs text-slate-500 block mb-1">이동평균 기간</label>
            <input
              type="number"
              className="w-full rounded-xl border border-white/10 bg-white/5 px-3 py-2 text-sm text-white"
              value={ma}
              onChange={(e) => setMa(e.target.value)}
            />
          </div>
          <div>
            <label className="text-xs text-slate-500 block mb-1">손절 비율 (%)</label>
            <input
              type="number"
              step="0.1"
              className="w-full rounded-xl border border-white/10 bg-white/5 px-3 py-2 text-sm text-white"
              value={stopPct}
              onChange={(e) => setStopPct(e.target.value)}
            />
          </div>
          <div>
            <label className="text-xs text-slate-500 block mb-1">최대 비중 (%)</label>
            <input
              type="number"
              min={1}
              max={100}
              className="w-full rounded-xl border border-white/10 bg-white/5 px-3 py-2 text-sm text-white"
              value={maxPct}
              onChange={(e) => setMaxPct(e.target.value)}
            />
          </div>
          <div>
            <label className="text-xs text-slate-500 block mb-1">일일 목표 수익 (%)</label>
            <input
              type="number"
              step="0.1"
              className="w-full rounded-xl border border-white/10 bg-white/5 px-3 py-2 text-sm text-white"
              value={dailyPct}
              onChange={(e) => setDailyPct(e.target.value)}
            />
          </div>
          <div className="md:col-span-2">
            <label className="text-xs text-slate-500 block mb-1">
              한국 감시 종목 (쉼표)
            </label>
            <input
              className="w-full rounded-xl border border-white/10 bg-white/5 px-3 py-2 text-sm text-white"
              value={krWl}
              onChange={(e) => setKrWl(e.target.value)}
              placeholder="005930,000660"
            />
          </div>
          <div className="md:col-span-2">
            <label className="text-xs text-slate-500 block mb-1">
              미국 감시 종목 (쉼표)
            </label>
            <input
              className="w-full rounded-xl border border-white/10 bg-white/5 px-3 py-2 text-sm text-white"
              value={usWl}
              onChange={(e) => setUsWl(e.target.value)}
              placeholder="AAPL,NVDA"
            />
          </div>
          <div>
            <label className="text-xs text-slate-500 block mb-1">투자 모드</label>
            <select
              className="w-full rounded-xl border border-white/10 bg-white/5 px-3 py-2 text-sm text-white"
              value={isMock ? "mock" : "real"}
              onChange={(e) => setIsMock(e.target.value === "mock")}
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
                  onClick={() => setAiCount(n)}
                  className={`flex-1 py-2 rounded-lg text-sm font-bold ${
                    aiCount === n
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
        <div className="flex flex-wrap items-center gap-3">
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
