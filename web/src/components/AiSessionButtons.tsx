import { useState } from "react";
import { apiFetch } from "../api/client";

export function AiSessionButtons({
  idToken,
  market,
  aiStockCount,
  onDone,
}: {
  idToken: string;
  market: "KR" | "US";
  aiStockCount: number;
  onDone: () => void;
}) {
  const [busy, setBusy] = useState(false);
  const [msg, setMsg] = useState("");

  async function run(session: "manual" | "morning" | "afternoon" | "late") {
    const labels: Record<string, string> = {
      manual: "즉시 실행",
      morning: "오전 세션",
      afternoon: "오후 세션",
      late: "마감 세션",
    };
    if (
      !window.confirm(
        `AI ${labels[session]} (${market}) · 추천 ${aiStockCount}종목 분석 후 조건 충족 시 매수\n\n진행할까요?`,
      )
    )
      return;
    setBusy(true);
    setMsg("AI 분석 중… (20~40초)");
    try {
      const data = await apiFetch<{ ok: boolean; error?: string }>(
        "/api/ai/run",
        {
          method: "POST",
          idToken,
          body: JSON.stringify({ session, market }),
        },
      );
      if (data.ok) {
        setMsg("✅ 완료");
        onDone();
        setTimeout(() => setMsg(""), 3000);
      } else {
        setMsg("❌ " + (data.error || "오류"));
      }
    } catch (e: unknown) {
      setMsg("❌ " + (e instanceof Error ? e.message : "오류"));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="mt-4 glass rounded-2xl p-3 sm:p-4">
      <h3 className="text-sm font-semibold text-white mb-3">AI 추천 매매</h3>
      <div className="flex flex-col gap-3 md:grid md:grid-cols-2 md:gap-3">
        <button
          type="button"
          disabled={busy}
          onClick={() => run("manual")}
          className="tap-target min-h-12 py-3 rounded-xl text-sm font-bold text-white border border-emerald-500/40 bg-gradient-to-r from-green-800 to-emerald-700 disabled:opacity-50"
        >
          ⚡ 지금 즉시 AI 매매
        </button>
        <div className="grid grid-cols-3 gap-2">
          {(
            [
              ["morning", "오전"],
              ["afternoon", "오후"],
              ["late", "마감"],
            ] as const
          ).map(([s, label]) => (
            <button
              key={s}
              type="button"
              disabled={busy}
              onClick={() => run(s)}
              className="tap-target min-h-11 py-2.5 rounded-lg text-[11px] font-semibold bg-white/5 border border-white/10 text-slate-300 hover:bg-white/10 disabled:opacity-50"
            >
              {label}
            </button>
          ))}
        </div>
      </div>
      {msg && <p className="mt-2 text-xs text-center text-slate-400">{msg}</p>}
    </div>
  );
}
