import { useEffect, useState } from "react";
import { apiFetch } from "../api/client";

function formatElapsed(sec: number): string {
  const m = Math.floor(sec / 60);
  const s = sec % 60;
  return `${m}:${String(s).padStart(2, "0")}`;
}

function AiLoadingOverlay({ step, elapsed }: { step: number; elapsed: number }) {
  const steps = ["① 데이터", "② AI", "③ 점수"] as const;
  return (
    <div
      className="fixed inset-0 z-[200] flex items-center justify-center px-4"
      style={{
        background: "rgba(6,11,24,.82)",
        backdropFilter: "blur(10px)",
      }}
      role="alertdialog"
      aria-busy="true"
      aria-label="AI 분석 중"
    >
      <div
        className="relative w-full max-w-[22rem] overflow-hidden rounded-2xl p-6 shadow-2xl"
        style={{
          background: "linear-gradient(165deg,rgba(30,27,75,.95),rgba(15,23,42,.98))",
          border: "1px solid rgba(139,92,246,.35)",
          boxShadow:
            "0 0 80px rgba(139,92,246,.12),0 25px 50px -12px rgba(0,0,0,.55)",
        }}
      >
        <div
          className="pointer-events-none absolute -top-24 left-1/2 h-48 w-64 -translate-x-1/2 rounded-full opacity-40"
          style={{
            background: "radial-gradient(ellipse,rgba(139,92,246,.45),transparent 70%)",
          }}
        />
        <div className="relative flex flex-col items-center text-center">
          <div className="relative mb-5 flex h-[5.25rem] w-[5.25rem] items-center justify-center [animation:aiPulse_2s_ease-in-out_infinite]">
            <style>{`
              @keyframes aiPulse{0%,100%{opacity:.85;transform:scale(1)}50%{opacity:1;transform:scale(1.05)}}
              @keyframes aiSpin{to{transform:rotate(360deg)}}
              @keyframes aiSpinRev{to{transform:rotate(-360deg)}}
              @keyframes aiBar{0%{transform:translateX(-100%)}100%{transform:translateX(320%)}}
            `}</style>
            <div className="absolute inset-0 rounded-full border-2 border-violet-500/25" />
            <div
              className="absolute inset-0 rounded-full border-2 border-transparent border-t-violet-400 border-r-fuchsia-400"
              style={{ animation: "aiSpin 1.15s linear infinite" }}
            />
            <div
              className="absolute inset-[7px] rounded-full border border-emerald-500/20 border-b-teal-400/50"
              style={{ animation: "aiSpinRev 1.65s linear infinite" }}
            />
            <span className="select-none text-2xl" aria-hidden>
              ✨
            </span>
          </div>
          <p className="mb-0.5 text-[15px] font-bold tracking-tight text-white">AI 분석 실행 중</p>
          <p className="mb-4 text-[11px] leading-relaxed text-slate-500">
            시세 수집 → Gemini 분석 → 스코어링
          </p>
          <div
            className="mb-1 font-mono text-[1.65rem] font-bold tabular-nums tracking-[0.2em] text-transparent"
            style={{
              background: "linear-gradient(135deg,#e9d5ff,#a78bfa,#67e8f9)",
              WebkitBackgroundClip: "text",
              backgroundClip: "text",
            }}
          >
            {formatElapsed(elapsed)}
          </div>
          <p className="mb-4 text-[10px] text-slate-600">경과 시간 · 예상 1~3분 소요</p>
          <div
            className="mb-5 h-1.5 w-full overflow-hidden rounded-full"
            style={{ background: "rgba(30,41,59,.9)" }}
          >
            <div
              className="h-full w-[38%] rounded-full"
              style={{
                background: "linear-gradient(90deg,#7c3aed,#c084fc,#22d3ee,#7c3aed)",
                backgroundSize: "200% 100%",
                animation: "aiBar 2.2s ease-in-out infinite",
              }}
            />
          </div>
          <div className="flex w-full flex-wrap justify-center gap-2">
            {steps.map((label, i) => (
              <span
                key={label}
                className={`rounded-lg px-2.5 py-1 text-[10px] font-medium transition-all ${
                  i === step
                    ? "bg-violet-500/25 text-violet-200 shadow-[0_0_20px_rgba(139,92,246,.2)]"
                    : "bg-slate-800/80 text-slate-500"
                }`}
              >
                {label}
              </span>
            ))}
          </div>
          <p className="mt-4 text-[10px] leading-snug text-slate-600">
            창을 닫거나 새로고침하지 마세요.
            <br />
            서버가 끝날 때까지 기다려 주세요.
          </p>
        </div>
      </div>
    </div>
  );
}

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
  const [elapsed, setElapsed] = useState(0);
  const [step, setStep] = useState(0);
  /** "auto" = 서버에서 상한까지 자동, 숫자 = 이번에 추가 매수할 종목 수(최대 5, 잔여 슬롯 내) */
  const [addBuyMode, setAddBuyMode] = useState<"auto" | number>("auto");

  useEffect(() => {
    if (!busy) {
      setElapsed(0);
      setStep(0);
      return;
    }
    const t0 = Date.now();
    const id = window.setInterval(() => {
      setElapsed(Math.floor((Date.now() - t0) / 1000));
    }, 1000);
    const id2 = window.setInterval(() => {
      setStep((s) => (s + 1) % 3);
    }, 3800);
    return () => {
      window.clearInterval(id);
      window.clearInterval(id2);
    };
  }, [busy]);

  async function run(session: "manual" | "morning" | "afternoon" | "late") {
    const labels: Record<string, string> = {
      manual: "즉시 실행",
      morning: "오전 세션",
      afternoon: "오후 세션",
      late: "마감 세션",
    };
    const addLabel =
      addBuyMode === "auto"
        ? `잔여 슬롯까지 자동 매수 (최대 보유 ${aiStockCount}종)`
        : `이번에만 신규 ${addBuyMode}종목 매수 시도`;
    if (
      !window.confirm(
        `AI ${labels[session]} (${market})\n추천 ${aiStockCount}종 분석 → ${addLabel}\n\n진행할까요?`,
      )
    )
      return;
    setBusy(true);
    setMsg("");
    try {
      const payload: Record<string, unknown> = { session, market };
      if (addBuyMode !== "auto") payload.add_buy_count = addBuyMode;
      const data = await apiFetch<{ ok: boolean; error?: string }>(
        "/api/ai/run",
        {
          method: "POST",
          idToken,
          body: JSON.stringify(payload),
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
    <>
      {busy && <AiLoadingOverlay step={step} elapsed={elapsed} />}
      <div className="mt-4 glass rounded-2xl p-3 sm:p-4">
        <h3 className="mb-2 text-sm font-semibold text-white">AI 추천 매매</h3>
        <p className="mb-3 text-[11px] leading-relaxed text-slate-400">
          <span className="text-emerald-400/90 font-medium">① 아래에서 이번에 살 종목 수</span>를 고른 뒤{" "}
          <span className="text-white/90">② 즉시 실행</span>을 누르세요.
        </p>

        <div className="mb-4 rounded-xl border border-emerald-500/25 bg-emerald-950/30 p-3 sm:p-3.5 shadow-[inset_0_1px_0_0_rgba(52,211,153,.12)]">
          <label
            htmlFor="ai-add-buy"
            className="mb-2 block text-xs font-semibold tracking-tight text-emerald-100/95"
          >
            이번 실행 · 신규 매수 종목 수
          </label>
          <select
            id="ai-add-buy"
            disabled={busy}
            value={addBuyMode === "auto" ? "auto" : String(addBuyMode)}
            onChange={(e) => {
              const v = e.target.value;
              setAddBuyMode(v === "auto" ? "auto" : Number(v));
            }}
            className="tap-target min-h-12 w-full rounded-lg border border-white/15 bg-slate-950/90 px-3 py-2.5 text-sm font-medium text-white shadow-inner outline-none ring-1 ring-white/5 focus:border-emerald-400/50 focus:ring-emerald-500/20 disabled:opacity-50"
          >
            <option value="auto">자동 — 잔여 슬롯만큼 (최대 보유 {aiStockCount}종까지)</option>
            <option value="0">0종 — 분석만, 매수 안 함</option>
            <option value="1">정확히 1종만 매수 시도</option>
            <option value="2">2종까지</option>
            <option value="3">3종까지</option>
            <option value="4">4종까지</option>
            <option value="5">5종까지</option>
          </select>
          <p className="mt-2 text-[10px] leading-snug text-slate-500">
            「AI 종목 수」({aiStockCount}종)은 계좌당 최대 보유 상한입니다. 자동이면 (상한 − 현재 보유)만큼만
            삽니다.
          </p>
        </div>

        <div className="flex flex-col gap-3 md:grid md:grid-cols-2 md:gap-3">
          <button
            type="button"
            disabled={busy}
            onClick={() => run("manual")}
            className="tap-target min-h-14 rounded-xl border border-emerald-400/45 bg-gradient-to-r from-green-800 to-emerald-700 py-3.5 text-sm font-bold text-white shadow-lg shadow-emerald-950/40 disabled:opacity-50"
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
                className="tap-target min-h-11 rounded-lg border border-white/10 bg-white/5 py-2.5 text-[11px] font-semibold text-slate-300 hover:bg-white/10 disabled:opacity-50"
              >
                {label}
              </button>
            ))}
          </div>
        </div>
        {msg && !busy && (
          <p className="mt-2 text-center text-xs text-slate-400">{msg}</p>
        )}
      </div>
    </>
  );
}
