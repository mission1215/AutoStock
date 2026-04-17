import { apiFetch } from "../api/client";
import type { PositionKr, PositionUs } from "../types";

function fmtPrice(mkt: "KR" | "US", v: number | undefined) {
  if (v == null || Number.isNaN(v)) return "—";
  return mkt === "US" ? `$${v.toFixed(2)}` : `${Math.round(v).toLocaleString()}원`;
}

export function HoldingsPanel({
  idToken,
  positionsKr,
  positionsUs,
  onChange,
}: {
  idToken: string;
  positionsKr: Record<string, PositionKr>;
  positionsUs: Record<string, PositionUs>;
  onChange: () => void;
}) {
  const rows: { code: string; market: "KR" | "US"; p: PositionKr | PositionUs }[] = [
    ...Object.entries(positionsKr).map(([code, p]) => ({
      code,
      market: "KR" as const,
      p,
    })),
    ...Object.entries(positionsUs).map(([code, p]) => ({
      code,
      market: "US" as const,
      p,
    })),
  ];

  async function quickSell(sym: string, market: "KR" | "US") {
    if (
      !window.confirm(
        `[매도][${market}] ${sym} 전량 시장가 매도\n\n진행하시겠습니까?`,
      )
    )
      return;
    try {
      const data = await apiFetch<{ ok: boolean; error?: string }>(
        "/api/order",
        {
          method: "POST",
          idToken,
          body: JSON.stringify({
            stock_code: sym,
            side: "sell",
            quantity: 0,
            price: 0,
            market,
          }),
        },
      );
      if (data.ok) onChange();
      else alert(data.error || "오류");
    } catch (e: unknown) {
      alert(e instanceof Error ? e.message : "오류");
    }
  }

  return (
    <div className="glass rounded-2xl p-3 sm:p-4">
      <div className="flex flex-col sm:flex-row sm:items-baseline sm:justify-between gap-1 mb-2">
        <h3 className="text-sm font-semibold text-slate-200">
          보유 포지션
          {rows.length > 0 && (
            <span className="ml-1.5 text-xs font-normal text-slate-500">
              {rows.length}종목
            </span>
          )}
        </h3>
        <p className="text-[10px] text-slate-600 leading-snug sm:text-right sm:max-w-[60%]">
          매수가·목표·손절은 전략·주문 시점 기준입니다.
        </p>
      </div>

      <div className="rounded-xl border border-white/[0.06] bg-[#060a12]/60 overflow-hidden">
        {rows.length === 0 ? (
          <p className="text-slate-600 text-xs text-center py-10">보유 포지션 없음</p>
        ) : (
          rows.map(({ code, market: mkt, p }) => {
            const pnl = p.pnl ?? 0;
            const pr = p.pnl_ratio ?? 0;
            const posPnl = pnl >= 0;
            const accent = posPnl ? "#22c55e" : "#f87171";
            const pc = posPnl ? "text-emerald-400" : "text-red-400";
            const target = p.target_sell_price ?? 0;
            const bp = p.buy_price ?? 0;
            const cur = p.current_price ?? 0;
            const progress =
              target > 0 && bp > 0
                ? Math.min(
                    100,
                    Math.max(0, ((cur - bp) / (target - bp)) * 100),
                  )
                : 0;
            const isAI = (p.source || "").includes("AI");
            const pnlStr =
              mkt === "US"
                ? `$${Math.abs(pnl).toFixed(2)}`
                : `${Math.abs(pnl).toLocaleString()}원`;

            return (
              <div
                key={mkt + code}
                className="group relative border-b border-white/[0.05] last:border-b-0 hover:bg-white/[0.02] transition-colors"
              >
                <div
                  className="absolute left-0 top-0 bottom-0 w-[3px] opacity-90"
                  style={{ background: accent }}
                  aria-hidden
                />
                <div className="pl-3.5 pr-2 py-2 sm:py-2.5">
                  <div className="flex items-start gap-2">
                    <div className="min-w-0 flex-1 space-y-1">
                      <div className="flex flex-wrap items-start gap-x-2 gap-y-1">
                        <div className="min-w-0 flex-1">
                          <div className="text-sm sm:text-[15px] font-semibold text-white leading-snug truncate">
                            {p.stock_name || code}
                          </div>
                          <div className="flex flex-wrap items-center gap-x-2 gap-y-0.5 mt-0.5">
                            {(p.stock_name || "").trim() ? (
                              <span className="font-mono text-[11px] text-slate-500 tabular-nums">
                                {code}
                              </span>
                            ) : null}
                            <span className="text-[9px] font-medium uppercase tracking-wide text-slate-500 border border-white/10 rounded px-1 py-px">
                              {mkt}
                            </span>
                            {isAI && (
                              <span className="text-[9px] font-semibold text-violet-300/90 bg-violet-500/15 border border-violet-500/25 rounded px-1 py-px">
                                AI
                              </span>
                            )}
                          </div>
                        </div>
                      </div>

                      <div className="flex flex-wrap items-baseline gap-x-3 gap-y-0.5 text-[11px]">
                        <span className="text-slate-500">
                          평균{" "}
                          <span className="text-slate-300 tabular-nums">
                            {fmtPrice(mkt, bp)}
                          </span>
                        </span>
                        <span className="text-slate-500">
                          현재{" "}
                          <span className={`tabular-nums ${cur ? pc : "text-slate-500"}`}>
                            {cur ? fmtPrice(mkt, cur) : "—"}
                          </span>
                        </span>
                        <span className={`font-medium tabular-nums ${pc}`}>
                          {(pr >= 0 ? "+" : "") + pr}%
                          <span className="text-slate-500 font-normal mx-1">·</span>
                          <span>
                            {pnl >= 0 ? "+" : "-"}
                            {pnlStr}
                          </span>
                        </span>
                      </div>

                      {(target > 0 || (p.stop_loss_price ?? 0) > 0) && (
                        <div className="flex flex-col sm:flex-row sm:items-center gap-1.5 sm:gap-3 pt-0.5">
                          <div className="text-[10px] text-slate-500 flex flex-wrap gap-x-2 gap-y-0">
                            <span>
                              목표{" "}
                              <span className="text-emerald-400/90 tabular-nums">
                                {target > 0 ? fmtPrice(mkt, target) : "—"}
                              </span>
                            </span>
                            <span className="text-slate-600">·</span>
                            <span>
                              손절{" "}
                              <span className="text-red-400/80 tabular-nums">
                                {p.stop_loss_price != null && p.stop_loss_price > 0
                                  ? fmtPrice(mkt, p.stop_loss_price)
                                  : "—"}
                              </span>
                            </span>
                          </div>
                          {target > 0 && (
                            <div className="flex items-center gap-2 flex-1 min-w-0 sm:max-w-[200px]">
                              <div className="h-1 flex-1 rounded-full bg-white/[0.07] overflow-hidden">
                                <div
                                  className="h-full rounded-full bg-gradient-to-r from-sky-500/80 to-emerald-500/90"
                                  style={{ width: `${progress}%` }}
                                />
                              </div>
                              <span className="text-[10px] text-slate-600 tabular-nums w-8 text-right shrink-0">
                                {progress.toFixed(0)}%
                              </span>
                            </div>
                          )}
                        </div>
                      )}
                    </div>

                    <button
                      type="button"
                      onClick={() => quickSell(code, mkt)}
                      className="tap-target shrink-0 min-h-10 min-w-[3.25rem] text-xs sm:text-xs font-semibold px-3 py-2 rounded-lg bg-red-500/15 text-red-300/95 border border-red-500/25 hover:bg-red-500/25 opacity-90 group-hover:opacity-100 transition-opacity"
                    >
                      매도
                    </button>
                  </div>
                </div>
              </div>
            );
          })
        )}
      </div>
    </div>
  );
}
