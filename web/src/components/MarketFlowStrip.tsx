import { tvSymbol } from "../utils/tradingViewSymbol";

/**
 * TradingView 티커 테이프 임베드는 KRX 등에서 느낌표·깨진 표시가 나는 경우가 있어,
 * 공식 차트로 가는 링크 스트립만 앱에서 제공합니다.
 */
export function MarketFlowStrip({
  market,
  watchlistCodes,
  className = "",
}: {
  market: "KR" | "US";
  watchlistCodes: string[];
  className?: string;
}) {
  const base: { label: string; sym: string }[] =
    market === "KR"
      ? [
          { label: "KOSPI", sym: "KRX:KOSPI" },
          { label: "KOSDAQ", sym: "KRX:KOSDAQ" },
        ]
      : [
          { label: "QQQ", sym: "NASDAQ:QQQ" },
          { label: "SPY", sym: "NASDAQ:SPY" },
        ];

  const seen = new Set<string>();
  const items: { label: string; sym: string }[] = [];
  for (const x of base) {
    if (!seen.has(x.sym)) {
      seen.add(x.sym);
      items.push(x);
    }
  }
  for (const c of watchlistCodes) {
    const sym = tvSymbol(c, market);
    if (!seen.has(sym)) {
      seen.add(sym);
      items.push({ label: c, sym });
    }
  }

  return (
    <div
      className={`rounded-2xl border border-white/[0.08] bg-[#060a12]/90 px-3 py-3 shadow-[inset_0_1px_0_0_rgba(255,255,255,.04)] ${className}`}
    >
      <div className="mb-2 flex items-center justify-between gap-2">
        <span className="text-[11px] font-semibold tracking-wide text-slate-400">
          시장 · 관심 바로가기
        </span>
        <span className="text-[10px] text-slate-600">TradingView(새 창)</span>
      </div>
      <div className="flex flex-wrap gap-2">
        {items.map(({ label, sym }) => (
          <a
            key={sym}
            href={`https://www.tradingview.com/chart/?symbol=${encodeURIComponent(sym)}`}
            target="_blank"
            rel="noopener noreferrer"
            className="rounded-lg border border-white/10 bg-white/[0.04] px-2.5 py-1.5 text-[11px] font-medium text-slate-200 hover:border-indigo-500/35 hover:bg-indigo-500/10 hover:text-white transition-colors"
          >
            {label}
            <span className="ml-0.5 text-slate-600" aria-hidden>
              ↗
            </span>
          </a>
        ))}
      </div>
    </div>
  );
}
