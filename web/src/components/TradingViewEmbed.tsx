import { useEffect, useRef } from "react";
function mountTvWidget(
  el: HTMLElement,
  scriptFile: string,
  config: Record<string, unknown>,
) {
  el.innerHTML = "";
  const wrap = document.createElement("div");
  wrap.className = "tradingview-widget-container";
  wrap.style.height = "100%";
  wrap.style.width = "100%";
  const inner = document.createElement("div");
  inner.className = "tradingview-widget-container__widget";
  inner.style.height = "100%";
  inner.style.minHeight = "100%";
  wrap.appendChild(inner);
  const sc = document.createElement("script");
  sc.src = `https://s3.tradingview.com/external-embedding/${scriptFile}`;
  sc.type = "text/javascript";
  sc.async = true;
  sc.innerHTML = JSON.stringify(config);
  wrap.appendChild(sc);
  el.appendChild(wrap);
}

type BaseProps = {
  className?: string;
  /** config가 바뀌면 위젯 재마운트 */
  configKey: string;
  scriptFile: string;
  config: Record<string, unknown>;
};

function TradingViewMount({ className, configKey, scriptFile, config }: BaseProps) {
  const ref = useRef<HTMLDivElement>(null);
  const cfgJson = JSON.stringify(config);
  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    mountTvWidget(el, scriptFile, JSON.parse(cfgJson) as Record<string, unknown>);
    return () => {
      el.innerHTML = "";
    };
  }, [configKey, scriptFile, cfgJson]);
  return <div ref={ref} className={className} />;
}

const TV_NOTE = (
  <p className="text-[10px] text-slate-600 text-center pt-2 border-t border-white/[0.06]">
    ©{" "}
    <a
      href="https://kr.tradingview.com/"
      target="_blank"
      rel="noopener noreferrer"
      className="text-slate-500 hover:text-slate-400"
    >
      TradingView
    </a>{" "}
    — 시세는 제3자 제공이며 참고용입니다.
  </p>
);

export function TradingViewMiniSymbol({
  symbol,
  className = "",
}: {
  symbol: string;
  className?: string;
}) {
  const config: Record<string, unknown> = {
    symbol,
    width: "100%",
    height: "100%",
    locale: "kr",
    dateRange: "3M",
    colorTheme: "dark",
    isTransparent: true,
    autosize: true,
    largeChartUrl: "",
  };
  return (
    <div
      className={`rounded-xl overflow-hidden bg-[#060a12] ring-1 ring-white/[0.06] ${className}`}
      style={{ minHeight: 132, height: 132 }}
    >
      <TradingViewMount
        configKey={symbol}
        scriptFile="embed-widget-mini-symbol-overview.js"
        config={config}
        className="h-full w-full"
      />
    </div>
  );
}

export function TradingViewAdvancedChart({
  symbol,
  className = "",
}: {
  symbol: string;
  className?: string;
}) {
  const config: Record<string, unknown> = {
    autosize: true,
    symbol,
    interval: "D",
    timezone: "Asia/Seoul",
    theme: "dark",
    style: "1",
    locale: "kr",
    allow_symbol_change: true,
    hide_top_toolbar: false,
    hide_side_toolbar: true,
    hide_legend: false,
    save_image: false,
    hide_volume: false,
    backgroundColor: "#080c16",
    gridColor: "rgba(42,46,57,0.45)",
    support_host: "https://www.tradingview.com",
    width: "100%",
    height: 400,
  };
  return (
    <div
      className={`rounded-2xl border border-white/[0.08] overflow-hidden bg-[#080c16] ${className}`}
    >
      <div className="flex items-center justify-between px-3 py-2 border-b border-white/[0.06]">
        <span className="text-xs font-semibold text-slate-300 font-mono">{symbol}</span>
        <span className="text-[10px] font-medium px-2 py-0.5 rounded-md bg-indigo-500/15 text-indigo-300/90">
          TradingView
        </span>
      </div>
      <div className="w-full" style={{ minHeight: 320, height: "min(52vh, 440px)" }}>
        <TradingViewMount
          configKey={symbol}
          scriptFile="embed-widget-advanced-chart.js"
          config={config}
          className="h-full w-full min-h-[320px]"
        />
      </div>
      {TV_NOTE}
    </div>
  );
}
