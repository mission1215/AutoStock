import { useCallback, useEffect, useState } from "react";
import { apiFetch } from "../api/client";
import type { PositionKr, PositionUs } from "../types";
import { TradingViewAdvancedChart } from "./TradingViewEmbed";
import { tvSymbol } from "../utils/tradingViewSymbol";
import type { MarketScope } from "../utils/marketScope";

function inferMarket(code: string): "KR" | "US" {
  const c = code.trim().toUpperCase();
  return /^[A-Z]+$/.test(c) ? "US" : "KR";
}

export function ManualTradingPanel({
  idToken,
  positionsKr,
  positionsUs,
  onOrderSuccess,
  marketScope = "kr",
}: {
  idToken: string;
  positionsKr: Record<string, PositionKr>;
  positionsUs: Record<string, PositionUs>;
  onOrderSuccess: () => void;
  marketScope?: MarketScope;
}) {
  const [code, setCode] = useState("");
  const [qty, setQty] = useState("");
  const [price, setPrice] = useState("");
  const [quoteLoading, setQuoteLoading] = useState(false);
  const [quote, setQuote] = useState<{
    name: string;
    price: number;
    market: "KR" | "US";
  } | null>(null);
  const [quoteErr, setQuoteErr] = useState("");
  const [orderBusy, setOrderBusy] = useState(false);
  const [orderMsg, setOrderMsg] = useState("");
  const [showTvChart, setShowTvChart] = useState(false);

  const refreshQuote = useCallback(async () => {
    const c = code.trim().toUpperCase();
    if (!c) {
      setQuote(null);
      setQuoteErr("");
      return;
    }
    const mkt = inferMarket(c);
    setQuoteLoading(true);
    setQuoteErr("");
    try {
      const data = await apiFetch<{
        ok: boolean;
        name?: string;
        price?: number;
        market?: string;
        error?: string;
      }>(
        `/api/quote?stock_code=${encodeURIComponent(c)}&market=${mkt}`,
        { idToken },
      );
      if (data.ok && data.price != null) {
        setQuote({
          name: data.name || "",
          price: Number(data.price),
          market: mkt,
        });
      } else {
        setQuote(null);
        setQuoteErr(data.error || "조회 실패");
      }
    } catch (e: unknown) {
      setQuote(null);
      setQuoteErr(e instanceof Error ? e.message : "오류");
    } finally {
      setQuoteLoading(false);
    }
  }, [code, idToken]);

  useEffect(() => {
    const t = setTimeout(() => {
      void refreshQuote();
    }, 450);
    return () => clearTimeout(t);
  }, [code, refreshQuote]);

  async function placeOrder(side: "buy" | "sell") {
    const c = code.trim().toUpperCase();
    if (!c) {
      setOrderMsg("종목코드를 입력하세요.");
      return;
    }
    const mkt = inferMarket(c);
    const q = parseInt(qty, 10) || 0;
    const p = parseFloat(price) || 0;
    const label = side === "buy" ? "매수" : "매도";
    const refLine =
      quote && quote.market === mkt
        ? `\n\n참고 현재가: ${mkt === "US" ? "$" + quote.price.toFixed(2) : quote.price.toLocaleString() + "원"}`
        : "";
    if (
      !window.confirm(
        `[${label}][${mkt}] ${c}${q > 0 ? " " + q + "주" : " (자동수량)"}${p > 0 ? " 지정가 " + p : " 시장가"}${refLine}\n\n진행하시겠습니까?`,
      )
    )
      return;
    setOrderBusy(true);
    setOrderMsg(`${label} 주문 중…`);
    try {
      const data = await apiFetch<{
        ok: boolean;
        error?: string;
        quantity?: number;
        price?: number;
        stock_name?: string;
        note?: string;
      }>("/api/order", {
        method: "POST",
        idToken,
        body: JSON.stringify({
          stock_code: c,
          side,
          quantity: q,
          price: p,
          market: mkt,
        }),
      });
      if (data.ok) {
        setOrderMsg(
          `✅ ${label} 완료 · ${data.quantity}주 · 기준가 ${data.price != null ? (mkt === "US" ? "$" + data.price : data.price.toLocaleString() + "원") : ""}${data.stock_name ? " · " + data.stock_name : ""}`,
        );
        onOrderSuccess();
      } else {
        setOrderMsg("❌ " + (data.error || "오류"));
      }
    } catch (e: unknown) {
      setOrderMsg("❌ " + (e instanceof Error ? e.message : "네트워크 오류"));
    } finally {
      setOrderBusy(false);
    }
  }

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
      if (data.ok) {
        setOrderMsg(`✅ ${sym} 매도 완료`);
        onOrderSuccess();
      } else {
        setOrderMsg("❌ " + (data.error || "오류"));
      }
    } catch (e: unknown) {
      setOrderMsg("❌ " + (e instanceof Error ? e.message : "오류"));
    }
  }

  const posList: { code: string; market: "KR" | "US"; p: PositionKr | PositionUs }[] = [
    ...(marketScope !== "us"
      ? Object.entries(positionsKr).map(([code, p]) => ({
          code,
          market: "KR" as const,
          p,
        }))
      : []),
    ...(marketScope !== "kr"
      ? Object.entries(positionsUs).map(([code, p]) => ({
          code,
          market: "US" as const,
          p,
        }))
      : []),
  ];

  const chartMarket: "KR" | "US" =
    quote?.market ?? (code.trim() ? inferMarket(code.trim()) : "KR");

  return (
    <div className="mt-4 space-y-6">
    <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
      <div className="space-y-3">
        <h3 className="text-sm font-semibold text-slate-300">
          직접 주문
          <span className="text-xs text-blue-400 font-normal ml-2">
            {code ? (inferMarket(code) === "KR" ? "🇰🇷 한국" : "🇺🇸 미국") : ""}
          </span>
        </h3>
        <p className="text-[11px] text-slate-500 leading-relaxed">
          시장가는 가격 0. 미국은 수량을 직접 입력하세요. 매수가 기록은 주문 직전 조회 시세를
          사용합니다.
        </p>
        <div>
          <div className="flex items-center justify-between mb-1">
            <label className="text-xs text-slate-500">종목코드 / 티커</label>
            <button
              type="button"
              disabled={!code.trim()}
              onClick={() => setShowTvChart((v) => !v)}
              className="text-[11px] font-medium text-cyan-400/90 hover:text-cyan-300 disabled:opacity-40 disabled:cursor-not-allowed"
            >
              {showTvChart ? "차트 닫기" : "TradingView 차트"}
            </button>
          </div>
          <input
            className="w-full rounded-xl border border-white/10 bg-white/5 px-3 py-2.5 text-sm text-white uppercase"
            value={code}
            onChange={(e) => setCode(e.target.value.toUpperCase())}
            placeholder="005930 또는 AAPL"
          />
        </div>
        <div className="rounded-xl border border-white/5 bg-white/[0.03] px-3 py-2.5 text-xs min-h-[72px]">
          {quoteLoading ? (
            <span className="text-slate-500">시세 조회 중…</span>
          ) : quoteErr ? (
            <span className="text-amber-400/90">{quoteErr}</span>
          ) : quote ? (
            <>
              <div className="text-[11px] text-slate-500 mb-1">
                현재가(참고)
                {quote.name ? (
                  <span className="text-slate-400"> · {quote.name}</span>
                ) : null}
              </div>
              <div className="text-base font-bold text-white">
                {quote.market === "US"
                  ? `$${quote.price.toFixed(2)}`
                  : `${quote.price.toLocaleString()}원`}
              </div>
            </>
          ) : (
            <span className="text-slate-500">종목을 입력하면 시세가 표시됩니다.</span>
          )}
        </div>
        <div className="grid grid-cols-2 gap-3">
          <div>
            <label className="text-xs text-slate-500 block mb-1">
              수량 <span className="text-slate-600">(빈칸=자동)</span>
            </label>
            <input
              type="number"
              min={1}
              className="w-full rounded-xl border border-white/10 bg-white/5 px-3 py-2 text-sm text-white"
              value={qty}
              onChange={(e) => setQty(e.target.value)}
              placeholder="자동"
            />
          </div>
          <div>
            <label className="text-xs text-slate-500 block mb-1">
              가격 <span className="text-slate-600">(0=시장가)</span>
            </label>
            <input
              type="number"
              min={0}
              className="w-full rounded-xl border border-white/10 bg-white/5 px-3 py-2 text-sm text-white"
              value={price}
              onChange={(e) => setPrice(e.target.value)}
              placeholder="0"
            />
          </div>
        </div>
        <div className="grid grid-cols-2 gap-3">
          <button
            type="button"
            disabled={orderBusy}
            onClick={() => placeOrder("buy")}
            className="py-3 rounded-xl font-bold text-sm bg-gradient-to-br from-blue-700 to-blue-600 text-white disabled:opacity-50"
          >
            매수
          </button>
          <button
            type="button"
            disabled={orderBusy}
            onClick={() => placeOrder("sell")}
            className="py-3 rounded-xl font-bold text-sm bg-gradient-to-br from-red-800 to-red-600 text-white disabled:opacity-50"
          >
            매도
          </button>
        </div>
        {orderMsg && (
          <p className="text-xs text-center text-slate-400 py-1">{orderMsg}</p>
        )}
      </div>

      <div>
        <h3 className="text-sm font-semibold text-slate-300 mb-3">보유 포지션</h3>
        <div className="space-y-2 max-h-80 overflow-y-auto pr-1">
          {posList.length === 0 ? (
            <p className="text-slate-600 text-xs text-center py-8">보유 포지션 없음</p>
          ) : (
            posList.map(({ code: sym, market: mkt, p }) => (
              <div
                key={mkt + sym}
                className="flex items-center justify-between gap-2 rounded-xl border border-white/5 bg-white/[0.03] px-3 py-2 text-xs"
              >
                <div className="min-w-0">
                  <span className="font-bold text-white">{sym}</span>
                  <span className="text-slate-500 ml-1">{mkt}</span>
                  {p.stock_name && (
                    <div className="text-slate-500 truncate">{p.stock_name}</div>
                  )}
                  <div className="text-slate-400">
                    {(p.quantity ?? 0).toLocaleString()}주
                  </div>
                </div>
                <div className="flex flex-col gap-1 items-end shrink-0">
                  <button
                    type="button"
                    onClick={() => {
                      setCode(sym);
                      setShowTvChart(true);
                    }}
                    className="rounded-lg border border-cyan-500/25 px-2 py-1 text-[10px] text-cyan-300/90 hover:bg-cyan-500/10"
                  >
                    차트
                  </button>
                  <button
                    type="button"
                    onClick={() => quickSell(sym, mkt)}
                    className="rounded-lg border border-red-500/30 px-2 py-1 text-red-300 hover:bg-red-500/10"
                  >
                    전량 매도
                  </button>
                </div>
              </div>
            ))
          )}
        </div>
      </div>
    </div>

    {showTvChart && code.trim() ? (
      <TradingViewAdvancedChart symbol={tvSymbol(code.trim(), chartMarket)} />
    ) : null}
    </div>
  );
}
