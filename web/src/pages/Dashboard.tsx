import { useEffect, useState, useCallback } from "react";
import { apiFetch } from "../api/client";
import type { StatusResponse, TradeRow, LogEntry } from "../types";

function tradePriceStr(t: TradeRow) {
  return t.market === "US"
    ? `$${Number(t.price || 0).toFixed(2)}`
    : `${Number(t.price || 0).toLocaleString()}원`;
}
import { EquityChart } from "../components/charts/EquityChart";
import { PositionMixChart } from "../components/charts/PositionMixChart";
import { ManualTradingPanel } from "../components/ManualTradingPanel";
import { StrategySettings } from "../components/StrategySettings";
import { AiSessionButtons } from "../components/AiSessionButtons";
import { HoldingsPanel } from "../components/HoldingsPanel";
import { MarketFlowStrip } from "../components/MarketFlowStrip";
import {
  pickRecord,
  watchlistCodeKeys,
  buildSparklineCloses,
} from "../utils/sparklineCloses";
import { Sparkline } from "../components/Sparkline";
import { tvSymbol } from "../utils/tradingViewSymbol";
import { formatKst } from "../utils/formatKst";

const POLL_MS = 20000;

export function Dashboard({
  idToken,
  currentMarket,
  setMarket,
}: {
  idToken: string;
  currentMarket: "KR" | "US";
  setMarket: (m: "KR" | "US") => void;
}) {
  const [status, setStatus] = useState<StatusResponse | null>(null);
  const [trades, setTrades] = useState<TradeRow[]>([]);
  const [logs, setLogs] = useState<LogEntry[]>([]);
  const [tab, setTab] = useState<"auto" | "manual">("auto");
  const [err, setErr] = useState("");
  const [loading, setLoading] = useState(true);
  const [botBusy, setBotBusy] = useState(false);

  const loadStatus = useCallback(async () => {
    try {
      const data = await apiFetch<StatusResponse>("/api/status", { idToken });
      setStatus(data);
      if (!data.ok) setErr(data.error || "");
      else setErr("");
    } catch (e: unknown) {
      setErr(e instanceof Error ? e.message : "상태 조회 실패");
    } finally {
      setLoading(false);
    }
  }, [idToken]);

  const loadTrades = useCallback(async () => {
    try {
      const data = await apiFetch<{ ok: boolean; trades?: TradeRow[] }>(
        "/api/trades",
        { idToken },
      );
      if (data.ok && data.trades) setTrades(data.trades);
    } catch {
      /* ignore */
    }
  }, [idToken]);

  const loadLogs = useCallback(async () => {
    try {
      const data = await apiFetch<{ ok: boolean; logs?: LogEntry[] }>(
        "/api/logs",
        { idToken },
      );
      if (data.ok && data.logs) setLogs(data.logs);
    } catch {
      /* ignore */
    }
  }, [idToken]);

  useEffect(() => {
    loadStatus();
    loadTrades();
    loadLogs();
    const t = setInterval(() => {
      loadStatus();
      loadTrades();
      loadLogs();
    }, POLL_MS);
    return () => clearInterval(t);
  }, [loadStatus, loadTrades, loadLogs]);

  async function botControl(action: "start" | "stop" | "resume") {
    const labels = { start: "시작", stop: "중지", resume: "매매 재개" };
    if (!window.confirm(`자동매매 봇을 ${labels[action]}하시겠습니까?`)) return;
    setBotBusy(true);
    try {
      const data = await apiFetch<{ ok: boolean; message?: string; error?: string }>(
        "/api/bot",
        {
          method: "POST",
          idToken,
          body: JSON.stringify({ action }),
        },
      );
      if (data.ok) {
        await loadStatus();
      } else {
        alert(data.error || "오류");
      }
    } catch (e: unknown) {
      alert(e instanceof Error ? e.message : "요청 실패");
    } finally {
      setBotBusy(false);
    }
  }

  const cfg = status?.config;
  const botState = status?.state;
  /** Firestore에 없으면 기본 켜짐 */
  const botOn = botState?.bot_enabled !== false;
  const wl =
    currentMarket === "KR"
      ? cfg?.kr_watchlist || []
      : cfg?.us_watchlist || [];
  const wlData =
    currentMarket === "KR"
      ? status?.watchlist_data || {}
      : status?.us_watchlist_data || {};
  const positionsKr = status?.positions_kr || {};
  const positionsUs = status?.positions_us || {};
  const bal = status?.balance as Record<string, string> | undefined;

  if (loading && !status) {
    return (
      <div className="mx-auto w-full max-w-6xl px-4 sm:px-6 py-16 flex justify-center">
        <div className="h-10 w-10 animate-spin rounded-full border-2 border-blue-500 border-t-transparent" />
      </div>
    );
  }

  return (
    <div className="mx-auto w-full max-w-6xl px-4 sm:px-5 md:px-6 lg:px-8 pb-6 sm:pb-8">
      {/* 헤더 */}
      <header className="pt-2 pb-5 border-b border-white/[0.06]">
        <div className="flex flex-col gap-4 sm:flex-row sm:items-end sm:justify-between">
          <div>
            <p className="text-[10px] uppercase tracking-[0.2em] text-slate-500 mb-1">
              AutoStock
            </p>
            <h1 className="text-xl sm:text-2xl font-semibold tracking-tight text-white">
              대시보드
            </h1>
            <p className="text-xs text-slate-500 mt-1">
              {status?.updated_at
                ? `마지막 갱신 ${status.updated_at} · ${Math.round(POLL_MS / 1000)}초마다 동기화`
                : ""}
            </p>
          </div>
          <div
            className="flex w-full sm:w-auto rounded-xl bg-slate-950/80 p-1 border border-white/[0.08] shadow-inner self-stretch sm:self-auto"
            role="group"
            aria-label="시장 선택"
          >
            <button
              type="button"
              onClick={() => setMarket("KR")}
              className={`tap-target min-h-11 flex-1 sm:flex-initial rounded-lg px-4 py-2.5 text-sm font-medium transition-colors ${
                currentMarket === "KR"
                  ? "bg-blue-600 text-white shadow-sm"
                  : "text-slate-400 hover:text-slate-200"
              }`}
            >
              한국
            </button>
            <button
              type="button"
              onClick={() => setMarket("US")}
              className={`tap-target min-h-11 flex-1 sm:flex-initial rounded-lg px-4 py-2.5 text-sm font-medium transition-colors ${
                currentMarket === "US"
                  ? "bg-blue-600 text-white shadow-sm"
                  : "text-slate-400 hover:text-slate-200"
              }`}
            >
              미국
            </button>
          </div>
        </div>
      </header>

      {err && (
        <div className="mt-3 rounded-xl border border-red-500/30 bg-red-500/10 px-4 py-2 text-sm text-red-300">
          {err}
        </div>
      )}
      {status?.kis_error && (
        <div className="mt-2 text-xs text-amber-400/90">KIS: {status.kis_error}</div>
      )}

      {/* 잔고 */}
      <section className="mt-6">
        <h2 className="text-[11px] font-medium uppercase tracking-wider text-slate-500 mb-3">
          계좌 요약
        </h2>
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
          <div className="rounded-2xl border border-white/[0.07] bg-gradient-to-b from-white/[0.05] to-transparent px-4 py-3.5">
            <p className="text-[11px] text-slate-500">총 평가</p>
            <p className="text-lg font-semibold text-white tabular-nums mt-1">
              {bal?.total_equity
                ? `${Number(bal.total_equity).toLocaleString()}원`
                : "—"}
            </p>
          </div>
          <div className="rounded-2xl border border-white/[0.07] bg-gradient-to-b from-emerald-500/[0.07] to-transparent px-4 py-3.5">
            <p className="text-[11px] text-slate-500">주문가능</p>
            <p className="text-lg font-semibold text-emerald-400 tabular-nums mt-1">
              {bal?.available_cash
                ? `${Number(bal.available_cash).toLocaleString()}원`
                : "—"}
            </p>
          </div>
          <div className="rounded-2xl border border-white/[0.07] bg-gradient-to-b from-white/[0.04] to-transparent px-4 py-3.5">
            <p className="text-[11px] text-slate-500">주식 평가</p>
            <p className="text-lg font-semibold text-slate-200 tabular-nums mt-1">
              {bal?.stock_value
                ? `${Number(bal.stock_value).toLocaleString()}원`
                : "—"}
            </p>
          </div>
        </div>
      </section>

      {/* 누적 손익·비중 차트 — 대시보드 핵심 위젯(레이아웃 수정 시 이 블록 유지 권장) */}
      <section
        className="mt-7 grid grid-cols-1 lg:grid-cols-2 gap-4 md:gap-5"
        aria-label="실현손익 및 보유 비중"
      >
        <div className="glass rounded-2xl p-4 min-w-0">
          <div className="mb-3">
            <h2 className="text-[11px] font-medium uppercase tracking-wider text-slate-500">
              누적 실현손익
            </h2>
            <p className="text-[10px] text-slate-600 mt-0.5">매도 체결 기준</p>
          </div>
          <EquityChart trades={trades} />
        </div>
        <div className="glass rounded-2xl p-4 min-w-0">
          <div className="mb-3">
            <h2 className="text-[11px] font-medium uppercase tracking-wider text-slate-500">
              보유 비중
            </h2>
            <p className="text-[10px] text-slate-600 mt-0.5">종목별 평가액</p>
          </div>
          <PositionMixChart
            positionsKr={positionsKr}
            positionsUs={positionsUs}
          />
        </div>
      </section>

      {/* 자동매매 봇 */}
      <section className="mt-7 glass rounded-2xl p-4 sm:p-5">
        <div className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
          <div>
            <h2 className="text-sm font-semibold text-white mb-1">자동매매 봇</h2>
            <div className="flex flex-wrap items-center gap-2 text-xs">
              {!botOn ? (
                <span className="rounded-full bg-slate-600/40 px-2.5 py-1 text-slate-300">
                  중지됨
                </span>
              ) : botState?.trading_halted ? (
                <span className="rounded-full bg-amber-500/20 px-2.5 py-1 text-amber-300">
                  목표 달성 등으로 매매 일시 중단
                </span>
              ) : botState?.is_market_open ? (
                <span className="rounded-full bg-emerald-500/20 px-2.5 py-1 text-emerald-300">
                  장중 · 자동매매 가능
                </span>
              ) : (
                <span className="rounded-full bg-slate-600/40 px-2.5 py-1 text-slate-400">
                  봇 켜짐 · 장 외 대기
                </span>
              )}
            </div>
          </div>
          <div className="grid grid-cols-3 gap-2 w-full sm:w-auto sm:flex sm:flex-wrap sm:gap-2">
            <button
              type="button"
              disabled={botBusy || botOn}
              onClick={() => botControl("start")}
              className="tap-target min-h-11 rounded-xl bg-gradient-to-r from-green-700 to-emerald-600 px-2 sm:px-4 py-2.5 text-xs sm:text-sm font-semibold text-white disabled:opacity-40 disabled:cursor-not-allowed"
            >
              시작
            </button>
            <button
              type="button"
              disabled={botBusy || !botOn}
              onClick={() => botControl("stop")}
              className="tap-target min-h-11 rounded-xl bg-gradient-to-r from-red-800 to-red-600 px-2 sm:px-4 py-2.5 text-xs sm:text-sm font-semibold text-white disabled:opacity-40 disabled:cursor-not-allowed"
            >
              중지
            </button>
            <button
              type="button"
              disabled={botBusy || !botState?.trading_halted}
              onClick={() => botControl("resume")}
              className="tap-target min-h-11 rounded-xl border border-white/15 bg-white/5 px-2 sm:px-4 py-2.5 text-[11px] sm:text-sm font-medium text-slate-300 disabled:opacity-40 disabled:cursor-not-allowed leading-tight"
            >
              <span className="sm:hidden">재개</span>
              <span className="hidden sm:inline">매매 재개</span>
            </button>
          </div>
        </div>
        <p className="mt-3 text-[11px] text-slate-600 leading-relaxed">
          중지 시 스케줄 AI·규칙 매매가 실행되지 않습니다. 「매매 재개」는 일일 목표 달성 등으로
          일시 중단된 경우에만 사용합니다.
        </p>
      </section>

      {/* 탭 */}
      <div className="mt-8">
        <h2 className="text-[11px] font-medium uppercase tracking-wider text-slate-500 mb-3">
          주문
        </h2>
        <div
          className="flex w-full sm:w-auto rounded-xl bg-slate-950/90 p-1 border border-white/[0.08]"
          role="tablist"
          aria-label="주문 방식"
        >
          <button
            type="button"
            role="tab"
            aria-selected={tab === "auto"}
            onClick={() => setTab("auto")}
            className={`tap-target min-h-11 flex-1 sm:flex-initial rounded-lg px-3 sm:px-4 py-2.5 text-sm font-medium transition-colors ${
              tab === "auto"
                ? "bg-blue-600 text-white shadow-sm"
                : "text-slate-400 hover:text-slate-200"
            }`}
          >
            <span className="sm:hidden">자동 · AI</span>
            <span className="hidden sm:inline">자동 · AI 세션</span>
          </button>
          <button
            type="button"
            role="tab"
            aria-selected={tab === "manual"}
            onClick={() => setTab("manual")}
            className={`tap-target min-h-11 flex-1 sm:flex-initial rounded-lg px-3 sm:px-4 py-2.5 text-sm font-medium transition-colors ${
              tab === "manual"
                ? "bg-blue-600 text-white shadow-sm"
                : "text-slate-400 hover:text-slate-200"
            }`}
          >
            <span className="sm:hidden">수동</span>
            <span className="hidden sm:inline">수동 매매</span>
          </button>
        </div>
      </div>

      <MarketFlowStrip market={currentMarket} watchlistCodes={wl} className="mt-5" />

      {tab === "auto" && (
        <section className="mt-5 space-y-5">
          <AiSessionButtons
            idToken={idToken}
            market={currentMarket}
            aiStockCount={cfg?.ai_stock_count ?? 3}
            onDone={() => {
              void loadStatus();
              void loadTrades();
            }}
          />
          <HoldingsPanel
            idToken={idToken}
            positionsKr={positionsKr}
            positionsUs={positionsUs}
            onChange={() => {
              void loadStatus();
              void loadTrades();
            }}
          />
          <div>
            <h2 className="text-[11px] font-medium uppercase tracking-wider text-slate-500 mb-3">
              감시 종목
            </h2>
            <p className="text-[10px] text-slate-600 -mt-2 mb-3">
              {wl.length}종목 · 앱 시세·손익 · 자세한 차트는 링크로 TradingView(새 창)
            </p>
          <div className="grid grid-cols-1 sm:grid-cols-2 xl:grid-cols-3 gap-3 md:gap-4">
            {wl.map((code) => {
              const keys = watchlistCodeKeys(currentMarket, code);
              const wd =
                pickRecord(wlData, keys) ??
                ({} as (typeof wlData)[string]);
              const pos = pickRecord(
                currentMarket === "KR" ? positionsKr : positionsUs,
                keys,
              );
              const owned = !!pos;
              const cr = parseFloat(String(wd.change_rate || "0"));
              const displayPrice =
                pos?.current_price != null && Number(pos.current_price) > 0
                  ? Number(pos.current_price)
                  : wd.current_price != null
                    ? Number(wd.current_price)
                    : null;
              const priceStr =
                currentMarket === "US"
                  ? displayPrice != null
                    ? `$${displayPrice.toFixed(2)}`
                    : "—"
                  : displayPrice != null
                    ? `${displayPrice.toLocaleString()}원`
                    : "—";
              const breakout = wd.target_breakout ?? 0;
              const ma5wl = wd.ma5 ?? 0;
              const showBuySignal =
                !owned &&
                currentMarket === "KR" &&
                breakout > 0 &&
                displayPrice != null &&
                displayPrice >= breakout;
              const sparkCloses = buildSparklineCloses(wd, pos);
              return (
                <div
                  key={code}
                  className={`glass rounded-xl p-3 flex flex-col min-h-[200px] ${owned ? "ring-1 ring-blue-500/30" : ""}`}
                >
                  <div className="flex items-center justify-between mb-0.5 shrink-0 gap-2">
                    <span className="font-bold text-white text-sm font-mono tabular-nums">
                      {code}
                    </span>
                    {owned ? (
                      <span className="shrink-0 text-[10px] bg-blue-500/20 text-blue-300 px-2 py-0.5 rounded-full">
                        보유
                      </span>
                    ) : showBuySignal ? (
                      <span className="shrink-0 text-[10px] bg-emerald-500/20 text-emerald-400 px-2 py-0.5 rounded-full font-medium">
                        매수신호
                      </span>
                    ) : (
                      <span className="text-[10px] text-transparent select-none" aria-hidden>
                        ·
                      </span>
                    )}
                  </div>
                  {(wd.stock_name || "").trim() ? (
                    <div className="text-xs text-slate-400 mb-1 truncate shrink-0">
                      {wd.stock_name}
                    </div>
                  ) : (
                    <div className="mb-1 h-4 shrink-0" aria-hidden />
                  )}
                  <div className="flex items-baseline gap-1.5 mb-1 shrink-0">
                    <span className="text-sm font-bold text-slate-200">
                      {priceStr}
                    </span>
                    <span
                      className={
                        cr > 0
                          ? "text-xs font-semibold text-emerald-400"
                          : cr < 0
                            ? "text-xs font-semibold text-red-400"
                            : "text-xs font-semibold text-slate-500"
                      }
                    >
                      {cr !== 0
                        ? `${cr > 0 ? "▲" : "▼"}${Math.abs(cr).toFixed(2)}%`
                        : ""}
                    </span>
                  </div>
                  <div
                    className="shrink-0 space-y-0.5"
                    style={{ minHeight: "3.25rem" }}
                  >
                    {owned && pos ? (
                      <>
                        {pos.pnl != null && (
                          <div
                            className={`text-xs font-semibold ${(pos.pnl ?? 0) >= 0 ? "text-emerald-400" : "text-red-400"}`}
                          >
                            {(pos.pnl ?? 0) >= 0 ? "+" : ""}
                            {currentMarket === "US"
                              ? `$${Math.abs(pos.pnl ?? 0).toFixed(2)}`
                              : `${Math.abs(pos.pnl ?? 0).toLocaleString()}원`}{" "}
                            ({(pos.pnl_ratio ?? 0) >= 0 ? "+" : ""}
                            {pos.pnl_ratio ?? 0}%)
                          </div>
                        )}
                        <div className="text-[11px] text-slate-500 space-y-0.5">
                          <div>
                            매수{" "}
                            <span className="text-slate-400">
                              {currentMarket === "US"
                                ? pos.buy_price != null
                                  ? `$${Number(pos.buy_price).toFixed(2)}`
                                  : "—"
                                : pos.buy_price != null
                                  ? `${Number(pos.buy_price).toLocaleString()}원`
                                  : "—"}
                            </span>
                          </div>
                          <div>
                            목표{" "}
                            <span className="text-emerald-400/90">
                              {(pos.target_sell_price ?? 0) > 0
                                ? currentMarket === "US"
                                  ? `$${Number(pos.target_sell_price).toFixed(2)}`
                                  : `${Number(pos.target_sell_price).toLocaleString()}`
                                : "—"}
                            </span>{" "}
                            / 손절{" "}
                            <span className="text-red-400/90">
                              {(pos.stop_loss_price ?? 0) > 0
                                ? currentMarket === "US"
                                  ? `$${Number(pos.stop_loss_price).toFixed(2)}`
                                  : `${Number(pos.stop_loss_price).toLocaleString()}`
                                : "—"}
                            </span>
                          </div>
                        </div>
                      </>
                    ) : currentMarket === "KR" && breakout > 0 ? (
                      <div className="mt-1 pt-1 border-t border-white/5">
                        <div className="flex justify-between text-xs gap-2">
                          <span
                            className={
                              showBuySignal
                                ? "text-emerald-400 font-semibold"
                                : "text-slate-500"
                            }
                          >
                            돌파가 {breakout.toLocaleString()}
                          </span>
                          {ma5wl > 0 ? (
                            <span className="text-slate-600 shrink-0">
                              MA {ma5wl.toLocaleString()}
                            </span>
                          ) : null}
                        </div>
                      </div>
                    ) : null}
                  </div>
                  <div className="spark-slot mt-auto pt-2 shrink-0 h-[60px] w-full min-w-0">
                    <Sparkline closes={sparkCloses} changeRate={cr} />
                  </div>
                  <a
                    href={`https://www.tradingview.com/chart/?symbol=${encodeURIComponent(tvSymbol(code, currentMarket))}`}
                    target="_blank"
                    rel="noopener noreferrer"
                    onClick={(e) => e.stopPropagation()}
                    className="shrink-0 mt-1.5 flex items-center justify-center gap-1 rounded-lg border border-indigo-500/[0.18] bg-indigo-500/10 py-1.5 text-[11px] text-[#818cf8] hover:bg-indigo-500/15 transition-colors"
                  >
                    TradingView에서 차트 열기 ↗
                  </a>
                </div>
              );
            })}
          </div>
          </div>
        </section>
      )}

      {tab === "manual" && (
        <section className="mt-5">
          <ManualTradingPanel
            idToken={idToken}
            positionsKr={positionsKr}
            positionsUs={positionsUs}
            onOrderSuccess={() => {
              void loadStatus();
              void loadTrades();
            }}
          />
        </section>
      )}

      <StrategySettings
        idToken={idToken}
        config={cfg}
        onSaved={() => loadStatus()}
      />

      {/* 매매 이력 */}
      <section className="mt-10 glass rounded-2xl overflow-hidden">
        <div className="px-4 py-3 border-b border-white/5">
          <h2 className="text-[11px] font-medium uppercase tracking-wider text-slate-500">
            매매 이력
          </h2>
          <p className="text-[10px] text-slate-600 mt-1">최근 30건</p>
        </div>
        {trades.length === 0 ? (
          <p className="text-center py-10 text-slate-600 text-sm">이력 없음</p>
        ) : (
          <>
            <div className="md:hidden divide-y divide-white/5 border-t border-white/5">
              {trades.slice(0, 30).map((t, i) => (
                <div key={i} className="px-4 py-3.5 space-y-2">
                  <div className="flex justify-between items-start gap-3">
                    <div className="min-w-0">
                      <div className="font-mono font-semibold text-white text-sm">
                        {t.stock_code}
                      </div>
                      {t.stock_name && (
                        <div className="text-[11px] text-slate-500 truncate">
                          {t.stock_name}
                        </div>
                      )}
                    </div>
                    <span
                      className={
                        t.side === "buy"
                          ? "shrink-0 rounded-md bg-blue-500/15 text-blue-300 text-xs font-medium px-2 py-1"
                          : "shrink-0 rounded-md bg-red-500/15 text-red-300 text-xs font-medium px-2 py-1"
                      }
                    >
                      {t.side === "buy" ? "매수" : "매도"}
                    </span>
                  </div>
                  <div className="flex flex-wrap gap-x-4 gap-y-1 text-xs text-slate-400">
                    <span className="text-[11px] text-slate-500 whitespace-nowrap">
                      {formatKst(t.timestamp)}
                    </span>
                    <span className="tabular-nums text-slate-300">
                      {tradePriceStr(t)}
                    </span>
                    <span className="tabular-nums">
                      {(t.quantity || 0).toLocaleString()}주
                    </span>
                  </div>
                  {t.reason ? (
                    <p className="text-[11px] text-slate-600 leading-snug">{t.reason}</p>
                  ) : null}
                </div>
              ))}
            </div>
            <div className="hidden md:block overflow-x-auto">
              <table className="w-full text-sm min-w-[640px]">
                <thead>
                  <tr className="text-left text-xs text-slate-500 border-b border-white/5">
                    <th className="px-4 py-3">시간</th>
                    <th className="px-3 py-3">종목</th>
                    <th className="px-3 py-3">구분</th>
                    <th className="px-3 py-3 text-right">가격</th>
                    <th className="px-3 py-3 text-right">수량</th>
                    <th className="px-4 py-3">사유</th>
                  </tr>
                </thead>
                <tbody>
                  {trades.slice(0, 30).map((t, i) => (
                    <tr key={i} className="border-b border-white/5 hover:bg-white/[0.03]">
                      <td className="px-4 py-2.5 text-xs text-slate-500 whitespace-nowrap">
                        {formatKst(t.timestamp)}
                      </td>
                      <td className="px-3 py-2.5">
                        <div className="font-bold text-white">{t.stock_code}</div>
                        {t.stock_name && (
                          <div className="text-[10px] text-slate-500">{t.stock_name}</div>
                        )}
                      </td>
                      <td className="px-3 py-2.5">
                        <span
                          className={
                            t.side === "buy"
                              ? "text-blue-300 text-xs"
                              : "text-red-300 text-xs"
                          }
                        >
                          {t.side === "buy" ? "매수" : "매도"}
                        </span>
                      </td>
                      <td className="px-3 py-2.5 text-right tabular-nums text-slate-300">
                        {tradePriceStr(t)}
                      </td>
                      <td className="px-3 py-2.5 text-right tabular-nums text-slate-400">
                        {(t.quantity || 0).toLocaleString()}주
                      </td>
                      <td className="px-4 py-2.5 text-xs text-slate-600 max-w-[140px] truncate">
                        {t.reason || "—"}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </>
        )}
      </section>

      {/* 시스템 로그 */}
      <section className="mt-6 glass rounded-2xl overflow-hidden">
        <details open className="group">
          <summary className="cursor-pointer list-none px-4 py-3 border-b border-white/5 flex items-center justify-between gap-2">
            <span className="flex flex-col gap-0.5">
              <span className="text-[11px] font-medium uppercase tracking-wider text-slate-500 flex items-center gap-2">
                <span className="inline-block w-1 h-3 rounded-full bg-emerald-500/90" />
                시스템 로그
              </span>
              <span className="text-[10px] text-slate-600 pl-3">최근 100건</span>
            </span>
            <span className="text-xs text-slate-500 group-open:rotate-180 transition-transform">
              ▼
            </span>
          </summary>
          <div className="max-h-[min(420px,50vh)] overflow-y-auto overscroll-contain px-4 py-3 space-y-2 sm:space-y-0.5 font-mono text-[11px] sm:text-xs leading-relaxed">
            {logs.length === 0 ? (
              <p className="text-slate-600 text-center py-6">로그 없음</p>
            ) : (
              logs.map((log, i) => {
                const lv = (log.level || "INFO").toUpperCase();
                const color =
                  lv === "ERROR"
                    ? "text-red-400"
                    : lv === "WARNING"
                      ? "text-yellow-400"
                      : lv === "DEBUG"
                        ? "text-slate-600"
                        : "text-slate-300";
                return (
                  <div
                    key={i}
                    className="flex flex-col gap-1 sm:flex-row sm:items-start sm:gap-2 py-2 sm:py-0.5 border-b border-white/[0.04] last:border-0"
                  >
                    <div className="flex items-center gap-2 shrink-0 sm:w-[min(100%,280px)]">
                      <span className="text-slate-600 text-[10px] sm:text-[11px] whitespace-nowrap">
                        {formatKst(log.timestamp)}
                      </span>
                      <span className={`text-[10px] sm:text-xs font-semibold w-12 ${color}`}>
                        {lv}
                      </span>
                    </div>
                    <span className={`break-all min-w-0 ${color}`}>{log.message || ""}</span>
                  </div>
                );
              })
            )}
          </div>
        </details>
      </section>
    </div>
  );
}
