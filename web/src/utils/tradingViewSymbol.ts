/** TradingView 심볼 — 국내: KRX:6자리 / 미국: NASDAQ:티커 */
export function tvSymbol(code: string, market: "KR" | "US"): string {
  const c = String(code || "")
    .trim()
    .toUpperCase();
  if (market === "US") {
    if (!/^[A-Z0-9.-]{1,12}$/.test(c)) return "NASDAQ:AAPL";
    return `NASDAQ:${c.replace(/\./g, "-")}`;
  }
  const n = c.replace(/\D/g, "").padStart(6, "0");
  return `KRX:${n}`;
}
