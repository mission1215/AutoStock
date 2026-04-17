import type { PositionKr, PositionUs, WatchlistEntry } from "../types";

/** KR/US 종목코드·감시 설정과 API 키 불일치(5930 vs 005930 등) 대응 */
export function watchlistCodeKeys(
  market: "KR" | "US",
  code: string | number,
): string[] {
  const s = String(code).trim();
  const out = new Set<string>([s, s.toUpperCase()]);
  if (market === "KR" && /^\d+$/.test(s)) {
    out.add(s.padStart(6, "0"));
    const n = Number(s);
    if (!Number.isNaN(n)) out.add(String(n));
  }
  return [...out];
}

export function pickRecord<T>(
  rec: Record<string, T | undefined>,
  keys: string[],
): T | undefined {
  for (const k of keys) {
    if (Object.prototype.hasOwnProperty.call(rec, k)) {
      const v = rec[k];
      if (v !== undefined) return v;
    }
  }
  return undefined;
}

/** API closes 누락·1개일 때 현재가·매수가로 보강 (스파크라인 최소 2포인트) */
export function buildSparklineCloses(
  wd: WatchlistEntry,
  pos: PositionKr | PositionUs | undefined,
): number[] {
  const num = (v: unknown): number => {
    const x = Number(v);
    return Number.isFinite(x) ? x : NaN;
  };
  const raw = wd.closes;
  if (Array.isArray(raw) && raw.length >= 2) {
    const xs = raw.map(num).filter((x) => x > 0);
    if (xs.length >= 2) return xs;
  }
  if (Array.isArray(raw) && raw.length === 1) {
    const a = num(raw[0]);
    if (a > 0) return [a, a];
  }
  const cur = num(wd.current_price);
  const bp = pos != null ? num(pos.buy_price) : NaN;
  if (cur > 0) {
    if (bp > 0) return [bp, cur];
    return [cur, cur];
  }
  return [];
}
