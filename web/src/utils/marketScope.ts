/** Firestore `market_scope` — 자동매매·스케줄이 다루는 시장 (서버 `market_scope`와 동일) */
export type MarketScope = "kr" | "us" | "both";

export function normalizeMarketScope(raw: string | undefined | null): MarketScope {
  const s = (raw ?? "kr").toString().trim().toLowerCase();
  if (s === "kr" || s === "korea" || s === "domestic" || s === "ko") return "kr";
  if (s === "us" || s === "usa" || s === "us_only") return "us";
  if (s === "both" || s === "all" || s === "every") return "both";
  return "kr";
}
