/** Cloud Functions(2세대) = Cloud Run. 배포 시 콘솔의 api URL과 동일하게 유지하세요. */
export const AUTOSTOCK_API_DIRECT_BASE = "https://api-eq2ncfx6gq-uc.a.run.app";

/**
 * Firebase Hosting → /api 리라이트는 약 60초 제한 → AI 등 장시간 요청은 502.
 * localhost/127만 상대 경로(프록시·에뮬). 그 외 HTTPS는 전부 Cloud Run 직접 호출(커스텀 도메인 포함).
 */
function apiBase(): string {
  if (typeof window === "undefined") return "";
  const w = window as Window & { __AUTOSTOCK_API_BASE__?: string };
  if (typeof w.__AUTOSTOCK_API_BASE__ === "string") return w.__AUTOSTOCK_API_BASE__;
  const env = import.meta.env.VITE_API_BASE as string | undefined;
  if (typeof env === "string" && env.length > 0) return env.replace(/\/$/, "");
  const h = window.location.hostname;
  if (h === "localhost" || h === "127.0.0.1") return "";
  if (window.location.protocol === "https:") return AUTOSTOCK_API_DIRECT_BASE;
  return "";
}

export function resolveApiUrl(path: string): string {
  const b = apiBase();
  if (!b || !path.startsWith("/api")) return path;
  return `${b.replace(/\/$/, "")}${path}`;
}

export async function apiFetch<T = unknown>(
  path: string,
  opts: RequestInit & { idToken?: string | null } = {},
): Promise<T> {
  const { idToken, ...rest } = opts;
  const headers: HeadersInit = {
    "Content-Type": "application/json",
    ...(rest.headers || {}),
  };
  if (idToken) {
    (headers as Record<string, string>)["Authorization"] = `Bearer ${idToken}`;
  }
  const res = await fetch(resolveApiUrl(path), { ...rest, headers });
  const text = await res.text();
  try {
    return JSON.parse(text) as T;
  } catch {
    if (text.includes("<!DOCTYPE") || text.includes("Error 502") || text.includes("502")) {
      throw new Error(
        "서버 게이트웨이 오류(502). Firebase Hosting 60초 제한이거나 백엔드 지연입니다. 페이지를 새로고침한 뒤 다시 시도해 주세요.",
      );
    }
    throw new Error(text.slice(0, 200) || `HTTP ${res.status}`);
  }
}
