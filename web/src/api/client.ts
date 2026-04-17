/** Same-origin /api (Firebase Hosting rewrite → Cloud Function). */
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
  const res = await fetch(path, { ...rest, headers });
  const text = await res.text();
  try {
    return JSON.parse(text) as T;
  } catch {
    throw new Error(text.slice(0, 200) || `HTTP ${res.status}`);
  }
}
