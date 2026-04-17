import { useState } from "react";
import { apiFetch } from "../api/client";

export function SetupForm({
  idToken,
  onDone,
}: {
  idToken: string;
  onDone: () => void;
}) {
  const [appKey, setAppKey] = useState("");
  const [appSecret, setAppSecret] = useState("");
  const [accountNo, setAccountNo] = useState("");
  const [isMock, setIsMock] = useState(true);
  const [err, setErr] = useState("");
  const [loading, setLoading] = useState(false);

  async function submit(e: React.FormEvent) {
    e.preventDefault();
    setErr("");
    setLoading(true);
    try {
      const data = await apiFetch<{ ok: boolean; error?: string }>("/api/setup", {
        method: "POST",
        idToken,
        body: JSON.stringify({
          app_key: appKey,
          app_secret: appSecret,
          account_no: accountNo,
          is_mock: isMock,
        }),
      });
      if (!data.ok) {
        setErr(data.error || "설정 실패");
        return;
      }
      onDone();
    } catch (e: unknown) {
      setErr(e instanceof Error ? e.message : "오류");
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="mx-auto max-w-md px-4 py-12">
      <h1 className="text-xl font-bold text-white mb-2">KIS 연동</h1>
      <p className="text-sm text-slate-400 mb-6">
        한국투자증권 Open API 앱키·시크릿·계좌번호를 입력하세요.
      </p>
      <form onSubmit={submit} className="space-y-4 glass rounded-2xl p-6">
        <div>
          <label className="text-xs text-slate-500 block mb-1">App Key</label>
          <input
            className="w-full rounded-xl border border-white/10 bg-white/5 px-3 py-2.5 text-sm text-white"
            value={appKey}
            onChange={(e) => setAppKey(e.target.value)}
            autoComplete="off"
            required
          />
        </div>
        <div>
          <label className="text-xs text-slate-500 block mb-1">App Secret</label>
          <input
            type="password"
            className="w-full rounded-xl border border-white/10 bg-white/5 px-3 py-2.5 text-sm text-white"
            value={appSecret}
            onChange={(e) => setAppSecret(e.target.value)}
            required
          />
        </div>
        <div>
          <label className="text-xs text-slate-500 block mb-1">계좌번호 (8-2 또는 붙여넣기)</label>
          <input
            className="w-full rounded-xl border border-white/10 bg-white/5 px-3 py-2.5 text-sm text-white"
            value={accountNo}
            onChange={(e) => setAccountNo(e.target.value)}
            required
          />
        </div>
        <div>
          <label className="text-xs text-slate-500 block mb-1">모드</label>
          <select
            className="w-full rounded-xl border border-white/10 bg-white/5 px-3 py-2.5 text-sm text-white"
            value={isMock ? "mock" : "real"}
            onChange={(e) => setIsMock(e.target.value === "mock")}
          >
            <option value="mock">모의투자</option>
            <option value="real">실전투자</option>
          </select>
        </div>
        {err && <p className="text-sm text-red-400">{err}</p>}
        <button
          type="submit"
          disabled={loading}
          className="w-full rounded-xl bg-gradient-to-r from-blue-600 to-indigo-600 py-3 font-semibold text-white disabled:opacity-50"
        >
          {loading ? "저장 중…" : "저장하고 시작"}
        </button>
      </form>
    </div>
  );
}
