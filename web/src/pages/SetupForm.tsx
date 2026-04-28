import { useState } from "react";
import { apiFetch } from "../api/client";

const inp = "w-full rounded-xl border border-white/10 bg-white/5 px-3 py-2.5 text-sm text-white";

export function SetupForm({
  idToken,
  onDone,
}: {
  idToken: string;
  onDone: () => void;
}) {
  const [mKey, setMKey] = useState("");
  const [mSec, setMSec] = useState("");
  const [mAcc, setMAcc] = useState("");
  const [lKey, setLKey] = useState("");
  const [lSec, setLSec] = useState("");
  const [lAcc, setLAcc] = useState("");
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
          mock: {
            app_key: mKey.trim(),
            app_secret: mSec.trim(),
            account_no: mAcc.trim(),
          },
          live: {
            app_key: lKey.trim(),
            app_secret: lSec.trim(),
            account_no: lAcc.trim(),
          },
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
    <div className="mx-auto max-w-lg px-4 py-10">
      <h1 className="text-xl font-bold text-white mb-2">KIS 연동</h1>
      <p className="text-sm text-slate-400 mb-6">
        모의투자(VTS)와 실전은 <strong className="text-slate-300">앱·계좌가 다를 수 있으니</strong> 각각
        입력합니다. 처음에 쓸 모드만 아래에서 고르면 됩니다.
      </p>
      <form onSubmit={submit} className="space-y-4">
        <div className="glass rounded-2xl p-4 space-y-3 border border-amber-500/15">
          <h2 className="text-xs font-semibold text-amber-200/90 uppercase tracking-wider">모의투자</h2>
          <div>
            <label className="text-xs text-slate-500 block mb-1">App Key</label>
            <input className={inp} value={mKey} onChange={(e) => setMKey(e.target.value)} autoComplete="off" required />
          </div>
          <div>
            <label className="text-xs text-slate-500 block mb-1">App Secret</label>
            <input
              type="password"
              className={inp}
              value={mSec}
              onChange={(e) => setMSec(e.target.value)}
              required
            />
          </div>
          <div>
            <label className="text-xs text-slate-500 block mb-1">계좌번호 (모의)</label>
            <input className={inp} value={mAcc} onChange={(e) => setMAcc(e.target.value)} required />
          </div>
        </div>

        <div className="glass rounded-2xl p-4 space-y-3 border border-emerald-500/15">
          <h2 className="text-xs font-semibold text-emerald-200/90 uppercase tracking-wider">실전투자</h2>
          <div>
            <label className="text-xs text-slate-500 block mb-1">App Key</label>
            <input className={inp} value={lKey} onChange={(e) => setLKey(e.target.value)} autoComplete="off" required />
          </div>
          <div>
            <label className="text-xs text-slate-500 block mb-1">App Secret</label>
            <input
              type="password"
              className={inp}
              value={lSec}
              onChange={(e) => setLSec(e.target.value)}
              required
            />
          </div>
          <div>
            <label className="text-xs text-slate-500 block mb-1">계좌번호 (실전)</label>
            <input className={inp} value={lAcc} onChange={(e) => setLAcc(e.target.value)} required />
          </div>
        </div>

        <div className="glass rounded-2xl p-4">
          <label className="text-xs text-slate-500 block mb-1">지금 쓸 모드 (나중에 전략 설정에서 토글)</label>
          <select
            className={inp}
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
