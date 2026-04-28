import { useEffect, useState } from "react";
import { createPortal } from "react-dom";
import { apiFetch } from "../api/client";
import type { AppConfig, ModeProfiles } from "../types";

const inp = "w-full rounded-xl border border-white/10 bg-white/5 px-3 py-2.5 text-sm text-white";

export function KisCredentialsModal({
  open,
  onClose,
  idToken,
  onSaved,
}: {
  open: boolean;
  onClose: () => void;
  idToken: string;
  onSaved?: () => void;
}) {
  const [mKey, setMKey] = useState("");
  const [mSec, setMSec] = useState("");
  const [mAcc, setMAcc] = useState("");
  const [lKey, setLKey] = useState("");
  const [lSec, setLSec] = useState("");
  const [lAcc, setLAcc] = useState("");
  const [err, setErr] = useState("");
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    if (!open) return;
    setErr("");
    setMKey("");
    setMSec("");
    setLKey("");
    setLSec("");
    let cancelled = false;
    (async () => {
      try {
        const r = await apiFetch<{
          ok: boolean;
          config?: AppConfig;
          profiles?: ModeProfiles;
        }>("/api/config", { idToken });
        if (cancelled || !r.ok) return;
        const pm = r.profiles?.mock as { account_no?: string } | undefined;
        const pl = r.profiles?.live as { account_no?: string } | undefined;
        setMAcc((pm?.account_no || r.config?.account_no || "").trim());
        setLAcc((pl?.account_no || r.config?.account_no || "").trim());
      } catch {
        if (!cancelled) {
          setMAcc("");
          setLAcc("");
        }
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [open, idToken]);

  useEffect(() => {
    if (!open) return;
    function onKey(e: KeyboardEvent) {
      if (e.key === "Escape") onClose();
    }
    document.addEventListener("keydown", onKey);
    const prevOverflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    return () => {
      document.removeEventListener("keydown", onKey);
      document.body.style.overflow = prevOverflow;
    };
  }, [open, onClose]);

  if (!open) return null;

  async function submit(e: React.FormEvent) {
    e.preventDefault();
    setErr("");
    setLoading(true);
    try {
      const data = await apiFetch<{ ok: boolean; error?: string; message?: string }>(
        "/api/credentials/dual",
        {
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
          }),
        },
      );
      if (!data.ok) {
        setErr(data.error || "저장 실패");
        return;
      }
      onClose();
      onSaved?.();
      if (data.message) window.alert(data.message);
    } catch (e: unknown) {
      setErr(e instanceof Error ? e.message : "오류");
    } finally {
      setLoading(false);
    }
  }

  const overlay = (
    <div
      className="fixed inset-0 z-[10000] overflow-y-auto overflow-x-hidden overscroll-contain bg-black/60 backdrop-blur-sm"
      role="dialog"
      aria-modal
      aria-labelledby="kis-cred-title"
      onClick={onClose}
      style={{
        paddingTop: "max(0.75rem, env(safe-area-inset-top))",
        paddingBottom: "max(0.75rem, env(safe-area-inset-bottom))",
        paddingLeft: "max(0.75rem, env(safe-area-inset-left))",
        paddingRight: "max(0.75rem, env(safe-area-inset-right))",
      }}
    >
      <div className="flex min-h-[100dvh] w-full flex-col items-center justify-center sm:min-h-full sm:py-6">
        <div
          className="relative z-[10001] my-auto w-full max-w-lg max-h-[min(90dvh,40rem)] overflow-y-auto rounded-2xl border border-white/10 bg-[#0c1220] p-4 shadow-2xl sm:max-h-[min(85vh,44rem)] sm:p-5 [overscroll-behavior:contain]"
          onClick={(e) => e.stopPropagation()}
        >
        <h2 id="kis-cred-title" className="text-lg font-semibold text-white">
          KIS 연동 정보 수정
        </h2>
        <p className="text-xs text-slate-500 mt-1 leading-relaxed">
          <strong className="text-amber-300/90">모의</strong>와 <strong className="text-emerald-300/90">실전</strong>은
          앱이 다르면 키·계좌도 각각 저장됩니다.{" "}
          <span className="text-slate-400">
            바꾸지 않을 항목은 <strong className="text-slate-300">비워 두면 기존 값 유지</strong>
            (앱키·시크릿·계좌 각각).
          </span>
        </p>
        <form onSubmit={submit} className="mt-4 space-y-4">
          <div className="rounded-xl border border-amber-500/20 bg-amber-500/5 p-3 space-y-2">
            <h3 className="text-xs font-semibold text-amber-200/90 uppercase tracking-wider">모의투자 (VTS)</h3>
            <div>
              <label className="text-xs text-slate-500 block mb-1">App Key</label>
              <input className={inp} value={mKey} onChange={(e) => setMKey(e.target.value)} autoComplete="off" placeholder="변경 시만 입력" />
            </div>
            <div>
              <label className="text-xs text-slate-500 block mb-1">App Secret</label>
              <input
                type="password"
                className={inp}
                value={mSec}
                onChange={(e) => setMSec(e.target.value)}
                placeholder="변경 시만 입력"
              />
            </div>
            <div>
              <label className="text-xs text-slate-500 block mb-1">계좌번호 (모의)</label>
              <input className={inp} value={mAcc} onChange={(e) => setMAcc(e.target.value)} placeholder="기존 값이 있으면 불러옵니다" />
            </div>
          </div>

          <div className="rounded-xl border border-emerald-500/20 bg-emerald-500/5 p-3 space-y-2">
            <h3 className="text-xs font-semibold text-emerald-200/90 uppercase tracking-wider">실전투자</h3>
            <div>
              <label className="text-xs text-slate-500 block mb-1">App Key</label>
              <input className={inp} value={lKey} onChange={(e) => setLKey(e.target.value)} autoComplete="off" placeholder="변경 시만 입력" />
            </div>
            <div>
              <label className="text-xs text-slate-500 block mb-1">App Secret</label>
              <input
                type="password"
                className={inp}
                value={lSec}
                onChange={(e) => setLSec(e.target.value)}
                placeholder="변경 시만 입력"
              />
            </div>
            <div>
              <label className="text-xs text-slate-500 block mb-1">계좌번호 (실전)</label>
              <input className={inp} value={lAcc} onChange={(e) => setLAcc(e.target.value)} placeholder="기존 값이 있으면 불러옵니다" />
            </div>
          </div>

          {err && <p className="text-sm text-red-400">{err}</p>}
          <div className="flex gap-2 pt-1">
            <button
              type="button"
              onClick={onClose}
              className="flex-1 tap-target min-h-11 rounded-xl border border-white/10 py-2.5 text-sm text-slate-300 hover:bg-white/5"
            >
              취소
            </button>
            <button
              type="submit"
              disabled={loading}
              className="flex-1 tap-target min-h-11 rounded-xl bg-gradient-to-r from-blue-600 to-indigo-600 py-2.5 text-sm font-semibold text-white disabled:opacity-50"
            >
              {loading ? "저장 중…" : "저장"}
            </button>
          </div>
        </form>
        </div>
      </div>
    </div>
  );

  return createPortal(overlay, document.body);
}
