import { useEffect, useRef, useState } from "react";
import type { User } from "firebase/auth";
import { apiFetch } from "../api/client";
import { KisCredentialsModal } from "./KisCredentialsModal";
import type { AppConfig } from "../types";

function maskKisAccount(raw: string | undefined | null): string {
  if (!raw?.trim()) return "—";
  const s = raw.trim();
  if (s.includes("-")) {
    const [a, b] = s.split("-", 2);
    const cano = (a || "").replace(/\D/g, "");
    const prd = (b || "").replace(/\D/g, "");
    if (cano.length < 2) return "—";
    const mid =
      cano.length <= 4
        ? "****"
        : `${cano.slice(0, 2)}****${cano.slice(-2)}`;
    return prd ? `${mid}-${prd}` : mid;
  }
  const d = s.replace(/\D/g, "");
  if (d.length < 4) return "****";
  return `${d.slice(0, 2)}****${d.slice(-2)}`;
}

export function AccountMenu({
  user,
  idToken,
  onSignOut,
  onWithdraw,
}: {
  user: User;
  idToken: string;
  onSignOut: () => void;
  onWithdraw: () => void | Promise<void>;
}) {
  const [open, setOpen] = useState(false);
  const [cfg, setCfg] = useState<AppConfig | null>(null);
  const [loadErr, setLoadErr] = useState("");
  const [credsOpen, setCredsOpen] = useState(false);
  const rootRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    if (!open) return;
    let cancelled = false;
    setLoadErr("");
    (async () => {
      try {
        const r = await apiFetch<{ ok: boolean; config?: AppConfig; error?: string }>(
          "/api/config",
          { idToken },
        );
        if (cancelled) return;
        if (r.ok && r.config) setCfg(r.config);
        else setLoadErr(r.error || "설정을 불러오지 못했습니다");
      } catch (e: unknown) {
        if (!cancelled)
          setLoadErr(e instanceof Error ? e.message : "계좌 정보 조회 실패");
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [open, idToken]);

  useEffect(() => {
    if (!open) return;
    function onDoc(e: MouseEvent) {
      if (rootRef.current && !rootRef.current.contains(e.target as Node)) {
        setOpen(false);
      }
    }
    function onKey(e: KeyboardEvent) {
      if (e.key === "Escape") setOpen(false);
    }
    document.addEventListener("mousedown", onDoc);
    document.addEventListener("keydown", onKey);
    return () => {
      document.removeEventListener("mousedown", onDoc);
      document.removeEventListener("keydown", onKey);
    };
  }, [open]);

  const shortLabel =
    user.displayName?.trim() || user.email?.split("@")[0] || "계정";
  const initial = (shortLabel[0] || "?").toUpperCase();
  const isMock = cfg?.is_mock !== false;
  const serverName = (cfg?.display_name || "").trim();
  const primaryName =
    serverName || user.displayName?.trim() || user.email || "사용자";
  const accountMasked = maskKisAccount(cfg?.account_no);
  const showEmailSub = Boolean(user.email && primaryName !== user.email);

  async function openCredentialsEditor() {
    if (!cfg) {
      try {
        const r = await apiFetch<{ ok: boolean; config?: AppConfig; error?: string }>(
          "/api/config",
          { idToken },
        );
        if (r.ok && r.config) setCfg(r.config);
      } catch {
        /* 모달은 그대로 열고 계좌만 비울 수 있음 */
      }
    }
    setOpen(false);
    setCredsOpen(true);
  }

  return (
    <div className="relative" ref={rootRef}>
      <button
        type="button"
        onClick={() => setOpen((o) => !o)}
        className="tap-target flex h-9 w-9 min-h-9 min-w-9 items-center justify-center overflow-hidden rounded-full border border-white/10 bg-slate-800/80 text-sm font-bold text-slate-200 ring-offset-2 ring-offset-[#060b18] transition hover:border-white/20 hover:bg-slate-800 focus:outline-none focus:ring-2 focus:ring-blue-500/50"
        aria-label="계정·계좌 정보"
        aria-haspopup="dialog"
        aria-expanded={open}
      >
        {user.photoURL ? (
          <img
            src={user.photoURL}
            alt=""
            className="h-full w-full object-cover"
            referrerPolicy="no-referrer"
            loading="lazy"
            decoding="async"
          />
        ) : (
          <span className="select-none" aria-hidden>
            {initial}
          </span>
        )}
      </button>

      {open && (
        <div
          className="absolute right-0 top-full z-[100] mt-1.5 w-[min(100vw-2rem,20rem)] max-h-[min(85dvh,24rem)] overflow-y-auto overflow-x-hidden overscroll-contain rounded-xl border border-white/10 bg-[#0c1220] p-3 pb-2 shadow-xl shadow-black/40"
          role="dialog"
          aria-label="계정 및 연동 계좌"
        >
          <div className="border-b border-white/[0.06] pb-2 mb-2">
            <p className="text-[10px] uppercase tracking-wider text-slate-500">로그인</p>
            <p
              className="text-sm font-semibold text-white truncate"
              title={primaryName}
            >
              {primaryName}
            </p>
            {showEmailSub && user.email && (
              <p className="text-xs text-slate-500 truncate mt-0.5" title={user.email}>
                {user.email}
              </p>
            )}
          </div>

          <div className="space-y-2 text-xs">
            <div>
              <button
                type="button"
                onClick={() => void openCredentialsEditor()}
                className="w-full text-left tap-target min-h-9 rounded-lg border border-cyan-500/25 bg-cyan-500/10 px-2.5 py-2 text-[12px] font-medium text-cyan-200/95 hover:bg-cyan-500/16"
              >
                연동 정보 수정 (앱키·시크릿·계좌)
              </button>
            </div>

            <div className="rounded-lg border border-white/[0.06] bg-white/[0.02] p-2 space-y-1.5">
              <button
                type="button"
                onClick={() => {
                  setOpen(false);
                  void onSignOut();
                }}
                className="w-full tap-target min-h-10 rounded-lg border border-white/10 bg-white/[0.04] px-3 py-2 text-sm font-medium text-slate-200 hover:bg-white/[0.08]"
              >
                로그아웃
              </button>
              <button
                type="button"
                onClick={() => {
                  setOpen(false);
                  void onWithdraw();
                }}
                className="w-full tap-target min-h-11 rounded-lg border border-red-500/30 bg-red-500/10 px-3 py-2.5 text-sm font-semibold text-red-100 hover:bg-red-500/18"
              >
                탈퇴하기
              </button>
              <p className="text-[10px] text-slate-600 leading-snug px-0.5">
                탈퇴 시 DB·KIS 연동·거래기록·로그인 계정이 삭제됩니다. 로그아웃은 데이터를 유지합니다.
              </p>
            </div>

            <div>
              <p className="text-[10px] text-slate-500 mb-0.5">KIS 모드</p>
              <p className="text-slate-200">
                {cfg ? (
                  <span
                    className={isMock ? "text-amber-300/95" : "text-emerald-300/95"}
                  >
                    {isMock ? "모의투자" : "실전투자"}
                  </span>
                ) : loadErr ? (
                  <span className="text-slate-500">—</span>
                ) : (
                  <span className="text-slate-500">불러오는 중…</span>
                )}
              </p>
            </div>
            <div>
              <p className="text-[10px] text-slate-500 mb-0.5">연동 계좌 (일부 가림)</p>
              <p className="text-slate-200 font-mono tabular-nums break-all" title="동일·유사한 숫자가 있어도 끝자리로 대략 식별할 수 있습니다.">
                {loadErr ? (
                  <span className="text-amber-400/90 text-[11px]">{loadErr}</span>
                ) : cfg ? (
                  accountMasked
                ) : (
                  <span className="text-slate-500">—</span>
                )}
              </p>
            </div>
          </div>
        </div>
      )}

      <KisCredentialsModal
        open={credsOpen}
        onClose={() => setCredsOpen(false)}
        idToken={idToken}
      />
    </div>
  );
}
