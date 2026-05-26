import { useEffect, useState, useRef } from "react";
import {
  onAuthStateChanged,
  signInWithPopup,
  signOut,
  type User,
} from "firebase/auth";
import { auth, googleProvider } from "./firebase";
import { apiFetch } from "./api/client";
import type { StatusResponse } from "./types";
import { AccountMenu } from "./components/AccountMenu";
import { SetupForm } from "./pages/SetupForm";
import { Dashboard } from "./pages/Dashboard";

function formatBootElapsed(sec: number): string {
  const m = Math.floor(sec / 60);
  const s = sec % 60;
  return `${m}:${String(s).padStart(2, "0")}`;
}

/** 로그인 직후 첫 `/api/status` 대기 중에만 표시(토큰 자동 갱신 때는 띄우지 않음) */
function BootStatusOverlay({ elapsedSec }: { elapsedSec: number }) {
  return (
    <div
      className="fixed inset-0 z-[100] flex items-center justify-center px-4 bg-[#060b18]/88 backdrop-blur-md"
      role="alertdialog"
      aria-busy="true"
      aria-label="서버 동기화 중"
    >
      <div className="w-full max-w-sm rounded-2xl border border-blue-500/25 bg-slate-900/95 p-6 text-center shadow-2xl shadow-blue-950/40">
        <div className="mx-auto mb-4 h-10 w-10 animate-spin rounded-full border-2 border-blue-500 border-t-transparent" />
        <p className="text-sm font-semibold text-white">대시보드 연결 중</p>
        <p className="mt-1 text-xs text-slate-500">서버 상태를 불러오는 중입니다.</p>
        <p
          className="mt-4 font-mono text-2xl font-bold tabular-nums tracking-widest text-blue-300"
          aria-live="polite"
        >
          {formatBootElapsed(elapsedSec)}
        </p>
        <p className="mt-1 text-[10px] text-slate-600">경과 시간</p>
      </div>
    </div>
  );
}

function App() {
  const [user, setUser] = useState<User | null>(null);
  const [idToken, setIdToken] = useState<string | null>(null);
  const [boot, setBoot] = useState(true);
  const [bootElapsedSec, setBootElapsedSec] = useState(0);
  const [setupRequired, setSetupRequired] = useState(false);
  const [market, setMarket] = useState<"KR" | "US">("KR");
  /** 부트 시 받은 /api/status — Dashboard 첫 그림에서 중복 호출 제거(토큰 변경 시엔 대시보드가 다시 조회) */
  const [statusBootstrap, setStatusBootstrap] = useState<StatusResponse | null>(null);
  /** 같은 세션에서 idToken이 주기 갱신될 때 전체 화면 부트를 다시 띄우지 않기 위함 */
  const initialStatusFetchRef = useRef(true);

  useEffect(() => {
    return onAuthStateChanged(auth, async (u) => {
      setUser(u);
      if (u) {
        try {
          const t = await u.getIdToken();
          setIdToken(t);
        } catch {
          setIdToken(null);
        }
      } else {
        setIdToken(null);
      }
    });
  }, []);

  useEffect(() => {
    if (!user || !idToken) {
      setBoot(false);
      setStatusBootstrap(null);
      initialStatusFetchRef.current = true;
      return;
    }
    let cancelled = false;
    const showFullBoot = initialStatusFetchRef.current;
    if (showFullBoot) setBoot(true);
    (async () => {
      try {
        const s = await apiFetch<StatusResponse>("/api/status", { idToken });
        if (cancelled) return;
        if (s.ok && s.setup_required) {
          setSetupRequired(true);
          setStatusBootstrap(null);
        } else {
          setSetupRequired(false);
          setStatusBootstrap(s.ok ? s : null);
        }
      } catch {
        if (!cancelled) {
          setSetupRequired(false);
          setStatusBootstrap(null);
        }
      } finally {
        if (!cancelled) {
          setBoot(false);
          initialStatusFetchRef.current = false;
        }
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [user, idToken]);

  useEffect(() => {
    if (!boot || !user) return;
    const t0 = Date.now();
    setBootElapsedSec(0);
    const id = setInterval(() => {
      setBootElapsedSec(Math.floor((Date.now() - t0) / 1000));
    }, 1000);
    return () => clearInterval(id);
  }, [boot, user]);

  useEffect(() => {
    if (!user) return;
    const id = setInterval(async () => {
      try {
        const t = await user.getIdToken(true);
        setIdToken(t);
      } catch {
        /* ignore */
      }
    }, 55 * 60 * 1000);
    return () => clearInterval(id);
  }, [user]);

  async function signIn() {
    await signInWithPopup(auth, googleProvider);
  }

  async function doSignOut() {
    await signOut(auth);
    setSetupRequired(false);
    setStatusBootstrap(null);
  }

  async function doWithdraw() {
    if (!user || !idToken) return;
    if (
      !window.confirm(
        "회원 탈퇴 시 이 앱에 저장된 KIS 연동, 포지션, 매매·로그, AI 캐시(Firestore)가 모두 지워지고, " +
          "Google 로그인과 연동된 앱의 로그인 계정(Firebase Auth)도 삭제됩니다. 되돌릴 수 없습니다. 계속할까요?",
      )
    ) {
      return;
    }
    if (
      !window.confirm(
        "최종 확인: 지금 탈퇴하면 동일 Google 계정으로도 데이터 없이 다시 가입(설정)해야 합니다. 정말 탈퇴하시겠어요?",
      )
    ) {
      return;
    }
    try {
      const r = await apiFetch<{ ok: boolean; error?: string }>("/api/account/withdraw", {
        method: "POST",
        idToken,
        body: "{}",
      });
      if (!r.ok) {
        alert(r.error || "탈퇴 처리에 실패했습니다. 잠시 후 다시 시도해 주세요.");
        return;
      }
    } catch (e: unknown) {
      alert(
        e instanceof Error
          ? e.message
          : "서버에 연결할 수 없습니다. 네트워크를 확인한 뒤 다시 시도해 주세요.",
      );
      return;
    }
    try {
      await signOut(auth);
    } catch {
      /* auth 계정이 이미 삭제된 경우에도 클라이언트만 정리 */
    }
    setSetupRequired(false);
    setStatusBootstrap(null);
  }

  if (boot && user) {
    return (
      <div className="relative flex min-h-dvh items-center justify-center bg-[#060b18]">
        <BootStatusOverlay elapsedSec={bootElapsedSec} />
      </div>
    );
  }

  if (!user) {
    return (
      <div className="min-h-dvh flex flex-col items-center justify-center px-4 bg-[#060b18]">
        <div className="mb-6 h-14 w-14 rounded-2xl bg-gradient-to-br from-blue-500 to-violet-600 flex items-center justify-center text-xl font-black">
          A
        </div>
        <h1 className="text-2xl font-bold text-white mb-2">AutoStock</h1>
        <p className="text-slate-400 text-sm text-center mb-8 max-w-sm">
          KIS Open API · Firebase · 반응형 대시보드
        </p>
        <button
          type="button"
          onClick={() => signIn()}
          className="rounded-xl bg-white text-slate-900 font-semibold px-8 py-3 text-sm shadow-lg"
        >
          Google로 로그인
        </button>
      </div>
    );
  }

  if (!idToken) {
    return (
      <div className="flex min-h-dvh items-center justify-center text-slate-400">
        토큰 준비 중…
      </div>
    );
  }

  if (setupRequired) {
    return (
      <div className="min-h-dvh bg-[#060b18]">
        <header className="flex justify-end p-4 border-b border-white/5">
          <button
            type="button"
            onClick={doSignOut}
            className="text-sm text-slate-400 hover:text-white"
          >
            로그아웃
          </button>
        </header>
        <SetupForm
          idToken={idToken}
          onDone={() => {
            setSetupRequired(false);
            window.location.reload();
          }}
        />
      </div>
    );
  }

  return (
    <div className="min-h-dvh bg-[#060b18] pb-safe">
      <nav className="sticky top-0 z-50 border-b border-white/5 bg-[#060b18]/90 backdrop-blur-md supports-[backdrop-filter]:bg-[#060b18]/80 overflow-visible">
        <div className="mx-auto w-full max-w-6xl flex items-center justify-between px-4 sm:px-5 md:px-6 h-14 min-h-[3.5rem] overflow-visible">
          <div className="flex items-center gap-2">
            <div className="h-9 w-9 rounded-xl bg-gradient-to-br from-blue-500 to-violet-600 flex items-center justify-center text-sm font-black">
              A
            </div>
            <span className="font-bold text-white gradient-text">AutoStock</span>
          </div>
          <div className="flex items-center gap-1 sm:gap-2">
            <AccountMenu
              user={user}
              idToken={idToken}
              onSignOut={doSignOut}
              onWithdraw={doWithdraw}
            />
          </div>
        </div>
      </nav>
      <Dashboard
        idToken={idToken}
        currentMarket={market}
        setMarket={setMarket}
        statusBootstrap={statusBootstrap}
      />
    </div>
  );
}

export default App;
