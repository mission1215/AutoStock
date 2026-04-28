import { useEffect, useState } from "react";
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

function App() {
  const [user, setUser] = useState<User | null>(null);
  const [idToken, setIdToken] = useState<string | null>(null);
  const [boot, setBoot] = useState(true);
  const [setupRequired, setSetupRequired] = useState(false);
  const [market, setMarket] = useState<"KR" | "US">("KR");
  /** 부트 시 받은 /api/status — Dashboard 첫 그림에서 중복 호출 제거(토큰 변경 시엔 대시보드가 다시 조회) */
  const [statusBootstrap, setStatusBootstrap] = useState<StatusResponse | null>(null);

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
      return;
    }
    let cancelled = false;
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
        if (!cancelled) setBoot(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [user, idToken]);

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
      <div className="flex min-h-dvh items-center justify-center bg-[#060b18]">
        <div className="h-8 w-8 animate-spin rounded-full border-2 border-blue-500 border-t-transparent" />
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
