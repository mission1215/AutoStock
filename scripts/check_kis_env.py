#!/usr/bin/env python3
"""KIS 모의/실전 프로필 검증 — OAuth 토큰 + 시세 API 스모크 테스트."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import requests

from config import Config, KISCredentials


def _mask(s: str, show: int = 4) -> str:
    if not s:
        return "(비어있음)"
    if len(s) <= show * 2:
        return "*" * len(s)
    return f"{s[:show]}...{s[-show:]}"


def _test_oauth(cred: KISCredentials, *, mock: bool) -> tuple[bool, str]:
    base = (
        "https://openapivts.koreainvestment.com:29443"
        if mock
        else "https://openapi.koreainvestment.com:9443"
    )
    label = "모의" if mock else "실전"
    if not cred.app_key or not cred.app_secret:
        return False, f"{label}: 키/시크릿 미설정"

    try:
        resp = requests.post(
            base + "/oauth2/tokenP",
            json={
                "grant_type": "client_credentials",
                "appkey": cred.app_key,
                "appsecret": cred.app_secret,
            },
            timeout=15,
        )
        data = resp.json()
        if resp.status_code == 200 and "access_token" in data:
            return True, f"{label}: 토큰 OK (expires_in={data.get('expires_in', '?')}s)"
        err = data.get("error_description") or data.get("msg1") or str(data)
        return False, f"{label}: HTTP {resp.status_code} — {err}"
    except requests.RequestException as exc:
        return False, f"{label}: 네트워크 오류 — {exc}"


def main() -> int:
    print("=== KIS 프로필 상태 ===\n")
    for line in Config.profile_status_lines():
        print(f"  {line}")

    mock = Config.mock_credentials()
    live = Config.live_credentials()

    print("\n=== 프로필 상세 ===")
    for name, cred, prefix in (
        ("모의", mock, "KIS_MOCK_"),
        ("실전", live, "KIS_LIVE_"),
    ):
        print(f"\n  [{name}]")
        print(f"    KEY    : {_mask(cred.app_key)}")
        print(f"    SECRET : {_mask(cred.app_secret)}")
        print(f"    ACCOUNT: {cred.account_no or '(비어있음)'}")
        missing = cred.missing_fields(prefix)
        if missing:
            print(f"    미설정 : {', '.join(missing)}")
        else:
            print("    형식   : ✓")

    print("\n=== OAuth 검증 ===")
    exit_code = 0
    for mock_flag, cred in ((True, mock), (False, live)):
        if not cred.app_key and not cred.app_secret:
            label = "모의" if mock_flag else "실전"
            print(f"  — {label}: 스킵 (키 없음)")
            continue
        ok, msg = _test_oauth(cred, mock=mock_flag)
        print(f"  {'✓' if ok else '✗'} {msg}")
        if not ok and (mock_flag == Config.IS_MOCK):
            exit_code = 1

    print("\n=== 활성 모드 validate() ===")
    try:
        Config.validate()
        print("  ✓ 통과")
    except EnvironmentError as exc:
        print(f"  ✗ {exc}")
        exit_code = 1

    if not live.configured:
        print("\n💡 실전 전환 시:")
        print("  1. KIS_LIVE_APP_KEY / SECRET / ACCOUNT_NO 입력")
        print("  2. python scripts/check_kis_env.py 로 실전 토큰 확인")
        print("  3. KIS_IS_MOCK=false 후 main.py 또는 파이프라인 실행")

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
