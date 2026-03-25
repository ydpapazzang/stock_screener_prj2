from __future__ import annotations

from datetime import datetime, timedelta

import requests

from .config import get_kis_app_key, get_kis_app_secret, get_kis_base_url, is_kis_configured


_TOKEN_CACHE: dict[str, object] = {
    "access_token": "",
    "expires_at": None,
}


def _token_is_valid() -> bool:
    expires_at = _TOKEN_CACHE.get("expires_at")
    access_token = _TOKEN_CACHE.get("access_token")
    return bool(access_token and isinstance(expires_at, datetime) and datetime.utcnow() < expires_at)


def get_kis_access_token(force_refresh: bool = False) -> str:
    if not force_refresh and _token_is_valid():
        return str(_TOKEN_CACHE["access_token"])

    if not is_kis_configured():
        raise RuntimeError("KIS Open API 시크릿이 설정되어 있지 않습니다.")

    url = f"{get_kis_base_url().rstrip('/')}/oauth2/tokenP"
    payload = {
        "grant_type": "client_credentials",
        "appkey": get_kis_app_key(),
        "appsecret": get_kis_app_secret(),
    }

    response = requests.post(url, json=payload, timeout=15)
    response.raise_for_status()
    data = response.json()

    access_token = data.get("access_token", "")
    expires_in = int(data.get("expires_in", 0) or 0)
    if not access_token:
        raise RuntimeError(f"KIS 접근토큰 발급 실패: {data}")

    _TOKEN_CACHE["access_token"] = access_token
    _TOKEN_CACHE["expires_at"] = datetime.utcnow() + timedelta(seconds=max(expires_in - 60, 0))
    return access_token
