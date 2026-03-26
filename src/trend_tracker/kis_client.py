from __future__ import annotations

from typing import Any

import requests

from .config import get_kis_app_key, get_kis_app_secret, get_kis_base_url
from .kis_auth import get_kis_access_token


class KISClient:
    def __init__(self, timeout: int = 15):
        self.base_url = get_kis_base_url().rstrip("/")
        self.timeout = timeout

    def get(self, path: str, tr_id: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        headers = {
            "content-type": "application/json; charset=utf-8",
            "authorization": f"Bearer {get_kis_access_token()}",
            "appkey": get_kis_app_key(),
            "appsecret": get_kis_app_secret(),
            "tr_id": tr_id,
            "custtype": "P",
        }
        response = requests.get(
            f"{self.base_url}/{path.lstrip('/')}",
            headers=headers,
            params=params or {},
            timeout=self.timeout,
        )
        response.raise_for_status()
        payload = response.json()
        rt_cd = str(payload.get("rt_cd", "0"))
        if rt_cd not in {"0", ""}:
            msg_cd = payload.get("msg_cd", "")
            msg1 = payload.get("msg1", "KIS API request failed")
            raise RuntimeError(f"{msg_cd} {msg1}".strip())
        return payload
