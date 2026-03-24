from __future__ import annotations

from datetime import datetime

import pandas as pd
import requests

from .config import get_public_app_url, get_telegram_bot_token, get_telegram_chat_id
from .formatting import format_number


def build_telegram_message(results_df: pd.DataFrame, base_date: str, market: str) -> str:
    label_date = datetime.strptime(base_date, "%Y%m%d").strftime("%Y년 %m월")
    breakouts = results_df[results_df["월봉10개월선돌파여부"] == "예"].head(10)

    lines = [f"[{label_date} 말일 {market} 10개월선 스크리닝]", ""]
    lines.append("● 월봉 10개월선 돌파 종목")

    if breakouts.empty:
        lines.append("- 이번 조회에서는 돌파 종목이 없습니다.")
    else:
        for _, row in breakouts.iterrows():
            lines.append(f"- {row['종목명']} ({row['종목코드']})")
            lines.append(f"  현재가: {format_number(row['현재가'])}원")
            lines.append(f"  10개월선: {format_number(row['10개월선'])}원")
            lines.append(f"  한달 거래량: {format_number(row['한달간 거래량'])}")
            lines.append(f"  백테스트: {row['백테스팅 결과']}")

    app_url = get_public_app_url().strip()
    if app_url:
        lines.extend(["", f"스크리너 바로가기: {app_url}"])

    return "\n".join(lines)


def build_app_link_message() -> str:
    app_url = get_public_app_url().strip()
    if not app_url:
        return "스크리너 URL이 설정되어 있지 않습니다."
    return f"[스크리너 바로가기]\n{app_url}"


def send_telegram_message(message: str) -> tuple[bool, str]:
    bot_token = get_telegram_bot_token()
    chat_id = get_telegram_chat_id()
    if not bot_token or not chat_id:
        return False, "텔레그램 시크릿이 설정되어 있지 않습니다."

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
    }

    try:
        response = requests.post(url, json=payload, timeout=15)
        response.raise_for_status()
        data = response.json()
    except requests.RequestException as exc:
        return False, f"텔레그램 요청 실패: {exc}"

    if not data.get("ok"):
        return False, f"텔레그램 전송 실패: {data.get('description', '알 수 없는 오류')}"

    return True, "텔레그램 알림을 전송했습니다."
