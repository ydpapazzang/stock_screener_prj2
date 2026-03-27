from __future__ import annotations

from datetime import datetime

import pandas as pd
import requests

from .config import get_public_app_url, get_telegram_bot_token, get_telegram_chat_id
from .formatting import format_number


def build_telegram_message(results_df: pd.DataFrame, base_date: str, market: str) -> str:
    label_date = datetime.strptime(base_date, "%Y%m%d").strftime("%Y년 %m월 %d일") if base_date else "-"
    market = market or "-"
    breakouts = results_df[results_df["월봉10개월선돌파여부"].astype(str) == "예"].head(10)

    lines = [f"[{label_date} 마감 {market} 월봉 스크리닝]", ""]
    lines.append("월봉 10개월선 돌파 종목")

    if breakouts.empty:
        lines.append("- 이번 조회에서는 돌파 종목이 없습니다.")
    else:
        for _, row in breakouts.iterrows():
            lines.append(f"- {row['종목명']} ({row['종목코드']})")
            lines.append(f"  현재가: {format_number(row['현재가'])}원")
            lines.append(f"  10개월선: {format_number(row['10개월선'])}원")
            lines.append(f"  한달간 거래량: {format_number(row['한달간 거래량'])}")

    app_url = get_public_app_url().strip()
    if app_url:
        lines.extend(["", f"스크리너 바로가기: {app_url}"])

    return "\n".join(lines)


def build_weekly_telegram_message(results_df: pd.DataFrame, base_date: str, market: str) -> str:
    label_date = datetime.strptime(base_date, "%Y%m%d").strftime("%Y년 %m월 %d일") if base_date else "-"
    market = market or "-"
    setups = results_df[results_df["최종조건충족"].astype(str) == "예"].head(10)

    lines = [f"[{label_date} 기준 {market} 주봉 조건 검색]", ""]
    lines.append("10·20·40주선 밀집 + 20·40주 돌파 + 초기 추세 전환 종목")

    if setups.empty:
        lines.append("- 이번 조회에서는 최종 조건 충족 종목이 없습니다.")
    else:
        for _, row in setups.iterrows():
            lines.append(f"- {row['종목명']} ({row['종목코드']})")
            lines.append(f"  현재가: {format_number(row['현재가'])}원")
            lines.append(f"  10주선/20주선/40주선: {format_number(row['10주선'])} / {format_number(row['20주선'])} / {format_number(row['40주선'])}")
            spread_text = "미계산" if pd.isna(row["이평선이격률"]) else f"{float(row['이평선이격률']):.2f}%"
            hold_text = "미계산" if pd.isna(row.get("예상보유기간")) else f"{float(row['예상보유기간']):.1f}주"
            return_text = "미계산" if pd.isna(row.get("예상수익률")) else f"{float(row['예상수익률']):.1f}%"
            lines.append(f"  이평선 이격률: {spread_text}")
            lines.append(f"  예상 보유기간: {hold_text}")
            lines.append(f"  예상 수익률: {return_text}")

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
