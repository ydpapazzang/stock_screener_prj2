from __future__ import annotations

import os


DEFAULT_TOP_N = 200
DEFAULT_LOOKBACK_DAYS = 7000
MA_WINDOW = 10
BACKTEST_BAR_LIMIT = 200
MAX_ANALYSIS_WORKERS = 12

MARKET_OPTIONS = {
    "KOSPI": "KOSPI",
    "KOSDAQ": "KOSDAQ",
}


def get_secret(name: str, default: str = "") -> str:
    env_value = os.getenv(name)
    if env_value:
        return env_value

    try:
        import streamlit as st

        if name in st.secrets:
            return str(st.secrets[name])
    except Exception:
        pass

    return default


def get_telegram_chat_id() -> str:
    return get_secret("TELEGRAM_CHAT_ID")


def get_telegram_bot_token() -> str:
    return get_secret("TELEGRAM_BOT_TOKEN")


def is_telegram_configured() -> bool:
    return bool(get_telegram_chat_id() and get_telegram_bot_token())
