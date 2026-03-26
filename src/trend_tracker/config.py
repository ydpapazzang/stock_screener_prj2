from __future__ import annotations

import os


DEFAULT_TOP_N = 200
DEFAULT_LOOKBACK_DAYS = 900
BACKTEST_LOOKBACK_DAYS = 7000
MA_WINDOW = 10
BACKTEST_BAR_LIMIT = 200
MAX_ANALYSIS_WORKERS = 12
DEFAULT_PUBLIC_APP_URL = "https://stockscreenerprj2-zcmcytyti5uvk4shz8ovzu.streamlit.app"
DEFAULT_KIS_BASE_URL = "https://openapi.koreainvestment.com:9443"

MARKET_OPTIONS = {
    "KOSPI": "KOSPI",
    "KOSDAQ": "KOSDAQ",
    "NASDAQ": "NASDAQ",
    "S&P500": "S&P500",
    "다우산업": "DOW",
}

ALERT_MARKETS = [
    ("KOSPI", "KOSPI"),
    ("KOSDAQ", "KOSDAQ"),
    ("NASDAQ", "NASDAQ"),
    ("S&P500", "S&P500"),
    ("다우산업", "DOW"),
]


DOW_COMPONENTS = [
    ("MMM", "3M"),
    ("AXP", "American Express"),
    ("AMGN", "Amgen"),
    ("AMZN", "Amazon"),
    ("AAPL", "Apple"),
    ("BA", "Boeing"),
    ("CAT", "Caterpillar"),
    ("CVX", "Chevron"),
    ("CSCO", "Cisco"),
    ("KO", "Coca-Cola"),
    ("DIS", "Disney"),
    ("GS", "Goldman Sachs"),
    ("HD", "Home Depot"),
    ("HON", "Honeywell"),
    ("IBM", "IBM"),
    ("JNJ", "Johnson & Johnson"),
    ("JPM", "JPMorgan Chase"),
    ("MCD", "McDonald's"),
    ("MRK", "Merck"),
    ("MSFT", "Microsoft"),
    ("NKE", "Nike"),
    ("PG", "Procter & Gamble"),
    ("CRM", "Salesforce"),
    ("SHW", "Sherwin-Williams"),
    ("TRV", "Travelers"),
    ("UNH", "UnitedHealth"),
    ("VZ", "Verizon"),
    ("V", "Visa"),
    ("WMT", "Walmart"),
    ("NVDA", "NVIDIA"),
]


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


def get_public_app_url() -> str:
    return get_secret("APP_PUBLIC_URL", DEFAULT_PUBLIC_APP_URL)


def get_kis_app_key() -> str:
    return get_secret("KIS_APP_KEY")


def get_kis_app_secret() -> str:
    return get_secret("KIS_APP_SECRET")


def get_kis_base_url() -> str:
    return get_secret("KIS_BASE_URL", DEFAULT_KIS_BASE_URL)


def is_kis_configured() -> bool:
    return bool(get_kis_app_key() and get_kis_app_secret())
