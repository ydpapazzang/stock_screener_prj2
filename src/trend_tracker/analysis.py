from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta

import pandas as pd
import requests
import streamlit as st
import holidays
from pykrx import stock

from .config import (
    BACKTEST_BAR_LIMIT,
    BACKTEST_LOOKBACK_DAYS,
    DEFAULT_LOOKBACK_DAYS,
    DOW_COMPONENTS,
    MA_WINDOW,
    MAX_ANALYSIS_WORKERS,
    is_kis_configured,
)
from .formatting import format_percent, to_krx_date
from .providers import KISMarketDataProvider

LAST_DATA_ERROR: str | None = None
LAST_DATA_DIAGNOSTICS: dict[str, object] = {}
KIS_PROVIDER = KISMarketDataProvider()


@dataclass
class AnalysisResult:
    ticker: str
    name: str
    market: str
    close: float
    ma10: float
    previous_close: float
    previous_ma10: float
    monthly_volume: float
    volume_change_pct: float | None
    breakout: bool
    signal: str
    latest_breakout_date: pd.Timestamp | None
    months_since_breakout: int | None
    ma10_rising: bool = False
    above_ma20: bool = False
    ma10_above_ma20: bool = False
    backtest_summary: str = "미계산"
    backtest_return_pct: float | None = None
    backtest_mdd_pct: float | None = None
    backtest_cagr_pct: float | None = None
    average_hold_months: float | None = None
    trade_count: int = 0
    win_rate_pct: float | None = None
    market_cap: int = 0


@dataclass
class BacktestMetrics:
    summary: str
    cumulative_return_pct: float | None
    mdd_pct: float | None
    cagr_pct: float | None
    trade_count: int
    win_rate_pct: float | None
    average_hold_months: float | None
    trade_log: list[dict[str, object]]


def _load_fdr():
    try:
        import FinanceDataReader as fdr
    except ImportError:
        return None
    return fdr


@st.cache_data(ttl=60 * 30, show_spinner=False)
def get_cnn_fear_greed_snapshot() -> dict[str, object] | None:
    headers = {
        "user-agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        )
    }
    try:
        response = requests.get(
            "https://production.dataviz.cnn.io/index/fearandgreed/graphdata",
            headers=headers,
            timeout=10,
        )
        response.raise_for_status()
        payload = response.json()
    except Exception:
        return None

    fear_greed = payload.get("fear_and_greed") or {}
    score = fear_greed.get("score")
    rating = fear_greed.get("rating")
    timestamp = fear_greed.get("timestamp")
    previous_close = fear_greed.get("previous_close")

    if score is None:
        return None

    return {
        "score": float(score),
        "rating": str(rating) if rating is not None else "",
        "timestamp": str(timestamp) if timestamp is not None else "",
        "previous_close": float(previous_close) if previous_close is not None else None,
        "source": "CNN Fear & Greed Index",
    }


@st.cache_data(ttl=60 * 30, show_spinner=False)
def get_market_index_snapshots() -> list[dict[str, object]]:
    fdr = _load_fdr()
    if fdr is None:
        return []

    symbols = [
        ("KOSPI", "KS11"),
        ("KOSDAQ", "KQ11"),
        ("NASDAQ", "IXIC"),
        ("DOW", "DJI"),
        ("S&P500", "US500"),
    ]
    end_date = datetime.now().date() + timedelta(days=1)
    start_date = end_date - timedelta(days=14)
    snapshots: list[dict[str, object]] = []

    for label, symbol in symbols:
        try:
            frame = fdr.DataReader(symbol, start_date, end_date)
        except Exception:
            continue
        normalized = _normalize_daily_ohlcv(frame)
        if len(normalized) < 2:
            continue
        # Avoid relying on locale-sensitive column labels for cached index frames.
        latest_close = float(normalized.iloc[-1, 0])
        previous_close = float(normalized.iloc[-2, 0])
        change_pct = ((latest_close / previous_close) - 1) * 100 if previous_close else None
        snapshots.append(
            {
                "label": label,
                "value": latest_close,
                "change_pct": change_pct,
            }
        )

    return snapshots


def _get_kis_provider() -> KISMarketDataProvider | None:
    if not is_kis_configured():
        return None
    return KIS_PROVIDER


def _reset_diagnostics() -> None:
    global LAST_DATA_DIAGNOSTICS
    LAST_DATA_DIAGNOSTICS = {
        "pool_source": "",
        "pool_fallbacks": [],
        "ohlcv_sources": {},
        "errors": [],
    }


def _set_pool_source(source: str) -> None:
    LAST_DATA_DIAGNOSTICS["pool_source"] = source


def _add_pool_fallback(message: str) -> None:
    LAST_DATA_DIAGNOSTICS.setdefault("pool_fallbacks", []).append(message)


def _increment_ohlcv_source(source: str) -> None:
    counts = LAST_DATA_DIAGNOSTICS.setdefault("ohlcv_sources", {})
    counts[source] = counts.get(source, 0) + 1


def _add_error(message: str) -> None:
    errors = LAST_DATA_DIAGNOSTICS.setdefault("errors", [])
    if len(errors) < 10:
        errors.append(message)


@st.cache_data(ttl=60 * 60 * 6, show_spinner=False)
def get_latest_business_day() -> str:
    today = datetime.now().date()
    kr_holidays = holidays.country_holidays("KR")
    for offset in range(15):
        current = today - timedelta(days=offset)
        if current.weekday() >= 5:
            continue
        if current in kr_holidays:
            continue
        return to_krx_date(current)
    return to_krx_date(today)


@st.cache_data(ttl=60 * 60 * 6, show_spinner=False)
def get_market_cap_pool(base_date: str, market: str, top_n: int) -> pd.DataFrame:
    global LAST_DATA_ERROR
    kis_provider = _get_kis_provider()
    if kis_provider is not None:
        try:
            kis_pool = kis_provider.get_universe(market=market, top_n=top_n, base_date=base_date)
        except Exception as exc:
            LAST_DATA_ERROR = f"KIS universe 조회 실패: {exc}"
            _add_pool_fallback("KIS universe -> legacy providers")
            _add_error(LAST_DATA_ERROR)
        else:
            if not kis_pool.empty:
                _set_pool_source("KIS Open API universe")
                return kis_pool

    if market in {"NASDAQ", "S&P500", "DOW"}:
        return _get_global_market_pool_from_fdr(market, top_n)

    fdr_pool = _get_market_cap_pool_from_fdr(market, top_n)
    if not fdr_pool.empty:
        _set_pool_source("FinanceDataReader")
        return fdr_pool

    try:
        market_cap = stock.get_market_cap_by_ticker(base_date, market=market)
    except Exception as exc:
        LAST_DATA_ERROR = f"FDR 시총 풀 조회 실패 후 pykrx 시가총액 조회도 실패: {exc}"
        _add_pool_fallback("FinanceDataReader -> pykrx")
        ticker_pool = _get_ticker_pool_from_pykrx(base_date, market, top_n)
        if not ticker_pool.empty:
            return ticker_pool
        _add_error(LAST_DATA_ERROR)
        return pd.DataFrame(columns=["티커", "종목명", "시장", "시가총액"])

    required_columns = {"시가총액"}
    if market_cap.empty or not required_columns.issubset(set(market_cap.columns)):
        LAST_DATA_ERROR = "FDR 시총 풀 조회 실패 후 pykrx 시가총액 조회 응답에도 필요한 컬럼이 없습니다."
        _add_pool_fallback("FinanceDataReader -> pykrx")
        ticker_pool = _get_ticker_pool_from_pykrx(base_date, market, top_n)
        if not ticker_pool.empty:
            return ticker_pool
        _add_error(LAST_DATA_ERROR)
        return pd.DataFrame(columns=["티커", "종목명", "시장", "시가총액"])

    market_cap = market_cap.reset_index()
    if "티커" not in market_cap.columns:
        first_column = market_cap.columns[0]
        market_cap = market_cap.rename(columns={first_column: "티커"})
    market_cap["종목명"] = market_cap["티커"].apply(stock.get_market_ticker_name)
    market_cap["시장"] = market
    market_cap = market_cap.sort_values("시가총액", ascending=False).head(top_n)
    _set_pool_source("pykrx")
    _add_pool_fallback("FinanceDataReader -> pykrx")
    return market_cap[["티커", "종목명", "시장", "시가총액"]]


def _get_ticker_pool_from_pykrx(base_date: str, market: str, top_n: int) -> pd.DataFrame:
    global LAST_DATA_ERROR
    base_dt = datetime.strptime(base_date, "%Y%m%d").date()
    tickers: list[str] = []

    for offset in range(15):
        current_date = to_krx_date(base_dt - timedelta(days=offset))
        try:
            tickers = stock.get_market_ticker_list(date=current_date, market=market)
        except Exception as exc:
            LAST_DATA_ERROR = f"{LAST_DATA_ERROR} / pykrx 티커 목록 조회도 실패: {exc}" if LAST_DATA_ERROR else f"pykrx 티커 목록 조회도 실패: {exc}"
            continue
        if tickers:
            break

    if not tickers:
        LAST_DATA_ERROR = f"{LAST_DATA_ERROR} / pykrx 티커 목록이 비어 있습니다." if LAST_DATA_ERROR else "pykrx 티커 목록이 비어 있습니다."
        _add_error(LAST_DATA_ERROR)
        return pd.DataFrame(columns=["티커", "종목명", "시장", "시가총액"])

    selected_tickers = list(tickers)[:top_n]
    pool_df = pd.DataFrame(
        {
            "티커": selected_tickers,
            "종목명": [stock.get_market_ticker_name(ticker) for ticker in selected_tickers],
            "시장": market,
            "시가총액": [0] * len(selected_tickers),
        }
    )
    _set_pool_source("pykrx ticker list")
    _add_pool_fallback("pykrx ticker list without market cap")
    return pool_df


def _get_market_cap_pool_from_fdr(market: str, top_n: int) -> pd.DataFrame:
    global LAST_DATA_ERROR
    fdr = _load_fdr()
    if fdr is None:
        LAST_DATA_ERROR = "FinanceDataReader가 설치되어 있지 않습니다."
        return pd.DataFrame(columns=["티커", "종목명", "시장", "시가총액"])

    try:
        listing = fdr.StockListing("KRX-MARCAP")
    except Exception as exc:
        LAST_DATA_ERROR = f"FDR KRX-MARCAP 조회 실패: {exc}"
        _add_pool_fallback("KRX-MARCAP -> KRX")
        try:
            listing = fdr.StockListing("KRX")
        except Exception as inner_exc:
            LAST_DATA_ERROR = f"{LAST_DATA_ERROR} / FDR KRX 조회 실패: {inner_exc}"
            _add_error(LAST_DATA_ERROR)
            return pd.DataFrame(columns=["티커", "종목명", "시장", "시가총액"])

    market_column = "Market" if "Market" in listing.columns else None
    marcap_column = "Marcap" if "Marcap" in listing.columns else "시가총액" if "시가총액" in listing.columns else None
    code_column = "Code" if "Code" in listing.columns else None
    name_column = "Name" if "Name" in listing.columns else None

    if not all([market_column, code_column, name_column]):
        LAST_DATA_ERROR = f"{LAST_DATA_ERROR} / FDR 종목목록 컬럼 부족: {list(listing.columns)}" if LAST_DATA_ERROR else f"FDR 종목목록 컬럼 부족: {list(listing.columns)}"
        _add_error(LAST_DATA_ERROR)
        return pd.DataFrame(columns=["티커", "종목명", "시장", "시가총액"])

    filtered = listing[listing[market_column].astype(str).str.upper() == market.upper()].copy()
    if filtered.empty:
        return pd.DataFrame(columns=["티커", "종목명", "시장", "시가총액"])

    if marcap_column:
        filtered = filtered.sort_values(marcap_column, ascending=False).head(top_n)
        market_caps = filtered[marcap_column].fillna(0).astype("int64")
        _set_pool_source("FinanceDataReader")
    else:
        filtered = filtered.head(top_n)
        market_caps = pd.Series([0] * len(filtered), index=filtered.index, dtype="int64")
        _set_pool_source("FinanceDataReader (listing only)")
        _add_pool_fallback("KRX listing without market cap column")

    return pd.DataFrame(
        {
            "티커": filtered[code_column].astype(str).str.zfill(6),
            "종목명": filtered[name_column],
            "시장": market,
            "시가총액": market_caps,
        }
    )


def _get_global_market_pool_from_fdr(market: str, top_n: int) -> pd.DataFrame:
    global LAST_DATA_ERROR
    if market == "DOW":
        _set_pool_source("Static DOW30")
        return pd.DataFrame(
            {
                "티커": [symbol for symbol, _ in DOW_COMPONENTS[:top_n]],
                "종목명": [name for _, name in DOW_COMPONENTS[:top_n]],
                "시장": ["DOW"] * min(top_n, len(DOW_COMPONENTS)),
                "시가총액": [0] * min(top_n, len(DOW_COMPONENTS)),
            }
        )

    fdr = _load_fdr()
    if fdr is None:
        LAST_DATA_ERROR = "FinanceDataReader가 설치되어 있지 않습니다."
        return pd.DataFrame(columns=["티커", "종목명", "시장", "시가총액"])

    source_map = {
        "NASDAQ": "NASDAQ",
        "S&P500": "S&P500",
    }
    source = source_map[market]

    try:
        listing = fdr.StockListing(source)
    except Exception as exc:
        LAST_DATA_ERROR = f"{market} 종목목록 조회 실패: {exc}"
        _add_error(LAST_DATA_ERROR)
        return pd.DataFrame(columns=["티커", "종목명", "시장", "시가총액"])

    code_column = "Symbol" if "Symbol" in listing.columns else "Code" if "Code" in listing.columns else None
    name_column = "Name" if "Name" in listing.columns else None
    market_cap_candidates = ["Market Cap", "Marcap", "MarketCap", "시가총액"]
    marcap_column = next((column for column in market_cap_candidates if column in listing.columns), None)

    if not code_column or not name_column:
        LAST_DATA_ERROR = f"{market} 종목목록 컬럼 부족: {list(listing.columns)}"
        _add_error(LAST_DATA_ERROR)
        return pd.DataFrame(columns=["티커", "종목명", "시장", "시가총액"])

    filtered = listing.copy()
    if marcap_column:
        filtered = filtered.sort_values(marcap_column, ascending=False)
        market_caps = pd.to_numeric(filtered[marcap_column], errors="coerce").fillna(0)
    else:
        market_caps = pd.Series([0] * len(filtered), index=filtered.index)

    filtered = filtered.head(top_n)
    market_caps = market_caps.loc[filtered.index]
    _set_pool_source(f"FinanceDataReader {market}")

    return pd.DataFrame(
        {
            "티커": filtered[code_column].astype(str),
            "종목명": filtered[name_column].astype(str),
            "시장": market,
            "시가총액": market_caps.astype("int64"),
        }
    )


def _normalize_daily_ohlcv(daily_df: pd.DataFrame) -> pd.DataFrame:
    if daily_df.empty:
        return daily_df

    df = daily_df.copy()
    column_map = {}
    open_kr = "시가"
    close_kr = "종가"
    volume_kr = "거래량"

    if "Open" in df.columns:
        column_map["Open"] = "open"
    if "Close" in df.columns:
        column_map["Close"] = "close"
    if "Volume" in df.columns:
        column_map["Volume"] = "volume"
    if open_kr in df.columns:
        column_map[open_kr] = "open"
    if close_kr in df.columns:
        column_map[close_kr] = "close"
    if volume_kr in df.columns:
        column_map[volume_kr] = "volume"
    if column_map:
        df = df.rename(columns=column_map)

    required = {"open", "close", "volume"}
    if not required.issubset(set(df.columns)):
        return pd.DataFrame(columns=["open", "close", "volume"])

    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index)

    return df[["open", "close", "volume"]].dropna()


@st.cache_data(ttl=60 * 60 * 24, show_spinner=False)
def _has_enough_history_for_monthly_signal(daily_df: pd.DataFrame) -> bool:
    if daily_df.empty:
        return False
    monthly_count = daily_df.resample("M").size()
    return len(monthly_count) >= MA_WINDOW + 1


@st.cache_data(ttl=60 * 60 * 24, show_spinner=False)
def _get_daily_ohlcv(base_date: str, start_date: str, ticker: str, market: str) -> tuple[pd.DataFrame, str]:
    global LAST_DATA_ERROR
    kis_provider = _get_kis_provider()
    if kis_provider is not None:
        try:
            kis_daily_df = kis_provider.get_daily_ohlcv(
                ticker=ticker,
                market=market,
                start_date=start_date,
                end_date=base_date,
            )
        except Exception as exc:
            LAST_DATA_ERROR = f"KIS OHLCV 조회 실패({ticker}): {exc}"
        else:
            if not kis_daily_df.empty and _has_enough_history_for_monthly_signal(kis_daily_df):
                return kis_daily_df, "KIS Open API"
            if not kis_daily_df.empty:
                _add_pool_fallback(f"KIS OHLCV history too short -> fallback ({ticker})")

    fdr = _load_fdr()

    if fdr is not None:
        try:
            start_dt = datetime.strptime(start_date, "%Y%m%d").date()
            end_dt = datetime.strptime(base_date, "%Y%m%d").date() + timedelta(days=1)
            daily_df = fdr.DataReader(ticker, start_dt, end_dt)
            normalized = _normalize_daily_ohlcv(daily_df)
            if not normalized.empty:
                return normalized, "FinanceDataReader"
        except Exception as exc:
            LAST_DATA_ERROR = f"FDR 개별 종목 OHLCV 조회 실패({ticker}): {exc}"

    if market not in {"KOSPI", "KOSDAQ"}:
        if LAST_DATA_ERROR:
            _add_error(LAST_DATA_ERROR)
        return pd.DataFrame(columns=["open", "close", "volume"]), "none"

    try:
        daily_df = stock.get_market_ohlcv_by_date(
            fromdate=start_date,
            todate=base_date,
            ticker=ticker,
        )
        normalized = _normalize_daily_ohlcv(daily_df)
        if not normalized.empty:
            return normalized, "pykrx"
    except Exception as exc:
        LAST_DATA_ERROR = f"{LAST_DATA_ERROR} / pykrx 개별 종목 OHLCV 조회 실패({ticker}): {exc}" if LAST_DATA_ERROR else f"pykrx 개별 종목 OHLCV 조회 실패({ticker}): {exc}"

    if LAST_DATA_ERROR:
        _add_error(LAST_DATA_ERROR)
    return pd.DataFrame(columns=["open", "close", "volume"]), "none"


def _analyze_single_ticker(item, start_date: str, base_date: str) -> tuple[AnalysisResult | None, pd.DataFrame | None, str]:
    daily_df, source_used = _get_daily_ohlcv(
        base_date=base_date,
        start_date=start_date,
        ticker=item.티커,
        market=item.시장,
    )

    if daily_df.empty:
        return None, None, source_used

    monthly_df = build_monthly_frame(daily_df)
    if len(monthly_df) < 2:
        return None, None, source_used

    breakout, signal = evaluate_signal(monthly_df)
    latest_breakout_date, months_since_breakout = find_latest_breakout(monthly_df)
    current = monthly_df.iloc[-1]
    previous = monthly_df.iloc[-2]

    result = AnalysisResult(
        ticker=item.티커,
        name=item.종목명,
        market=item.시장,
        close=float(current["close"]),
        ma10=float(current["ma10"]),
        previous_close=float(previous["close"]),
        previous_ma10=float(previous["ma10"]),
        monthly_volume=float(current["volume"]),
        volume_change_pct=float(current["volume_change_pct"]) if pd.notna(current["volume_change_pct"]) else None,
        breakout=breakout,
        signal=signal,
        latest_breakout_date=latest_breakout_date,
        months_since_breakout=months_since_breakout,
        ma10_rising=_is_ma10_rising(monthly_df),
        above_ma20=_is_above_ma20(monthly_df),
        ma10_above_ma20=_is_ma10_above_ma20(monthly_df),
        backtest_summary="미계산",
        backtest_return_pct=None,
        backtest_mdd_pct=None,
        backtest_cagr_pct=None,
        average_hold_months=None,
        trade_count=0,
        win_rate_pct=None,
        market_cap=int(item.시가총액),
    )
    return result, monthly_df, source_used


def build_monthly_frame(daily_df: pd.DataFrame) -> pd.DataFrame:
    monthly = (
        daily_df[["open", "close", "volume"]]
        .resample("M")
        .agg({"open": "first", "close": "last", "volume": "sum"})
        .dropna(subset=["open", "close"])
    )
    monthly["ma10"] = monthly["close"].rolling(MA_WINDOW).mean()
    monthly["ma20"] = monthly["close"].rolling(20).mean()
    monthly["volume_change_pct"] = monthly["volume"].pct_change() * 100
    monthly["monthly_return_pct"] = monthly["close"].pct_change() * 100
    return monthly.dropna(subset=["ma10"])


def _is_ma10_rising(monthly_df: pd.DataFrame) -> bool:
    if len(monthly_df) < 2:
        return False
    current = monthly_df.iloc[-1]
    previous = monthly_df.iloc[-2]
    return pd.notna(current.get("ma10")) and pd.notna(previous.get("ma10")) and float(current["ma10"]) > float(previous["ma10"])


def _is_above_ma20(monthly_df: pd.DataFrame) -> bool:
    current = monthly_df.iloc[-1]
    ma20 = current.get("ma20")
    return pd.notna(ma20) and float(current["close"]) > float(ma20)


def _is_ma10_above_ma20(monthly_df: pd.DataFrame) -> bool:
    current = monthly_df.iloc[-1]
    ma20 = current.get("ma20")
    ma10 = current.get("ma10")
    return pd.notna(ma20) and pd.notna(ma10) and float(ma10) > float(ma20)


def evaluate_signal(monthly_df: pd.DataFrame) -> tuple[bool, str]:
    current = monthly_df.iloc[-1]
    previous = monthly_df.iloc[-2]

    crossed_up = previous["close"] < previous["ma10"] and current["close"] > current["ma10"]
    crossed_down = previous["close"] > previous["ma10"] and current["close"] < current["ma10"]

    if crossed_up:
        return True, "돌파"
    if crossed_down:
        return False, "이탈"
    if current["close"] > current["ma10"]:
        return False, "상단 유지"
    return False, "하단 위치"


def find_latest_breakout(monthly_df: pd.DataFrame) -> tuple[pd.Timestamp | None, int | None]:
    crossed_up = (monthly_df["close"].shift(1) < monthly_df["ma10"].shift(1)) & (monthly_df["close"] > monthly_df["ma10"])
    breakout_rows = monthly_df[crossed_up.fillna(False)]
    if breakout_rows.empty:
        return None, None

    latest_breakout_date = breakout_rows.index[-1]
    current_date = monthly_df.index[-1]
    months_since_breakout = (current_date.year - latest_breakout_date.year) * 12 + (current_date.month - latest_breakout_date.month)
    return latest_breakout_date, months_since_breakout


def run_backtest(monthly_df: pd.DataFrame, limit: int = BACKTEST_BAR_LIMIT) -> BacktestMetrics:
    backtest_df = monthly_df.tail(limit).copy()
    if len(backtest_df) < 3:
        return BacktestMetrics(
            summary="Data insufficient",
            cumulative_return_pct=None,
            mdd_pct=None,
            cagr_pct=None,
            trade_count=0,
            win_rate_pct=None,
            average_hold_months=None,
            trade_log=[],
        )

    in_position = False
    entry_price = 0.0
    entry_date = None
    equity = 1.0
    wins = 0
    trades = 0
    hold_months_total = 0
    trade_log: list[dict[str, object]] = []
    equity_curve: list[float] = [1.0]

    for index in range(1, len(backtest_df) - 1):
        signal_row = backtest_df.iloc[index]
        next_row = backtest_df.iloc[index + 1]
        signal_date = pd.Timestamp(backtest_df.index[index])
        next_date = pd.Timestamp(backtest_df.index[index + 1])

        close = float(signal_row["close"])
        ma10 = float(signal_row["ma10"])
        next_open = float(next_row["open"])

        if not in_position and close > ma10:
            in_position = True
            entry_price = next_open
            entry_date = next_date
            continue

        if in_position and close < ma10:
            trade_return = (next_open / entry_price) - 1
            equity *= 1 + trade_return
            trades += 1
            if trade_return > 0:
                wins += 1
            hold_months = ((next_date.year - entry_date.year) * 12 + (next_date.month - entry_date.month) + 1) if entry_date is not None else 0
            hold_months_total += hold_months
            trade_log.append(
                {
                    "entry_date": entry_date.strftime("%Y-%m") if entry_date is not None else "-",
                    "exit_date": next_date.strftime("%Y-%m"),
                    "entry_price": entry_price,
                    "exit_price": next_open,
                    "return_pct": trade_return * 100,
                    "hold_months": hold_months,
                    "signal_rule": f"{signal_date.strftime('%Y-%m')} month-end close confirmed -> next month open fill",
                }
            )
            equity_curve.append(equity)
            in_position = False
            entry_date = None

    if in_position:
        last_close = float(backtest_df.iloc[-1]["close"])
        last_date = pd.Timestamp(backtest_df.index[-1])
        trade_return = (last_close / entry_price) - 1
        equity *= 1 + trade_return
        trades += 1
        if trade_return > 0:
            wins += 1
        hold_months = ((last_date.year - entry_date.year) * 12 + (last_date.month - entry_date.month) + 1) if entry_date is not None else 0
        hold_months_total += hold_months
        trade_log.append(
            {
                "entry_date": entry_date.strftime("%Y-%m") if entry_date is not None else "-",
                "exit_date": f"{last_date.strftime('%Y-%m')} (open)",
                "entry_price": entry_price,
                "exit_price": last_close,
                "return_pct": trade_return * 100,
                "hold_months": hold_months,
                "signal_rule": "Month-end signal confirmed, final bar marked to latest close",
            }
        )
        equity_curve.append(equity)

    cumulative_return_pct = (equity - 1) * 100
    win_rate_pct = (wins / trades * 100) if trades else None
    average_hold_months = (hold_months_total / trades) if trades else None
    years = max(len(backtest_df) / 12, 1 / 12)
    cagr_pct = (((equity ** (1 / years)) - 1) * 100) if equity > 0 else None
    equity_series = pd.Series(equity_curve, dtype="float64")
    running_max = equity_series.cummax()
    drawdowns = (equity_series / running_max) - 1
    mdd_pct = abs(drawdowns.min()) * 100 if not drawdowns.empty else None

    summary = f"Total {format_percent(cumulative_return_pct)}, Trades {trades}"
    if win_rate_pct is not None:
        summary += f", Win {format_percent(win_rate_pct)}"
    if mdd_pct is not None:
        summary += f", MDD {format_percent(-mdd_pct)}"

    return BacktestMetrics(
        summary=summary,
        cumulative_return_pct=cumulative_return_pct,
        mdd_pct=mdd_pct,
        cagr_pct=cagr_pct,
        trade_count=trades,
        win_rate_pct=win_rate_pct,
        average_hold_months=average_hold_months,
        trade_log=trade_log,
    )


def enrich_results_with_backtests(
    results_df: pd.DataFrame,
    monthly_frames: dict[str, pd.DataFrame],
    base_date: str,
    tickers: list[str] | None = None,
) -> pd.DataFrame:
    if results_df.empty:
        return results_df

    updated_df = results_df.copy()
    target_tickers = tickers or updated_df["종목코드"].tolist()
    backtest_start_date = to_krx_date(datetime.strptime(base_date, "%Y%m%d").date() - timedelta(days=BACKTEST_LOOKBACK_DAYS))

    for ticker in target_tickers:
        market_series = updated_df.loc[updated_df["종목코드"] == ticker, "시장"]
        market = market_series.iloc[0] if not market_series.empty else ""
        daily_df, source_used = _get_daily_ohlcv(
            base_date=base_date,
            start_date=backtest_start_date,
            ticker=ticker,
            market=market,
        )
        if daily_df.empty:
            continue

        monthly_df = build_monthly_frame(daily_df)
        if monthly_df.empty:
            continue

        monthly_frames[ticker] = monthly_df
        backtest = run_backtest(monthly_df)
        ticker_mask = updated_df["종목코드"] == ticker
        row_indexes = updated_df.index[ticker_mask].tolist()
        for row_index in row_indexes:
            updated_df.at[row_index, "백테스팅 결과"] = backtest.summary
            updated_df.at[row_index, "백테스트 수익률"] = backtest.cumulative_return_pct
            updated_df.at[row_index, "MDD"] = backtest.mdd_pct
            updated_df.at[row_index, "CAGR"] = backtest.cagr_pct
            updated_df.at[row_index, "평균보유개월"] = backtest.average_hold_months
            updated_df.at[row_index, "매매 횟수"] = backtest.trade_count
            updated_df.at[row_index, "승률"] = backtest.win_rate_pct
            updated_df.at[row_index, "매매로그"] = backtest.trade_log
            updated_df.at[row_index, "가격데이터소스"] = source_used

    breakout_sort = pd.CategoricalDtype(["예", "아니오"], ordered=True)
    signal_sort = pd.CategoricalDtype(["돌파", "상단 유지", "하단 위치", "이탈"], ordered=True)
    if "월봉10개월선돌파여부" in updated_df.columns:
        updated_df["월봉10개월선돌파여부"] = updated_df["월봉10개월선돌파여부"].astype(breakout_sort)
    if "현재상태" in updated_df.columns:
        updated_df["현재상태"] = updated_df["현재상태"].astype(signal_sort)
    return updated_df


@st.cache_data(ttl=60 * 30, show_spinner=False)
def analyze_market(base_date: str, market: str, top_n: int) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
    global LAST_DATA_ERROR
    LAST_DATA_ERROR = None
    _reset_diagnostics()
    end_date = datetime.strptime(base_date, "%Y%m%d").date()
    start_date = end_date - timedelta(days=DEFAULT_LOOKBACK_DAYS)
    pool = get_market_cap_pool(base_date, market, top_n)

    results: list[AnalysisResult] = []
    monthly_frames: dict[str, pd.DataFrame] = {}

    pool_items = list(pool.itertuples(index=False))
    start_date_str = to_krx_date(start_date)
    worker_count = min(MAX_ANALYSIS_WORKERS, max(1, len(pool_items)))

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = [
            executor.submit(_analyze_single_ticker, item, start_date_str, base_date)
            for item in pool_items
        ]
        for future in as_completed(futures):
            result, monthly_df, source_used = future.result()
            if source_used and source_used != "none":
                _increment_ohlcv_source(source_used)
            if result is None or monthly_df is None:
                continue
            results.append(result)
            monthly_frames[result.ticker] = monthly_df

    frame = pd.DataFrame(
        [
            {
                "시장": item.market,
                "종목명": item.name,
                "종목코드": item.ticker,
                "현재가": item.close,
                "10개월선": item.ma10,
                "월봉10개월선돌파여부": "예" if item.breakout else "아니오",
                "현재상태": item.signal,
                "최근 돌파월": item.latest_breakout_date.strftime("%Y-%m") if item.latest_breakout_date is not None else "-",
                "돌파경과개월": item.months_since_breakout,
                "한달간 거래량": item.monthly_volume,
                "거래량 증감률": item.volume_change_pct,
                "백테스팅 결과": item.backtest_summary,
                "백테스트 수익률": item.backtest_return_pct,
                "MDD": item.backtest_mdd_pct,
                "CAGR": item.backtest_cagr_pct,
                "평균보유개월": item.average_hold_months,
                "매매 횟수": item.trade_count,
                "승률": item.win_rate_pct,
                "시가총액": item.market_cap,
                "가격데이터소스": "",
                "매매로그": [],
            }
            for item in results
        ]
    )

    if frame.empty:
        return frame, monthly_frames

    breakout_sort = pd.CategoricalDtype(["예", "아니오"], ordered=True)
    signal_sort = pd.CategoricalDtype(["돌파", "상단 유지", "하단 위치", "이탈"], ordered=True)
    frame["월봉10개월선돌파여부"] = frame["월봉10개월선돌파여부"].astype(breakout_sort)
    frame["현재상태"] = frame["현재상태"].astype(signal_sort)
    frame = frame.sort_values(
        ["월봉10개월선돌파여부", "백테스트 수익률", "시가총액"],
        ascending=[True, False, False],
    ).reset_index(drop=True)
    ticker_column = frame.columns[2] if len(frame.columns) > 2 else None
    if ticker_column is not None:
        frame["ma10_rising"] = frame[ticker_column].map(
            lambda ticker: _is_ma10_rising(monthly_frames[ticker]) if ticker in monthly_frames else False
        )
        frame["above_ma20"] = frame[ticker_column].map(
            lambda ticker: _is_above_ma20(monthly_frames[ticker]) if ticker in monthly_frames else False
        )
        frame["ma10_above_ma20"] = frame[ticker_column].map(
            lambda ticker: _is_ma10_above_ma20(monthly_frames[ticker]) if ticker in monthly_frames else False
        )
    return frame, monthly_frames


def apply_result_filters(
    results_df: pd.DataFrame,
    name_query: str,
    only_breakouts: bool,
    ma10_rising_only: bool,
    dual_trend_only: bool,
    volume_up_only: bool,
    min_backtest_return: float,
    breakout_within_months: int,
    sort_by: str,
    ascending: bool,
) -> pd.DataFrame:
    filtered_df = results_df.copy()
    columns = list(filtered_df.columns)
    name_col = columns[1] if len(columns) > 1 else None
    ticker_col = columns[2] if len(columns) > 2 else None
    price_col = columns[3] if len(columns) > 3 else None
    breakout_flag_col = columns[5] if len(columns) > 5 else None
    breakout_elapsed_col = columns[8] if len(columns) > 8 else None
    monthly_volume_col = columns[9] if len(columns) > 9 else None
    volume_change_col = columns[10] if len(columns) > 10 else None
    backtest_return_col = columns[12] if len(columns) > 12 else None
    market_cap_col = columns[18] if len(columns) > 18 else None

    sort_column_map = {
        "돌파경과개월": breakout_elapsed_col,
        "백테스트 수익률": backtest_return_col,
        "거래량 증감률": volume_change_col,
        "현재가": price_col,
        "전월거래량": monthly_volume_col,
        "시가총액": market_cap_col,
        "종목명": name_col,
    }
    actual_sort_by = sort_column_map.get(sort_by, breakout_elapsed_col or price_col or columns[0])
    has_backtest_values = bool(backtest_return_col and filtered_df[backtest_return_col].notna().any())

    if only_breakouts and breakout_flag_col:
        breakout_series = filtered_df[breakout_flag_col]
        if pd.api.types.is_categorical_dtype(breakout_series):
            yes_value = breakout_series.cat.categories[0]
        else:
            unique_values = [value for value in breakout_series.dropna().astype(str).unique().tolist() if value.strip()]
            yes_value = "예" if "예" in unique_values else (unique_values[0] if unique_values else None)
        if yes_value is not None:
            filtered_df = filtered_df[filtered_df[breakout_flag_col].astype(str) == str(yes_value)]

    query = name_query.strip()
    if query and name_col and ticker_col:
        filtered_df = filtered_df[
            filtered_df[name_col].astype(str).str.contains(query, case=False, na=False)
            | filtered_df[ticker_col].astype(str).str.contains(query, case=False, na=False)
        ]

    if ma10_rising_only and "ma10_rising" in filtered_df.columns:
        filtered_df = filtered_df[filtered_df["ma10_rising"].fillna(False)]

    if dual_trend_only and "above_ma20" in filtered_df.columns and "ma10_above_ma20" in filtered_df.columns:
        filtered_df = filtered_df[
            filtered_df["above_ma20"].fillna(False) & filtered_df["ma10_above_ma20"].fillna(False)
        ]

    if volume_up_only and volume_change_col:
        filtered_df = filtered_df[filtered_df[volume_change_col].fillna(float("-inf")) > 0]

    if has_backtest_values and backtest_return_col:
        filtered_df = filtered_df[filtered_df[backtest_return_col].fillna(float("-inf")) >= min_backtest_return]

    if breakout_within_months > 0 and breakout_elapsed_col:
        filtered_df = filtered_df[
            filtered_df[breakout_elapsed_col].notna() & (filtered_df[breakout_elapsed_col] <= breakout_within_months)
        ]

    return filtered_df.sort_values(actual_sort_by, ascending=ascending).reset_index(drop=True)


def get_last_data_error() -> str | None:
    return LAST_DATA_ERROR


def get_last_data_diagnostics() -> dict[str, object]:
    return LAST_DATA_DIAGNOSTICS.copy()
