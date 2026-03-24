from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta

import pandas as pd
import streamlit as st
from pykrx import stock

from .config import BACKTEST_BAR_LIMIT, BACKTEST_LOOKBACK_DAYS, DEFAULT_LOOKBACK_DAYS, MA_WINDOW, MAX_ANALYSIS_WORKERS
from .formatting import format_percent, to_krx_date

LAST_DATA_ERROR: str | None = None


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
    backtest_summary: str
    backtest_return_pct: float | None
    trade_count: int
    win_rate_pct: float | None
    market_cap: int


def _load_fdr():
    try:
        import FinanceDataReader as fdr
    except ImportError:
        return None
    return fdr


@st.cache_data(ttl=60 * 60 * 6, show_spinner=False)
def get_latest_business_day() -> str:
    today = datetime.now().date()
    for offset in range(15):
        current = today - timedelta(days=offset)
        try:
            market_cap = stock.get_market_cap_by_ticker(to_krx_date(current), market="KOSPI")
        except Exception:
            continue
        if not market_cap.empty:
            return to_krx_date(current)
    return to_krx_date(today)


@st.cache_data(ttl=60 * 60 * 6, show_spinner=False)
def get_market_cap_pool(base_date: str, market: str, top_n: int) -> pd.DataFrame:
    global LAST_DATA_ERROR
    fdr_pool = _get_market_cap_pool_from_fdr(market, top_n)
    if not fdr_pool.empty:
        return fdr_pool

    try:
        market_cap = stock.get_market_cap_by_ticker(base_date, market=market)
    except Exception as exc:
        LAST_DATA_ERROR = f"FDR 시총 풀 조회 실패 후 pykrx 시가총액 조회도 실패: {exc}"
        return pd.DataFrame(columns=["티커", "종목명", "시장", "시가총액"])

    required_columns = {"시가총액"}
    if market_cap.empty or not required_columns.issubset(set(market_cap.columns)):
        LAST_DATA_ERROR = "FDR 시총 풀 조회 실패 후 pykrx 시가총액 조회 응답에도 필요한 컬럼이 없습니다."
        return pd.DataFrame(columns=["티커", "종목명", "시장", "시가총액"])

    market_cap = market_cap.reset_index()
    if "티커" not in market_cap.columns:
        first_column = market_cap.columns[0]
        market_cap = market_cap.rename(columns={first_column: "티커"})
    market_cap["종목명"] = market_cap["티커"].apply(stock.get_market_ticker_name)
    market_cap["시장"] = market
    market_cap = market_cap.sort_values("시가총액", ascending=False).head(top_n)
    return market_cap[["티커", "종목명", "시장", "시가총액"]]


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
        try:
            listing = fdr.StockListing("KRX")
        except Exception as inner_exc:
            LAST_DATA_ERROR = f"{LAST_DATA_ERROR} / FDR KRX 조회 실패: {inner_exc}"
            return pd.DataFrame(columns=["티커", "종목명", "시장", "시가총액"])

    market_column = "Market" if "Market" in listing.columns else None
    marcap_column = "Marcap" if "Marcap" in listing.columns else "시가총액" if "시가총액" in listing.columns else None
    code_column = "Code" if "Code" in listing.columns else None
    name_column = "Name" if "Name" in listing.columns else None

    if not all([market_column, marcap_column, code_column, name_column]):
        LAST_DATA_ERROR = f"{LAST_DATA_ERROR} / FDR 종목목록 컬럼 부족: {list(listing.columns)}" if LAST_DATA_ERROR else f"FDR 종목목록 컬럼 부족: {list(listing.columns)}"
        return pd.DataFrame(columns=["티커", "종목명", "시장", "시가총액"])

    filtered = listing[listing[market_column].astype(str).str.upper() == market.upper()].copy()
    if filtered.empty:
        return pd.DataFrame(columns=["티커", "종목명", "시장", "시가총액"])

    filtered = filtered.sort_values(marcap_column, ascending=False).head(top_n)
    return pd.DataFrame(
        {
            "티커": filtered[code_column].astype(str).str.zfill(6),
            "종목명": filtered[name_column],
            "시장": market,
            "시가총액": filtered[marcap_column].fillna(0).astype("int64"),
        }
    )


def _normalize_daily_ohlcv(daily_df: pd.DataFrame) -> pd.DataFrame:
    if daily_df.empty:
        return daily_df

    df = daily_df.copy()
    column_map = {}
    if "Close" in df.columns:
        column_map["Close"] = "종가"
    if "Volume" in df.columns:
        column_map["Volume"] = "거래량"
    if column_map:
        df = df.rename(columns=column_map)

    required = {"종가", "거래량"}
    if not required.issubset(set(df.columns)):
        return pd.DataFrame(columns=["종가", "거래량"])

    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index)

    return df[["종가", "거래량"]].dropna()


@st.cache_data(ttl=60 * 60 * 24, show_spinner=False)
def _get_daily_ohlcv(base_date: str, start_date: str, ticker: str) -> pd.DataFrame:
    global LAST_DATA_ERROR
    try:
        daily_df = stock.get_market_ohlcv_by_date(
            fromdate=start_date,
            todate=base_date,
            ticker=ticker,
        )
        normalized = _normalize_daily_ohlcv(daily_df)
        if not normalized.empty:
            return normalized
    except Exception as exc:
        LAST_DATA_ERROR = f"pykrx 개별 종목 OHLCV 조회 실패({ticker}): {exc}"

    fdr = _load_fdr()
    if fdr is None:
        return pd.DataFrame(columns=["종가", "거래량"])

    try:
        start_dt = datetime.strptime(start_date, "%Y%m%d").date()
        end_dt = datetime.strptime(base_date, "%Y%m%d").date() + timedelta(days=1)
        daily_df = fdr.DataReader(ticker, start_dt, end_dt)
        normalized = _normalize_daily_ohlcv(daily_df)
        if not normalized.empty:
            return normalized
    except Exception as exc:
        LAST_DATA_ERROR = f"{LAST_DATA_ERROR} / FDR 개별 종목 OHLCV 조회 실패({ticker}): {exc}" if LAST_DATA_ERROR else f"FDR 개별 종목 OHLCV 조회 실패({ticker}): {exc}"

    return pd.DataFrame(columns=["종가", "거래량"])


def _analyze_single_ticker(item, start_date: str, base_date: str) -> tuple[AnalysisResult | None, pd.DataFrame | None]:
    daily_df = _get_daily_ohlcv(
        base_date=base_date,
        start_date=start_date,
        ticker=item.티커,
    )

    if daily_df.empty:
        return None, None

    monthly_df = build_monthly_frame(daily_df)
    if len(monthly_df) < 2:
        return None, None

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
        backtest_summary="미계산",
        backtest_return_pct=None,
        trade_count=0,
        win_rate_pct=None,
        market_cap=int(item.시가총액),
    )
    return result, monthly_df


def build_monthly_frame(daily_df: pd.DataFrame) -> pd.DataFrame:
    monthly = (
        daily_df[["종가", "거래량"]]
        .rename(columns={"종가": "close", "거래량": "volume"})
        .resample("M")
        .agg({"close": "last", "volume": "sum"})
        .dropna(subset=["close"])
    )
    monthly["ma10"] = monthly["close"].rolling(MA_WINDOW).mean()
    monthly["volume_change_pct"] = monthly["volume"].pct_change() * 100
    monthly["monthly_return_pct"] = monthly["close"].pct_change() * 100
    return monthly.dropna(subset=["ma10"])


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


def run_backtest(monthly_df: pd.DataFrame, limit: int = BACKTEST_BAR_LIMIT) -> tuple[str, float | None, int, float | None]:
    backtest_df = monthly_df.tail(limit).copy()
    if len(backtest_df) < 2:
        return "데이터 부족", None, 0, None

    in_position = False
    entry_price = 0.0
    equity = 1.0
    wins = 0
    trades = 0

    for row in backtest_df.itertuples():
        close = float(row.close)
        ma10 = float(row.ma10)

        if not in_position and close > ma10:
            in_position = True
            entry_price = close
            continue

        if in_position and close < ma10:
            trade_return = (close / entry_price) - 1
            equity *= 1 + trade_return
            trades += 1
            if trade_return > 0:
                wins += 1
            in_position = False

    if in_position:
        last_close = float(backtest_df.iloc[-1]["close"])
        trade_return = (last_close / entry_price) - 1
        equity *= 1 + trade_return
        trades += 1
        if trade_return > 0:
            wins += 1

    cumulative_return_pct = (equity - 1) * 100
    win_rate_pct = (wins / trades * 100) if trades else None
    summary = f"누적 {format_percent(cumulative_return_pct)}, 매매 {trades}회"
    if win_rate_pct is not None:
        summary += f", 승률 {format_percent(win_rate_pct)}"
    return summary, cumulative_return_pct, trades, win_rate_pct


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
        daily_df = _get_daily_ohlcv(
            base_date=base_date,
            start_date=backtest_start_date,
            ticker=ticker,
        )
        if daily_df.empty:
            continue

        monthly_df = build_monthly_frame(daily_df)
        if monthly_df.empty:
            continue

        monthly_frames[ticker] = monthly_df
        backtest_summary, backtest_return_pct, trade_count, win_rate_pct = run_backtest(monthly_df)
        ticker_mask = updated_df["종목코드"] == ticker
        updated_df.loc[ticker_mask, "백테스팅 결과"] = backtest_summary
        updated_df.loc[ticker_mask, "백테스트 수익률"] = backtest_return_pct
        updated_df.loc[ticker_mask, "매매 횟수"] = trade_count
        updated_df.loc[ticker_mask, "승률"] = win_rate_pct

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
            result, monthly_df = future.result()
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
                "매매 횟수": item.trade_count,
                "승률": item.win_rate_pct,
                "시가총액": item.market_cap,
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
    return frame, monthly_frames


def apply_result_filters(
    results_df: pd.DataFrame,
    name_query: str,
    only_breakouts: bool,
    volume_up_only: bool,
    min_backtest_return: float,
    breakout_within_months: int,
    sort_by: str,
    ascending: bool,
) -> pd.DataFrame:
    filtered_df = results_df.copy()
    has_backtest_values = filtered_df["백테스트 수익률"].notna().any() if "백테스트 수익률" in filtered_df.columns else False

    if only_breakouts:
        filtered_df = filtered_df[filtered_df["월봉10개월선돌파여부"] == "예"]

    query = name_query.strip()
    if query:
        filtered_df = filtered_df[
            filtered_df["종목명"].str.contains(query, case=False, na=False)
            | filtered_df["종목코드"].str.contains(query, case=False, na=False)
        ]

    if volume_up_only:
        filtered_df = filtered_df[filtered_df["거래량 증감률"].fillna(float("-inf")) > 0]

    if has_backtest_values:
        filtered_df = filtered_df[filtered_df["백테스트 수익률"].fillna(float("-inf")) >= min_backtest_return]

    if breakout_within_months > 0:
        filtered_df = filtered_df[
            filtered_df["돌파경과개월"].notna() & (filtered_df["돌파경과개월"] <= breakout_within_months)
        ]

    return filtered_df.sort_values(sort_by, ascending=ascending).reset_index(drop=True)


def get_last_data_error() -> str | None:
    return LAST_DATA_ERROR
