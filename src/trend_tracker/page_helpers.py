from __future__ import annotations

from datetime import datetime

import pandas as pd
import streamlit as st

from .analysis import (
    analyze_market,
    analyze_weekly_market,
    apply_result_filters,
    enrich_results_with_backtests,
    evaluate_signal,
    get_cnn_fear_greed_snapshot,
    get_last_data_diagnostics,
    get_last_data_error,
    get_latest_business_day,
    get_market_index_snapshots,
)
from .charts import create_monthly_chart, create_weekly_chart
from .config import DEFAULT_TOP_N, MARKET_OPTIONS, get_telegram_chat_id
from .formatting import format_number, format_percent, to_krx_date
from .notifications import build_telegram_message, send_telegram_message


SESSION_RESULTS_KEY = "results_df"
SESSION_FRAMES_KEY = "monthly_frames"
SESSION_MARKET_KEY = "screen_market"
SESSION_DATE_KEY = "screen_base_date"
SESSION_BASE_DATE_INPUT_KEY = "base_date_input"
WEEKLY_SESSION_RESULTS_KEY = "weekly_results_df"
WEEKLY_SESSION_FRAMES_KEY = "weekly_frames"
WEEKLY_SESSION_MARKET_KEY = "weekly_screen_market"
WEEKLY_SESSION_DATE_KEY = "weekly_screen_base_date"
WEEKLY_SESSION_BASE_DATE_INPUT_KEY = "weekly_base_date_input"
WEEKLY_SESSION_MAX_SPREAD_KEY = "weekly_max_spread_pct"
WEEKLY_SESSION_MIN_VOLUME_MULTIPLE_KEY = "weekly_min_volume_multiple"
SESSION_DATA_DIAGNOSTICS_KEY = "last_data_diagnostics"


def _set_today_base_date(latest_business_day) -> None:
    st.session_state[SESSION_BASE_DATE_INPUT_KEY] = latest_business_day


def _set_today_weekly_base_date(latest_business_day) -> None:
    st.session_state[WEEKLY_SESSION_BASE_DATE_INPUT_KEY] = latest_business_day


def _run_default_screening_query() -> None:
    latest_business_day = datetime.strptime(get_latest_business_day(), "%Y%m%d").date()
    market_label = next(iter(MARKET_OPTIONS.keys()))
    results_df, monthly_frames = analyze_market(
        base_date=to_krx_date(latest_business_day),
        market=MARKET_OPTIONS[market_label],
        top_n=DEFAULT_TOP_N,
    )
    st.session_state[SESSION_RESULTS_KEY] = results_df
    st.session_state[SESSION_FRAMES_KEY] = monthly_frames
    st.session_state[SESSION_MARKET_KEY] = market_label
    st.session_state[SESSION_DATE_KEY] = to_krx_date(latest_business_day)
    st.session_state["last_data_error"] = get_last_data_error()
    st.session_state[SESSION_DATA_DIAGNOSTICS_KEY] = get_last_data_diagnostics()


class PageLoadingOverlay:
    def __init__(self, message: str = "페이지를 불러오고 있습니다...", progress: int = 10):
        self.placeholder = st.empty()
        self.update(message=message, progress=progress)

    def update(self, message: str, progress: int) -> None:
        safe_progress = max(0, min(100, int(progress)))
        self.placeholder.markdown(
            f"""
            <style>
            .page-loading-wrap {{
                position: fixed;
                top: 0;
                left: 0;
                width: 100%;
                z-index: 999998;
                pointer-events: none;
            }}
            .page-loading-bar {{
                height: 6px;
                width: 100%;
                overflow: hidden;
                background: rgba(37, 99, 235, 0.12);
            }}
            .page-loading-fill {{
                height: 100%;
                width: {safe_progress}%;
                background: linear-gradient(90deg, #2563eb 0%, #60a5fa 100%);
                transition: width 0.25s ease;
            }}
            .page-loading-label {{
                position: fixed;
                top: 10px;
                right: 16px;
                background: rgba(15, 23, 42, 0.88);
                color: white;
                font-size: 12px;
                padding: 8px 12px;
                border-radius: 999px;
                z-index: 999999;
                display: flex;
                gap: 8px;
                align-items: center;
            }}
            .page-loading-percent {{
                font-weight: 700;
                color: #93c5fd;
            }}
            </style>
            <div class="page-loading-wrap">
                <div class="page-loading-bar">
                    <div class="page-loading-fill"></div>
                </div>
            </div>
            <div class="page-loading-label">
                <span>{message}</span>
                <span class="page-loading-percent">{safe_progress}%</span>
            </div>
            """,
            unsafe_allow_html=True,
        )

    def empty(self) -> None:
        self.placeholder.empty()


def show_page_loading_bar(message: str = "페이지를 불러오고 있습니다...", progress: int = 10) -> PageLoadingOverlay:
    return PageLoadingOverlay(message=message, progress=progress)


def render_execution_rule_badge() -> None:
    st.markdown(
        """
        <div style="
            margin: 8px 0 18px 0;
            padding: 12px 14px;
            border-radius: 14px;
            border: 1px solid rgba(14, 165, 233, 0.28);
            background: linear-gradient(135deg, rgba(14, 165, 233, 0.14), rgba(59, 130, 246, 0.08));
            display: inline-flex;
            align-items: center;
            gap: 10px;
            font-weight: 700;
            color: #e0f2fe;">
            <span style="
                padding: 4px 10px;
                border-radius: 999px;
                background: rgba(14, 165, 233, 0.18);
                color: #7dd3fc;
                font-size: 12px;
                letter-spacing: 0.02em;">매매 기준</span>
            <span>당월 종가 확정</span>
            <span style="opacity:0.7;">-&gt;</span>
            <span>익월 시가 체결</span>
        </div>
        """,
        unsafe_allow_html=True,
    )

def _show_loading_modal(message: str = "데이터를 조회하고 있습니다...") -> st.delta_generator.DeltaGenerator:
    placeholder = st.empty()
    placeholder.markdown(
        f"""
        <style>
        .loading-overlay {{
            position: fixed;
            inset: 0;
            background: rgba(15, 23, 42, 0.45);
            display: flex;
            align-items: center;
            justify-content: center;
            z-index: 999999;
        }}
        .loading-modal {{
            background: white;
            padding: 28px 32px;
            border-radius: 16px;
            box-shadow: 0 20px 60px rgba(15, 23, 42, 0.25);
            min-width: 320px;
            text-align: center;
            font-weight: 600;
        }}
        .loading-spinner {{
            width: 48px;
            height: 48px;
            margin: 0 auto 16px auto;
            border: 5px solid #e5e7eb;
            border-top: 5px solid #2563eb;
            border-radius: 50%;
            animation: loading-spin 0.9s linear infinite;
        }}
        @keyframes loading-spin {{
            0% {{ transform: rotate(0deg); }}
            100% {{ transform: rotate(360deg); }}
        }}
        </style>
        <div class="loading-overlay">
            <div class="loading-modal">
                <div class="loading-spinner"></div>
                <div>{message}</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    return placeholder


def render_query_sidebar() -> None:
    with st.sidebar:
        st.header("조회 설정")
        latest_business_day = datetime.strptime(get_latest_business_day(), "%Y%m%d").date()
        if SESSION_BASE_DATE_INPUT_KEY not in st.session_state:
            st.session_state[SESSION_BASE_DATE_INPUT_KEY] = latest_business_day

        date_col, today_col = st.columns([3, 1])
        with date_col:
            base_date = st.date_input(
                "기준일자",
                max_value=latest_business_day,
                key=SESSION_BASE_DATE_INPUT_KEY,
            )
        with today_col:
            st.write("")
            st.write("")
            st.button(
                "Today",
                use_container_width=True,
                on_click=_set_today_base_date,
                args=(latest_business_day,),
            )

        market_label = st.selectbox("시장", list(MARKET_OPTIONS.keys()), index=0)
        top_n = st.slider("대상 종목 수", min_value=30, max_value=300, value=DEFAULT_TOP_N, step=10)
        query_button = st.button("조회", type="primary", use_container_width=True)

    if query_button:
        loading_modal = _show_loading_modal("스크리닝 데이터를 조회하고 있습니다...")
        try:
            with st.spinner("스크리닝 데이터를 조회하고 있습니다..."):
                results_df, monthly_frames = analyze_market(
                    base_date=to_krx_date(base_date),
                    market=MARKET_OPTIONS[market_label],
                    top_n=top_n,
                )
        finally:
            loading_modal.empty()

        st.session_state[SESSION_RESULTS_KEY] = results_df
        st.session_state[SESSION_FRAMES_KEY] = monthly_frames
        st.session_state[SESSION_MARKET_KEY] = market_label
        st.session_state[SESSION_DATE_KEY] = to_krx_date(base_date)
        st.session_state["last_data_error"] = get_last_data_error()
        st.session_state[SESSION_DATA_DIAGNOSTICS_KEY] = get_last_data_diagnostics()


def get_session_results() -> tuple[pd.DataFrame | None, dict[str, pd.DataFrame], str | None, str | None]:
    return (
        st.session_state.get(SESSION_RESULTS_KEY),
        st.session_state.get(SESSION_FRAMES_KEY, {}),
        st.session_state.get(SESSION_MARKET_KEY),
        st.session_state.get(SESSION_DATE_KEY),
    )


def ensure_default_screening_results() -> tuple[pd.DataFrame | None, dict[str, pd.DataFrame], str | None, str | None]:
    results = get_session_results()
    results_df, _, _, _ = results
    if results_df is None or results_df.empty:
        _run_default_screening_query()
        results = get_session_results()
    return results


def render_weekly_query_sidebar() -> None:
    with st.sidebar:
        st.header("주봉 조회 설정")
        latest_business_day = datetime.strptime(get_latest_business_day(), "%Y%m%d").date()
        if WEEKLY_SESSION_BASE_DATE_INPUT_KEY not in st.session_state:
            st.session_state[WEEKLY_SESSION_BASE_DATE_INPUT_KEY] = latest_business_day
        if WEEKLY_SESSION_MAX_SPREAD_KEY not in st.session_state:
            st.session_state[WEEKLY_SESSION_MAX_SPREAD_KEY] = 10.0
        if WEEKLY_SESSION_MIN_VOLUME_MULTIPLE_KEY not in st.session_state:
            st.session_state[WEEKLY_SESSION_MIN_VOLUME_MULTIPLE_KEY] = 1.5

        date_col, today_col = st.columns([3, 1])
        with date_col:
            base_date = st.date_input(
                "기준일자",
                max_value=latest_business_day,
                key=WEEKLY_SESSION_BASE_DATE_INPUT_KEY,
            )
        with today_col:
            st.write("")
            st.write("")
            st.button(
                "Today",
                use_container_width=True,
                on_click=_set_today_weekly_base_date,
                args=(latest_business_day,),
                key="weekly_today_button",
            )

        market_label = st.selectbox("시장", list(MARKET_OPTIONS.keys()), index=0, key="weekly_market_label")
        top_n = st.slider("대상 종목 수", min_value=30, max_value=300, value=DEFAULT_TOP_N, step=10, key="weekly_top_n")
        max_spread_pct = st.slider("이평선 이격 최대값(%)", min_value=5.0, max_value=10.0, value=float(st.session_state[WEEKLY_SESSION_MAX_SPREAD_KEY]), step=0.5)
        min_volume_multiple = st.slider("거래량 배수 최소값", min_value=1.5, max_value=2.0, value=float(st.session_state[WEEKLY_SESSION_MIN_VOLUME_MULTIPLE_KEY]), step=0.1)
        query_button = st.button("주봉 조건 조회", type="primary", use_container_width=True)

    st.session_state[WEEKLY_SESSION_MAX_SPREAD_KEY] = max_spread_pct
    st.session_state[WEEKLY_SESSION_MIN_VOLUME_MULTIPLE_KEY] = min_volume_multiple

    if query_button:
        loading_modal = _show_loading_modal("주봉 조건 종목을 조회하고 있습니다...")
        try:
            with st.spinner("주봉 조건 종목을 조회하고 있습니다..."):
                results_df, weekly_frames = analyze_weekly_market(
                    base_date=to_krx_date(base_date),
                    market=MARKET_OPTIONS[market_label],
                    top_n=top_n,
                    max_ma_spread_pct=max_spread_pct,
                    min_volume_multiple=min_volume_multiple,
                )
        finally:
            loading_modal.empty()

        st.session_state[WEEKLY_SESSION_RESULTS_KEY] = results_df
        st.session_state[WEEKLY_SESSION_FRAMES_KEY] = weekly_frames
        st.session_state[WEEKLY_SESSION_MARKET_KEY] = market_label
        st.session_state[WEEKLY_SESSION_DATE_KEY] = to_krx_date(base_date)
        st.session_state["last_data_error"] = get_last_data_error()
        st.session_state[SESSION_DATA_DIAGNOSTICS_KEY] = get_last_data_diagnostics()


def get_weekly_session_results() -> tuple[pd.DataFrame | None, dict[str, pd.DataFrame], str | None, str | None]:
    return (
        st.session_state.get(WEEKLY_SESSION_RESULTS_KEY),
        st.session_state.get(WEEKLY_SESSION_FRAMES_KEY, {}),
        st.session_state.get(WEEKLY_SESSION_MARKET_KEY),
        st.session_state.get(WEEKLY_SESSION_DATE_KEY),
    )


def _build_source_badges_html() -> str:
    diagnostics = st.session_state.get(SESSION_DATA_DIAGNOSTICS_KEY, {})
    if not diagnostics:
        return ""

    pool_source = diagnostics.get("pool_source") or "미확인"
    pool_fallbacks = diagnostics.get("pool_fallbacks") or []
    ohlcv_sources = diagnostics.get("ohlcv_sources") or {}

    badges = []
    badges.append(("종목풀", str(pool_source), "#0f766e"))
    for source_name, count in ohlcv_sources.items():
        color = "#0f766e" if "KIS" in source_name else "#1d4ed8" if "FinanceDataReader" in source_name else "#7c3aed" if "pykrx" in source_name else "#475569"
        badges.append(("가격", f"{source_name} {count}건", color))
    if pool_fallbacks:
        badges.append(("fallback", "적용됨", "#b45309"))
    else:
        badges.append(("fallback", "없음", "#166534"))

    parts = []
    for label, value, color in badges:
        parts.append(
            f"""
            <span style="
                display:inline-flex;
                align-items:center;
                gap:6px;
                margin:4px 8px 4px 0;
                padding:6px 10px;
                border-radius:999px;
                background:{color};
                color:white;
                font-size:12px;
                font-weight:600;">
                <span style="opacity:0.78;">{label}</span>
                <span>{value}</span>
            </span>
            """
        )
    return "".join(parts)


def render_data_source_badges() -> None:
    badges_html = _build_source_badges_html()
    if not badges_html:
        return
    st.markdown(badges_html, unsafe_allow_html=True)


def render_empty_state(results_df: pd.DataFrame | None) -> bool:
    if results_df is None:
        st.info("왼쪽 사이드바에서 시장과 기준일을 고른 뒤 `조회` 버튼을 눌러주세요.")
        if st.button("기본값으로 바로 조회", type="primary", use_container_width=True):
            with st.spinner("기본값으로 스크리닝을 조회하고 있습니다..."):
                _run_default_screening_query()
            st.rerun()
        return True
    if results_df.empty:
        st.warning("조회 결과가 없습니다. 기준일자나 시장을 바꿔 다시 시도해주세요.")
        last_error = st.session_state.get("last_data_error")
        if last_error:
            st.error(f"데이터 진단: {last_error}")
        render_data_source_diagnostics()
        return True
    return False


def render_weekly_empty_state(results_df: pd.DataFrame | None) -> bool:
    if results_df is None:
        st.info("왼쪽 사이드바에서 시장과 기준일을 고른 뒤 `주봉 조건 조회` 버튼을 눌러주세요.")
        return True
    if results_df.empty:
        st.warning("주봉 조건 분석 결과가 없습니다. 기준일자나 시장을 바꿔 다시 시도해주세요.")
        last_error = st.session_state.get("last_data_error")
        if last_error:
            st.error(f"데이터 진단: {last_error}")
        render_data_source_diagnostics()
        return True
    return False


def render_summary_metrics(results_df: pd.DataFrame) -> None:
    breakout_count = int((results_df["월봉10개월선돌파여부"] == "예").sum())
    hold_count = int((results_df["현재상태"] == "상단 유지").sum())
    below_count = int((results_df["현재상태"] == "하단 위치").sum())
    exit_count = int((results_df["현재상태"] == "이탈").sum())

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("돌파 종목", breakout_count)
    col2.metric("상단 유지", hold_count)
    col3.metric("하단 위치", below_count)
    col4.metric("이탈", exit_count)


def _compute_market_dashboard(results_df: pd.DataFrame, monthly_frames: dict[str, pd.DataFrame]) -> dict[str, object]:
    total = len(results_df)
    if total == 0:
        return {
            "above_ratio": 0.0,
            "breakout_ratio": 0.0,
            "exit_count": 0,
            "exit_delta": 0,
            "fear_greed_score": 0,
            "fear_greed_label": "데이터 없음",
            "market_label": "판단 불가",
        }

    above_count = int(results_df["현재상태"].isin(["돌파", "상단 유지"]).sum())
    breakout_count = int((results_df["월봉10개월선돌파여부"] == "예").sum())
    exit_count = int((results_df["현재상태"] == "이탈").sum())

    previous_exit_count = 0
    for monthly_df in monthly_frames.values():
        if monthly_df is None or len(monthly_df) < 3:
            continue
        _, previous_signal = evaluate_signal(monthly_df.iloc[:-1])
        if previous_signal == "이탈":
            previous_exit_count += 1

    exit_delta = exit_count - previous_exit_count
    above_ratio = above_count / total * 100
    breakout_ratio = breakout_count / total * 100
    exit_ratio = exit_count / total * 100

    score = 50
    score += (above_ratio - 50) * 0.6
    score += breakout_ratio * 0.8
    score -= exit_ratio * 0.7
    score = int(max(0, min(100, round(score))))

    if score >= 80:
        fear_greed_label = "극단적 탐욕"
    elif score >= 60:
        fear_greed_label = "탐욕"
    elif score >= 40:
        fear_greed_label = "중립"
    elif score >= 20:
        fear_greed_label = "공포"
    else:
        fear_greed_label = "극단적 공포"

    if above_ratio >= 65:
        market_label = "강세 우위"
    elif above_ratio >= 45:
        market_label = "중립"
    else:
        market_label = "약세 우위"

    return {
        "above_ratio": above_ratio,
        "breakout_ratio": breakout_ratio,
        "exit_count": exit_count,
        "exit_delta": exit_delta,
        "fear_greed_score": score,
        "fear_greed_label": fear_greed_label,
        "market_label": market_label,
    }


def render_market_dashboard(results_df: pd.DataFrame, monthly_frames: dict[str, pd.DataFrame]) -> None:
    dashboard = _compute_market_dashboard(results_df, monthly_frames)

    st.subheader("시장 온도계")
    st.caption("현재 조회된 시장 전체를 기준으로 10개월선 강도와 내부 심리를 요약합니다.")

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("시장 상태", dashboard["market_label"], f"상단 비율 {dashboard['above_ratio']:.1f}%")
    col2.metric("돌파 종목 비율", f"{dashboard['breakout_ratio']:.1f}%")
    col3.metric("이탈 종목 수", dashboard["exit_count"], f"{dashboard['exit_delta']:+d} vs 전월")
    col4.metric("공포탐욕 지수", f"{dashboard['fear_greed_score']}", dashboard["fear_greed_label"])

    score = dashboard["fear_greed_score"]
    if score >= 80:
        gauge_color = "#b91c1c"
    elif score >= 60:
        gauge_color = "#ea580c"
    elif score >= 40:
        gauge_color = "#2563eb"
    elif score >= 20:
        gauge_color = "#0891b2"
    else:
        gauge_color = "#1d4ed8"

    st.markdown(
        f"""
        <div style="margin: 8px 0 18px 0; padding: 14px 16px; border: 1px solid rgba(148,163,184,0.25); border-radius: 14px;">
            <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:8px;">
                <strong>시장 공포탐욕 지수</strong>
                <span style="font-weight:700; color:{gauge_color};">{score} / 100 · {dashboard['fear_greed_label']}</span>
            </div>
            <div style="height:12px; background:rgba(148,163,184,0.18); border-radius:999px; overflow:hidden;">
                <div style="width:{score}%; height:100%; background:{gauge_color};"></div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_market_index_overview() -> None:
    index_snapshots = get_market_index_snapshots()
    if not index_snapshots:
        return

    st.subheader("주요 지수")
    index_columns = st.columns(len(index_snapshots))
    for column, snapshot in zip(index_columns, index_snapshots):
        value = format_number(snapshot["value"], digits=2)
        delta = format_percent(snapshot["change_pct"])
        column.metric(snapshot["label"], value, delta)


def render_cnn_fear_greed_card() -> None:
    snapshot = get_cnn_fear_greed_snapshot()
    if not snapshot:
        return

    score = float(snapshot["score"])
    previous_close = snapshot.get("previous_close")
    delta = score - float(previous_close) if previous_close is not None else None

    if score >= 75:
        color = "#b91c1c"
    elif score >= 55:
        color = "#ea580c"
    elif score >= 45:
        color = "#475569"
    elif score >= 25:
        color = "#2563eb"
    else:
        color = "#be123c"

    timestamp = str(snapshot.get("timestamp") or "").strip()
    caption = f"CNN Fear & Greed Index | {timestamp}" if timestamp else "CNN Fear & Greed Index"

    st.subheader("CNN 공포탐욕지수")
    metric_col, text_col = st.columns([1, 2])
    metric_col.metric("CNN Score", f"{score:.0f}", None if delta is None else f"{delta:+.0f} vs prev")
    text_col.markdown(
        f"""
        <div style="margin-top: 8px; padding: 16px; border-radius: 14px; border: 1px solid rgba(148,163,184,0.25);">
            <div style="font-size: 12px; color: #64748b; margin-bottom: 8px;">{caption}</div>
            <div style="font-size: 20px; font-weight: 700; color: {color}; margin-bottom: 10px;">{snapshot['rating']}</div>
            <div style="height: 12px; background: rgba(148,163,184,0.18); border-radius: 999px; overflow: hidden;">
                <div style="width: {score}%; height: 100%; background: {color};"></div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_filter_controls(
    results_df: pd.DataFrame,
    default_sort_by: str = "돌파경과개월",
    key_prefix: str = "default",
) -> pd.DataFrame:
    filter_col1, filter_col2 = st.columns([1, 1])
    with filter_col1:
        only_breakouts = st.checkbox("돌파 종목만 보기", value=True, key=f"{key_prefix}_only_breakouts")
    with filter_col2:
        name_query = st.text_input("종목명/코드 검색", value="", key=f"{key_prefix}_name_query")

    sort_col1, sort_col2, sort_col3 = st.columns([1, 1, 1])
    sort_options = ["돌파경과개월", "백테스트 수익률", "거래량 증감률", "현재가", "전월거래량", "시가총액", "종목명"]
    default_index = sort_options.index(default_sort_by) if default_sort_by in sort_options else 0
    with sort_col1:
        sort_by = st.selectbox("정렬 기준", sort_options, index=default_index, key=f"{key_prefix}_sort_by")
    with sort_col2:
        sort_direction = st.selectbox("정렬 방향", ["오름차순", "내림차순"], index=0, key=f"{key_prefix}_sort_direction")
    with sort_col3:
        breakout_within_months = st.selectbox(
            "최근 돌파 기준",
            [0, 1, 3, 6, 12],
            index=2,
            format_func=lambda value: "전체" if value == 0 else f"{value}개월 이내",
            key=f"{key_prefix}_breakout_within_months",
        )

    advanced_col1, advanced_col2 = st.columns([1, 1])
    with advanced_col1:
        volume_up_only = st.checkbox("거래량 증가 종목만", value=False, key=f"{key_prefix}_volume_up_only")
    with advanced_col2:
        min_backtest_return = st.number_input(
            "백테스트 수익률 최소값(%)",
            value=0.0,
            step=5.0,
            key=f"{key_prefix}_min_backtest_return",
        )

    trend_col1, trend_col2 = st.columns([1, 1])
    with trend_col1:
        ma10_rising_only = st.checkbox("MA10 상승 종목만", value=False, key=f"{key_prefix}_ma10_rising_only")
    with trend_col2:
        dual_trend_only = st.checkbox("장기 추세 이중 확인", value=False, key=f"{key_prefix}_dual_trend_only")

    return apply_result_filters(
        results_df=results_df,
        name_query=name_query,
        only_breakouts=only_breakouts,
        ma10_rising_only=ma10_rising_only,
        dual_trend_only=dual_trend_only,
        volume_up_only=volume_up_only,
        min_backtest_return=min_backtest_return,
        breakout_within_months=breakout_within_months,
        sort_by=sort_by,
        ascending=sort_direction == "오름차순",
    )


def render_screening_table(filtered_df: pd.DataFrame, results_df: pd.DataFrame) -> None:
    st.subheader("스크리닝 결과")
    st.caption(f"현재 표시 종목 수 {len(filtered_df)} / 전체 조회 종목 수 {len(results_df)}")
    render_data_source_badges()

    display_df = _format_common_display_df(filtered_df)
    if display_df.empty:
        st.warning("현재 필터 조건에 맞는 종목이 없습니다.")
        return

    csv_df = filtered_df.copy()
    if len(csv_df.columns) > 7:
        csv_df.iloc[:, 7] = csv_df.iloc[:, 7].fillna("-")

    st.download_button(
        "스크리닝 결과 CSV 다운로드",
        data=csv_df.to_csv(index=False, encoding="utf-8-sig"),
        file_name="screening_results.csv",
        mime="text/csv",
        use_container_width=True,
    )

    visible_columns = [column for column in [
        display_df.columns[0] if len(display_df.columns) > 0 else None,
        display_df.columns[1] if len(display_df.columns) > 1 else None,
        display_df.columns[2] if len(display_df.columns) > 2 else None,
        display_df.columns[6] if len(display_df.columns) > 6 else None,
        display_df.columns[3] if len(display_df.columns) > 3 else None,
        display_df.columns[5] if len(display_df.columns) > 5 else None,
        display_df.columns[7] if len(display_df.columns) > 7 else None,
        display_df.columns[8] if len(display_df.columns) > 8 else None,
        display_df.columns[9] if len(display_df.columns) > 9 else None,
        display_df.columns[10] if len(display_df.columns) > 10 else None,
        "ma10_rising_label",
        "dual_trend_label",
    ] if column and column in display_df.columns]

    st.dataframe(
        display_df[visible_columns],
        use_container_width=True,
        hide_index=True,
        column_config={
            display_df.columns[1] if len(display_df.columns) > 1 else "": st.column_config.TextColumn(width="medium"),
            display_df.columns[2] if len(display_df.columns) > 2 else "": st.column_config.TextColumn(width="small"),
            display_df.columns[6] if len(display_df.columns) > 6 else "": st.column_config.TextColumn(width="small"),
            display_df.columns[5] if len(display_df.columns) > 5 else "": st.column_config.TextColumn(label="돌파"),
            display_df.columns[7] if len(display_df.columns) > 7 else "": st.column_config.TextColumn(width="small"),
            display_df.columns[8] if len(display_df.columns) > 8 else "": st.column_config.NumberColumn(format="%d"),
            "ma10_rising_label": st.column_config.TextColumn(label="MA10 상승", width="small"),
            "dual_trend_label": st.column_config.TextColumn(label="장기추세", width="small"),
        },
    )


def render_weekly_summary_metrics(results_df: pd.DataFrame) -> None:
    setup_count = int((results_df["최종조건충족"].astype(str) == "예").sum())
    dense_count = int((results_df["밀집조건"].astype(str) == "예").sum())
    breakout_count = int((results_df["돌파조건"].astype(str) == "예").sum())
    volume_count = int((results_df["거래량조건"].astype(str) == "예").sum())

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("최종 조건 충족", setup_count)
    col2.metric("이평선 밀집", dense_count)
    col3.metric("20·40주 돌파", breakout_count)
    col4.metric("거래량 급증", volume_count)


def render_weekly_filter_controls(results_df: pd.DataFrame) -> pd.DataFrame:
    filter_col1, filter_col2 = st.columns([1, 1])
    with filter_col1:
        only_setups = st.checkbox("최종 조건 충족 종목만 보기", value=True, key="weekly_only_setups")
    with filter_col2:
        name_query = st.text_input("종목명/코드 검색", value="", key="weekly_name_query")

    sort_col1, sort_col2 = st.columns([1, 1])
    with sort_col1:
        sort_by = st.selectbox("정렬 기준", ["거래량배수", "이평선이격률", "시가총액", "현재가", "종목명"], index=0, key="weekly_sort_by")
    with sort_col2:
        sort_direction = st.selectbox("정렬 방향", ["내림차순", "오름차순"], index=0, key="weekly_sort_direction")

    filtered_df = results_df.copy()
    if only_setups:
        filtered_df = filtered_df[filtered_df["최종조건충족"].astype(str) == "예"]

    query = name_query.strip()
    if query:
        filtered_df = filtered_df[
            filtered_df["종목명"].astype(str).str.contains(query, case=False, na=False)
            | filtered_df["종목코드"].astype(str).str.contains(query, case=False, na=False)
        ]

    ascending = sort_direction == "오름차순"
    filtered_df = filtered_df.sort_values(sort_by, ascending=ascending).reset_index(drop=True)
    return filtered_df


def render_weekly_screening_table(filtered_df: pd.DataFrame, results_df: pd.DataFrame) -> None:
    st.subheader("주봉 조건 검색 결과")
    st.caption(f"현재 표시 종목 수 {len(filtered_df)} / 전체 분석 종목 수 {len(results_df)}")
    render_data_source_badges()

    if filtered_df.empty:
        st.warning("현재 주봉 조건에 맞는 종목이 없습니다.")
        return

    display_df = filtered_df.copy()
    for column in ["현재가", "10주선", "20주선", "40주선", "거래량", "10주평균거래량", "시가총액"]:
        if column in display_df.columns:
            display_df[column] = display_df[column].map(format_number)
    for column in ["이평선이격률", "거래량배수"]:
        if column in display_df.columns:
            display_df[column] = display_df[column].map(lambda value: "미계산" if pd.isna(value) else f"{float(value):.2f}")

    st.download_button(
        "주봉 검색 결과 CSV 다운로드",
        data=filtered_df.to_csv(index=False, encoding="utf-8-sig"),
        file_name="weekly_screening_results.csv",
        mime="text/csv",
        use_container_width=True,
    )

    st.dataframe(
        display_df[
            [
                "시장",
                "종목명",
                "종목코드",
                "현재가",
                "10주선",
                "20주선",
                "40주선",
                "이평선이격률",
                "거래량배수",
                "밀집조건",
                "돌파조건",
                "거래량조건",
                "최종조건충족",
                "기준주",
            ]
        ],
        use_container_width=True,
        hide_index=True,
        column_config={
            "종목명": st.column_config.TextColumn(width="medium"),
            "종목코드": st.column_config.TextColumn(width="small"),
            "최종조건충족": st.column_config.TextColumn(label="최종 조건", width="small"),
        },
    )


def render_backtest_table(filtered_df: pd.DataFrame, results_df: pd.DataFrame) -> None:
    st.subheader("백테스트 결과")
    st.caption(f"현재 표시 종목 수 {len(filtered_df)} / 전체 조회 종목 수 {len(results_df)}")

    display_df = _format_common_display_df(filtered_df)
    if display_df.empty:
        st.warning("현재 필터 조건에 맞는 종목이 없습니다.")
        return

    action_col1, action_col2 = st.columns(2)
    with action_col1:
        st.download_button(
            "백테스트 결과 CSV 다운로드",
            data=filtered_df.copy().to_csv(index=False, encoding="utf-8-sig"),
            file_name="backtest_results.csv",
            mime="text/csv",
            use_container_width=True,
        )
    with action_col2:
        trade_log_csv = _build_trade_log_csv(filtered_df)
        if trade_log_csv is not None:
            st.download_button(
                "매매 로그 CSV 다운로드",
                data=trade_log_csv,
                file_name="backtest_trade_logs.csv",
                mime="text/csv",
                use_container_width=True,
            )

    st.dataframe(
        display_df[
            [
                "시장",
                "종목명",
                "종목코드",
                "백테스팅 결과",
                "백테스트 수익률",
                "MDD",
                "CAGR",
                "평균보유개월",
                "매매 횟수",
                "승률",
            ]
        ],
        use_container_width=True,
        hide_index=True,
        column_config={
            "종목명": st.column_config.TextColumn(width="medium"),
            "종목코드": st.column_config.TextColumn(width="small"),
            "백테스팅 결과": st.column_config.TextColumn(width="large"),
        },
    )


def run_manual_backtest_for_filtered(
    filtered_df: pd.DataFrame,
    monthly_frames: dict[str, pd.DataFrame],
    screen_base_date: str,
) -> pd.DataFrame | None:
    if filtered_df.empty:
        st.warning("백테스트 대상이 없습니다.")
        return None

    st.subheader("수동 백테스트")
    st.caption("이 페이지에서는 현재 필터된 종목만 순차적으로 백테스트합니다.")
    if not st.button("현재 필터 대상 백테스팅 실행", type="primary", use_container_width=True):
        return None

    target_tickers = filtered_df["종목코드"].tolist()
    progress_title = st.empty()
    progress_caption = st.empty()
    progress_bar = st.progress(0, text="백테스트 준비 중...")

    updated_df = st.session_state.get(SESSION_RESULTS_KEY)
    if updated_df is None or updated_df.empty:
        progress_bar.empty()
        progress_title.empty()
        progress_caption.empty()
        return None

    total = len(target_tickers)
    for index, ticker in enumerate(target_tickers, start=1):
        ticker_name_series = filtered_df.loc[filtered_df["종목코드"] == ticker, "종목명"]
        ticker_name = ticker_name_series.iloc[0] if not ticker_name_series.empty else ticker
        percent_complete = int(index / total * 100)
        progress_title.markdown(f"**백테스트 진행률: {percent_complete}%**")
        progress_caption.caption(f"현재 계산 중: {ticker_name} ({ticker}) | {index}/{total}")
        updated_df = enrich_results_with_backtests(
            updated_df,
            monthly_frames,
            screen_base_date,
            [ticker],
        )
        progress_bar.progress(index / total, text=f"{ticker_name} ({ticker}) 계산 중...")

    progress_bar.empty()
    progress_title.success(f"백테스트 계산이 완료되었습니다. 대상 {total}종목")
    progress_caption.caption("현재 필터 기준 결과가 최신 백테스트 값으로 갱신되었습니다.")
    st.session_state[SESSION_RESULTS_KEY] = updated_df
    return updated_df


def render_telegram_panel(filtered_df: pd.DataFrame, results_df: pd.DataFrame, screen_base_date: str, screen_market: str) -> None:
    telegram_source_df = filtered_df if not filtered_df.empty else results_df[results_df["월봉10개월선돌파여부"] == "예"]
    telegram_message = build_telegram_message(telegram_source_df, screen_base_date, screen_market)
    is_configured = bool(get_telegram_chat_id())

    tab_preview, tab_send = st.tabs(["미리보기", "수동 전송"])
    with tab_preview:
        st.subheader("텔레그램 알림 미리보기")
        st.code(telegram_message, language="text")

    with tab_send:
        if is_configured:
            st.caption("텔레그램 수동 전송이 설정되어 있습니다.")
        else:
            st.caption("텔레그램 시크릿이 없어서 수동 전송은 동작하지 않습니다.")

        if st.button("텔레그램 알림 전송", use_container_width=True):
            with st.spinner("텔레그램으로 전송하는 중입니다..."):
                success, message = send_telegram_message(telegram_message)
            if success:
                st.success(message)
            else:
                st.error(message)


def render_detail(filtered_df: pd.DataFrame, monthly_frames: dict[str, pd.DataFrame]) -> None:
    if filtered_df.empty:
        return

    st.subheader("종목 상세")
    selected_name = st.selectbox("상세 조회 종목", filtered_df["종목명"].tolist())
    selected_row = filtered_df[filtered_df["종목명"] == selected_name].iloc[0]
    selected_monthly = monthly_frames[selected_row["종목코드"]]

    info1, info2, info3, info4 = st.columns(4)
    info1.metric("현재 상태", str(selected_row["현재상태"]))
    info2.metric("현재가", f"{format_number(selected_row['현재가'])}원")
    info3.metric("10개월선", f"{format_number(selected_row['10개월선'])}원")
    info4.metric("백테스트 수익률", format_percent(selected_row["백테스트 수익률"]))

    backtest_col1, backtest_col2, backtest_col3 = st.columns(3)
    backtest_col1.metric("MDD", format_percent(selected_row["MDD"]))
    backtest_col2.metric("CAGR", format_percent(selected_row["CAGR"]))
    backtest_col3.metric(
        "평균 보유개월",
        "미계산" if pd.isna(selected_row["평균보유개월"]) else f"{selected_row['평균보유개월']:.1f}개월",
    )

    tab_chart, tab_history, tab_trades = st.tabs(["차트", "월별 데이터", "매매 로그"])

    with tab_chart:
        st.plotly_chart(create_monthly_chart(selected_monthly, selected_name), use_container_width=True)

    with tab_history:
        history_df = selected_monthly.tail(24).copy().reset_index()
        date_column = history_df.columns[0]
        history_df = history_df.rename(columns={date_column: "날짜"})
        history_df["날짜"] = pd.to_datetime(history_df["날짜"]).dt.strftime("%Y-%m")
        history_df = history_df.rename(
            columns={
                "close": "월봉 종가",
                "ma10": "10개월선",
                "volume": "월 거래량",
                "volume_change_pct": "거래량 증감률",
                "monthly_return_pct": "월간 수익률",
            }
        )
        history_df["월봉 종가"] = history_df["월봉 종가"].map(format_number)
        history_df["10개월선"] = history_df["10개월선"].map(format_number)
        history_df["월 거래량"] = history_df["월 거래량"].map(format_number)
        history_df["거래량 증감률"] = history_df["거래량 증감률"].map(format_percent)
        history_df["월간 수익률"] = history_df["월간 수익률"].map(format_percent)
        st.dataframe(history_df, use_container_width=True, hide_index=True)

    with tab_trades:
        trade_logs = selected_row["매매로그"] if "매매로그" in selected_row.index else []
        if isinstance(trade_logs, list) and trade_logs:
            trade_log_df = pd.DataFrame(trade_logs)
            trade_log_df = trade_log_df.rename(
                columns={
                    "entry_date": "진입일",
                    "exit_date": "청산일",
                    "entry_price": "진입가",
                    "exit_price": "청산가",
                    "return_pct": "수익률",
                    "hold_months": "보유개월",
                    "signal_rule": "신호기준",
                }
            )
            entry_price_col = "진입가" if "진입가" in trade_log_df.columns else None
            exit_price_col = "청산가" if "청산가" in trade_log_df.columns else None
            return_col = "수익률" if "수익률" in trade_log_df.columns else None
            if entry_price_col:
                trade_log_df[entry_price_col] = trade_log_df[entry_price_col].map(format_number)
            if exit_price_col:
                trade_log_df[exit_price_col] = trade_log_df[exit_price_col].map(format_number)
            if return_col:
                trade_log_df[return_col] = trade_log_df[return_col].map(format_percent)
            st.dataframe(trade_log_df, use_container_width=True, hide_index=True)
        else:
            st.info("아직 기록된 매매 로그가 없습니다. 백테스트 실행 후 이력에서 확인할 수 있습니다.")


def render_weekly_detail(filtered_df: pd.DataFrame, weekly_frames: dict[str, pd.DataFrame]) -> None:
    if filtered_df.empty:
        return

    st.subheader("주봉 상세")
    selected_name = st.selectbox("상세 조회 종목", filtered_df["종목명"].tolist(), key="weekly_detail_name")
    selected_row = filtered_df[filtered_df["종목명"] == selected_name].iloc[0]
    selected_weekly = weekly_frames[selected_row["종목코드"]]

    info1, info2, info3, info4 = st.columns(4)
    info1.metric("현재가", f"{format_number(selected_row['현재가'])}원")
    info2.metric("이평선 이격률", "미계산" if pd.isna(selected_row["이평선이격률"]) else f"{selected_row['이평선이격률']:.2f}%")
    info3.metric("거래량 배수", "미계산" if pd.isna(selected_row["거래량배수"]) else f"{selected_row['거래량배수']:.2f}배")
    info4.metric("최종 조건", str(selected_row["최종조건충족"]))

    cond_col1, cond_col2, cond_col3 = st.columns(3)
    cond_col1.metric("밀집조건", str(selected_row["밀집조건"]))
    cond_col2.metric("돌파조건", str(selected_row["돌파조건"]))
    cond_col3.metric("거래량조건", str(selected_row["거래량조건"]))

    tab_chart, tab_history = st.tabs(["차트", "주별 데이터"])
    with tab_chart:
        st.plotly_chart(create_weekly_chart(selected_weekly, selected_name), use_container_width=True)

    with tab_history:
        history_df = selected_weekly.tail(30).copy().reset_index()
        date_column = history_df.columns[0]
        history_df = history_df.rename(columns={date_column: "날짜"})
        history_df["날짜"] = pd.to_datetime(history_df["날짜"]).dt.strftime("%Y-%m-%d")
        history_df = history_df.rename(
            columns={
                "close": "주봉 종가",
                "ma10": "10주선",
                "ma20": "20주선",
                "ma40": "40주선",
                "volume": "주간 거래량",
                "avg_volume_10": "10주 평균 거래량",
                "volume_multiple": "거래량 배수",
                "ma_spread_pct": "이평선 이격률",
            }
        )
        for column in ["주봉 종가", "10주선", "20주선", "40주선", "주간 거래량", "10주 평균 거래량"]:
            if column in history_df.columns:
                history_df[column] = history_df[column].map(format_number)
        for column in ["거래량 배수", "이평선 이격률"]:
            if column in history_df.columns:
                history_df[column] = history_df[column].map(lambda value: "미계산" if pd.isna(value) else f"{float(value):.2f}")
        st.dataframe(history_df, use_container_width=True, hide_index=True)


def render_settings_page() -> None:
    st.subheader("설정 정보")
    st.write("현재 앱 구성과 운영 기준을 한눈에 확인할 수 있습니다.")
    render_data_source_badges()

    config_df = pd.DataFrame(
        [
            {"항목": "대상 시장", "값": ", ".join(MARKET_OPTIONS.keys())},
            {"항목": "기본 조회 종목 수", "값": DEFAULT_TOP_N},
            {"항목": "스크리닝 목적", "값": "빠른 돌파 후보 탐색"},
            {"항목": "백테스트 목적", "값": "수동 실행 기반 성과 검증"},
            {"항목": "시총 풀 우선 데이터 소스", "값": "FinanceDataReader -> pykrx fallback"},
        ]
    )
    config_df = config_df.astype(str)
    st.dataframe(config_df, use_container_width=True, hide_index=True)

    st.markdown("**운영 메모**")
    st.info("텔레그램 토큰과 Chat ID는 화면에 노출하지 않고 Streamlit Secrets 또는 GitHub Secrets에서만 관리하는 것을 권장합니다.")
    st.caption("설정 파일 위치: src/trend_tracker/config.py")

    render_data_source_diagnostics()


def _format_common_display_df(filtered_df: pd.DataFrame) -> pd.DataFrame:
    display_df = filtered_df.copy()

    if len(display_df.columns) > 3:
        display_df.iloc[:, 3] = display_df.iloc[:, 3].map(format_number)
    if len(display_df.columns) > 4:
        display_df.iloc[:, 4] = display_df.iloc[:, 4].map(format_number)
    if len(display_df.columns) > 9:
        display_df.iloc[:, 9] = display_df.iloc[:, 9].map(format_number)
    if len(display_df.columns) > 10:
        display_df.iloc[:, 10] = display_df.iloc[:, 10].map(format_percent)
    if len(display_df.columns) > 12:
        display_df.iloc[:, 12] = display_df.iloc[:, 12].map(lambda value: "미계산" if pd.isna(value) else format_percent(value))
    if "MDD" in display_df.columns:
        display_df["MDD"] = display_df["MDD"].map(lambda value: "미계산" if pd.isna(value) else format_percent(-abs(value)))
    if "CAGR" in display_df.columns:
        display_df["CAGR"] = display_df["CAGR"].map(lambda value: "미계산" if pd.isna(value) else format_percent(value))
    if len(display_df.columns) > 15:
        display_df.iloc[:, 15] = display_df.iloc[:, 15].map(lambda value: "미계산" if pd.isna(value) else f"{value:.1f}")
    if len(display_df.columns) > 17:
        display_df.iloc[:, 17] = display_df.iloc[:, 17].map(lambda value: "미계산" if pd.isna(value) else format_percent(value))
    if len(display_df.columns) > 11:
        display_df.iloc[:, 11] = display_df.iloc[:, 11].fillna("미계산")
    if len(display_df.columns) > 8:
        display_df.iloc[:, 8] = display_df.iloc[:, 8].map(lambda value: "-" if pd.isna(value) else int(value))

    if "ma10_rising" in display_df.columns:
        display_df["ma10_rising_label"] = display_df["ma10_rising"].map(lambda value: "예" if bool(value) else "아니오")
    if "above_ma20" in display_df.columns and "ma10_above_ma20" in display_df.columns:
        display_df["dual_trend_label"] = (display_df["above_ma20"].fillna(False) & display_df["ma10_above_ma20"].fillna(False)).map(lambda value: "예" if bool(value) else "아니오")

    return display_df


def _build_trade_log_csv(filtered_df: pd.DataFrame) -> str | None:
    rows: list[dict[str, object]] = []
    if "매매로그" not in filtered_df.columns:
        return None

    for _, row in filtered_df.iterrows():
        trade_logs = row.get("매매로그", [])
        if not isinstance(trade_logs, list) or not trade_logs:
            continue
        for trade in trade_logs:
            rows.append(
                {
                    "시장": row.get("시장"),
                    "종목명": row.get("종목명"),
                    "종목코드": row.get("종목코드"),
                    "진입일": trade.get("진입일", trade.get("entry_date")),
                    "청산일": trade.get("청산일", trade.get("exit_date")),
                    "진입가": trade.get("진입가", trade.get("entry_price")),
                    "청산가": trade.get("청산가", trade.get("exit_price")),
                    "수익률": trade.get("수익률", trade.get("return_pct")),
                    "보유개월": trade.get("보유개월", trade.get("hold_months")),
                    "신호기준": trade.get("신호기준", trade.get("signal_rule")),
                }
            )

    if not rows:
        return None
    return pd.DataFrame(rows).to_csv(index=False, encoding="utf-8-sig")


def render_data_source_diagnostics() -> None:
    diagnostics = st.session_state.get(SESSION_DATA_DIAGNOSTICS_KEY, {})
    if not diagnostics:
        return

    with st.expander("데이터 소스 진단", expanded=False):
        pool_source = diagnostics.get("pool_source") or "-"
        pool_fallbacks = diagnostics.get("pool_fallbacks") or []
        ohlcv_sources = diagnostics.get("ohlcv_sources") or {}
        errors = diagnostics.get("errors") or []

        status_col1, status_col2 = st.columns(2)
        status_col1.metric("종목 풀 소스", pool_source)
        status_col2.metric("OHLCV 사용 소스 수", len(ohlcv_sources))

        if pool_fallbacks:
            st.markdown("**적용된 fallback 경로**")
            for item in pool_fallbacks:
                st.write(f"- {item}")
        else:
            st.success("현재 조회에서는 추가 fallback 없이 기본 경로로 처리되었습니다.")

        if ohlcv_sources:
            source_df = pd.DataFrame(
                [{"소스": key, "건수": value} for key, value in ohlcv_sources.items()]
            )
            source_df = source_df.astype({"소스": "string", "건수": "string"})
            st.markdown("**가격 데이터 사용 분포**")
            st.dataframe(source_df, use_container_width=True, hide_index=True)

        if errors:
            st.markdown("**최근 데이터 오류**")
            for item in errors[:5]:
                st.code(item, language="text")
        else:
            st.info("최근 조회에서 기록된 데이터 오류는 없습니다.")
