import streamlit as st

from src.trend_tracker.page_helpers import (
    get_session_results,
    render_detail,
    render_empty_state,
    render_filter_controls,
    render_query_sidebar,
    render_screening_table,
    render_summary_metrics,
    render_telegram_panel,
    show_page_loading_bar,
)


st.set_page_config(page_title="스크리닝", layout="wide")
page_loader = show_page_loading_bar("스크리닝 페이지를 불러오고 있습니다...")
st.title("스크리닝")
st.caption("월봉 10개월선 돌파 종목을 조회하고 필터링합니다.")
st.info("빠른 조회 전용 화면입니다. 백테스트 계산은 왼쪽 `Backtest` 페이지에서 `현재 필터 대상 백테스팅 실행` 버튼으로 수행합니다.")

render_query_sidebar()
results_df, monthly_frames, screen_market, screen_base_date = get_session_results()

if not render_empty_state(results_df):
    render_summary_metrics(results_df)
    filtered_df = render_filter_controls(results_df, default_sort_by="돌파경과개월")
    render_screening_table(filtered_df, results_df)
    render_telegram_panel(filtered_df, results_df, screen_base_date, screen_market)
    render_detail(filtered_df, monthly_frames)

page_loader.empty()
