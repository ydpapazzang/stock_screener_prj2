import streamlit as st

from src.trend_tracker.page_helpers import (
    get_session_results,
    render_detail,
    render_empty_state,
    render_filter_controls,
    render_market_dashboard,
    render_query_sidebar,
    render_screening_table,
    render_summary_metrics,
    render_telegram_panel,
    show_page_loading_bar,
)


st.set_page_config(page_title="Screening", layout="wide")
page_loader = show_page_loading_bar("스크리닝 페이지를 불러오고 있습니다...", progress=15)

st.title("스크리닝")
st.caption("월봉 10개월선 기준으로 빠르게 돌파 후보를 찾는 화면입니다.")
st.info("이 페이지는 빠른 후보 조회용입니다. 성과 검증은 `Backtest` 페이지에서 따로 실행합니다.")

page_loader.update("스크리닝 설정을 준비하고 있습니다...", 35)
render_query_sidebar()

page_loader.update("조회 결과를 구성하고 있습니다...", 70)
results_df, monthly_frames, screen_market, screen_base_date = get_session_results()

if not render_empty_state(results_df):
    render_market_dashboard(results_df, monthly_frames)
    render_summary_metrics(results_df)
    filtered_df = render_filter_controls(results_df, default_sort_by="돌파경과개월", key_prefix="screening")
    render_screening_table(filtered_df, results_df)
    render_telegram_panel(filtered_df, results_df, screen_base_date, screen_market)
    render_detail(filtered_df, monthly_frames)

page_loader.update("스크리닝 페이지 표시를 마무리하고 있습니다...", 100)
page_loader.empty()
