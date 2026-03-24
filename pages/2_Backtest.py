import streamlit as st

from src.trend_tracker.page_helpers import (
    get_session_results,
    render_backtest_table,
    render_detail,
    render_empty_state,
    render_filter_controls,
    render_query_sidebar,
    render_summary_metrics,
    run_manual_backtest_for_filtered,
    show_page_loading_bar,
)


st.set_page_config(page_title="백테스트", layout="wide")
page_loader = show_page_loading_bar("백테스트 페이지를 불러오고 있습니다...")
st.title("백테스트")
st.caption("최근 200봉 기준 MA10 전략 성과를 종목별로 비교합니다.")

render_query_sidebar()
results_df, monthly_frames, _, _ = get_session_results()

if not render_empty_state(results_df):
    render_summary_metrics(results_df)
    filtered_df = render_filter_controls(results_df, default_sort_by="돌파경과개월")
    updated_results_df = run_manual_backtest_for_filtered(filtered_df, monthly_frames)
    if updated_results_df is not None:
        results_df = updated_results_df
        filtered_df = render_filter_controls(results_df, default_sort_by="백테스트 수익률")
    render_backtest_table(filtered_df, results_df)
    render_detail(filtered_df, monthly_frames)

page_loader.empty()
