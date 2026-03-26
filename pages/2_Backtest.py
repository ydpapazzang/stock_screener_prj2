import sys
from pathlib import Path

import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.trend_tracker.page_helpers import (
    get_session_results,
    render_backtest_table,
    render_detail,
    render_empty_state,
    render_execution_rule_badge,
    render_filter_controls,
    render_query_sidebar,
    render_summary_metrics,
    run_manual_backtest_for_filtered,
    show_page_loading_bar,
)


st.set_page_config(page_title="Backtest", layout="wide")
page_loader = show_page_loading_bar("백테스트 페이지를 불러오고 있습니다...", progress=15)

st.title("백테스트")
st.caption("현재 필터 대상 종목으로 월봉 MA10 전략 성과를 계산합니다.")
st.info("이 페이지는 수동 검증용입니다. 먼저 조회로 후보를 만든 뒤 백테스트를 실행해 주세요.")
render_execution_rule_badge()

page_loader.update("백테스트 설정을 준비하고 있습니다...", 35)
render_query_sidebar()

page_loader.update("백테스트 대상을 정리하고 있습니다...", 70)
results_df, monthly_frames, _, screen_base_date = get_session_results()

if not render_empty_state(results_df):
    render_summary_metrics(results_df)
    filtered_df = render_filter_controls(results_df, default_sort_by="돌파경과개월", key_prefix="backtest")
    updated_results_df = run_manual_backtest_for_filtered(filtered_df, monthly_frames, screen_base_date)
    if updated_results_df is not None:
        results_df = updated_results_df
        filtered_df = render_filter_controls(
            results_df,
            default_sort_by="백테스트 수익률",
            key_prefix="backtest_after_run",
        )
    render_backtest_table(filtered_df, results_df)
    render_detail(filtered_df, monthly_frames)

page_loader.update("백테스트 페이지 표시를 마무리하고 있습니다...", 100)
page_loader.empty()
