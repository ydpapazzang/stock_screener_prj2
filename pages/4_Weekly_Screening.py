import sys
from pathlib import Path

import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.trend_tracker import page_helpers as helpers


st.set_page_config(page_title="Weekly Screening", layout="wide")
page_loader = helpers.show_page_loading_bar("주봉 조회 페이지를 불러오고 있습니다...", progress=15)

st.title("주봉 조회")
st.caption("10주·20주·40주 이동평균선 밀집 후 20주·40주 상향 돌파와 거래량 급증이 동시에 나온 종목을 찾습니다.")
st.info("기본 로직은 직전 주 이평선 밀집, 현재 주 20·40주선 종가 돌파, 최근 10주 평균 대비 거래량 급증의 교집합입니다.")
helpers.render_execution_rule_badge()

required_functions = [
    "render_weekly_query_sidebar",
    "get_weekly_session_results",
    "render_weekly_empty_state",
    "render_weekly_summary_metrics",
    "render_weekly_filter_controls",
    "render_weekly_screening_table",
    "render_weekly_detail",
]
missing_functions = [name for name in required_functions if not hasattr(helpers, name)]
if missing_functions:
    st.error(
        "주봉 조회 화면 구성 함수가 아직 배포되지 않았습니다. "
        f"누락 항목: {', '.join(missing_functions)}"
    )
    page_loader.empty()
    st.stop()

page_loader.update("주봉 조회 설정을 준비하고 있습니다...", 35)
helpers.render_weekly_query_sidebar()

page_loader.update("주봉 조건 결과를 구성하고 있습니다...", 70)
results_df, weekly_frames, _, _ = helpers.get_weekly_session_results()

if not helpers.render_weekly_empty_state(results_df):
    helpers.render_weekly_summary_metrics(results_df)
    filtered_df = helpers.render_weekly_filter_controls(results_df)
    helpers.render_weekly_screening_table(filtered_df, results_df)
    helpers.render_weekly_detail(filtered_df, weekly_frames)

page_loader.update("주봉 조회 페이지 표시를 마무리하고 있습니다...", 100)
page_loader.empty()
