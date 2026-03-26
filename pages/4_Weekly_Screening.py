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
st.caption("10주·20주·40주 이동평균선 밀집 뒤 초기 추세 전환이 나오고 아직 과열되지 않은 주봉 구간을 찾습니다.")
st.info("기본 로직은 최근 3주 이평선 밀집, 20·40주선 상향 돌파, 10주선 상승 전환, 과도한 이격 아님의 교집합입니다.")
helpers.render_execution_rule_badge("week")

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
results_df, weekly_frames, screen_market, screen_base_date = helpers.get_weekly_session_results()

if not helpers.render_weekly_empty_state(results_df):
    helpers.render_weekly_summary_metrics(results_df)
    filtered_df = helpers.render_weekly_filter_controls(results_df)
    helpers.render_weekly_screening_table(filtered_df, results_df)
    helpers.render_weekly_telegram_panel(filtered_df, results_df, screen_base_date, screen_market)
    helpers.render_weekly_detail(filtered_df, weekly_frames)

page_loader.update("주봉 조회 페이지 표시를 마무리하고 있습니다...", 100)
page_loader.empty()
