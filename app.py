import sys
from pathlib import Path

import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.trend_tracker.page_helpers import (
    ensure_default_screening_results,
    render_cnn_fear_greed_card,
    render_execution_rule_badge,
    render_market_dashboard,
    render_market_index_overview,
    show_page_loading_bar,
)


st.set_page_config(page_title="woni 추세 트래커", layout="wide")
page_loader = show_page_loading_bar("앱을 불러오고 있습니다...", progress=20)

st.title("woni 추세 트래커")
st.caption("왼쪽 메뉴에서 Month Screening, Weekly Screening 페이지로 이동할 수 있습니다.")

page_loader.update("메인 화면을 준비하고 있습니다...", 70)

st.markdown(
    """
    이 앱은 월봉 10개월선 기준 스크리닝과
    주봉 10·20·40주선 돌파 조건 검색 흐름으로 구성되어 있습니다.

    - `Month Screening`: 월봉 기준 빠른 후보 조회와 필터링
    - `Weekly Screening`: 주봉 밀집·돌파·거래량 조건 검색
    """
)
render_execution_rule_badge("month")

action_col1, action_col2 = st.columns(2)
action_col1.page_link("pages/1_Month_Screening.py", label="월봉 조회 바로가기")
action_col2.page_link("pages/4_Weekly_Screening.py", label="주봉 조회 바로가기")

st.markdown("---")
results_df, monthly_frames, _, _ = ensure_default_screening_results()
if results_df is not None and not results_df.empty:
    render_market_dashboard(results_df, monthly_frames)
render_cnn_fear_greed_card()
render_market_index_overview()

page_loader.update("메인 화면 표시를 마무리하고 있습니다...", 100)
page_loader.empty()
