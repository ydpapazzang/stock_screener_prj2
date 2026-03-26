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


st.set_page_config(page_title="10개월선 추세 트래커", layout="wide")
page_loader = show_page_loading_bar("앱을 불러오고 있습니다...", progress=20)

st.title("10개월선 추세 트래커")
st.caption("왼쪽 메뉴에서 Screening, Backtest, Settings 페이지로 이동할 수 있습니다.")

page_loader.update("메인 화면을 준비하고 있습니다...", 70)

st.markdown(
    """
    이 앱은 월봉 10개월선 기준으로 시장별 돌파 후보를 찾고,
    필요한 종목만 선별해 백테스트하는 흐름으로 구성되어 있습니다.

    - `Screening`: 빠른 후보 조회와 필터링
    - `Backtest`: 현재 후보 종목 기준 수동 백테스트
    - `Settings`: 운영 정보와 데이터 소스 진단 확인
    """
)
render_execution_rule_badge()

action_col1, action_col2, action_col3 = st.columns(3)
action_col1.page_link("pages/1_Screening.py", label="스크리닝 바로가기")
action_col2.page_link("pages/2_Backtest.py", label="백테스트 바로가기")
action_col3.page_link("pages/3_Settings.py", label="설정 바로가기")

st.markdown("---")
results_df, monthly_frames, _, _ = ensure_default_screening_results()
if results_df is not None and not results_df.empty:
    render_market_dashboard(results_df, monthly_frames)
render_cnn_fear_greed_card()
render_market_index_overview()

page_loader.update("메인 화면 표시를 마무리하고 있습니다...", 100)
page_loader.empty()
