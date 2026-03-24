import streamlit as st

from src.trend_tracker.page_helpers import show_page_loading_bar


st.set_page_config(page_title="10개월선 추세 트래커", layout="wide")
page_loader = show_page_loading_bar("앱을 불러오고 있습니다...", progress=20)

st.title("10개월선 추세 트래커")
st.caption("왼쪽 메뉴에서 Screening, Backtest, Settings 페이지로 이동할 수 있습니다.")

page_loader.update("홈 화면 구성을 준비하고 있습니다...", 70)

st.markdown(
    """
    이 앱은 월봉 10개월선 기준으로 시장별 돌파 후보를 찾고,
    필요한 종목만 나중에 백테스트하는 흐름으로 설계되어 있습니다.

    - `Screening`: 빠른 후보 조회, 필터링, 텔레그램 수동 전송
    - `Backtest`: 현재 필터 대상 종목의 수동 백테스트 실행
    - `Settings`: 운영 기준, 데이터 소스 진단, 배포 메모 확인
    """
)

st.info("실행은 `streamlit run app.py`로 할 수 있습니다.")

page_loader.update("홈 화면 표시를 마무리하고 있습니다...", 100)
page_loader.empty()
