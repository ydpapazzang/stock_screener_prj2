import streamlit as st

from src.trend_tracker.page_helpers import show_page_loading_bar


st.set_page_config(page_title="10개월선 추세 트래커", layout="wide")
page_loader = show_page_loading_bar("앱 홈을 불러오고 있습니다...")
st.title("10개월선 추세 트래커")
st.caption("왼쪽 페이지 메뉴에서 스크리닝, 백테스트, 설정 화면으로 이동할 수 있습니다.")

st.markdown(
    """
    이 앱은 Streamlit `pages/` 구조로 분리되어 있습니다.

    - `스크리닝`: 월봉 10개월선 돌파 종목 조회와 텔레그램 수동 전송
    - `백테스트`: 최근 200봉 기준 MA10 전략 성과 비교
    - `설정`: 현재 구성값과 텔레그램 설정 위치 확인
    """
)

st.info("실행은 그대로 `streamlit run app.py`로 하면 됩니다.")
page_loader.empty()
