import streamlit as st

from src.trend_tracker.page_helpers import render_settings_page, show_page_loading_bar


st.set_page_config(page_title="Settings", layout="wide")
page_loader = show_page_loading_bar("설정 페이지를 불러오고 있습니다...", progress=20)

st.title("설정")
st.caption("앱 구성, 데이터 소스 진단, 운영 메모를 확인하는 화면입니다.")

page_loader.update("설정 정보를 불러오고 있습니다...", 70)
render_settings_page()

page_loader.update("설정 페이지 표시를 마무리하고 있습니다...", 100)
page_loader.empty()
