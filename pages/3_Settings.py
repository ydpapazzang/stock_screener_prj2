import streamlit as st

from src.trend_tracker.page_helpers import render_settings_page, show_page_loading_bar


st.set_page_config(page_title="설정", layout="wide")
page_loader = show_page_loading_bar("설정 페이지를 불러오고 있습니다...")
st.title("설정")
st.caption("현재 앱 구성과 텔레그램 연동 설정 위치를 확인합니다.")

render_settings_page()
page_loader.empty()
