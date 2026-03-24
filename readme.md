# 10개월선 추세 트래커

월봉 10개월선 기준으로 돌파 후보를 찾고, 필요한 종목만 나중에 백테스트하는 Streamlit 앱입니다.

## 주요 기능

- `Screening`
  - 시장별 종목 조회
  - 월봉 10개월선 돌파 후보 필터링
  - 텔레그램 수동 전송
- `Backtest`
  - 현재 필터된 종목만 수동 백테스트 실행
  - 누적 수익률, MDD, CAGR, 평균 보유개월, 승률 확인
  - 매매 로그 확인
- `Settings`
  - 운영 기준 확인
  - 데이터 소스 진단 확인
  - 배포/시크릿 운영 메모 확인
- `Monthly Alert`
  - GitHub Actions로 월말 마지막 영업일 오후 5시(KST) 실행
  - 텔레그램 자동 전송

## 페이지 역할

- `Screening`은 빠른 후보 조회용입니다.
- `Backtest`는 성과 검증용입니다.
- 조회 속도를 위해 기본 스크리닝 단계에서는 백테스트를 수행하지 않습니다.

## 실행 방법

```bash
pip install -r requirements.txt
streamlit run app.py
```

## 프로젝트 구조

```text
app.py
pages/
  1_Screening.py
  2_Backtest.py
  3_Settings.py
scripts/
  monthly_alert.py
src/trend_tracker/
  analysis.py
  charts.py
  config.py
  formatting.py
  notifications.py
  page_helpers.py
.github/workflows/
  monthly-alert.yml
.streamlit/
  secrets.toml.example
```

## 의존성

- `streamlit`
- `pandas`
- `plotly`
- `pykrx`
- `finance-datareader`
- `requests`
- `holidays`

## Streamlit Community Cloud 배포

### 1. 기본 설정

- Repository: 현재 GitHub 저장소
- Branch: `main`
- Main file path: `app.py`
- Python version: `3.11` 권장

### 2. Secrets 설정

`Advanced settings` 또는 앱의 `Settings > Secrets`에 아래 값을 넣습니다.

```toml
TELEGRAM_BOT_TOKEN = "your-telegram-bot-token"
TELEGRAM_CHAT_ID = "your-chat-id"
APP_PUBLIC_URL = "https://your-app.streamlit.app"
```

`APP_PUBLIC_URL`은 선택값이지만, 넣어두면 텔레그램 메시지 마지막에 스크리너 링크가 함께 전송됩니다.

### 3. 배포 후 확인

- 홈 페이지가 열리는지
- `Screening / Backtest / Settings` 페이지가 보이는지
- `Screening`에서 조회가 되는지
- `Backtest`에서 수동 백테스트가 실행되는지

## GitHub Actions 월말 배치

워크플로 파일:

- `.github/workflows/monthly-alert.yml`

동작 방식:

- 매달 `28~31일` `08:00 UTC` 실행
- 한국시간 기준 `17:00`
- 스크립트 내부에서 `한국 마지막 영업일`인지 한 번 더 확인
- 마지막 영업일인 경우에만 텔레그램 전송

수동 실행:

```bash
python scripts/monthly_alert.py --force
```

## GitHub Secrets

월말 자동 전송을 쓰려면 저장소 `Settings > Secrets and variables > Actions`에 아래 값을 넣습니다.

- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`
- `APP_PUBLIC_URL` 선택

## 데이터 소스 정책

- `KOSPI`, `KOSDAQ`
  - `FinanceDataReader` 우선
  - 실패 시 `pykrx` fallback
- `NASDAQ`, `S&P500`
  - `FinanceDataReader`
- `다우산업`
  - 내부 고정 `DOW 30` 리스트 사용

진단 정보는 앱의 `Settings` 페이지와 빈 결과 화면의 `데이터 소스 진단`에서 확인할 수 있습니다.

## 보안 메모

- 텔레그램 토큰과 Chat ID는 코드에 하드코딩하지 않습니다.
- `Streamlit Secrets` 또는 `GitHub Secrets`로만 관리합니다.
- 이전 토큰이 대화나 커밋, 스크린샷 등 외부에 노출된 적이 있다면 반드시 재발급하세요.

## 토큰 재발급 최종 확인

코드 저장소만 기준으로 보면 현재 앱 코드에는 텔레그램 토큰이 하드코딩되어 있지 않습니다.

다만 실제로 재발급이 끝났는지는 코드만으로 확인할 수 없습니다. 아래를 직접 확인해야 합니다.

1. `BotFather`에서 새 토큰을 발급받았는지
2. Streamlit Cloud Secrets가 새 토큰으로 갱신됐는지
3. GitHub Actions Secrets도 같은 새 토큰으로 갱신됐는지

## 추천 운영 순서

1. 로컬에서 `streamlit run app.py`
2. Streamlit Community Cloud 배포
3. Secrets 설정
4. Screening 조회 테스트
5. Backtest 실행 테스트
6. GitHub Actions `workflow_dispatch`로 월말 배치 테스트
