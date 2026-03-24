from __future__ import annotations

import argparse
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import holidays
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.trend_tracker.analysis import analyze_market, get_last_data_error, get_latest_business_day
from src.trend_tracker.config import ALERT_MARKETS, DEFAULT_TOP_N
from src.trend_tracker.formatting import format_number
from src.trend_tracker.notifications import send_telegram_message


SEOUL_TZ = ZoneInfo("Asia/Seoul")


def get_last_business_day_of_month(target_date: date) -> date:
    kr_holidays = holidays.country_holidays("KR", years=[target_date.year])
    if target_date.month == 12:
        month_end = date(target_date.year, 12, 31)
    else:
        month_end = date(target_date.year, target_date.month + 1, 1) - timedelta(days=1)

    current = month_end
    while current.weekday() >= 5 or current in kr_holidays:
        current -= timedelta(days=1)
    return current


def is_last_business_day_in_seoul(now: datetime | None = None) -> bool:
    current = now.astimezone(SEOUL_TZ) if now else datetime.now(SEOUL_TZ)
    return current.date() == get_last_business_day_of_month(current.date())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="월말 10개월선 돌파 텔레그램 알림")
    parser.add_argument("--date", help="기준 일자 YYYYMMDD. 미입력 시 최근 영업일 사용")
    parser.add_argument("--top-n", type=int, default=DEFAULT_TOP_N, help="시장별 대상 종목 수")
    parser.add_argument("--force", action="store_true", help="월말 여부와 관계없이 강제 실행")
    parser.add_argument("--skip-send", action="store_true", help="텔레그램 전송 없이 메시지만 출력")
    return parser.parse_args()


def build_summary_message(base_date: str, section_results: list[dict[str, object]]) -> str:
    label_date = datetime.strptime(base_date, "%Y%m%d").strftime("%Y-%m-%d")
    lines = [f"[월말 10개월선 스크리닝 요약] {label_date}", ""]

    total_breakouts = 0
    for result in section_results:
        market_label = result["market_label"]
        breakout_count = result["breakout_count"]
        total_breakouts += breakout_count
        status = "정상" if result["ok"] else "오류"
        lines.append(f"- {market_label}: 돌파 {breakout_count}건 / 상태 {status}")

    lines.append("")
    lines.append(f"총 돌파 종목 수: {total_breakouts}건")
    lines.append("아래에 시장별 상세 메시지가 이어집니다.")
    return "\n".join(lines)


def build_market_message(base_date: str, market_label: str, market_code: str, results_df: pd.DataFrame, error: str | None) -> str:
    label_date = datetime.strptime(base_date, "%Y%m%d").strftime("%Y년 %m월")
    lines = [f"[{label_date} 말일 {market_label} 10개월선 스크리닝]", ""]

    if error:
        lines.append(f"데이터 조회 오류: {error}")
        return "\n".join(lines)

    if results_df.empty:
        lines.append("조회 결과가 없습니다.")
        return "\n".join(lines)

    breakouts = results_df[results_df["월봉10개월선돌파여부"] == "예"].copy()
    lines.append(f"대상 시장: {market_label} ({market_code})")
    lines.append(f"돌파 종목 수: {len(breakouts)}건")
    lines.append("")

    if breakouts.empty:
        lines.append("이번 조회에서는 돌파 종목이 없습니다.")
        return "\n".join(lines)

    for _, row in breakouts.head(15).iterrows():
        lines.append(f"- {row['종목명']} ({row['종목코드']})")
        lines.append(f"  현재가: {format_number(row['현재가'])}")
        lines.append(f"  10개월선: {format_number(row['10개월선'])}")
        lines.append(f"  한달간 거래량: {format_number(row['한달간 거래량'])}")
        latest_breakout = row["최근 돌파월"] if row["최근 돌파월"] != "-" else "신규/정보없음"
        lines.append(f"  최근 돌파월: {latest_breakout}")

    remaining = max(0, len(breakouts) - 15)
    if remaining:
        lines.append(f"... 외 {remaining}건")

    return "\n".join(lines)


def analyze_market_for_alert(base_date: str, market_label: str, market_code: str, top_n: int) -> dict[str, object]:
    try:
        results_df, _ = analyze_market(base_date=base_date, market=market_code, top_n=top_n)
        error = get_last_data_error()
    except Exception as exc:
        results_df = pd.DataFrame()
        error = str(exc)

    if not results_df.empty:
        error = None

    breakout_count = 0
    if not results_df.empty and "월봉10개월선돌파여부" in results_df.columns:
        breakout_count = int((results_df["월봉10개월선돌파여부"] == "예").sum())

    message = build_market_message(
        base_date=base_date,
        market_label=market_label,
        market_code=market_code,
        results_df=results_df,
        error=error,
    )

    return {
        "market_label": market_label,
        "market_code": market_code,
        "results_df": results_df,
        "breakout_count": breakout_count,
        "ok": error is None,
        "error": error,
        "message": message,
    }


def send_messages(messages: list[str]) -> bool:
    all_success = True
    for message in messages:
        success, status_message = send_telegram_message(message)
        print(status_message)
        if not success:
            all_success = False
    return all_success


def main() -> int:
    args = parse_args()

    if not args.force and not is_last_business_day_in_seoul():
        current = datetime.now(SEOUL_TZ).date()
        last_business_day = get_last_business_day_of_month(current)
        print(
            f"오늘은 월말 마지막 영업일이 아닙니다. "
            f"(오늘: {current.isoformat()}, 마지막 영업일: {last_business_day.isoformat()}) "
            "--force 로 강제 실행할 수 있습니다."
        )
        return 0

    base_date = args.date or get_latest_business_day()
    section_results = [
        analyze_market_for_alert(base_date=base_date, market_label=market_label, market_code=market_code, top_n=args.top_n)
        for market_label, market_code in ALERT_MARKETS
    ]

    messages = [build_summary_message(base_date, section_results)]
    messages.extend(result["message"] for result in section_results)

    for message in messages:
        print(message)
        print("\n" + "=" * 80 + "\n")

    if args.skip_send:
        return 0

    return 0 if send_messages(messages) else 1


if __name__ == "__main__":
    raise SystemExit(main())
