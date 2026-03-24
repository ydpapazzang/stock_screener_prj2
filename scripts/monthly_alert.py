from __future__ import annotations

import argparse
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd

from src.trend_tracker.analysis import analyze_market, get_last_data_error, get_latest_business_day
from src.trend_tracker.config import DEFAULT_TOP_N
from src.trend_tracker.notifications import build_telegram_message, send_telegram_message


SEOUL_TZ = ZoneInfo("Asia/Seoul")


def is_last_calendar_day_in_seoul(now: datetime | None = None) -> bool:
    current = now.astimezone(SEOUL_TZ) if now else datetime.now(SEOUL_TZ)
    tomorrow = current + timedelta(days=1)
    return current.month != tomorrow.month


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="월말 10개월선 돌파 텔레그램 알림")
    parser.add_argument("--date", help="기준 일자 YYYYMMDD. 미지정 시 최근 영업일 사용")
    parser.add_argument("--top-n", type=int, default=DEFAULT_TOP_N, help="시장별 대상 종목 수")
    parser.add_argument("--force", action="store_true", help="월말 여부와 관계없이 실행")
    parser.add_argument("--skip-send", action="store_true", help="텔레그램 전송 없이 메시지만 출력")
    return parser.parse_args()


def build_market_section(base_date: str, market: str, top_n: int) -> tuple[str, pd.DataFrame]:
    results_df, _ = analyze_market(base_date=base_date, market=market, top_n=top_n)
    breakouts = results_df[results_df["월봉10개월선돌파여부"] == "예"].copy() if not results_df.empty else pd.DataFrame()
    message = build_telegram_message(results_df if not results_df.empty else pd.DataFrame(columns=["월봉10개월선돌파여부"]), base_date, market)
    return message, breakouts


def main() -> int:
    args = parse_args()

    if not args.force and not is_last_calendar_day_in_seoul():
        print("월말 마지막 날이 아니므로 배치 전송을 건너뜁니다. --force 로 강제 실행할 수 있습니다.")
        return 0

    base_date = args.date or get_latest_business_day()
    messages: list[str] = []
    breakout_total = 0

    for market in ("KOSPI", "KOSDAQ"):
        message, breakouts = build_market_section(base_date=base_date, market=market, top_n=args.top_n)
        messages.append(message)
        breakout_total += len(breakouts)

    if breakout_total == 0:
        data_error = get_last_data_error()
        if data_error:
            messages.append(f"\n[데이터 진단]\n{data_error}")

    final_message = "\n\n".join(messages)
    print(final_message)

    if args.skip_send:
        return 0

    success, status_message = send_telegram_message(final_message)
    print(status_message)
    return 0 if success else 1


if __name__ == "__main__":
    raise SystemExit(main())
