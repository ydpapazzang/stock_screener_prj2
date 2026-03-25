from __future__ import annotations

import pandas as pd

from ..kis_client import KISClient
from .base import MarketDataProvider


class KISDomesticProvider(MarketDataProvider):
    """Skeleton provider for KOSPI/KOSDAQ via KIS Open API.

    Implementation notes:
    - Universe should be built from KIS 종목정보파일 or a ranking endpoint.
    - Daily OHLCV should be built from 국내주식기간별시세(일/주/월/년).
    """

    def __init__(self, client: KISClient | None = None):
        self.client = client or KISClient()

    def get_universe(self, market: str, top_n: int, base_date: str) -> pd.DataFrame:
        if market not in {"KOSPI", "KOSDAQ"}:
            raise ValueError(f"KISDomesticProvider does not support market: {market}")

        return pd.DataFrame(columns=["티커", "종목명", "시장", "시가총액"])

    def get_daily_ohlcv(self, ticker: str, market: str, start_date: str, end_date: str) -> pd.DataFrame:
        if market not in {"KOSPI", "KOSDAQ"}:
            raise ValueError(f"KISDomesticProvider does not support market: {market}")

        return pd.DataFrame(columns=["종가", "거래량"])
