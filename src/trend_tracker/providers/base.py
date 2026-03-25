from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd


class MarketDataProvider(ABC):
    @abstractmethod
    def get_universe(self, market: str, top_n: int, base_date: str) -> pd.DataFrame:
        """Return a universe DataFrame with columns: 티커, 종목명, 시장, 시가총액."""

    @abstractmethod
    def get_daily_ohlcv(self, ticker: str, market: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Return daily OHLCV DataFrame normalized enough for downstream monthly conversion."""
