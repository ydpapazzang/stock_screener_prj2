from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

import pandas as pd
from pykrx import stock

from ..config import DOW_COMPONENTS
from ..kis_client import KISClient
from .base import MarketDataProvider


def _load_fdr():
    try:
        import FinanceDataReader as fdr
    except ImportError:
        return None
    return fdr


def _as_frame(payload: Any, key: str) -> pd.DataFrame:
    rows = payload.get(key, [])
    if isinstance(rows, dict):
        return pd.DataFrame([rows])
    if isinstance(rows, list):
        return pd.DataFrame(rows)
    return pd.DataFrame()


def _pick_first(row: pd.Series, *candidates: str) -> Any:
    for candidate in candidates:
        if candidate in row.index:
            value = row[candidate]
            if pd.notna(value) and str(value).strip() != "":
                return value
    return None


def _to_int(value: Any) -> int:
    if value is None or pd.isna(value):
        return 0
    text = str(value).replace(",", "").strip()
    if not text:
        return 0
    try:
        return int(float(text))
    except ValueError:
        return 0


def _to_float(value: Any) -> float | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).replace(",", "").strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _empty_universe() -> pd.DataFrame:
    return pd.DataFrame(columns=["티커", "종목명", "시장", "시가총액"])


def _empty_ohlcv() -> pd.DataFrame:
    return pd.DataFrame(columns=["종가", "거래량"])


@dataclass(frozen=True)
class OverseasMarketMap:
    listing_source: str
    quote_exchange_code: str
    detail_exchange_code: str
    product_type_code: str


OVERSEAS_MARKET_MAP: dict[str, OverseasMarketMap] = {
    "NASDAQ": OverseasMarketMap("NASDAQ", "NAS", "NASD", "512"),
    "S&P500": OverseasMarketMap("S&P500", "NAS", "NASD", "512"),
    "DOW": OverseasMarketMap("DOW", "NYS", "NYSE", "513"),
}


class KISMarketDataProvider(MarketDataProvider):
    def __init__(self, client: KISClient | None = None):
        self.client = client or KISClient()

    def get_universe(self, market: str, top_n: int, base_date: str) -> pd.DataFrame:
        if market in {"KOSPI", "KOSDAQ"}:
            return self._get_domestic_universe(market=market, top_n=top_n, base_date=base_date)
        if market in OVERSEAS_MARKET_MAP:
            return self._get_overseas_universe(market=market, top_n=top_n)
        raise ValueError(f"Unsupported market for KIS provider: {market}")

    def get_daily_ohlcv(self, ticker: str, market: str, start_date: str, end_date: str) -> pd.DataFrame:
        if market in {"KOSPI", "KOSDAQ"}:
            return self._get_domestic_daily_ohlcv(ticker=ticker, start_date=start_date, end_date=end_date)
        if market in OVERSEAS_MARKET_MAP:
            return self._get_overseas_daily_ohlcv(ticker=ticker, market=market)
        raise ValueError(f"Unsupported market for KIS provider: {market}")

    def _get_domestic_universe(self, market: str, top_n: int, base_date: str) -> pd.DataFrame:
        market_code = "0001" if market == "KOSPI" else "1001"
        payload = self.client.get(
            path="/uapi/domestic-stock/v1/quotations/volume-rank",
            tr_id="FHPST01710000",
            params={
                "fid_cond_mrkt_div_code": "J",
                "fid_cond_scr_div_code": "20171",
                "fid_input_iscd": market_code,
                "fid_div_cls_code": "0",
                "fid_blng_cls_code": "0",
                "fid_trgt_cls_code": "0",
                "fid_trgt_exls_cls_code": "0",
                "fid_input_price_1": "",
                "fid_input_price_2": "",
                "fid_vol_cnt": "",
                "fid_input_date_1": "",
            },
        )
        rows = _as_frame(payload, "output")
        if rows.empty:
            return _empty_universe()

        normalized = pd.DataFrame(
            {
                "티커": rows.apply(
                    lambda row: str(
                        _pick_first(row, "mksc_shrn_iscd", "stck_shrn_iscd", "hts_kor_iscd", "pdno", "iscd") or ""
                    ).zfill(6),
                    axis=1,
                ),
                "종목명": rows.apply(
                    lambda row: str(
                        _pick_first(row, "hts_kor_isnm", "stck_kor_isnm", "data_rank_name", "prdt_name", "name") or ""
                    ),
                    axis=1,
                ),
                "시장": market,
                "시가총액": rows.apply(
                    lambda row: _to_int(_pick_first(row, "hts_avls", "data_rank_amt", "acml_tr_pbmn", "stck_avls")),
                    axis=1,
                ),
            }
        )
        normalized = normalized[normalized["티커"].str.strip() != ""]
        normalized = normalized.drop_duplicates(subset=["티커"]).head(top_n)
        if normalized.empty:
            return _empty_universe()
        return normalized

    def _get_overseas_universe(self, market: str, top_n: int) -> pd.DataFrame:
        market_meta = OVERSEAS_MARKET_MAP[market]
        if market == "DOW":
            rows = [
                {"티커": symbol, "종목명": name, "시장": "DOW", "시가총액": 0}
                for symbol, name in DOW_COMPONENTS[:top_n]
            ]
            return pd.DataFrame(rows)

        fdr = _load_fdr()
        if fdr is None:
            return pd.DataFrame(columns=["티커", "종목명", "시장", "시가총액"])

        listing = fdr.StockListing(market_meta.listing_source)
        if listing.empty:
            return pd.DataFrame(columns=["티커", "종목명", "시장", "시가총액"])

        code_column = "Symbol" if "Symbol" in listing.columns else "Code"
        name_column = "Name"
        marcap_column = next(
            (
                column
                for column in ["Market Cap", "Marcap", "MarketCap", "시가총액"]
                if column in listing.columns
            ),
            None,
        )
        if code_column not in listing.columns or name_column not in listing.columns:
            return pd.DataFrame(columns=["티커", "종목명", "시장", "시가총액"])

        if marcap_column:
            listing = listing.sort_values(marcap_column, ascending=False)
            market_caps = pd.to_numeric(listing[marcap_column], errors="coerce").fillna(0).astype("int64")
        else:
            market_caps = pd.Series([0] * len(listing), index=listing.index, dtype="int64")

        listing = listing.head(top_n)
        market_caps = market_caps.loc[listing.index]
        return pd.DataFrame(
            {
                "티커": listing[code_column].astype(str),
                "종목명": listing[name_column].astype(str),
                "시장": market,
                "시가총액": market_caps,
            }
        )

    def _get_domestic_daily_ohlcv(self, ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
        payload = self.client.get(
            path="/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice",
            tr_id="FHKST03010100",
            params={
                "FID_COND_MRKT_DIV_CODE": "J",
                "FID_INPUT_ISCD": ticker,
                "FID_INPUT_DATE_1": start_date,
                "FID_INPUT_DATE_2": end_date,
                "FID_PERIOD_DIV_CODE": "D",
                "FID_ORG_ADJ_PRC": "1",
            },
        )
        rows = _as_frame(payload, "output2")
        if rows.empty:
            rows = _as_frame(payload, "output")
        if rows.empty:
            return _empty_ohlcv()

        rows["date"] = pd.to_datetime(
            rows.apply(
                lambda row: _pick_first(row, "stck_bsop_date", "xymd", "bas_dt"),
                axis=1,
            ),
            format="%Y%m%d",
            errors="coerce",
        )
        rows["close"] = rows.apply(
            lambda row: _to_float(_pick_first(row, "stck_clpr", "ovrs_nmix_prpr", "clos", "last")),
            axis=1,
        )
        rows["volume"] = rows.apply(
            lambda row: _to_float(_pick_first(row, "acml_vol", "tvol", "vol")),
            axis=1,
        )
        normalized = rows.dropna(subset=["date", "close"]).copy()
        if normalized.empty:
            return _empty_ohlcv()

        normalized = normalized.sort_values("date").set_index("date")
        return normalized[["close", "volume"]].rename(columns={"close": "종가", "volume": "거래량"})

    def _get_overseas_daily_ohlcv(self, ticker: str, market: str) -> pd.DataFrame:
        market_meta = OVERSEAS_MARKET_MAP[market]
        payload = self.client.get(
            path="/uapi/overseas-price/v1/quotations/dailyprice",
            tr_id="HHDFS76240000",
            params={
                "AUTH": "",
                "EXCD": market_meta.detail_exchange_code,
                "SYMB": ticker,
                "GUBN": "0",
                "BYMD": "",
                "MODP": "1",
            },
        )
        rows = _as_frame(payload, "output2")
        if rows.empty:
            rows = _as_frame(payload, "output")
        if rows.empty:
            return _empty_ohlcv()

        rows["date"] = pd.to_datetime(
            rows.apply(lambda row: _pick_first(row, "xymd", "date", "bas_dt"), axis=1),
            format="%Y%m%d",
            errors="coerce",
        )
        rows["close"] = rows.apply(
            lambda row: _to_float(_pick_first(row, "clos", "last", "ovrs_nmix_prpr")),
            axis=1,
        )
        rows["volume"] = rows.apply(
            lambda row: _to_float(_pick_first(row, "tvol", "vol", "acml_vol")),
            axis=1,
        )
        normalized = rows.dropna(subset=["date", "close"]).copy()
        if normalized.empty:
            return _empty_ohlcv()

        normalized = normalized.sort_values("date").set_index("date")
        return normalized[["close", "volume"]].rename(columns={"close": "종가", "volume": "거래량"})

    def get_domestic_quote(self, ticker: str) -> dict[str, Any]:
        payload = self.client.get(
            path="/uapi/domestic-stock/v1/quotations/inquire-price",
            tr_id="FHKST01010100",
            params={
                "FID_COND_MRKT_DIV_CODE": "J",
                "FID_INPUT_ISCD": ticker,
            },
        )
        rows = _as_frame(payload, "output")
        if rows.empty:
            return {}
        row = rows.iloc[0]
        return {
            "ticker": ticker,
            "name": _pick_first(row, "hts_kor_isnm", "bstp_kor_isnm"),
            "close": _to_float(_pick_first(row, "stck_prpr", "stck_clpr")),
            "volume": _to_int(_pick_first(row, "acml_vol")),
            "as_of": datetime.utcnow().isoformat(),
        }

    def get_overseas_quote(self, ticker: str, market: str) -> dict[str, Any]:
        market_meta = OVERSEAS_MARKET_MAP[market]
        payload = self.client.get(
            path="/uapi/overseas-price/v1/quotations/price-detail",
            tr_id="HHDFS76200200",
            params={
                "AUTH": "",
                "EXCD": market_meta.detail_exchange_code,
                "SYMB": ticker,
            },
        )
        rows = _as_frame(payload, "output")
        if rows.empty:
            return {}
        row = rows.iloc[0]
        return {
            "ticker": ticker,
            "name": _pick_first(row, "e_icod", "rsym", "symb"),
            "close": _to_float(_pick_first(row, "last", "clos", "pvol")),
            "volume": _to_int(_pick_first(row, "tvol", "vol")),
            "as_of": datetime.utcnow().isoformat(),
        }
