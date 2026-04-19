from __future__ import annotations

import os
import json
import sqlite3
import time
from datetime import date, timedelta
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable
import re

import pandas as pd
import requests


_PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB_PATH = _PROJECT_ROOT / "data" / "financial_data.db"
FINMIND_V4_DATA_URL = "https://api.finmindtrade.com/api/v4/data"

# SQLite table names
TABLE_TAIWAN_STOCK_PRICE = "taiwan_stock_price"
TABLE_GOVERNMENT_BONDS_YIELD = "government_bonds_yield"
TABLE_TAIWAN_TOTAL_INSTITUTIONAL_INVESTORS = "taiwan_stock_total_institutional_investors"
TABLE_TAIWAN_STOCK_INSTITUTIONAL_INVESTORS_BUY_SELL = "taiwan_stock_institutional_investors_buy_sell"
TABLE_MONTHLY_REVENUE = "monthly_revenue"
TABLE_FINANCIAL_STATEMENTS = "financial_statements"
TABLE_BALANCE_SHEET = "taiwan_stock_balance_sheet"
TABLE_CASH_FLOWS_STATEMENT = "taiwan_stock_cash_flows_statement"
TABLE_TAIWAN_FUTURES_INSTITUTIONAL_INVESTORS = "taiwan_futures_institutional_investors"

# ~2.5 years for YoY comparisons and charts
FUNDAMENTAL_LOOKBACK_DAYS = int(365 * 2.5)
TABLE_TAIWAN_STOCK_MARGIN_PURCHASE_SHORT_SALE = "taiwan_stock_margin_purchase_short_sale"
TABLE_TAIWAN_STOCK_DAY_TRADING = "taiwan_stock_day_trading"
TABLE_MARKET_DAILY_SNAPSHOT = "market_daily_snapshot"
TABLE_TAIWAN_EXCHANGE_RATE = "taiwan_exchange_rate"
TABLE_US_STOCK_PRICE_INDEX = "us_stock_price_index"


def _to_date_str(value: Any) -> str:
    """
    Normalize date-like values to `YYYY-MM-DD` string.
    FinMind typically returns date as a string already; this is just defensive.
    """
    if pd.isna(value):
        return ""
    if isinstance(value, str):
        # assume already formatted
        return value[:10]
    # pandas Timestamp, datetime, etc.
    return pd.to_datetime(value).strftime("%Y-%m-%d")


def _extract_data_from_finmind(payload: Any) -> pd.DataFrame:
    """
    FinMind v4 typically responds with a JSON object containing a `data` array.
    We keep this tolerant to small response-shape differences.
    """
    if isinstance(payload, dict):
        if "data" in payload and isinstance(payload["data"], list):
            return pd.DataFrame(payload["data"])
        # Some endpoints may return the list directly under another key
        for k in ("result", "rows", "values"):
            if k in payload and isinstance(payload[k], list):
                return pd.DataFrame(payload[k])
    if isinstance(payload, list):
        return pd.DataFrame(payload)
    raise ValueError("Unexpected FinMind response format (no 'data' list found).")


def finmind_get_dataset_v4(
    *,
    token: str,
    dataset: str,
    data_id: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    timeout_seconds: int = 30,
) -> pd.DataFrame:
    """
    Call FinMind v4 Data API and return a DataFrame.
    """
    if not token:
        raise ValueError("FinMind token is required.")

    # 防呆：確保 token 沒有換行/不可見字元。
    # 不做「過度刪字元」以免誤刪 base64 token 本體。
    token = str(token).strip()
    # Remove whitespace/control chars commonly introduced by copy/paste
    token = re.sub(r"\s+", "", token)

    params: dict[str, Any] = {"dataset": dataset}
    if data_id:
        params["data_id"] = data_id
    if start_date:
        params["start_date"] = start_date
    if end_date:
        params["end_date"] = end_date

    # At least one of start_date/end_date should be set.
    if not start_date and not end_date:
        raise ValueError("Either start_date or end_date must be provided.")

    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(
        FINMIND_V4_DATA_URL,
        headers=headers,
        params=params,
        timeout=timeout_seconds,
    )
    if resp.status_code != 200:
        raise RuntimeError(
            f"FinMind API failed: status={resp.status_code}, body={resp.text[:500]}"
        )

    payload = resp.json()
    df = _extract_data_from_finmind(payload)
    return df


def ensure_data_dir(db_path: str | Path = DEFAULT_DB_PATH) -> Path:
    """
    Ensure the directory for SQLite db file exists.
    """
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return db_path


def get_db_connection(db_path: str | Path = DEFAULT_DB_PATH) -> sqlite3.Connection:
    """
    Create (or open) SQLite connection and apply sane defaults for concurrency.
    """
    db_path = ensure_data_dir(db_path)
    conn = sqlite3.connect(db_path.as_posix())
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def _quote_ident(ident: str) -> str:
    # Very small helper to prevent accidental SQL injection when forming identifiers.
    # Only allow alphanumerics and underscores.
    if not ident.replace("_", "").isalnum():
        raise ValueError(f"Unsafe SQL identifier: {ident}")
    return ident


def fetch_taiwan_stock_price_with_cache(
    *,
    token: str,
    stock_id: str,
    start_date: str,
    end_date: str,
    db_path: str | Path = DEFAULT_DB_PATH,
    table_name: str = TABLE_TAIWAN_STOCK_PRICE,
    api_delay_seconds: float = 0.0,
) -> pd.DataFrame:
    """
    Read-through cache for `TaiwanStockPrice`.

    Logic:
    - Query SQLite to see if we already cover [start_date, end_date] for this stock.
    - If covered, return cached DataFrame (via pandas.read_sql).
    - Otherwise, call FinMind API, deduplicate, and append into SQLite.
    """
    if not stock_id:
        raise ValueError("stock_id is required.")

    table_name = _quote_ident(table_name)
    start_date = str(start_date)
    end_date = str(end_date)

    # 1) Try read from cache
    with get_db_connection(db_path) as conn:
        # Ensure table exists even if empty; pandas.read_sql would error on missing table.
        # We rely on to_sql to create the table later; cache-hit check will just fail.
        try:
            cover = pd.read_sql_query(
                f"""
                SELECT
                  MIN(date) AS min_date,
                  MAX(date) AS max_date,
                  COUNT(*) AS row_count
                FROM {table_name}
                WHERE stock_id = ?
                  AND date BETWEEN ? AND ?
                """,
                conn,
                params=[stock_id, start_date, end_date],
            )
        except Exception:
            cover = pd.DataFrame({"min_date": [None], "max_date": [None], "row_count": [0]})

        min_date = cover.loc[0, "min_date"]
        max_date = cover.loc[0, "max_date"]
        row_count = int(cover.loc[0, "row_count"] or 0)

        # Coverage check:
        # - if DB has rows and min/max span the requested interval.
        # Note: this doesn't detect intra-range gaps; it is a pragmatic cache strategy.
        if row_count > 0 and min_date is not None and max_date is not None:
            if str(min_date) <= start_date and str(max_date) >= end_date:
                df_cached = pd.read_sql_query(
                    f"SELECT * FROM {table_name} WHERE stock_id = ? AND date BETWEEN ? AND ?",
                    conn,
                    params=[stock_id, start_date, end_date],
                )
                if not df_cached.empty:
                    if "date" in df_cached.columns:
                        df_cached["date"] = df_cached["date"].map(_to_date_str)
                    if "stock_id" in df_cached.columns:
                        df_cached["stock_id"] = df_cached["stock_id"].astype(str)
                    return df_cached.sort_values("date")

        # 2) Cache miss -> fetch from API
        if api_delay_seconds > 0:
            time.sleep(api_delay_seconds)

        df_api = finmind_get_dataset_v4(
            token=token,
            dataset="TaiwanStockPrice",
            data_id=stock_id,
            start_date=start_date,
            end_date=end_date,
        )

        if df_api.empty:
            return df_api

        # 3) Normalize + dedup
        if "date" in df_api.columns:
            df_api["date"] = df_api["date"].map(_to_date_str)
        if "stock_id" in df_api.columns:
            df_api["stock_id"] = df_api["stock_id"].astype(str)

        # Deduplicate on primary identifiers used for caching.
        if {"stock_id", "date"}.issubset(df_api.columns):
            df_api = df_api.drop_duplicates(subset=["stock_id", "date"])

        # 4) Prevent duplicates by deleting the overlapping window first.
        # Table may not exist yet (first run). In that case, skip delete;
        # `to_sql(..., if_exists="append")` will create the table.
        try:
            conn.execute(
                f"DELETE FROM {table_name} WHERE stock_id = ? AND date BETWEEN ? AND ?",
                (stock_id, start_date, end_date),
            )
            conn.commit()
        except sqlite3.OperationalError:
            pass

        df_api.to_sql(table_name, conn, if_exists="append", index=False)

        # 5) Return normalized
        return df_api.sort_values("date").reset_index(drop=True)


def fetch_government_bonds_yield_with_cache(
    *,
    token: str,
    start_date: str,
    end_date: str,
    data_id: str = "United States 10-Year",
    table_name: str = TABLE_GOVERNMENT_BONDS_YIELD,
    db_path: str | Path = DEFAULT_DB_PATH,
    api_delay_seconds: float = 0.0,
) -> pd.DataFrame:
    """
    Read-through cache for `GovernmentBondsYield` (US 10Y).

    Cache key:
      - (name, date)
    """
    table_name = _quote_ident(table_name)
    start_date = str(start_date)
    end_date = str(end_date)

    name_key = data_id

    with get_db_connection(db_path) as conn:
        # Cache hit check
        try:
            cover = pd.read_sql_query(
                f"""
                SELECT
                  MIN(date) AS min_date,
                  MAX(date) AS max_date,
                  COUNT(*) AS row_count
                FROM {table_name}
                WHERE name = ?
                  AND date BETWEEN ? AND ?
                """,
                conn,
                params=[name_key, start_date, end_date],
            )
        except Exception:
            cover = pd.DataFrame({"min_date": [None], "max_date": [None], "row_count": [0]})

        min_date = cover.loc[0, "min_date"]
        max_date = cover.loc[0, "max_date"]
        row_count = int(cover.loc[0, "row_count"] or 0)

        if row_count > 0 and min_date is not None and max_date is not None:
            if str(min_date) <= start_date and str(max_date) >= end_date:
                df_cached = pd.read_sql_query(
                    f"SELECT * FROM {table_name} WHERE name = ? AND date BETWEEN ? AND ?",
                    conn,
                    params=[name_key, start_date, end_date],
                )
                if not df_cached.empty:
                    if "date" in df_cached.columns:
                        df_cached["date"] = df_cached["date"].map(_to_date_str)
                    return df_cached.sort_values("date")

        if api_delay_seconds > 0:
            time.sleep(api_delay_seconds)

        df_api = finmind_get_dataset_v4(
            token=token,
            dataset="GovernmentBondsYield",
            data_id=data_id,
            start_date=start_date,
            end_date=end_date,
        )
        if df_api.empty:
            return df_api

        if "date" in df_api.columns:
            df_api["date"] = df_api["date"].map(_to_date_str)
        if "name" in df_api.columns:
            df_api["name"] = df_api["name"].astype(str)
        if "value" in df_api.columns:
            df_api["value"] = pd.to_numeric(df_api["value"], errors="coerce")

        if {"name", "date"}.issubset(df_api.columns):
            df_api = df_api.drop_duplicates(subset=["name", "date"])

        try:
            conn.execute(
                f"DELETE FROM {table_name} WHERE name = ? AND date BETWEEN ? AND ?",
                (name_key, start_date, end_date),
            )
            conn.commit()
        except sqlite3.OperationalError:
            pass

        df_api.to_sql(table_name, conn, if_exists="append", index=False)
        return df_api.sort_values("date").reset_index(drop=True)


def fetch_taiwan_exchange_rate_usd_with_cache(
    *,
    token: str,
    start_date: str,
    end_date: str,
    data_id: str = "USD",
    table_name: str = TABLE_TAIWAN_EXCHANGE_RATE,
    db_path: str | Path = DEFAULT_DB_PATH,
    api_delay_seconds: float = 0.0,
) -> pd.DataFrame:
    """
    台銀外幣對台幣（FinMind: `TaiwanExchangeRate`，`data_id`=USD，即常見 USD/TWD 參考）。
    SQLite：`taiwan_exchange_rate`，以 (currency, date) 去重。
    """
    table_name = _quote_ident(table_name)
    start_date = str(start_date)
    end_date = str(end_date)
    cur = str(data_id).upper()

    with get_db_connection(db_path) as conn:
        try:
            cover = pd.read_sql_query(
                f"""
                SELECT MIN(date) AS min_date, MAX(date) AS max_date, COUNT(*) AS row_count
                FROM {table_name}
                WHERE currency = ? AND date BETWEEN ? AND ?
                """,
                conn,
                params=[cur, start_date, end_date],
            )
        except Exception:
            cover = pd.DataFrame({"min_date": [None], "max_date": [None], "row_count": [0]})

        min_date = cover.loc[0, "min_date"]
        max_date = cover.loc[0, "max_date"]
        row_count = int(cover.loc[0, "row_count"] or 0)

        if row_count > 0 and min_date is not None and max_date is not None:
            if str(min_date) <= start_date and str(max_date) >= end_date:
                df_cached = pd.read_sql_query(
                    f"SELECT * FROM {table_name} WHERE currency = ? AND date BETWEEN ? AND ?",
                    conn,
                    params=[cur, start_date, end_date],
                )
                if not df_cached.empty and "date" in df_cached.columns:
                    df_cached["date"] = df_cached["date"].map(_to_date_str)
                    return df_cached.sort_values("date")

        if api_delay_seconds > 0:
            time.sleep(api_delay_seconds)

        df_api = finmind_get_dataset_v4(
            token=token,
            dataset="TaiwanExchangeRate",
            data_id=cur,
            start_date=start_date,
            end_date=end_date,
        )
        if df_api.empty:
            return df_api

        if "date" in df_api.columns:
            df_api["date"] = df_api["date"].map(_to_date_str)
        if "currency" in df_api.columns:
            df_api["currency"] = df_api["currency"].astype(str)
        for c in ("cash_buy", "cash_sell", "spot_buy", "spot_sell"):
            if c in df_api.columns:
                df_api[c] = pd.to_numeric(df_api[c], errors="coerce")

        if {"currency", "date"}.issubset(df_api.columns):
            df_api = df_api.drop_duplicates(subset=["currency", "date"])

        try:
            conn.execute(
                f"DELETE FROM {table_name} WHERE currency = ? AND date BETWEEN ? AND ?",
                (cur, start_date, end_date),
            )
            conn.commit()
        except sqlite3.OperationalError:
            pass

        df_api.to_sql(table_name, conn, if_exists="append", index=False)
        return df_api.sort_values("date").reset_index(drop=True)


def fetch_us_stock_index_with_cache(
    *,
    token: str,
    stock_id: str,
    start_date: str,
    end_date: str,
    table_name: str = TABLE_US_STOCK_PRICE_INDEX,
    db_path: str | Path = DEFAULT_DB_PATH,
    api_delay_seconds: float = 0.0,
) -> pd.DataFrame:
    """
    美股指數日線（FinMind: `USStockPrice`），例如 `^SOX`（費半）、`^IXIC`（那斯達克）。
    """
    if not stock_id:
        raise ValueError("stock_id is required.")

    table_name = _quote_ident(table_name)
    start_date = str(start_date)
    end_date = str(end_date)
    stock_id = str(stock_id)

    with get_db_connection(db_path) as conn:
        try:
            cover = pd.read_sql_query(
                f"""
                SELECT MIN(date) AS min_date, MAX(date) AS max_date, COUNT(*) AS row_count
                FROM {table_name}
                WHERE stock_id = ? AND date BETWEEN ? AND ?
                """,
                conn,
                params=[stock_id, start_date, end_date],
            )
        except Exception:
            cover = pd.DataFrame({"min_date": [None], "max_date": [None], "row_count": [0]})

        min_date = cover.loc[0, "min_date"]
        max_date = cover.loc[0, "max_date"]
        row_count = int(cover.loc[0, "row_count"] or 0)

        if row_count > 0 and min_date is not None and max_date is not None:
            if str(min_date) <= start_date and str(max_date) >= end_date:
                df_cached = pd.read_sql_query(
                    f"SELECT * FROM {table_name} WHERE stock_id = ? AND date BETWEEN ? AND ?",
                    conn,
                    params=[stock_id, start_date, end_date],
                )
                if not df_cached.empty:
                    if "date" in df_cached.columns:
                        df_cached["date"] = df_cached["date"].map(_to_date_str)
                    if "stock_id" in df_cached.columns:
                        df_cached["stock_id"] = df_cached["stock_id"].astype(str)
                    return df_cached.sort_values("date")

        if api_delay_seconds > 0:
            time.sleep(api_delay_seconds)

        df_api = finmind_get_dataset_v4(
            token=token,
            dataset="USStockPrice",
            data_id=stock_id,
            start_date=start_date,
            end_date=end_date,
        )
        if df_api.empty:
            return df_api

        if "date" in df_api.columns:
            df_api["date"] = df_api["date"].map(_to_date_str)
        if "stock_id" in df_api.columns:
            df_api["stock_id"] = df_api["stock_id"].astype(str)
        close_col = "close" if "close" in df_api.columns else ("Close" if "Close" in df_api.columns else None)
        if close_col:
            df_api[close_col] = pd.to_numeric(df_api[close_col], errors="coerce")
        if "open" in df_api.columns:
            df_api["open"] = pd.to_numeric(df_api["open"], errors="coerce")

        if {"stock_id", "date"}.issubset(df_api.columns):
            df_api = df_api.drop_duplicates(subset=["stock_id", "date"])

        try:
            conn.execute(
                f"DELETE FROM {table_name} WHERE stock_id = ? AND date BETWEEN ? AND ?",
                (stock_id, start_date, end_date),
            )
            conn.commit()
        except sqlite3.OperationalError:
            pass

        df_api.to_sql(table_name, conn, if_exists="append", index=False)
        return df_api.sort_values("date").reset_index(drop=True)


def fetch_taiwan_stock_total_institutional_investors_with_cache(
    *,
    token: str,
    start_date: str,
    end_date: str,
    table_name: str = TABLE_TAIWAN_TOTAL_INSTITUTIONAL_INVESTORS,
    db_path: str | Path = DEFAULT_DB_PATH,
    api_delay_seconds: float = 0.0,
) -> pd.DataFrame:
    """
    Read-through cache for `TaiwanStockTotalInstitutionalInvestors`.

    Cache key:
      - date (table stores multiple `name` categories per date)
    """
    table_name = _quote_ident(table_name)
    start_date = str(start_date)
    end_date = str(end_date)

    with get_db_connection(db_path) as conn:
        try:
            cover = pd.read_sql_query(
                f"""
                SELECT
                  MIN(date) AS min_date,
                  MAX(date) AS max_date,
                  COUNT(*) AS row_count
                FROM {table_name}
                WHERE date BETWEEN ? AND ?
                """,
                conn,
                params=[start_date, end_date],
            )
        except Exception:
            cover = pd.DataFrame({"min_date": [None], "max_date": [None], "row_count": [0]})

        min_date = cover.loc[0, "min_date"]
        max_date = cover.loc[0, "max_date"]
        row_count = int(cover.loc[0, "row_count"] or 0)

        if row_count > 0 and min_date is not None and max_date is not None:
            if str(min_date) <= start_date and str(max_date) >= end_date:
                df_cached = pd.read_sql_query(
                    f"SELECT * FROM {table_name} WHERE date BETWEEN ? AND ?",
                    conn,
                    params=[start_date, end_date],
                )
                if not df_cached.empty:
                    if "date" in df_cached.columns:
                        df_cached["date"] = df_cached["date"].map(_to_date_str)
                    return df_cached.sort_values("date")

        if api_delay_seconds > 0:
            time.sleep(api_delay_seconds)

        df_api = finmind_get_dataset_v4(
            token=token,
            dataset="TaiwanStockTotalInstitutionalInvestors",
            start_date=start_date,
            end_date=end_date,
        )
        if df_api.empty:
            return df_api

        if "date" in df_api.columns:
            df_api["date"] = df_api["date"].map(_to_date_str)
        for c in ("buy", "sell"):
            if c in df_api.columns:
                df_api[c] = pd.to_numeric(df_api[c], errors="coerce")
        if "name" in df_api.columns:
            df_api["name"] = df_api["name"].astype(str)

        if {"date", "name"}.issubset(df_api.columns):
            df_api = df_api.drop_duplicates(subset=["date", "name"])

        try:
            conn.execute(
                f"DELETE FROM {table_name} WHERE date BETWEEN ? AND ?",
                (start_date, end_date),
            )
            conn.commit()
        except sqlite3.OperationalError:
            pass

        df_api.to_sql(table_name, conn, if_exists="append", index=False)
        return df_api.sort_values("date").reset_index(drop=True)


def fetch_taiwan_stock_institutional_investors_buy_sell_with_cache(
    *,
    token: str,
    stock_id: str,
    start_date: str,
    end_date: str,
    table_name: str = TABLE_TAIWAN_STOCK_INSTITUTIONAL_INVESTORS_BUY_SELL,
    db_path: str | Path = DEFAULT_DB_PATH,
    api_delay_seconds: float = 0.0,
) -> pd.DataFrame:
    """
    Read-through cache for `TaiwanStockInstitutionalInvestorsBuySell` (per stock).

    Cache key:
      - (stock_id, date, name)
    """
    if not stock_id:
        raise ValueError("stock_id is required.")

    table_name = _quote_ident(table_name)
    start_date = str(start_date)
    end_date = str(end_date)
    stock_id = str(stock_id)

    with get_db_connection(db_path) as conn:
        try:
            cover = pd.read_sql_query(
                f"""
                SELECT
                  MIN(date) AS min_date,
                  MAX(date) AS max_date,
                  COUNT(*) AS row_count
                FROM {table_name}
                WHERE stock_id = ?
                  AND date BETWEEN ? AND ?
                """,
                conn,
                params=[stock_id, start_date, end_date],
            )
        except Exception:
            cover = pd.DataFrame({"min_date": [None], "max_date": [None], "row_count": [0]})

        min_date = cover.loc[0, "min_date"]
        max_date = cover.loc[0, "max_date"]
        row_count = int(cover.loc[0, "row_count"] or 0)

        if row_count > 0 and min_date is not None and max_date is not None:
            if str(min_date) <= start_date and str(max_date) >= end_date:
                df_cached = pd.read_sql_query(
                    f"""
                    SELECT * FROM {table_name}
                    WHERE stock_id = ? AND date BETWEEN ? AND ?
                    """,
                    conn,
                    params=[stock_id, start_date, end_date],
                )
                if not df_cached.empty:
                    if "date" in df_cached.columns:
                        df_cached["date"] = df_cached["date"].map(_to_date_str)
                    if "stock_id" in df_cached.columns:
                        df_cached["stock_id"] = df_cached["stock_id"].astype(str)
                    return df_cached.sort_values("date")

        if api_delay_seconds > 0:
            time.sleep(api_delay_seconds)

        df_api = finmind_get_dataset_v4(
            token=token,
            dataset="TaiwanStockInstitutionalInvestorsBuySell",
            data_id=stock_id,
            start_date=start_date,
            end_date=end_date,
        )
        if df_api.empty:
            return df_api

        if "date" in df_api.columns:
            df_api["date"] = df_api["date"].map(_to_date_str)
        if "stock_id" in df_api.columns:
            df_api["stock_id"] = df_api["stock_id"].astype(str)
        for c in ("buy", "sell"):
            if c in df_api.columns:
                df_api[c] = pd.to_numeric(df_api[c], errors="coerce")
        if "name" in df_api.columns:
            df_api["name"] = df_api["name"].astype(str)

        if {"stock_id", "date", "name"}.issubset(df_api.columns):
            df_api = df_api.drop_duplicates(subset=["stock_id", "date", "name"])

        try:
            conn.execute(
                f"DELETE FROM {table_name} WHERE stock_id = ? AND date BETWEEN ? AND ?",
                (stock_id, start_date, end_date),
            )
            conn.commit()
        except sqlite3.OperationalError:
            pass

        df_api.to_sql(table_name, conn, if_exists="append", index=False)
        return df_api.sort_values("date").reset_index(drop=True)


def _normalize_financial_long_df(df: pd.DataFrame, stock_id: str) -> pd.DataFrame:
    """Long-format 財報：確保有 stock_id / date / type / value，並以 (stock_id, date, type) 去重。"""
    if df.empty:
        return df
    out = df.copy()
    if "stock_id" not in out.columns:
        out["stock_id"] = str(stock_id)
    if "date" not in out.columns:
        raise KeyError("Financial long dataframe missing column: date")
    if "type" not in out.columns:
        raise KeyError("Financial long dataframe missing column: type")
    out["date"] = out["date"].map(_to_date_str)
    out["stock_id"] = out["stock_id"].astype(str)
    out["type"] = out["type"].astype(str)
    if "value" in out.columns:
        out["value"] = pd.to_numeric(out["value"], errors="coerce")
    if "origin_name" in out.columns:
        out["origin_name"] = out["origin_name"].astype(str)
    dedup_cols = ["stock_id", "date", "type"]
    return out.drop_duplicates(subset=dedup_cols, keep="last")


def fetch_fundamental_data(
    stock_list: list[str],
    *,
    token: str | None = None,
    db_path: str | Path = DEFAULT_DB_PATH,
    api_delay_seconds: float = 0.0,
    end_date: date | None = None,
) -> dict[str, int]:
    """
    Fetch fundamental data and write to SQLite with dedup.

    Datasets (FinMind v4):
      - TaiwanStockMonthRevenue -> `monthly_revenue`
      - TaiwanStockFinancialStatements (綜合損益) -> `financial_statements`
      - TaiwanStockBalanceSheet (資產負債) -> `taiwan_stock_balance_sheet`
      - TaiwanStockCashFlowsStatement (現金流量) -> `taiwan_stock_cash_flows_statement`

    Lookback: ~2.5 years (FUNDAMENTAL_LOOKBACK_DAYS).

    Dedup:
      - monthly_revenue: (stock_id, revenue_year, revenue_month)
      - each long-format table: (stock_id, date, type)
    """
    empty_ret = {
        "monthly_revenue_rows_written": 0,
        "financial_statements_rows_written": 0,
        "balance_sheet_rows_written": 0,
        "cash_flows_statement_rows_written": 0,
    }
    if not stock_list:
        return empty_ret.copy()

    token = token or os.getenv("FINMIND_TOKEN") or os.getenv("FINMIND_API_TOKEN") or ""
    if not token:
        raise RuntimeError("Missing FinMind token. Provide `token=` or set FINMIND_TOKEN in config/.env.")

    end_date = end_date or date.today()
    start_date = end_date - timedelta(days=FUNDAMENTAL_LOOKBACK_DAYS)
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")

    monthly_subset = ["stock_id", "revenue_year", "revenue_month"]
    statement_dedup = ["stock_id", "date", "type"]

    # ---- monthly revenue ----
    monthly_frames: list[pd.DataFrame] = []
    for sid in stock_list:
        df_new = finmind_get_dataset_v4(
            token=token,
            dataset="TaiwanStockMonthRevenue",
            data_id=str(sid),
            start_date=start_date_str,
            end_date=end_date_str,
        )
        if df_new.empty:
            continue

        if "stock_id" not in df_new.columns:
            df_new["stock_id"] = str(sid)

        for col in ("revenue_year", "revenue_month"):
            if col not in df_new.columns:
                raise KeyError(f"TaiwanStockMonthRevenue missing column: {col}")
            df_new[col] = pd.to_numeric(df_new[col], errors="coerce").astype("Int64")

        df_new = df_new.dropna(subset=monthly_subset)
        monthly_frames.append(df_new)
        if api_delay_seconds > 0:
            time.sleep(api_delay_seconds)

    df_new_monthly = pd.concat(monthly_frames, ignore_index=True) if monthly_frames else pd.DataFrame()

    def _fetch_statement_dataset(dataset_name: str, sid: str) -> pd.DataFrame:
        df_new = finmind_get_dataset_v4(
            token=token,
            dataset=dataset_name,
            data_id=str(sid),
            start_date=start_date_str,
            end_date=end_date_str,
        )
        if df_new.empty:
            return df_new
        return _normalize_financial_long_df(df_new, sid)

    # ---- 綜合損益 / 資產負債 / 現金流量（長表）----
    income_frames: list[pd.DataFrame] = []
    balance_frames: list[pd.DataFrame] = []
    cash_frames: list[pd.DataFrame] = []

    for sid in stock_list:
        inc = _fetch_statement_dataset("TaiwanStockFinancialStatements", sid)
        if not inc.empty:
            income_frames.append(inc)

        bal = _fetch_statement_dataset("TaiwanStockBalanceSheet", sid)
        if not bal.empty:
            balance_frames.append(bal)

        cf = _fetch_statement_dataset("TaiwanStockCashFlowsStatement", sid)
        if not cf.empty:
            cash_frames.append(cf)

        if api_delay_seconds > 0:
            time.sleep(api_delay_seconds)

    df_new_income = pd.concat(income_frames, ignore_index=True) if income_frames else pd.DataFrame()
    df_new_balance = pd.concat(balance_frames, ignore_index=True) if balance_frames else pd.DataFrame()
    df_new_cash = pd.concat(cash_frames, ignore_index=True) if cash_frames else pd.DataFrame()

    def _read_existing(conn: sqlite3.Connection, table_name: str) -> pd.DataFrame:
        table_ident = _quote_ident(table_name)
        placeholders = ",".join(["?"] * len(stock_list))
        try:
            return pd.read_sql_query(
                f"SELECT * FROM {table_ident} WHERE stock_id IN ({placeholders})",
                conn,
                params=[str(x) for x in stock_list],
            )
        except (sqlite3.OperationalError, pd.errors.DatabaseError):
            return pd.DataFrame()

    def _merge_long_table(
        conn: sqlite3.Connection,
        table_key: str,
        df_new: pd.DataFrame,
    ) -> int:
        if df_new.empty:
            return 0
        if not set(statement_dedup + ["value"]).issubset(df_new.columns):
            missing = [c for c in statement_dedup + ["value"] if c not in df_new.columns]
            raise KeyError(f"{table_key} missing columns: {missing}")

        df_old = _read_existing(conn, table_key)
        if not df_old.empty:
            df_old = df_old.copy()
            for c in statement_dedup:
                if c in df_old.columns:
                    df_old[c] = df_old[c].astype(str) if c == "stock_id" else df_old[c]
            if "date" in df_old.columns:
                df_old["date"] = df_old["date"].map(_to_date_str)
            if "type" in df_old.columns:
                df_old["type"] = df_old["type"].astype(str)

        df_all = (
            pd.concat([df_old, df_new], ignore_index=True) if not df_old.empty else df_new
        )
        df_all = df_all.drop_duplicates(subset=statement_dedup, keep="last")

        try:
            placeholders = ",".join(["?"] * len(stock_list))
            conn.execute(
                f"DELETE FROM {_quote_ident(table_key)} WHERE stock_id IN ({placeholders})",
                [str(x) for x in stock_list],
            )
            conn.commit()
        except sqlite3.OperationalError:
            pass

        if df_all.empty:
            return 0
        df_all.to_sql(table_key, conn, if_exists="append", index=False)
        return len(df_all)

    monthly_written = 0
    income_written = 0
    balance_written = 0
    cash_written = 0

    with get_db_connection(db_path) as conn:
        if not df_new_monthly.empty:
            if not set(monthly_subset).issubset(df_new_monthly.columns):
                missing = [c for c in monthly_subset if c not in df_new_monthly.columns]
                raise KeyError(f"monthly_revenue missing dedup columns: {missing}")

            df_old_monthly = _read_existing(conn, TABLE_MONTHLY_REVENUE)
            if not df_old_monthly.empty:
                df_old_monthly = df_old_monthly.copy()

            df_all_monthly = (
                pd.concat([df_old_monthly, df_new_monthly], ignore_index=True)
                if not df_old_monthly.empty
                else df_new_monthly
            )
            df_all_monthly = df_all_monthly.drop_duplicates(subset=monthly_subset, keep="last")

            try:
                placeholders = ",".join(["?"] * len(stock_list))
                conn.execute(
                    f"DELETE FROM {_quote_ident(TABLE_MONTHLY_REVENUE)} WHERE stock_id IN ({placeholders})",
                    [str(x) for x in stock_list],
                )
                conn.commit()
            except sqlite3.OperationalError:
                pass

            if not df_all_monthly.empty:
                df_all_monthly.to_sql(TABLE_MONTHLY_REVENUE, conn, if_exists="append", index=False)
                monthly_written = len(df_all_monthly)

        income_written = _merge_long_table(conn, TABLE_FINANCIAL_STATEMENTS, df_new_income)
        balance_written = _merge_long_table(conn, TABLE_BALANCE_SHEET, df_new_balance)
        cash_written = _merge_long_table(conn, TABLE_CASH_FLOWS_STATEMENT, df_new_cash)

    return {
        "monthly_revenue_rows_written": int(monthly_written),
        "financial_statements_rows_written": int(income_written),
        "balance_sheet_rows_written": int(balance_written),
        "cash_flows_statement_rows_written": int(cash_written),
    }


def fetch_taiwan_futures_institutional_investors_with_cache(
    *,
    token: str,
    data_id: str = "TX",
    start_date: str,
    end_date: str,
    table_name: str = TABLE_TAIWAN_FUTURES_INSTITUTIONAL_INVESTORS,
    db_path: str | Path = DEFAULT_DB_PATH,
    api_delay_seconds: float = 0.0,
) -> pd.DataFrame:
    """
    Read-through cache for `TaiwanFuturesInstitutionalInvestors`.

    FinMind schema (per docs):
      - name, date, institutional_investors,
        long_open_interest_balance_volume, short_open_interest_balance_volume, ...

    Cache key:
      - (name, date, institutional_investors)
    """
    table_name = _quote_ident(table_name)
    data_id = str(data_id)
    start_date = str(start_date)
    end_date = str(end_date)

    with get_db_connection(db_path) as conn:
        try:
            cover = pd.read_sql_query(
                f"""
                SELECT
                  MIN(date) AS min_date,
                  MAX(date) AS max_date,
                  COUNT(*) AS row_count
                FROM {table_name}
                WHERE name = ?
                  AND date BETWEEN ? AND ?
                """,
                conn,
                params=[data_id, start_date, end_date],
            )
        except Exception:
            cover = pd.DataFrame({"min_date": [None], "max_date": [None], "row_count": [0]})

        min_date = cover.loc[0, "min_date"]
        max_date = cover.loc[0, "max_date"]
        row_count = int(cover.loc[0, "row_count"] or 0)

        if row_count > 0 and min_date is not None and max_date is not None:
            if str(min_date) <= start_date and str(max_date) >= end_date:
                df_cached = pd.read_sql_query(
                    f"SELECT * FROM {table_name} WHERE name = ? AND date BETWEEN ? AND ?",
                    conn,
                    params=[data_id, start_date, end_date],
                )
                if not df_cached.empty and "date" in df_cached.columns:
                    df_cached["date"] = df_cached["date"].map(_to_date_str)
                return df_cached.sort_values(["date", "institutional_investors"]) if not df_cached.empty else df_cached

        if api_delay_seconds > 0:
            time.sleep(api_delay_seconds)

        df_api = finmind_get_dataset_v4(
            token=token,
            dataset="TaiwanFuturesInstitutionalInvestors",
            data_id=data_id,
            start_date=start_date,
            end_date=end_date,
        )
        if df_api.empty:
            return df_api

        if "date" in df_api.columns:
            df_api["date"] = df_api["date"].map(_to_date_str)
        if "name" in df_api.columns:
            df_api["name"] = df_api["name"].astype(str)
        if "institutional_investors" in df_api.columns:
            df_api["institutional_investors"] = df_api["institutional_investors"].astype(str)

        dedup_cols = ["name", "date", "institutional_investors"]
        if set(dedup_cols).issubset(df_api.columns):
            df_api = df_api.drop_duplicates(subset=dedup_cols, keep="last")

        try:
            conn.execute(
                f"DELETE FROM {table_name} WHERE name = ? AND date BETWEEN ? AND ?",
                (data_id, start_date, end_date),
            )
            conn.commit()
        except sqlite3.OperationalError:
            pass

        df_api.to_sql(table_name, conn, if_exists="append", index=False)
        return df_api.sort_values(["date", "institutional_investors"]).reset_index(drop=True)


def fetch_taiwan_stock_margin_purchase_short_sale_with_cache(
    *,
    token: str,
    stock_id: str,
    start_date: str,
    end_date: str,
    table_name: str = TABLE_TAIWAN_STOCK_MARGIN_PURCHASE_SHORT_SALE,
    db_path: str | Path = DEFAULT_DB_PATH,
    api_delay_seconds: float = 0.0,
) -> pd.DataFrame:
    """
    Read-through cache for `TaiwanStockMarginPurchaseShortSale` (per stock).

    Cache key:
      - (stock_id, date)
    """
    if not stock_id:
        raise ValueError("stock_id is required.")

    table_name = _quote_ident(table_name)
    stock_id = str(stock_id)
    start_date = str(start_date)
    end_date = str(end_date)

    with get_db_connection(db_path) as conn:
        try:
            cover = pd.read_sql_query(
                f"""
                SELECT
                  MIN(date) AS min_date,
                  MAX(date) AS max_date,
                  COUNT(*) AS row_count
                FROM {table_name}
                WHERE stock_id = ?
                  AND date BETWEEN ? AND ?
                """,
                conn,
                params=[stock_id, start_date, end_date],
            )
        except Exception:
            cover = pd.DataFrame({"min_date": [None], "max_date": [None], "row_count": [0]})

        min_date = cover.loc[0, "min_date"]
        max_date = cover.loc[0, "max_date"]
        row_count = int(cover.loc[0, "row_count"] or 0)

        if row_count > 0 and min_date is not None and max_date is not None:
            if str(min_date) <= start_date and str(max_date) >= end_date:
                df_cached = pd.read_sql_query(
                    f"SELECT * FROM {table_name} WHERE stock_id = ? AND date BETWEEN ? AND ?",
                    conn,
                    params=[stock_id, start_date, end_date],
                )
                if not df_cached.empty and "date" in df_cached.columns:
                    df_cached["date"] = df_cached["date"].map(_to_date_str)
                return df_cached.sort_values("date") if not df_cached.empty else df_cached

        if api_delay_seconds > 0:
            time.sleep(api_delay_seconds)

        df_api = finmind_get_dataset_v4(
            token=token,
            dataset="TaiwanStockMarginPurchaseShortSale",
            data_id=stock_id,
            start_date=start_date,
            end_date=end_date,
        )
        if df_api.empty:
            return df_api

        if "date" in df_api.columns:
            df_api["date"] = df_api["date"].map(_to_date_str)
        if "stock_id" in df_api.columns:
            df_api["stock_id"] = df_api["stock_id"].astype(str)

        if {"stock_id", "date"}.issubset(df_api.columns):
            df_api = df_api.drop_duplicates(subset=["stock_id", "date"], keep="last")

        try:
            conn.execute(
                f"DELETE FROM {table_name} WHERE stock_id = ? AND date BETWEEN ? AND ?",
                (stock_id, start_date, end_date),
            )
            conn.commit()
        except sqlite3.OperationalError:
            pass

        df_api.to_sql(table_name, conn, if_exists="append", index=False)
        return df_api.sort_values("date").reset_index(drop=True)


def fetch_taiwan_stock_day_trading_with_cache(
    *,
    token: str,
    stock_id: str,
    start_date: str,
    end_date: str,
    table_name: str = TABLE_TAIWAN_STOCK_DAY_TRADING,
    db_path: str | Path = DEFAULT_DB_PATH,
    api_delay_seconds: float = 0.0,
) -> pd.DataFrame:
    """
    Read-through cache for `TaiwanStockDayTrading` (per stock).

    FinMind schema (per docs):
      - stock_id, date, BuyAfterSale, Volume, BuyAmount, SellAmount

    Cache key:
      - (stock_id, date)
    """
    if not stock_id:
        raise ValueError("stock_id is required.")

    table_name = _quote_ident(table_name)
    stock_id = str(stock_id)
    start_date = str(start_date)
    end_date = str(end_date)

    with get_db_connection(db_path) as conn:
        try:
            cover = pd.read_sql_query(
                f"""
                SELECT
                  MIN(date) AS min_date,
                  MAX(date) AS max_date,
                  COUNT(*) AS row_count
                FROM {table_name}
                WHERE stock_id = ?
                  AND date BETWEEN ? AND ?
                """,
                conn,
                params=[stock_id, start_date, end_date],
            )
        except Exception:
            cover = pd.DataFrame({"min_date": [None], "max_date": [None], "row_count": [0]})

        min_date = cover.loc[0, "min_date"]
        max_date = cover.loc[0, "max_date"]
        row_count = int(cover.loc[0, "row_count"] or 0)

        if row_count > 0 and min_date is not None and max_date is not None:
            if str(min_date) <= start_date and str(max_date) >= end_date:
                df_cached = pd.read_sql_query(
                    f"SELECT * FROM {table_name} WHERE stock_id = ? AND date BETWEEN ? AND ?",
                    conn,
                    params=[stock_id, start_date, end_date],
                )
                if not df_cached.empty and "date" in df_cached.columns:
                    df_cached["date"] = df_cached["date"].map(_to_date_str)
                return df_cached.sort_values("date") if not df_cached.empty else df_cached

        if api_delay_seconds > 0:
            time.sleep(api_delay_seconds)

        df_api = finmind_get_dataset_v4(
            token=token,
            dataset="TaiwanStockDayTrading",
            data_id=stock_id,
            start_date=start_date,
            end_date=end_date,
        )
        if df_api.empty:
            return df_api

        if "date" in df_api.columns:
            df_api["date"] = df_api["date"].map(_to_date_str)
        if "stock_id" in df_api.columns:
            df_api["stock_id"] = df_api["stock_id"].astype(str)
        for c in ("Volume", "BuyAmount", "SellAmount"):
            if c in df_api.columns:
                df_api[c] = pd.to_numeric(df_api[c], errors="coerce")
        if "BuyAfterSale" in df_api.columns:
            df_api["BuyAfterSale"] = df_api["BuyAfterSale"].astype(str)

        if {"stock_id", "date"}.issubset(df_api.columns):
            df_api = df_api.drop_duplicates(subset=["stock_id", "date"], keep="last")

        try:
            conn.execute(
                f"DELETE FROM {table_name} WHERE stock_id = ? AND date BETWEEN ? AND ?",
                (stock_id, start_date, end_date),
            )
            conn.commit()
        except sqlite3.OperationalError:
            pass

        df_api.to_sql(table_name, conn, if_exists="append", index=False)
        return df_api.sort_values("date").reset_index(drop=True)


def _finmind_full_market_tier_denied(error_text: str) -> bool:
    """
    FinMind 不帶 data_id 的 TaiwanStockPrice 全市場查詢，通常僅贊助以上方案可用；
    免費帳會回 400 並提示更新 user level。
    """
    t = error_text.lower()
    if "status=400" not in t and "400" not in t:
        return False
    return any(
        k in t
        for k in (
            "user level",
            "sponsor",
            "your level is register",
            "update your user level",
        )
    )


def fetch_market_hot_stocks_data(
    days: int = 5,
    *,
    token: str | None = None,
    end_date: date | None = None,
    db_path: str | Path = DEFAULT_DB_PATH,
    api_delay_seconds: float = 3.0,
    max_calendar_lookback: int = 60,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """
    依「單日全市場」策略抓取 TaiwanStockPrice（不帶 data_id，僅帶日期），
    合併最近 `days` 個有資料的交易日，並寫入 `market_daily_snapshot`（以 date + stock_id 去重）。

    注意：FinMind 可能僅對特定會員開放「不指定 data_id」之全市場查詢；失敗日會記錄於回傳的 errors。

    Returns:
        (merged_df, {
            "rows_written": int,
            "trading_dates_used": list[str],
            "errors": list[str],
            "tier_denied": bool,  # 帳戶不支援全市場 API，已停止重試
        })
    """
    token = token or os.getenv("FINMIND_TOKEN") or os.getenv("FINMIND_API_TOKEN") or ""
    if not token:
        raise RuntimeError("Missing FinMind token. Set FINMIND_TOKEN in config/.env.")

    days = max(1, int(days))
    end = end_date or date.today()
    daily_frames: list[pd.DataFrame] = []
    trading_dates_used: list[str] = []
    errors: list[str] = []
    tier_denied = False

    d = end
    scanned = 0
    while len(trading_dates_used) < days and scanned < max_calendar_lookback:
        ds = d.strftime("%Y-%m-%d")
        try:
            df_day = finmind_get_dataset_v4(
                token=token,
                dataset="TaiwanStockPrice",
                data_id=None,
                start_date=ds,
                end_date=ds,
            )
        except RuntimeError as exc:
            err_text = str(exc)
            errors.append(f"{ds}: {exc}")
            if _finmind_full_market_tier_denied(err_text):
                tier_denied = True
                errors.append(
                    "(已停止後續日期請求) 全市場 TaiwanStockPrice（不帶 data_id）"
                    " 需 FinMind 贊助或更高方案；免費註冊帳無法使用。熱門股區塊將略過。"
                )
                break
            df_day = pd.DataFrame()

        if df_day is not None and not df_day.empty:
            df_day = df_day.copy()
            if "date" in df_day.columns:
                df_day["date"] = df_day["date"].map(_to_date_str)
            if "stock_id" in df_day.columns:
                df_day["stock_id"] = df_day["stock_id"].astype(str)
            daily_frames.append(df_day)
            trading_dates_used.append(ds)

        d = d - timedelta(days=1)
        scanned += 1
        if api_delay_seconds > 0:
            time.sleep(float(api_delay_seconds))

    if not daily_frames:
        return pd.DataFrame(), {
            "rows_written": 0,
            "trading_dates_used": [],
            "errors": errors,
            "tier_denied": tier_denied,
        }

    merged = pd.concat(daily_frames, ignore_index=True)
    if "date" in merged.columns:
        merged["date"] = merged["date"].map(_to_date_str)
    if "stock_id" in merged.columns:
        merged["stock_id"] = merged["stock_id"].astype(str)
    merged = merged.drop_duplicates(subset=["stock_id", "date"], keep="last")

    table = _quote_ident(TABLE_MARKET_DAILY_SNAPSHOT)
    rows_written = 0
    with get_db_connection(db_path) as conn:
        uniq_dates = merged["date"].unique().tolist()
        if uniq_dates:
            placeholders = ",".join(["?"] * len(uniq_dates))
            try:
                conn.execute(
                    f"DELETE FROM {table} WHERE date IN ({placeholders})",
                    [str(x) for x in uniq_dates],
                )
                conn.commit()
            except sqlite3.OperationalError:
                pass
        merged.to_sql(TABLE_MARKET_DAILY_SNAPSHOT, conn, if_exists="append", index=False)
        rows_written = len(merged)

    return merged, {
        "rows_written": rows_written,
        "trading_dates_used": trading_dates_used,
        "errors": errors,
        "tier_denied": False,
    }

