"""
data_processor.py

Transform FinMind raw data into structured signals and a final push message.

This file is intentionally implemented with pandas vectorization/rolling.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

def load_settings(settings_path: str | Path | None = None) -> Dict[str, Any]:
    """
    Load and validate `config/settings.json`.

    By default, it resolves to:
      <project_root>/config/settings.json
    where project_root is inferred as the parent directory of `src/`.
    """
    if settings_path is None:
        project_root = Path(__file__).resolve().parent.parent
        settings_path = project_root / "config" / "settings.json"
    else:
        settings_path = Path(settings_path)

    if not settings_path.exists():
        raise FileNotFoundError(f"settings file not found: {settings_path}")

    try:
        with settings_path.open("r", encoding="utf-8") as f:
            settings: Dict[str, Any] = json.load(f)
    except json.JSONDecodeError as e:
        raise ValueError(f"settings.json is not valid JSON: {settings_path}") from e

    required_top_keys = [
        "system_config",
        "futures_alerts",
        "watch_list",
        "technical_alerts",
        "chip_alerts",
        "macro_thresholds",
    ]
    missing = [k for k in required_top_keys if k not in settings]
    if missing:
        raise KeyError(f"settings.json missing required keys: {missing}")

    # Basic type checks (lightweight; can be extended later)
    if not isinstance(settings["watch_list"], list) or not all(
        isinstance(x, str) for x in settings["watch_list"]
    ):
        raise TypeError("settings['watch_list'] must be a list of strings (stock codes)")

    technical = settings.get("technical_alerts", {})
    ma_tracking = technical.get("ma_tracking", [])
    if not isinstance(ma_tracking, list) or not all(isinstance(x, (int, float)) for x in ma_tracking):
        raise TypeError(
            "settings['technical_alerts']['ma_tracking'] must be a list of numbers (will be cast to int)"
        )
    # Normalize to ints (e.g. 20.0 -> 20)
    technical["ma_tracking"] = [int(x) for x in ma_tracking]

    chip = settings.get("chip_alerts", {})
    if not isinstance(chip.get("volume_threshold_shares", 0), (int, float)):
        raise TypeError("settings['chip_alerts']['volume_threshold_shares'] must be numeric")

    return settings


def _require_columns(df, required: Iterable[str], df_name: str) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise KeyError(f"{df_name} missing columns: {missing}")


def _normalize_date_column(df: "pd.DataFrame", date_col: str = "date") -> "pd.DataFrame":
    import pandas as pd

    if date_col in df.columns:
        df = df.copy()
        df[date_col] = df[date_col].map(lambda x: str(x)[:10])
    return df


def compute_technical_signals(
    prices_df: "pd.DataFrame",
    settings: Dict[str, Any],
) -> "pd.DataFrame":
    """
    Compute technical indicators and extract the latest row per stock.

    Input columns expected (FinMind TaiwanStockPrice):
      - date, stock_id, close, Trading_Volume
    """
    import pandas as pd

    _require_columns(prices_df, ["date", "stock_id", "close", "Trading_Volume"], "prices_df")

    df = prices_df.copy()
    df["date"] = df["date"].map(lambda x: str(x)[:10])
    df["stock_id"] = df["stock_id"].astype(str)
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df["Trading_Volume"] = pd.to_numeric(df["Trading_Volume"], errors="coerce")
    df = df.sort_values(["stock_id", "date"])

    ma_tracking: List[int] = settings["technical_alerts"]["ma_tracking"]
    ma_tracking = sorted(set(int(x) for x in ma_tracking))
    vol_breakout_ratio_threshold = float(settings["technical_alerts"]["volume_breakout_ratio"])
    price_change_threshold = float(settings["technical_alerts"]["price_change_pct_threshold"])

    # Rolling MA for `close`
    # (short list loops are fine; no per-row Python loops)
    for win in ma_tracking:
        df[f"ma_{win}"] = df.groupby("stock_id")["close"].transform(
            lambda s: s.rolling(window=win, min_periods=win).mean()
        )

    # Price change pct vs previous day close
    df["pct_change"] = (
        df.groupby("stock_id")["close"].transform(lambda s: s.pct_change() * 100.0)
    )

    # Volume breakout ratio:
    #   condition: Trading_Volume > (5-day rolling mean of volume * volume_breakout_ratio)
    volume_ma_win = 5
    df["volume_ma_5"] = df.groupby("stock_id")["Trading_Volume"].transform(
        lambda s: s.rolling(window=volume_ma_win, min_periods=volume_ma_win).mean()
    )
    df["volume_breakout_ratio"] = df["Trading_Volume"] / df["volume_ma_5"]

    # Take latest record per stock
    latest = df.groupby("stock_id", as_index=False).tail(1).reset_index(drop=True)

    # Alerts booleans
    latest["alert_price_change"] = latest["pct_change"] >= price_change_threshold
    latest["alert_volume_breakout"] = (
        latest["Trading_Volume"] > latest["volume_ma_5"] * vol_breakout_ratio_threshold
    )

    # MA alignment: close above all tracked MAs, and MAs stacked upward (short > long)
    ma_cols = [f"ma_{w}" for w in ma_tracking]
    # Close above all MAs
    close_above_all = True
    for c in ma_cols:
        close_above_all = close_above_all & (latest["close"] > latest[c])

    # Short-term MA above longer-term MAs
    ma_sorted = ma_tracking  # already ascending
    ma_stacked = True
    for i in range(len(ma_sorted) - 1):
        ma_stacked = ma_stacked & (latest[f"ma_{ma_sorted[i]}"] > latest[f"ma_{ma_sorted[i+1]}"])

    latest["alert_ma_bullish"] = close_above_all & ma_stacked

    # Keep some useful fields for formatting
    keep_cols = [
        "stock_id",
        "date",
        "close",
        "pct_change",
        "Trading_Volume",
        "volume_ma_5",
        "volume_breakout_ratio",
        *ma_cols,
        "alert_price_change",
        "alert_volume_breakout",
        "alert_ma_bullish",
    ]
    return latest[keep_cols]


def compute_chip_signals(
    chip_df: "pd.DataFrame",
    settings: Dict[str, Any],
    *,
    foreign_name: str = "Foreign_Investor",
    trust_name: str = "Investment_Trust",
) -> "pd.DataFrame":
    """
    Compute chip signals using consecutive net buy streaks:
      - foreign investors: consecutive days where (buy - sell) > 0
      - investment trusts: consecutive days where (buy - sell) > 0

    Input columns expected (FinMind TaiwanStockInstitutionalInvestorsBuySell):
      - date, stock_id, buy, sell, name
    """
    import pandas as pd

    _require_columns(chip_df, ["date", "stock_id", "buy", "sell", "name"], "chip_df")

    df = chip_df.copy()
    df["date"] = df["date"].map(lambda x: str(x)[:10])
    df["stock_id"] = df["stock_id"].astype(str)
    df["buy"] = pd.to_numeric(df["buy"], errors="coerce")
    df["sell"] = pd.to_numeric(df["sell"], errors="coerce")
    df["net_buy"] = df["buy"] - df["sell"]

    # Pivot net_buy into investor-type columns
    df = df[df["name"].isin([foreign_name, trust_name])]
    if df.empty:
        # Return empty frame with expected columns
        return pd.DataFrame(
            columns=[
                "stock_id",
                "foreign_consecutive_net_buy_days",
                "trust_consecutive_net_buy_days",
                "foreign_net_buy_alert",
                "trust_net_buy_alert",
            ]
        )

    net = (
        df.pivot_table(
            index=["stock_id", "date"],
            columns="name",
            values="net_buy",
            aggfunc="sum",
        )
        .reset_index()
    )
    # Ensure both columns exist
    if foreign_name not in net.columns:
        net[foreign_name] = 0
    if trust_name not in net.columns:
        net[trust_name] = 0

    # Sort for rolling computation
    net = net.sort_values(["stock_id", "date"])

    foreign_days_needed = int(settings["chip_alerts"]["foreign_investor_net_buy_days"])
    trust_days_needed = int(settings["chip_alerts"]["investment_trust_net_buy_days"])
    threshold = float(settings["chip_alerts"]["volume_threshold_shares"])

    # Current net_buy series
    foreign_series = net[foreign_name].fillna(0)
    trust_series = net[trust_name].fillna(0)

    # A) Last N days all have net_buy > 0 (vectorized via groupby.rolling)
    foreign_pos_int = (foreign_series > 0).astype(int)
    trust_pos_int = (trust_series > 0).astype(int)

    # foreign: rolling sum over the last N days, must equal N
    foreign_pos_sum = (
        net.assign(_foreign_pos_int=foreign_pos_int)
        .groupby("stock_id", group_keys=False)["_foreign_pos_int"]
        .rolling(window=foreign_days_needed, min_periods=foreign_days_needed)
        .sum()
        .reset_index(level=0, drop=True)
    )
    net["_foreign_consecutive_pos_ok"] = foreign_pos_sum == foreign_days_needed

    # trust
    trust_pos_sum = (
        net.assign(_trust_pos_int=trust_pos_int)
        .groupby("stock_id", group_keys=False)["_trust_pos_int"]
        .rolling(window=trust_days_needed, min_periods=trust_days_needed)
        .sum()
        .reset_index(level=0, drop=True)
    )
    net["_trust_consecutive_pos_ok"] = trust_pos_sum == trust_days_needed

    # B) Latest day's net_buy >= volume_threshold_shares
    net["_foreign_latest_net_buy_ok"] = foreign_series >= threshold
    net["_trust_latest_net_buy_ok"] = trust_series >= threshold

    net["foreign_net_buy_alert"] = net["_foreign_consecutive_pos_ok"] & net["_foreign_latest_net_buy_ok"]
    net["trust_net_buy_alert"] = net["_trust_consecutive_pos_ok"] & net["_trust_latest_net_buy_ok"]

    # Also compute actual positive streak length (for display)
    foreign_pos = foreign_series > 0
    trust_pos = trust_series > 0
    stock_ids = net["stock_id"]
    break_foreign = (~foreign_pos).groupby(stock_ids).cumsum()
    foreign_streak = foreign_pos.groupby(break_foreign).cumsum().where(foreign_pos, 0)
    break_trust = (~trust_pos).groupby(stock_ids).cumsum()
    trust_streak = trust_pos.groupby(break_trust).cumsum().where(trust_pos, 0)
    net["foreign_streak_len"] = foreign_streak.astype(int)
    net["trust_streak_len"] = trust_streak.astype(int)

    latest = net.groupby("stock_id", as_index=False).tail(1).reset_index(drop=True)

    latest["foreign_consecutive_net_buy_days"] = latest["foreign_streak_len"]
    latest["trust_consecutive_net_buy_days"] = latest["trust_streak_len"]
    latest["foreign_net_buy_alert"] = latest["foreign_net_buy_alert"].astype(bool)
    latest["trust_net_buy_alert"] = latest["trust_net_buy_alert"].astype(bool)

    return latest[
        [
            "stock_id",
            "foreign_consecutive_net_buy_days",
            "trust_consecutive_net_buy_days",
            "foreign_net_buy_alert",
            "trust_net_buy_alert",
        ]
    ]


def compute_day_trade_ratio_signals(
    *,
    prices_df: "pd.DataFrame",
    day_trading_df: "pd.DataFrame",
    settings: Dict[str, Any],
) -> "pd.DataFrame":
    """
    Compute day trade ratio per stock for the latest date:
      day_trade_ratio = TaiwanStockDayTrading.Volume / TaiwanStockPrice.Trading_Volume

    Output columns:
      - stock_id
      - date
      - day_trade_ratio
      - alert_day_trade_overheated (ratio > day_trade_ratio_threshold)
    """
    import pandas as pd

    _require_columns(prices_df, ["stock_id", "date", "Trading_Volume"], "prices_df")
    _require_columns(day_trading_df, ["stock_id", "date", "Volume"], "day_trading_df")

    df_prices = prices_df.copy()
    df_day = day_trading_df.copy()

    df_prices["date"] = df_prices["date"].map(lambda x: str(x)[:10])
    df_day["date"] = df_day["date"].map(lambda x: str(x)[:10])

    df_prices["stock_id"] = df_prices["stock_id"].astype(str)
    df_day["stock_id"] = df_day["stock_id"].astype(str)

    df_prices["Trading_Volume"] = pd.to_numeric(df_prices["Trading_Volume"], errors="coerce")
    df_day["Volume"] = pd.to_numeric(df_day["Volume"], errors="coerce")

    merged = df_day.merge(
        df_prices[["stock_id", "date", "Trading_Volume"]],
        on=["stock_id", "date"],
        how="left",
    )

    merged["day_trade_ratio"] = merged["Volume"] / merged["Trading_Volume"]

    # Keep latest row per stock
    merged = merged.sort_values(["stock_id", "date"]).groupby("stock_id", as_index=False).tail(1)

    threshold = float(settings["technical_alerts"]["day_trade_ratio_threshold"])
    merged["alert_day_trade_overheated"] = merged["day_trade_ratio"] > threshold

    return merged[["stock_id", "date", "day_trade_ratio", "alert_day_trade_overheated"]]


def compute_margin_reduction_signals(
    *,
    margin_df: "pd.DataFrame",
    settings: Dict[str, Any],
) -> "pd.DataFrame":
    """
    Compute consecutive financing reduction days per stock.

    We define a "reduction day" as:
      MarginPurchaseBalance(today) < MarginPurchaseBalance(previous day)

    Then consecutive N days reduction is checked via vectorized streak length.

    Output columns:
      - stock_id
      - date (latest row date)
      - margin_reduction_consecutive_days
      - alert_margin_reduction
    """
    import pandas as pd

    _require_columns(
        margin_df,
        ["stock_id", "date", "MarginPurchaseBalance"],
        "margin_df",
    )

    df = margin_df.copy()
    df["date"] = df["date"].map(lambda x: str(x)[:10])
    df["stock_id"] = df["stock_id"].astype(str)

    df["MarginPurchaseBalance"] = pd.to_numeric(df["MarginPurchaseBalance"], errors="coerce")
    df = df.sort_values(["stock_id", "date"])

    prev_balance = df.groupby("stock_id")["MarginPurchaseBalance"].shift(1)
    df["is_reduction_day"] = df["MarginPurchaseBalance"] < prev_balance
    df = df.sort_values(["stock_id", "date"])

    pos = df["is_reduction_day"].fillna(False)
    stock_ids = df["stock_id"]

    # Vectorized consecutive streak length ending at each row
    break_id = (~pos).groupby(stock_ids).cumsum()
    streak_len = pos.groupby(break_id).cumsum()
    df["margin_reduction_consecutive_days"] = streak_len.where(pos, 0).astype(int)

    latest = df.groupby("stock_id", as_index=False).tail(1).reset_index(drop=True)

    n = int(settings["chip_alerts"]["margin_reduction_days"])
    latest["alert_margin_reduction"] = latest["margin_reduction_consecutive_days"] >= n

    return latest[
        ["stock_id", "date", "margin_reduction_consecutive_days", "alert_margin_reduction"]
    ]


def compute_foreign_futures_net_oi_signal(
    *,
    futures_df: "pd.DataFrame",
    settings: Dict[str, Any],
    foreign_name: str = "外資",
) -> Dict[str, Any]:
    """
    Compute foreign futures net open interest (contracts/口數) from latest date:
      foreign_net_oi = long_open_interest_balance_volume - short_open_interest_balance_volume

    Alert if foreign_net_oi <= settings['futures_alerts']['foreign_futures_net_oi_alert'].
    """
    import pandas as pd

    _require_columns(
        futures_df,
        [
            "date",
            "institutional_investors",
            "long_open_interest_balance_volume",
            "short_open_interest_balance_volume",
        ],
        "futures_df",
    )

    df = futures_df.copy()
    df["date"] = df["date"].map(lambda x: str(x)[:10])
    df["institutional_investors"] = df["institutional_investors"].astype(str)

    df["long_open_interest_balance_volume"] = pd.to_numeric(
        df["long_open_interest_balance_volume"], errors="coerce"
    )
    df["short_open_interest_balance_volume"] = pd.to_numeric(
        df["short_open_interest_balance_volume"], errors="coerce"
    )

    df = df[df["institutional_investors"] == foreign_name]
    if df.empty:
        return {
            "foreign_futures_net_oi": None,
            "foreign_futures_net_oi_alert": False,
            "futures_date": None,
        }

    df = df.sort_values("date")
    latest_date = df["date"].iloc[-1]
    latest = df[df["date"] == latest_date].copy()

    # If multiple contracts appear on same date, aggregate by sum (defensive)
    long_oi = float(latest["long_open_interest_balance_volume"].sum())
    short_oi = float(latest["short_open_interest_balance_volume"].sum())
    foreign_net_oi = long_oi - short_oi

    threshold = float(settings["futures_alerts"]["foreign_futures_net_oi_alert"])
    alert = foreign_net_oi <= threshold

    return {
        "foreign_futures_net_oi": foreign_net_oi,
        "foreign_futures_net_oi_alert": alert,
        "futures_date": latest_date,
        "foreign_long_oi": long_oi,
        "foreign_short_oi": short_oi,
    }


def _latest_bond_yield(df: Optional["pd.DataFrame"]) -> tuple[Optional[float], Optional[str]]:
    import pandas as pd

    if df is None or df.empty:
        return None, None
    if "value" not in df.columns or "date" not in df.columns:
        return None, None
    m = df.copy()
    m["date"] = m["date"].map(lambda x: str(x)[:10])
    m["value"] = pd.to_numeric(m["value"], errors="coerce")
    m = m.dropna(subset=["value"]).sort_values("date")
    if m.empty:
        return None, None
    last = m.tail(1).iloc[0]
    return float(last["value"]), str(last["date"])


def _us_index_daily_pct(df: Optional["pd.DataFrame"]) -> tuple[Optional[float], Optional[str], Optional[float]]:
    """回傳 (當日漲跌幅%, 最新日期, 最新收盤)。"""
    import pandas as pd

    if df is None or df.empty:
        return None, None, None
    d = df.copy()
    d["date"] = pd.to_datetime(d["date"], errors="coerce")
    ccol = None
    for c in ("close", "Close", "adj_close", "Adj_Close"):
        if c in d.columns:
            ccol = c
            break
    if ccol is None:
        return None, None, None
    d[ccol] = pd.to_numeric(d[ccol], errors="coerce")
    d = d.dropna(subset=["date", ccol]).sort_values("date")
    if len(d) < 2:
        last = d.iloc[-1]
        return None, str(last["date"])[:10], float(last[ccol])
    last = d.iloc[-1]
    prev = d.iloc[-2]
    pct = (float(last[ccol]) / float(prev[ccol]) - 1.0) * 100.0 if float(prev[ccol]) != 0 else None
    return pct, str(last["date"])[:10], float(last[ccol])


def compute_macro_and_board_signals(
    macro_bonds_df: Optional["pd.DataFrame"],
    board_df: Optional["pd.DataFrame"],
    *,
    macro_bonds_2y_df: Optional["pd.DataFrame"] = None,
    twd_usd_fx_df: Optional["pd.DataFrame"] = None,
    us_sox_df: Optional["pd.DataFrame"] = None,
    us_ixic_df: Optional["pd.DataFrame"] = None,
    settings: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    總經／大盤：美債 10Y/2Y、USD/TWD、費半／那斯達克漲跌幅、門檻警示與費半「科技股承壓」標註。
    """
    import pandas as pd

    out: Dict[str, Any] = {}
    settings = settings or {}

    if macro_bonds_df is not None and not macro_bonds_df.empty:
        _require_columns(macro_bonds_df, ["date", "value"], "macro_bonds_df")
        m = macro_bonds_df.copy()
        m["date"] = m["date"].map(lambda x: str(x)[:10])
        m = m.sort_values("date")
        last = m.tail(1).iloc[0]
        out["us_10y_yield"] = float(last["value"])
        out["macro_date"] = str(last["date"])

    y2, d2 = _latest_bond_yield(macro_bonds_2y_df)
    if y2 is not None:
        out["us_2y_yield"] = y2
        out["us_2y_date"] = d2

    if twd_usd_fx_df is not None and not twd_usd_fx_df.empty:
        fx = twd_usd_fx_df.copy()
        fx["date"] = pd.to_datetime(fx["date"], errors="coerce")
        # 以即期賣出為代表匯率（常見報價）；無效則用現金賣出
        rate_col = None
        for c in ("spot_sell", "Spot_sell", "cash_sell"):
            if c in fx.columns:
                rate_col = c
                break
        if rate_col:
            fx["_rate"] = pd.to_numeric(fx[rate_col], errors="coerce")
        else:
            fx["_rate"] = pd.NA
        fx = fx.dropna(subset=["date", "_rate"]).sort_values("date")
        if not fx.empty and len(fx) >= 1:
            last_row = fx.iloc[-1]
            out["usd_twd_rate"] = float(last_row["_rate"])
            out["usd_twd_date"] = str(last_row["date"])[:10]
            if len(fx) >= 2:
                prev_r = float(fx.iloc[-2]["_rate"])
                cur_r = float(last_row["_rate"])
                if prev_r != 0:
                    out["usd_twd_daily_pct"] = (cur_r / prev_r - 1.0) * 100.0
                    # 匯率數字上升 = 同額美元需較多台幣 = 美元偏強 / 台幣貶值
                    out["usd_twd_fx_bias"] = (
                        "台幣貶值（美元偏強）" if out["usd_twd_daily_pct"] > 0 else "台幣升值（美元偏弱）"
                    )
                    if abs(out["usd_twd_daily_pct"]) < 1e-6:
                        out["usd_twd_fx_bias"] = "持平"

    thr = settings.get("macro_thresholds") or {}
    usd_ub = float(thr.get("usd_twd_upper_bound", 99))
    y10_ub = float(thr.get("us_10y_yield_upper_bound", 99))
    drop_thr = float(thr.get("us_index_drop_alert_pct", -2.0))

    out["alert_usd_twd_high"] = bool(
        out.get("usd_twd_rate") is not None and float(out["usd_twd_rate"]) >= usd_ub
    )
    out["alert_us_10y_high"] = bool(
        out.get("us_10y_yield") is not None and float(out["us_10y_yield"]) >= y10_ub
    )

    sox_pct, sox_d, _ = _us_index_daily_pct(us_sox_df)
    ixic_pct, ixic_d, _ = _us_index_daily_pct(us_ixic_df)
    if sox_pct is not None:
        out["sox_daily_pct"] = sox_pct
        out["sox_date"] = sox_d
    if ixic_pct is not None:
        out["ixic_daily_pct"] = ixic_pct
        out["ixic_date"] = ixic_d

    out["alert_us_tech_pressure"] = bool(sox_pct is not None and sox_pct <= drop_thr)
    if out["alert_us_tech_pressure"]:
        out["us_tech_pressure_tag"] = "[⚠️ 科技股承壓]"
    else:
        out["us_tech_pressure_tag"] = None

    if board_df is not None and not board_df.empty:
        _require_columns(board_df, ["date", "buy", "sell", "name"], "board_df")
        b = board_df.copy()
        b["date"] = b["date"].map(lambda x: str(x)[:10])
        b["net_buy"] = b["buy"] - b["sell"]

        dealer_names = {"Dealer_self", "Dealer_Hedging"}
        foreign_name = "Foreign_Investor"
        trust_name = "Investment_Trust"

        latest_date = str(b.sort_values("date").tail(1).iloc[0]["date"])
        bd = b[b["date"] == latest_date]

        net_foreign = bd.loc[bd["name"] == foreign_name, "net_buy"].sum()
        net_trust = bd.loc[bd["name"] == trust_name, "net_buy"].sum()
        net_dealers = bd.loc[bd["name"].isin(dealer_names), "net_buy"].sum()

        out["board_date"] = latest_date
        out["three_institutions_net_buy"] = float(net_foreign + net_trust + net_dealers)
        out["three_institutions_foreign_net_buy"] = float(net_foreign)
        out["three_institutions_trust_net_buy"] = float(net_trust)
        out["three_institutions_dealers_net_buy"] = float(net_dealers)

    return out


def format_global_macro_embed_value(
    macro_board_signals: Dict[str, Any],
    settings: Dict[str, Any],
) -> str:
    """
    Discord 欄位【🌐 總經與國際市場】純文字（Markdown 粗體用於跌幅過大之數字）。
    """
    lines: List[str] = []
    thr = settings.get("macro_thresholds") or {}
    usd_ub = float(thr.get("usd_twd_upper_bound", 32.5))
    y10_ub = float(thr.get("us_10y_yield_upper_bound", 4.5))
    drop_thr = float(thr.get("us_index_drop_alert_pct", -2.0))

    # 匯率 / 債市
    fx_line_parts: List[str] = []
    if macro_board_signals.get("usd_twd_rate") is not None:
        r = float(macro_board_signals["usd_twd_rate"])
        d = macro_board_signals.get("usd_twd_date", "")
        pct = macro_board_signals.get("usd_twd_daily_pct")
        bias = macro_board_signals.get("usd_twd_fx_bias", "")
        alert = macro_board_signals.get("alert_usd_twd_high", False)
        em = "🔴 " if alert else ""
        pct_s = f"{pct:+.2f}%" if pct is not None else "N/A"
        fx_line_parts.append(
            f"{em}**USD/TWD** `{r:.3f}`（{d}，日變動 {pct_s}｜{bias}）"
            + (f" ⚠️≥`{usd_ub}`" if alert else "")
        )
    else:
        fx_line_parts.append("**USD/TWD**：`N/A`")

    y10 = macro_board_signals.get("us_10y_yield")
    md10 = macro_board_signals.get("macro_date")
    a10 = macro_board_signals.get("alert_us_10y_high", False)
    if y10 is not None:
        em = "🔴 " if a10 else ""
        fx_line_parts.append(
            f"{em}**美債 10Y** `{y10:.2f}%`（{md10}）" + (f" ⚠️≥`{y10_ub}%`" if a10 else "")
        )
    else:
        fx_line_parts.append("**美債 10Y**：`N/A`")

    y2 = macro_board_signals.get("us_2y_yield")
    d2 = macro_board_signals.get("us_2y_date")
    if y2 is not None:
        fx_line_parts.append(f"**美債 2Y** `{y2:.2f}%`（{d2}）")
    else:
        fx_line_parts.append("**美債 2Y**：`N/A`")

    lines.append("**匯率／債市**")
    lines.append("\n".join(fx_line_parts))

    # 美股
    lines.append("")
    lines.append("**美股表現**")
    tag = macro_board_signals.get("us_tech_pressure_tag") or ""
    sox_pct = macro_board_signals.get("sox_daily_pct")
    ixic_pct = macro_board_signals.get("ixic_daily_pct")
    sox_d = macro_board_signals.get("sox_date", "")
    ixic_d = macro_board_signals.get("ixic_date", "")

    if sox_pct is not None:
        bold = sox_pct <= drop_thr
        num = f"**{sox_pct:+.2f}%**" if bold else f"`{sox_pct:+.2f}%`"
        lines.append(f"費半 SOX（{sox_d}）：{num} {tag}".strip())
    else:
        lines.append("費半 SOX：`N/A`")

    if ixic_pct is not None:
        bold = ixic_pct <= drop_thr
        num = f"**{ixic_pct:+.2f}%**" if bold else f"`{ixic_pct:+.2f}%`"
        lines.append(f"那斯達克 ^IXIC（{ixic_d}）：{num}")
    else:
        lines.append("那斯達克 ^IXIC：`N/A`")

    return "\n".join(lines)[:1024]


def _volume_shares_to_wan_zhang_label(shares: float) -> str:
    """成交量（股）→ 顯示為 萬張 / 張（1 張 = 1000 股）。"""
    import pandas as pd

    if shares is None or (isinstance(shares, float) and pd.isna(shares)):
        return "N/A"
    s = float(shares)
    zhang = s / 1000.0
    wan = zhang / 10000.0
    if wan >= 1.0:
        return f"{wan:.1f}萬張"
    if zhang >= 1.0:
        return f"{zhang:,.0f}張"
    return f"{s:,.0f}股"


def get_top_hot_stocks(
    df: "pd.DataFrame",
    top_n: int = 3,
    *,
    min_avg_volume: float = 5_000_000.0,
    min_trading_days: int = 5,
) -> "pd.DataFrame":
    """
    全市場多日股價長表：流動性篩選 + 近窗口累積報酬（最後收盤 / 最早開盤 - 1），取前 top_n。

    預期欄位：stock_id, date, open, close, Trading_Volume（可含 stock_name）。
    """
    import pandas as pd

    if df is None or df.empty:
        return pd.DataFrame(
            columns=[
                "stock_id",
                "stock_name",
                "cumulative_return_pct",
                "avg_volume_5d",
                "latest_volume_shares",
                "volume_label",
            ]
        )

    d = df.copy()
    if "stock_id" not in d.columns or "date" not in d.columns:
        raise KeyError("get_top_hot_stocks requires stock_id and date columns")

    vol_col = "Trading_Volume" if "Trading_Volume" in d.columns else ("Volume" if "Volume" in d.columns else None)
    open_col = "open" if "open" in d.columns else ("Open" if "Open" in d.columns else None)
    close_col = "close" if "close" in d.columns else ("Close" if "Close" in d.columns else None)
    if vol_col is None or open_col is None or close_col is None:
        raise KeyError(
            f"get_top_hot_stocks missing price/volume columns; got {list(d.columns)}"
        )

    d["date"] = pd.to_datetime(d["date"], errors="coerce")
    d[open_col] = pd.to_numeric(d[open_col], errors="coerce")
    d[close_col] = pd.to_numeric(d[close_col], errors="coerce")
    d[vol_col] = pd.to_numeric(d[vol_col], errors="coerce")
    d["stock_id"] = d["stock_id"].astype(str)
    d = d.dropna(subset=["date", open_col, close_col, vol_col])

    d = d.sort_values(["stock_id", "date"])
    n_per = d.groupby("stock_id").size()
    eligible = n_per[n_per >= min_trading_days].index
    d = d[d["stock_id"].isin(eligible)]
    if d.empty:
        return pd.DataFrame(
            columns=[
                "stock_id",
                "stock_name",
                "cumulative_return_pct",
                "avg_volume_5d",
                "latest_volume_shares",
                "volume_label",
            ]
        )

    first_open = d.groupby("stock_id")[open_col].first()
    last_close = d.groupby("stock_id")[close_col].last()
    avg_vol = d.groupby("stock_id")[vol_col].mean()
    last_vol = d.groupby("stock_id")[vol_col].last()
    if "stock_name" in d.columns:
        last_name = d.groupby("stock_id")["stock_name"].last()
    else:
        last_name = pd.Series(dtype=object)

    cum_ret = last_close / first_open.replace(0, pd.NA) - 1.0
    mask = avg_vol > float(min_avg_volume)
    cum_ret = cum_ret[mask]
    avg_vol = avg_vol[mask]
    last_vol = last_vol[mask]

    out = pd.DataFrame(
        {
            "stock_id": cum_ret.index.astype(str),
            "cumulative_return_pct": cum_ret.values * 100.0,
            "avg_volume_5d": avg_vol.values,
            "latest_volume_shares": last_vol.values,
        }
    )
    if not last_name.empty:
        out["stock_name"] = out["stock_id"].map(last_name).fillna("").astype(str)
    else:
        out["stock_name"] = ""

    out = out.sort_values("cumulative_return_pct", ascending=False).head(int(top_n))
    out["volume_label"] = out["latest_volume_shares"].map(_volume_shares_to_wan_zhang_label)
    return out.reset_index(drop=True)


def format_hot_stocks_discord_field_value(hot_df: "pd.DataFrame") -> str:
    """單一 Embed 欄位用多行文字。"""
    import pandas as pd

    if hot_df is None or hot_df.empty:
        return "（資料不足、API 未回傳全市場，或無符合流動性門檻之標的）"

    lines: List[str] = []
    for i, r in enumerate(hot_df.itertuples(index=False), 1):
        sid = str(getattr(r, "stock_id", ""))
        name = str(getattr(r, "stock_name", "") or "").strip()
        ret = float(getattr(r, "cumulative_return_pct", 0.0))
        vol_lbl = str(getattr(r, "volume_label", "N/A"))
        label = f"{name} ({sid})" if name else f"({sid})"
        lines.append(f"{i}. {label} | 近五日漲幅: {ret:+.1f}% | 今量: {vol_lbl}")
    return "\n".join(lines)[:1024]


def format_push_message(
    *,
    settings: Dict[str, Any],
    technical_df: "pd.DataFrame",
    chip_df_signals: "pd.DataFrame",
    macro_board_signals: Dict[str, Any],
    hot_stocks_field_text: Optional[str] = None,
    global_macro_text: Optional[str] = None,
) -> str:
    """
    Create a human-friendly Discord text message using 'exception report' principle:
    - Only triggered stocks are listed under 🚨 策略觸發名單
    - Non-triggered stocks are compressed into a single line under 📊 常規自選股狀態
    """
    import pandas as pd

    if technical_df.empty and chip_df_signals.empty:
        df = pd.DataFrame(columns=["stock_id"])
    elif technical_df.empty:
        df = chip_df_signals.copy()
    elif chip_df_signals.empty:
        df = technical_df.copy()
    else:
        df = technical_df.merge(chip_df_signals, on="stock_id", how="left")

    # Normalize boolean columns that may appear as NaN after merge
    for col in (
        "alert_price_change",
        "alert_volume_breakout",
        "alert_ma_bullish",
        "foreign_net_buy_alert",
        "trust_net_buy_alert",
    ):
        if col in df.columns:
            df[col] = df[col].fillna(False).astype(bool)

    df["foreign_consecutive_net_buy_days"] = (
        df.get("foreign_consecutive_net_buy_days", 0).fillna(0).astype(int)
    )
    df["trust_consecutive_net_buy_days"] = (
        df.get("trust_consecutive_net_buy_days", 0).fillna(0).astype(int)
    )

    # Header
    lines: List[str] = []
    lines.append("**台股與總經數據追蹤警示**")

    if macro_board_signals.get("board_date"):
        net = macro_board_signals.get("three_institutions_net_buy")
        date = macro_board_signals["board_date"]
        if net is None:
            lines.append(f"📈 **大盤(三大法人，{date})**: `N/A`")
        else:
            sign = "上漲動能" if net >= 0 else "偏弱動能"
            emoji = "🟢" if net >= 0 else "🔴"
            lines.append(f"{emoji} **大盤(三大法人，{date})**: `淨買超 {net:,.0f}`（{sign}）")

    if global_macro_text:
        lines.append("")
        lines.append("🌐 **【總經與國際市場】**")
        lines.append(global_macro_text)

    if hot_stocks_field_text:
        lines.append("")
        lines.append("🔥 **【近五日資金匯聚焦點】**")
        lines.append(hot_stocks_field_text)

    # Determine triggered vs normal
    technical_cols = ["alert_price_change", "alert_volume_breakout", "alert_ma_bullish"]
    chip_cols = ["foreign_net_buy_alert", "trust_net_buy_alert"]
    for _c in technical_cols + chip_cols:
        if _c not in df.columns:
            df[_c] = False
    df["has_technical_alert"] = df[technical_cols].any(axis=1)
    df["has_chip_alert"] = df[chip_cols].any(axis=1)
    df["has_any_alert"] = df["has_technical_alert"] | df["has_chip_alert"]

    # Sort triggered first
    df_trigger = df[df["has_any_alert"]].copy()
    df_normal = df[~df["has_any_alert"]].copy()

    ma_tracking = settings["technical_alerts"]["ma_tracking"]
    price_thr = float(settings["technical_alerts"]["price_change_pct_threshold"])
    vol_mul = float(settings["technical_alerts"]["volume_breakout_ratio"])
    chip_vol_thr = float(settings["chip_alerts"]["volume_threshold_shares"])

    # Strategy triggered list (exception report)
    lines.append("")
    lines.append("🚨 **【策略觸發名單】**")

    if df_trigger.empty:
        lines.append("（本輪無個股觸發技術/籌碼條件）")
    else:
        # Sort: technical first, then chip
        df_trigger = df_trigger.sort_values(
            by=["has_technical_alert", "has_chip_alert", "alert_price_change", "alert_volume_breakout", "alert_ma_bullish"],
            ascending=False,
        )

        for _, r in df_trigger.iterrows():
            stock_id = str(r["stock_id"])
            pct = float(r["pct_change"]) if pd.notna(r.get("pct_change")) else None
            close = float(r["close"]) if pd.notna(r.get("close")) else None
            vol = float(r.get("Trading_Volume")) if pd.notna(r.get("Trading_Volume")) else None
            vol_ratio = float(r.get("volume_breakout_ratio")) if pd.notna(r.get("volume_breakout_ratio")) else None

            # Build trigger labels
            labels: List[str] = []
            if bool(r.get("alert_price_change", False)):
                labels.append(f"📈 **漲跌幅** >= `{price_thr:.2f}%`")
            if bool(r.get("alert_volume_breakout", False)):
                labels.append(f"🔥 **爆量**：`Vol > 5MA * {vol_mul:.2f}`（倍數 `{vol_mul:.2f}`）")
            if bool(r.get("alert_ma_bullish", False)):
                labels.append(f"🧠 **均線多頭**（短 > 長，收盤 > 各MA）")
            if bool(r.get("foreign_net_buy_alert", False)):
                n = int(r.get("foreign_consecutive_net_buy_days", 0))
                labels.append(f"🏦 **外資連{n}天買超**（>= `{chip_vol_thr:,.0f}` 張）")
            if bool(r.get("trust_net_buy_alert", False)):
                n = int(r.get("trust_consecutive_net_buy_days", 0))
                labels.append(f"🏛️ **投信連{n}天買超**（>= `{chip_vol_thr:,.0f}` 張）")

            # One stock block
            meta = []
            if close is not None:
                meta.append(f"收盤 `{close:,.2f}`")
            if pct is not None:
                meta.append(f"漲跌幅 `{pct:+.2f}%`")
            if vol is not None:
                meta.append(f"成交量 `{vol:,.0f}`")
            if vol_ratio is not None and pd.notna(vol_ratio):
                meta.append(f"爆量倍數 `{vol_ratio:.2f}`")

            lines.append(f"**{stock_id}** | " + " | ".join(meta))
            lines.append("  " + " · ".join(labels))

    # Normal watchlist compression
    lines.append("")
    lines.append("📊 **【常規自選股狀態】**")

    if not df_normal.empty:
        # Use settings.watch_list order when possible
        watch_list = [str(x) for x in settings.get("watch_list", [])]
        normal_sorted = df_normal.copy()
        if watch_list:
            rank = {sid: i for i, sid in enumerate(watch_list)}
            normal_sorted["__rank"] = normal_sorted["stock_id"].map(lambda x: rank.get(str(x), 10**9))
            normal_sorted = normal_sorted.sort_values("__rank")
        parts: List[str] = []
        for _, r in normal_sorted.iterrows():
            stock_id = str(r["stock_id"])
            pct = float(r["pct_change"]) if pd.notna(r.get("pct_change")) else None
            if pct is None:
                parts.append(f"**{stock_id}**")
            else:
                parts.append(f"**{stock_id}**(`{pct:+.2f}%`)")
        lines.append(" | ".join(parts))
    elif df.empty:
        lines.append("（無自選股資料）")
    else:
        lines.append("（全部自選股均觸發）")

    return "\n".join(lines)


def build_discord_embeds_payload(
    *,
    text: str,
    index_change_pct: Optional[float] = None,
) -> Dict[str, Any]:
    """
    Wrap a prepared text message into Discord webhook payload with embeds.
    Side color is determined by `index_change_pct` (加權指數漲跌):
      - green: >= 0
      - red: < 0
    """
    # Discord embed color is an integer (decimal).
    green = 0x2ECC71
    red = 0xE74C3C
    color = green if (index_change_pct is not None and index_change_pct >= 0) else red
    if index_change_pct is None:
        color = 0x3498DB  # neutral blue

    return {
        "embeds": [
            {
                "title": "台股與總經數據追蹤警示",
                "description": text,
                "color": color,
            }
        ]
    }

