from __future__ import annotations

import argparse
import json
import os
from datetime import date, timedelta
from typing import List, Optional

import pandas as pd
from dotenv import load_dotenv

from src.data_fetcher import (
    fetch_taiwan_futures_institutional_investors_with_cache,
    fetch_taiwan_stock_day_trading_with_cache,
    fetch_government_bonds_yield_with_cache,
    fetch_taiwan_exchange_rate_usd_with_cache,
    fetch_taiwan_stock_margin_purchase_short_sale_with_cache,
    fetch_taiwan_stock_institutional_investors_buy_sell_with_cache,
    fetch_taiwan_stock_total_institutional_investors_with_cache,
    fetch_taiwan_stock_price_with_cache,
    fetch_us_stock_index_with_cache,
    fetch_fundamental_data,
    fetch_market_hot_stocks_data,
)
from src.data_processor import (
    compute_day_trade_ratio_signals,
    compute_foreign_futures_net_oi_signal,
    compute_margin_reduction_signals,
    compute_chip_signals,
    compute_macro_and_board_signals,
    compute_technical_signals,
    format_global_macro_embed_value,
    format_hot_stocks_discord_field_value,
    format_push_message,
    get_top_hot_stocks,
    load_settings,
)
from src.notifier import (
    StockEmbedItem,
    build_discord_embeds_payload_v2,
    send_discord_embeds,
    send_discord_text,
    send_discord_with_files,
)
from src.visualizer import generate_weekly_report_chart


_PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
_STATE_PATH = os.path.join(_PROJECT_ROOT, "data", "run_state.json")


def _date_minus_days(d: date, days: int) -> str:
    return (d - timedelta(days=days)).strftime("%Y-%m-%d")


def _load_run_state() -> dict:
    default_state = {
        "daily": {"last_run_date": None},
        "weekly": {"last_run_date": None},
    }
    try:
        if os.path.exists(_STATE_PATH):
            with open(_STATE_PATH, "r", encoding="utf-8") as f:
                state = json.load(f)
                if not isinstance(state, dict):
                    return default_state
                state.setdefault("daily", {"last_run_date": None})
                state.setdefault("weekly", {"last_run_date": None})
                if "last_run_date" not in state["daily"]:
                    state["daily"]["last_run_date"] = None
                if "last_run_date" not in state["weekly"]:
                    state["weekly"]["last_run_date"] = None
                return state
    except Exception:
        pass
    return default_state


def _get_last_run_date(mode: str) -> Optional[str]:
    state = _load_run_state()
    bucket = state.get(mode, {})
    last = bucket.get("last_run_date")
    return str(last) if last else None


def _save_run_state(mode: str, last_run_date: str) -> None:
    os.makedirs(os.path.dirname(_STATE_PATH), exist_ok=True)
    state = _load_run_state()
    state.setdefault(mode, {})
    state[mode]["last_run_date"] = last_run_date

    with open(_STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def _get_market_end_date(d: Optional[date] = None) -> date:
    """
    Heuristic:
    - if weekday (Mon-Fri), use that date
    - if weekend, use last Friday
    """
    dt = d or date.today()
    weekday = dt.weekday()  # Mon=0 ... Sun=6
    if weekday >= 5:
        return dt - timedelta(days=weekday - 4)
    return dt


def _fetch_weighted_index_change_pct(token: str, start_date: str, end_date: str) -> Optional[float]:
    """
    Try to compute weighted index (加權指數) change pct using FinMind dataset.
    """
    # Use cached `TaiwanStockPrice` with stock_id="^TWII".
    try:
        df = fetch_taiwan_stock_price_with_cache(
            token=token,
            stock_id="^TWII",
            start_date=start_date,
            end_date=end_date,
            api_delay_seconds=0.0,
        )
    except Exception:
        return None

    if df.empty or "close" not in df.columns or "date" not in df.columns:
        return None

    dff = df.copy()
    dff["date"] = dff["date"].map(lambda x: str(x)[:10])
    dff["close"] = pd.to_numeric(dff["close"], errors="coerce")
    dff = dff.dropna(subset=["close"]).sort_values("date")
    if len(dff) < 2:
        return None

    prev = float(dff.iloc[-2]["close"])
    last = float(dff.iloc[-1]["close"])
    if prev == 0:
        return None
    return (last - prev) / prev * 100.0


def run_once(end_date: Optional[date] = None) -> None:
    # Load env
    _env_path = os.path.join(_PROJECT_ROOT, "config", ".env")
    load_dotenv(dotenv_path=_env_path, encoding="utf-8")

    finmind_token = os.getenv("FINMIND_TOKEN") or os.getenv("FINMIND_API_TOKEN") or ""
    discord_webhook_url = os.getenv("DISCORD_WEBHOOK_URL") or ""

    if not finmind_token:
        raise RuntimeError("Missing FINMIND token. Please set FINMIND_TOKEN in config/.env")
    if not discord_webhook_url:
        raise RuntimeError("Missing DISCORD_WEBHOOK_URL. Please set it in config/.env")

    settings = load_settings()
    watch_list: List[str] = [str(x) for x in settings.get("watch_list", [])]
    if not watch_list:
        print("[skip] watch_list 為空，請在 config/settings.json 設定自選股代號。")
        return

    end_date = end_date or date.today()
    tech_max_ma = max(settings["technical_alerts"]["ma_tracking"])
    # +5 for volume 5MA, +2 for pct_change previous day safety
    technical_lookback_days = tech_max_ma + 12
    chip_lookback_days = max(
        settings["chip_alerts"]["foreign_investor_net_buy_days"],
        settings["chip_alerts"]["investment_trust_net_buy_days"],
    ) + 10

    start_date_all = end_date - timedelta(days=max(technical_lookback_days, chip_lookback_days))
    start_date_all_str = start_date_all.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")
    hot_enabled = settings.get("system_config", {}).get("enable_market_hot_stocks", True)
    if hot_enabled:
        print(
            f"[run_once] cwd={os.getcwd()} | end_date={end_date_str} | watch_list={watch_list}\n"
            f"        全市場熱門股會連續請求 5 個交易日 API（含延遲），終端機可能約 15～30 秒才有下一行…"
        )
    else:
        print(
            f"[run_once] cwd={os.getcwd()} | end_date={end_date_str} | watch_list={watch_list}\n"
            f"        [market_hot] 已在 settings 關閉 enable_market_hot_stocks，略過全市場抓取。"
        )

    api_delay = float(settings.get("system_config", {}).get("api_delay_seconds", 3.0))
    if hot_enabled:
        market_snap_df, market_meta = fetch_market_hot_stocks_data(
            days=5,
            token=finmind_token,
            end_date=end_date,
            api_delay_seconds=api_delay,
        )
    else:
        market_snap_df = pd.DataFrame()
        market_meta = {
            "rows_written": 0,
            "trading_dates_used": [],
            "errors": [],
            "tier_denied": False,
        }
    print(
        f"[market_hot] 完成：rows={market_meta.get('rows_written', 0)} "
        f"交易日={market_meta.get('trading_dates_used')}"
    )
    if market_meta.get("tier_denied"):
        print(
            "[market_hot] 目前 FinMind 帳戶為免費方案，無法使用「不指定股票代號」的全市場股價；"
            "熱門股區塊會略過。若需此功能請升級贊助：https://finmindtrade.com/analysis/#/Sponsor/sponsor"
        )
    for err in (market_meta.get("errors") or [])[:5]:
        print(f"[market_hot] {err}")
    hot_stocks_df = (
        get_top_hot_stocks(market_snap_df, top_n=3) if not market_snap_df.empty else pd.DataFrame()
    )
    hot_stocks_field_value: Optional[str] = (
        format_hot_stocks_discord_field_value(hot_stocks_df) if not market_snap_df.empty else None
    )

    # 1) Fetch technical: prices (concat all watchlist)
    prices_frames: List[pd.DataFrame] = []
    for sid in watch_list:
        df = fetch_taiwan_stock_price_with_cache(
            token=finmind_token,
            stock_id=sid,
            start_date=start_date_all_str,
            end_date=end_date_str,
        )
        if not df.empty:
            prices_frames.append(df)

    prices_df = pd.concat(prices_frames, ignore_index=True) if prices_frames else pd.DataFrame()

    technical_df = compute_technical_signals(prices_df, settings) if not prices_df.empty else pd.DataFrame()

    # 2) Fetch chip: institutional buy/sell details per stock, concat
    chip_frames: List[pd.DataFrame] = []
    for sid in watch_list:
        df = fetch_taiwan_stock_institutional_investors_buy_sell_with_cache(
            token=finmind_token,
            stock_id=sid,
            start_date=start_date_all_str,
            end_date=end_date_str,
        )
        if not df.empty:
            chip_frames.append(df)

    chip_df = pd.concat(chip_frames, ignore_index=True) if chip_frames else pd.DataFrame()
    chip_signals = compute_chip_signals(chip_df, settings) if not chip_df.empty else pd.DataFrame()

    # 2-1) Fetch margin purchase/short sale (per stock)
    margin_frames: List[pd.DataFrame] = []
    for sid in watch_list:
        df = fetch_taiwan_stock_margin_purchase_short_sale_with_cache(
            token=finmind_token,
            stock_id=sid,
            start_date=start_date_all_str,
            end_date=end_date_str,
        )
        if not df.empty:
            margin_frames.append(df)

    margin_df = pd.concat(margin_frames, ignore_index=True) if margin_frames else pd.DataFrame()
    # Compatibility: if dataset doesn't provide MarginPurchaseBalance, use today balance as proxy
    if not margin_df.empty and "MarginPurchaseBalance" not in margin_df.columns:
        if "MarginPurchaseTodayBalance" in margin_df.columns:
            margin_df["MarginPurchaseBalance"] = margin_df["MarginPurchaseTodayBalance"]
    margin_signals = (
        compute_margin_reduction_signals(margin_df=margin_df, settings=settings)
        if not margin_df.empty
        else pd.DataFrame()
    )

    # 2-2) Fetch day trading (per stock)
    day_trading_frames: List[pd.DataFrame] = []
    for sid in watch_list:
        df = fetch_taiwan_stock_day_trading_with_cache(
            token=finmind_token,
            stock_id=sid,
            start_date=start_date_all_str,
            end_date=end_date_str,
        )
        if not df.empty:
            day_trading_frames.append(df)

    day_trading_df = (
        pd.concat(day_trading_frames, ignore_index=True) if day_trading_frames else pd.DataFrame()
    )
    day_trade_signals = (
        compute_day_trade_ratio_signals(
            prices_df=prices_df, day_trading_df=day_trading_df, settings=settings
        )
        if (not day_trading_df.empty and not prices_df.empty)
        else pd.DataFrame()
    )

    # 2-3) Fetch futures institutional investors (market level)
    futures_start = _date_minus_days(end_date, 30)
    futures_df = fetch_taiwan_futures_institutional_investors_with_cache(
        token=finmind_token,
        data_id="TX",
        start_date=futures_start,
        end_date=end_date_str,
    )
    futures_signal = (
        compute_foreign_futures_net_oi_signal(futures_df=futures_df, settings=settings)
        if not futures_df.empty
        else {
            "foreign_futures_net_oi": None,
            "foreign_futures_net_oi_alert": False,
            "futures_date": None,
        }
    )

    # 3) Fetch macro and board
    # Use smaller lookback; we only need latest rows inside processors.
    macro_start = _date_minus_days(end_date, 30)
    board_start = _date_minus_days(end_date, 30)

    macro_df = fetch_government_bonds_yield_with_cache(
        token=finmind_token,
        start_date=macro_start,
        end_date=end_date_str,
        api_delay_seconds=api_delay,
    )
    macro_df_2y = fetch_government_bonds_yield_with_cache(
        token=finmind_token,
        start_date=macro_start,
        end_date=end_date_str,
        data_id="United States 2-Year",
        api_delay_seconds=api_delay,
    )
    fx_df = fetch_taiwan_exchange_rate_usd_with_cache(
        token=finmind_token,
        start_date=macro_start,
        end_date=end_date_str,
        api_delay_seconds=api_delay,
    )
    us_sox_df = fetch_us_stock_index_with_cache(
        token=finmind_token,
        stock_id="^SOX",
        start_date=macro_start,
        end_date=end_date_str,
        api_delay_seconds=api_delay,
    )
    us_ixic_df = fetch_us_stock_index_with_cache(
        token=finmind_token,
        stock_id="^IXIC",
        start_date=macro_start,
        end_date=end_date_str,
        api_delay_seconds=api_delay,
    )
    board_df = fetch_taiwan_stock_total_institutional_investors_with_cache(
        token=finmind_token,
        start_date=board_start,
        end_date=end_date_str,
    )

    macro_board_signals = compute_macro_and_board_signals(
        macro_df,
        board_df,
        macro_bonds_2y_df=macro_df_2y,
        twd_usd_fx_df=fx_df,
        us_sox_df=us_sox_df,
        us_ixic_df=us_ixic_df,
        settings=settings,
    )
    global_macro_embed_text = format_global_macro_embed_value(macro_board_signals, settings)

    # 4) Format fallback text message
    has_macro_board = bool(
        macro_board_signals.get("board_date")
        or macro_board_signals.get("macro_date")
        or macro_board_signals.get("usd_twd_date")
        or macro_board_signals.get("us_2y_date")
        or macro_board_signals.get("sox_date")
        or macro_board_signals.get("ixic_date")
    )
    if technical_df.empty and chip_signals.empty and market_snap_df.empty and not has_macro_board:
        print(
            "[skip] 無推播：自選股價/籌碼、全市場快照皆空，且無大盤/總經日期。"
            " 常見原因：休市日、FinMind 無該日資料，或全市場 API 需較高會員權限。"
        )
        return

    text = format_push_message(
        settings=settings,
        technical_df=technical_df,
        chip_df_signals=chip_signals,
        macro_board_signals=macro_board_signals,
        hot_stocks_field_text=hot_stocks_field_value,
        global_macro_text=global_macro_embed_text,
    )

    # 5) Build v2 embeds payload with weighted index change color + tags
    index_change_pct = _fetch_weighted_index_change_pct(
        token=finmind_token,
        start_date=_date_minus_days(end_date, 10),
        end_date=end_date_str,
    )

    # Merge stock-level signals for tag/triggers rendering
    if technical_df.empty and chip_signals.empty:
        merged = pd.DataFrame(columns=["stock_id"])
    elif technical_df.empty:
        merged = chip_signals.copy()
    elif chip_signals.empty:
        merged = technical_df.copy()
    else:
        merged = technical_df.merge(chip_signals, on="stock_id", how="left")
    if not day_trade_signals.empty:
        merged = merged.merge(
            day_trade_signals[["stock_id", "day_trade_ratio", "alert_day_trade_overheated"]],
            on="stock_id",
            how="left",
        )
    if not margin_signals.empty:
        merged = merged.merge(
            margin_signals[
                ["stock_id", "margin_reduction_consecutive_days", "alert_margin_reduction"]
            ],
            on="stock_id",
            how="left",
        )

    # Normalize booleans
    for col in [
        "alert_price_change",
        "alert_volume_breakout",
        "alert_ma_bullish",
        "foreign_net_buy_alert",
        "trust_net_buy_alert",
        "alert_day_trade_overheated",
        "alert_margin_reduction",
    ]:
        if col in merged.columns:
            merged[col] = merged[col].fillna(False).astype(bool)

    # Triggered is based on original strategy conditions; new signals become tags.
    base_trigger_cols = [
        "alert_price_change",
        "alert_volume_breakout",
        "alert_ma_bullish",
        "foreign_net_buy_alert",
        "trust_net_buy_alert",
    ]
    for _c in base_trigger_cols:
        if _c not in merged.columns:
            merged[_c] = False
    merged["is_triggered"] = merged[base_trigger_cols].any(axis=1)

    triggered_items: List[StockEmbedItem] = []
    normal_items: List[StockEmbedItem] = []

    for _, r in merged.iterrows():
        tags: List[str] = []
        if bool(r.get("alert_margin_reduction", False)):
            tags.append("🔥 融資大減")
        if bool(r.get("alert_day_trade_overheated", False)):
            tags.append("⚠️ 當沖過熱")

        triggers: List[str] = []
        if bool(r.get("alert_price_change", False)):
            triggers.append("漲跌幅")
        if bool(r.get("alert_volume_breakout", False)):
            triggers.append("爆量")
        if bool(r.get("alert_ma_bullish", False)):
            triggers.append("均線多頭")
        if bool(r.get("foreign_net_buy_alert", False)):
            triggers.append("外資連買")
        if bool(r.get("trust_net_buy_alert", False)):
            triggers.append("投信連買")

        item = StockEmbedItem(
            stock_id=str(r["stock_id"]),
            close=float(r["close"]) if pd.notna(r.get("close")) else None,
            pct_change=float(r["pct_change"]) if pd.notna(r.get("pct_change")) else None,
            volume=float(r["Trading_Volume"]) if pd.notna(r.get("Trading_Volume")) else None,
            volume_breakout_ratio=(
                float(r["volume_breakout_ratio"]) if pd.notna(r.get("volume_breakout_ratio")) else None
            ),
            tags=tags if tags else None,
            triggers=triggers if triggers else None,
        )
        if bool(r["is_triggered"]):
            triggered_items.append(item)
        else:
            normal_items.append(item)

    embeds_payload = build_discord_embeds_payload_v2(
        title="台股與總經數據追蹤警示",
        market_date=macro_board_signals.get("board_date"),
        three_institutions_net_buy=macro_board_signals.get("three_institutions_net_buy"),
        foreign_futures_net_oi=futures_signal.get("foreign_futures_net_oi"),
        foreign_futures_net_oi_alert=bool(futures_signal.get("foreign_futures_net_oi_alert", False)),
        foreign_futures_alert_threshold=float(
            settings.get("futures_alerts", {}).get("foreign_futures_net_oi_alert", -10000)
        ),
        triggered=triggered_items,
        normal=normal_items,
        index_change_pct=index_change_pct,
        hot_stocks_field_value=hot_stocks_field_value,
        global_macro_field_value=global_macro_embed_text,
    )

    # 6) Send
    try:
        send_discord_embeds(
            discord_webhook_url,
            embeds_payload,
            fallback_to_text={"content": text},
        )
        print("[ok] 已送出 Discord（embeds）。")
    except Exception as exc:
        print(f"[warn] embeds 失敗，改送純文字: {exc}")
        # fallback to plain text always
        send_discord_text(discord_webhook_url, text)
        print("[ok] 已送出 Discord（純文字 fallback）。")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode",
        choices=["once", "daily", "fundamentals"],
        default="once",
        help="once: alerts once; daily: daily+weekly rules; fundamentals: fetch monthly revenue & financials only (manual test).",
    )
    args = parser.parse_args()

    if args.mode == "once":
        run_once()
    elif args.mode == "fundamentals":
        load_dotenv(dotenv_path=os.path.join(_PROJECT_ROOT, "config", ".env"), encoding="utf-8")
        finmind_token = os.getenv("FINMIND_TOKEN") or os.getenv("FINMIND_API_TOKEN") or ""
        if not finmind_token:
            raise RuntimeError("Missing FINMIND token in config/.env (FINMIND_TOKEN).")
        settings = load_settings()
        watch_list_f: List[str] = [str(x) for x in settings.get("watch_list", [])]
        if not watch_list_f:
            raise RuntimeError("watch_list is empty in config/settings.json")
        result = fetch_fundamental_data(
            watch_list_f,
            token=finmind_token,
            end_date=date.today(),
        )
        print(result)
    else:
        import datetime

        # 1) 讀取狀態（daily / weekly 分開）
        # 同時讀取 settings/watch_list，供週末基本面抓取使用
        load_dotenv(dotenv_path=os.path.join(_PROJECT_ROOT, "config", ".env"), encoding="utf-8")
        finmind_token = os.getenv("FINMIND_TOKEN") or os.getenv("FINMIND_API_TOKEN") or ""

        settings = load_settings()
        watch_list: List[str] = [str(x) for x in settings.get("watch_list", [])]
        if not watch_list:
            raise RuntimeError("watch_list is empty in config/settings.json")

        today_date = datetime.datetime.today().date()
        market_end_date = _get_market_end_date(today_date)

        daily_last_str = _get_last_run_date("daily")
        weekly_last_str = _get_last_run_date("weekly")

        def _parse_date_str(s: Optional[str]) -> Optional[date]:
            if not s:
                return None
            try:
                return datetime.datetime.strptime(s, "%Y-%m-%d").date()
            except Exception:
                return None

        daily_last_date = _parse_date_str(daily_last_str)
        weekly_last_date = _parse_date_str(weekly_last_str)

        # 2) 每日任務：用 market_end_date 控制（避免週末送 Friday 重複資料）
        if daily_last_date is None or market_end_date > daily_last_date:
            daily_run_date_str = market_end_date.strftime("%Y-%m-%d")
            print(f"[run] daily: {daily_run_date_str}")
            run_once(end_date=market_end_date)
            _save_run_state("daily", daily_run_date_str)
        else:
            print(f"[skip] daily already ran for {market_end_date.strftime('%Y-%m-%d')}")

        # 3) 每週任務：只在週六執行
        if today_date.weekday() == 5 and (weekly_last_date is None or today_date > weekly_last_date):
            weekly_run_date_str = today_date.strftime("%Y-%m-%d")
            print(f"[run] weekly fundamentals: {weekly_run_date_str}")
            if not finmind_token:
                raise RuntimeError("Missing FINMIND token in config/.env (FINMIND_TOKEN).")

            fund_result = fetch_fundamental_data(
                watch_list,
                token=finmind_token,
                end_date=today_date,
            )
            print(fund_result)

            discord_weekly_url = os.getenv("DISCORD_WEBHOOK_URL") or ""
            if discord_weekly_url:
                for sid in watch_list:
                    try:
                        chart_buf = generate_weekly_report_chart(sid)
                        png_bytes = chart_buf.getvalue()
                        send_discord_with_files(
                            discord_weekly_url,
                            content=f"📊 **每週基本面診斷** `{sid}`（{weekly_run_date_str}）",
                            files=[(f"weekly_{sid}.png", png_bytes, "image/png")],
                        )
                        print(f"[ok] weekly chart sent: {sid}")
                    except Exception as exc:
                        print(f"[warn] weekly chart failed {sid}: {exc}")
                        try:
                            send_discord_text(
                                discord_weekly_url,
                                f"⚠️ 基本面圖表產生或傳送失敗 `{sid}`：{exc}",
                            )
                        except Exception:
                            pass
            else:
                print("[warn] DISCORD_WEBHOOK_URL missing; skip weekly chart push.")

            _save_run_state("weekly", weekly_run_date_str)
        else:
            # no-op
            pass

