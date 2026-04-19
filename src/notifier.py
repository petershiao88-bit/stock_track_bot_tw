from __future__ import annotations

import io
import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import requests


def send_discord_with_files(
    webhook_url: str,
    content: str,
    files: List[Tuple[str, bytes, str]],
    *,
    timeout_seconds: int = 120,
) -> None:
    """
    Send a message with attachments via Discord webhook (multipart/form-data).

    Each file tuple is (filename, raw_bytes, mime_type), e.g. ("chart.png", png_bytes, "image/png").
    """
    if not webhook_url:
        raise ValueError("Discord webhook URL is required.")
    if not files:
        send_discord_text(webhook_url, content, timeout_seconds=timeout_seconds)
        return

    payload = {"content": content[:2000]}
    multipart_files: Dict[str, Any] = {}
    for i, (filename, raw, mime) in enumerate(files):
        multipart_files[f"files[{i}]"] = (filename, io.BytesIO(raw), mime)

    resp = requests.post(
        webhook_url,
        data={"payload_json": json.dumps(payload)},
        files=multipart_files,
        timeout=timeout_seconds,
    )
    if resp.status_code >= 400:
        raise RuntimeError(f"Discord webhook (files) failed: status={resp.status_code}, body={resp.text[:500]}")


def send_discord_text(webhook_url: str, content: str, *, timeout_seconds: int = 15) -> None:
    """
    Send plain text to Discord webhook.
    """
    if not webhook_url:
        raise ValueError("Discord webhook URL is required.")

    payload: Dict[str, Any] = {"content": content}
    resp = requests.post(webhook_url, json=payload, timeout=timeout_seconds)
    if resp.status_code >= 400:
        raise RuntimeError(f"Discord webhook failed: status={resp.status_code}, body={resp.text[:500]}")


def send_discord_embeds(
    webhook_url: str,
    embeds_payload: Dict[str, Any],
    *,
    timeout_seconds: int = 15,
    fallback_to_text: Optional[Dict[str, str]] = None,
) -> None:
    """
    Send a Discord webhook payload that includes `embeds`.

    If it fails and fallback_to_text is provided, will try `fallback_to_text['content']`.
    """
    if not webhook_url:
        raise ValueError("Discord webhook URL is required.")
    if not isinstance(embeds_payload, dict):
        raise TypeError("embeds_payload must be a dict (Discord webhook JSON payload)")

    resp = requests.post(webhook_url, json=embeds_payload, timeout=timeout_seconds)
    if resp.status_code < 400:
        return

    if fallback_to_text and "content" in fallback_to_text:
        send_discord_text(webhook_url, fallback_to_text["content"], timeout_seconds=timeout_seconds)
        return

    raise RuntimeError(f"Discord webhook embeds failed: status={resp.status_code}, body={resp.text[:500]}")


@dataclass(frozen=True)
class StockEmbedItem:
    stock_id: str
    close: Optional[float] = None
    pct_change: Optional[float] = None
    volume: Optional[float] = None
    volume_breakout_ratio: Optional[float] = None
    tags: Optional[List[str]] = None  # e.g. ["🔥 融資大減", "⚠️ 當沖過熱"]
    triggers: Optional[List[str]] = None  # e.g. ["爆量", "外資連3天買超"]


def build_discord_embeds_payload_v2(
    *,
    title: str,
    market_date: Optional[str] = None,
    three_institutions_net_buy: Optional[float] = None,
    foreign_futures_net_oi: Optional[float] = None,
    foreign_futures_net_oi_alert: bool = False,
    foreign_futures_alert_threshold: Optional[float] = None,
    triggered: Optional[List[StockEmbedItem]] = None,
    normal: Optional[List[StockEmbedItem]] = None,
    index_change_pct: Optional[float] = None,
    hot_stocks_field_value: Optional[str] = None,
    global_macro_field_value: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Build a Discord webhook payload using embeds fields.

    Design goals:
    - Market overview shows futures net OI with emoji alert.
    - Triggered stocks list shows tags like [🔥 融資大減] [⚠️ 當沖過熱].
    - Normal stocks are compressed.
    - Embed side color uses index_change_pct: green (>=0), red (<0), blue (None).
    """
    triggered = triggered or []
    normal = normal or []

    green = 0x2ECC71
    red = 0xE74C3C
    blue = 0x3498DB
    color = blue if index_change_pct is None else (green if index_change_pct >= 0 else red)

    fields: List[Dict[str, Any]] = []

    # --- Market overview field ---
    overview_lines: List[str] = []
    if market_date and three_institutions_net_buy is not None:
        emoji = "🟢" if three_institutions_net_buy >= 0 else "🔴"
        overview_lines.append(
            f"{emoji} **三大法人({market_date})**：`{three_institutions_net_buy:,.0f}`"
        )

    if foreign_futures_net_oi is not None:
        if foreign_futures_net_oi_alert:
            warn = "🔴" if (foreign_futures_alert_threshold is None) else "⚠️"
            thr_txt = (
                f"（閾值 `{foreign_futures_alert_threshold:,.0f}`）"
                if foreign_futures_alert_threshold is not None
                else ""
            )
            overview_lines.append(
                f"{warn} **外資期貨淨未平倉**：`{foreign_futures_net_oi:,.0f}` {thr_txt}".strip()
            )
        else:
            overview_lines.append(f"🧾 **外資期貨淨未平倉**：`{foreign_futures_net_oi:,.0f}`")

    if overview_lines:
        fields.append(
            {
                "name": "📌 大盤速覽",
                "value": "\n".join(overview_lines),
                "inline": False,
            }
        )

    if global_macro_field_value:
        fields.append(
            {
                "name": "🌐 總經與國際市場",
                "value": global_macro_field_value[:1024],
                "inline": False,
            }
        )

    if hot_stocks_field_value:
        fields.append(
            {
                "name": "🔥 近五日資金匯聚焦點",
                "value": hot_stocks_field_value[:1024],
                "inline": False,
            }
        )

    # --- Triggered field ---
    if triggered:
        trig_lines: List[str] = []
        for item in triggered:
            tag_str = ""
            if item.tags:
                tag_str = " " + " ".join([f"[{t}]" for t in item.tags])
            meta_parts: List[str] = []
            if item.close is not None:
                meta_parts.append(f"收盤 `{item.close:,.2f}`")
            if item.pct_change is not None:
                meta_parts.append(f"漲跌 `{item.pct_change:+.2f}%`")
            if item.volume is not None:
                meta_parts.append(f"量 `{item.volume:,.0f}`")
            if item.volume_breakout_ratio is not None:
                meta_parts.append(f"爆量倍數 `{item.volume_breakout_ratio:.2f}`")

            trig = ""
            if item.triggers:
                trig = "｜" + "、".join(item.triggers)

            trig_lines.append(f"**{item.stock_id}**{tag_str}｜" + "｜".join(meta_parts) + trig)

        fields.append(
            {
                "name": "🚨 策略觸發名單",
                "value": "\n".join(trig_lines)[:1024],
                "inline": False,
            }
        )
    else:
        fields.append(
            {
                "name": "🚨 策略觸發名單",
                "value": "（本輪無觸發）",
                "inline": False,
            }
        )

    # --- Normal field (compressed) ---
    if normal:
        normal_parts: List[str] = []
        for item in normal:
            if item.pct_change is None:
                normal_parts.append(f"**{item.stock_id}**")
            else:
                normal_parts.append(f"**{item.stock_id}**(`{item.pct_change:+.2f}%`)")
        fields.append(
            {
                "name": "📊 常規自選股狀態",
                "value": " | ".join(normal_parts)[:1024],
                "inline": False,
            }
        )

    return {
        "embeds": [
            {
                "title": title,
                "color": color,
                "fields": fields,
            }
        ]
    }

