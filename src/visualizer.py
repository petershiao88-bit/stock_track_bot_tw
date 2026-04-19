"""
Weekly fundamental diagnosis charts (2x2) from SQLite data populated by data_fetcher.
"""

from __future__ import annotations

import io
import sqlite3
from pathlib import Path
from typing import Optional

import matplotlib.pyplot as plt
import pandas as pd
from matplotlib import font_manager

from src.data_fetcher import (
    DEFAULT_DB_PATH,
    TABLE_BALANCE_SHEET,
    TABLE_CASH_FLOWS_STATEMENT,
    TABLE_FINANCIAL_STATEMENTS,
    TABLE_MONTHLY_REVENUE,
)


def _setup_chinese_font() -> None:
    candidates = [
        "Microsoft JhengHei",
        "Microsoft YaHei",
        "Noto Sans CJK TC",
        "Noto Sans TC",
        "PingFang TC",
    ]
    try:
        available = {f.name for f in font_manager.fontManager.ttflist}
        for name in candidates:
            if name in available:
                plt.rcParams["font.sans-serif"] = [name, "DejaVu Sans"]
                plt.rcParams["axes.unicode_minus"] = False
                return
    except Exception:
        pass
    plt.rcParams["axes.unicode_minus"] = False


def _first_series(
    wide: pd.DataFrame,
    type_names: tuple[str, ...],
) -> Optional[pd.Series]:
    if wide is None or wide.empty:
        return None
    for n in type_names:
        if n in wide.columns:
            return wide[n]
    lower_map = {str(c).lower(): c for c in wide.columns}
    for n in type_names:
        k = n.lower()
        if k in lower_map:
            return wide[lower_map[k]]
    return None


def _bvps_series(pv_bal: pd.DataFrame) -> Optional[pd.Series]:
    """
    每股淨值：FinMind 資產負債表多半沒有單一 BVPS 欄位，改以
    歸屬母公司業主權益 / 普通股股數 計算（與季報常用口徑一致）。
    """
    direct = _first_series(
        pv_bal,
        (
            "NetAssetValuePerShare",
            "BookValuePerShare",
            "NetAssetValuePerShare(BVPS)",
        ),
    )
    if direct is not None and direct.notna().any():
        return direct

    eq = _first_series(
        pv_bal,
        (
            "EquityAttributableToOwnersOfParent",
            "Equity",
        ),
    )
    sh = _first_series(
        pv_bal,
        (
            "OrdinaryShare",
            "CapitalStock",
        ),
    )
    if eq is None or sh is None:
        return None
    idx = eq.index.intersection(sh.index)
    if len(idx) == 0:
        return None
    e = pd.to_numeric(eq.reindex(idx), errors="coerce")
    s = pd.to_numeric(sh.reindex(idx), errors="coerce").replace(0, pd.NA)
    out = e / s
    return out.dropna() if out.notna().any() else None


def _load_long_table(
    conn: sqlite3.Connection,
    table: str,
    stock_id: str,
) -> pd.DataFrame:
    q = f"SELECT * FROM {_quote_ident(table)} WHERE stock_id = ?"
    try:
        df = pd.read_sql_query(q, conn, params=[str(stock_id)])
    except (sqlite3.OperationalError, pd.errors.DatabaseError):
        return pd.DataFrame()
    return df


def _quote_ident(ident: str) -> str:
    if not ident.replace("_", "").isalnum():
        raise ValueError(f"Unsafe SQL identifier: {ident}")
    return ident


def _pivot_financial_long(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "type" not in df.columns or "value" not in df.columns:
        return pd.DataFrame()
    d = df.copy()
    d["date"] = d["date"].map(lambda x: str(x)[:10])
    pv = d.pivot_table(index="date", columns="type", values="value", aggfunc="first")
    pv = pv.sort_index()
    pv.index = pd.to_datetime(pv.index)
    return pv


def generate_weekly_report_chart(
    stock_id: str,
    *,
    db_path: str | Path | None = None,
) -> io.BytesIO:
    """
    Build a 16x12 inch 2x2 PNG chart into a BytesIO buffer.

    Panels:
      1) Monthly revenue (bars) + YoY% (line)
      2) Gross / operating / net margin trends (quarterly)
      3) Net income vs operating cash flow (quarterly)
      4) Total assets, total liabilities, BVPS (quarterly)
    """
    db_path = Path(db_path or DEFAULT_DB_PATH)
    stock_id = str(stock_id)

    _setup_chinese_font()

    with sqlite3.connect(db_path.as_posix()) as conn:
        df_rev = pd.read_sql_query(
            f"SELECT * FROM {_quote_ident(TABLE_MONTHLY_REVENUE)} WHERE stock_id = ? ORDER BY revenue_year, revenue_month",
            conn,
            params=[stock_id],
        )
        df_income = _load_long_table(conn, TABLE_FINANCIAL_STATEMENTS, stock_id)
        df_bal = _load_long_table(conn, TABLE_BALANCE_SHEET, stock_id)
        df_cash = _load_long_table(conn, TABLE_CASH_FLOWS_STATEMENT, stock_id)

    pv_income = _pivot_financial_long(df_income)
    pv_bal = _pivot_financial_long(df_bal)
    pv_cash = _pivot_financial_long(df_cash)

    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle(f"{stock_id} 基本面診斷（週報）", fontsize=16)

    # ---- Panel 1: monthly revenue + YoY ----
    ax1 = axes[0, 0]
    if not df_rev.empty and "revenue" in df_rev.columns:
        m = df_rev.copy()
        m["revenue"] = pd.to_numeric(m["revenue"], errors="coerce")
        m["revenue_year"] = pd.to_numeric(m["revenue_year"], errors="coerce")
        m["revenue_month"] = pd.to_numeric(m["revenue_month"], errors="coerce")
        m = m.dropna(subset=["revenue_year", "revenue_month", "revenue"])
        m["period"] = pd.to_datetime(
            m["revenue_year"].astype(int).astype(str)
            + "-"
            + m["revenue_month"].astype(int).astype(str).str.zfill(2)
            + "-01",
            errors="coerce",
        )
        m = m.sort_values("period")
        m["rev_yoy"] = m.groupby("stock_id")["revenue"].shift(12)
        m["yoy_pct"] = (m["revenue"] - m["rev_yoy"]) / m["rev_yoy"].replace(0, pd.NA) * 100.0

        x = range(len(m))
        ax1.bar(x, m["revenue"].values, color="steelblue", alpha=0.75, label="月營收")
        ax1.set_xticks(list(x))
        ax1.set_xticklabels([p.strftime("%Y-%m") if pd.notna(p) else "" for p in m["period"]], rotation=45, ha="right")
        ax1.set_ylabel("營收")
        ax1.legend(loc="upper left")
        ax1_t = ax1.twinx()
        ax1_t.plot(x, m["yoy_pct"].values, color="darkorange", marker="o", linewidth=2, label="YoY %")
        ax1_t.axhline(0, color="gray", linewidth=0.8)
        ax1_t.set_ylabel("年增率 %")
        ax1_t.legend(loc="upper right")
        ax1.set_title("月營收與年增率")
    else:
        ax1.text(0.5, 0.5, "無月營收資料", ha="center", va="center", transform=ax1.transAxes)

    # ---- Panel 2: margins (quarterly) ----
    ax2 = axes[0, 1]
    rev_q = _first_series(pv_income, ("Revenue", "OperatingRevenue", "NetRevenue"))
    gp = _first_series(pv_income, ("GrossProfit",))
    op_inc = _first_series(
        pv_income,
        ("OperatingIncome", "NetOperatingIncomeLoss", "IncomeFromOperations", "OperatingProfit"),
    )
    ni = _first_series(pv_income, ("IncomeAfterTaxes", "NetIncome", "ProfitLoss"))

    if rev_q is not None and rev_q.notna().any():
        rev_safe = rev_q.replace(0, pd.NA)
        idx = rev_q.index
        series_list = []
        labels = []
        if gp is not None:
            series_list.append((gp / rev_safe * 100.0).reindex(idx))
            labels.append("毛利率%")
        if op_inc is not None:
            series_list.append((op_inc / rev_safe * 100.0).reindex(idx))
            labels.append("營業利益率%")
        if ni is not None:
            series_list.append((ni / rev_safe * 100.0).reindex(idx))
            labels.append("稅後淨利率%")
        for s, lab in zip(series_list, labels):
            ax2.plot(s.index, s.values, marker="o", label=lab)
        ax2.legend()
        ax2.set_ylabel("%")
        ax2.set_title("獲利三率（季）")
        fig.autofmt_xdate()
    else:
        ax2.text(0.5, 0.5, "無損益表或營收欄位", ha="center", va="center", transform=ax2.transAxes)

    # ---- Panel 3: net income vs CFO ----
    ax3 = axes[1, 0]
    ni2 = _first_series(pv_income, ("IncomeAfterTaxes", "NetIncome"))
    cfo = _first_series(
        pv_cash,
        ("CashFlowsFromOperatingActivities", "NetCashProvidedByOperatingActivities"),
    )
    if ni2 is not None and ni2.notna().any():
        ax3.plot(ni2.index, ni2.values, marker="s", label="本期淨利", color="tab:blue")
    if cfo is not None and cfo.notna().any():
        ax3.plot(cfo.index, cfo.values, marker="^", label="營業活動現金流", color="tab:green")
    if (ni2 is not None and ni2.notna().any()) or (cfo is not None and cfo.notna().any()):
        ax3.legend()
        ax3.set_title("淨利 vs 營業現金流（季）")
        ax3.set_ylabel("金額")
        fig.autofmt_xdate()
    else:
        ax3.text(0.5, 0.5, "無損益或現金流量資料", ha="center", va="center", transform=ax3.transAxes)

    # ---- Panel 4: balance sheet ----
    ax4 = axes[1, 1]
    ta = _first_series(pv_bal, ("TotalAssets", "Assets"))
    tl = _first_series(pv_bal, ("TotalLiabilities", "Liabilities"))
    bv = _bvps_series(pv_bal)

    left_plotted = False
    if ta is not None and ta.notna().any():
        ax4.plot(ta.index, ta.values, marker="o", label="總資產", color="tab:blue")
        left_plotted = True
    if tl is not None and tl.notna().any():
        ax4.plot(tl.index, tl.values, marker="o", label="總負債", color="tab:orange")
        left_plotted = True
    if left_plotted:
        ax4.set_ylabel("金額")
        ax4.legend(loc="upper left")

    right_plotted = False
    if bv is not None and bv.notna().any():
        if left_plotted:
            ax4_r = ax4.twinx()
            ax4_r.plot(
                bv.index,
                bv.values,
                color="purple",
                marker="d",
                linestyle="--",
                label="每股淨值",
            )
            ax4_r.set_ylabel("每股淨值 (元)")
            ax4_r.legend(loc="upper right")
            right_plotted = True
        else:
            ax4.plot(
                bv.index,
                bv.values,
                color="purple",
                marker="d",
                linestyle="--",
                label="每股淨值",
            )
            ax4.set_ylabel("每股淨值 (元)")
            ax4.legend(loc="upper left")
            right_plotted = True

    if left_plotted or right_plotted:
        ax4.set_title("資產負債與每股淨值")
        fig.autofmt_xdate()
    else:
        ax4.text(0.5, 0.5, "無資產負債表資料", ha="center", va="center", transform=ax4.transAxes)

    plt.tight_layout(rect=[0, 0, 1, 0.96])
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=120, bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return buf
