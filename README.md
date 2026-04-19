# 台灣股票追蹤機器人
## 結合技術面、籌碼面與總經指標的 Discord 自動推播系統

本專案是一個以 Python 開發的自動化股票追蹤機器人，主要用於監控台股市場、法人籌碼、期貨部位與總經數據，並依照自訂規則產生警示訊息，透過 Discord Webhook 自動推送至指定頻道。

此專案的核心目標，是將原本需要手動整理與追蹤的市場資訊，整合為一套可排程、可重複執行的自動化流程，協助使用者更有效率地掌握每日市場變化。

![Discord 推播截圖](assets/discord_screenshot.png)

---

## 專案簡介

在股票觀察與市場追蹤的過程中，投資人往往需要同時關注多項資訊，例如個股漲跌幅、成交量異常、法人買賣超、期貨未平倉變化，以及匯率與美股指標等外部風險訊號。  
本專案將上述資料來源整合成一套自動化監控流程，透過每日排程執行，自動抓取資料、判斷條件、整理訊息，並推送至 Discord，降低人工追蹤成本並提升資訊整理效率。

---

## 主要功能

### 技術面警示
- 偵測漲跌幅異常
- 偵測爆量突破
- 追蹤均線多頭排列與趨勢結構

### 籌碼面警示
- 追蹤外資連續買超
- 追蹤投信連續買超
- 偵測融資減少訊號
- 偵測當沖比過熱情況

### 期貨部位監控
- 追蹤外資期貨未平倉淨口數變化

### 總經與國際市場監控
- 美國 10 年期 / 2 年期公債殖利率
- USD/TWD 匯率
- 費城半導體指數與那斯達克指數漲跌

### 大盤觀察
- 三大法人每日買賣超摘要

### 基本面補充
- 每週更新月營收與財報資料
- 自動產出基本面圖表供檢視


## 專案結構

```text
├── main.py                         # 主程式入口
├── src/
│   ├── data_fetcher.py             # 資料抓取與 SQLite 快取
│   ├── data_processor.py           # 訊號判斷與訊息整理
│   ├── notifier.py                 # Discord 訊息推送
│   └── visualizer.py               # 基本面圖表生成
├── config/
│   ├── .env.example                # 環境變數範本
│   └── settings.example.json       # 設定檔範本
├── assets/
│   └── discord_screenshot.png      # 推播畫面示意圖
├── data/                           # 本地快取與執行狀態（不進 git）
├── logs/                           # 排程執行紀錄（不進 git）
├── run_from_task_scheduler.bat     # Windows 排程執行輔助檔
└── requirements.txt
```

---

## 專案流程

```text
市場與總經資料抓取
        ↓
依照自訂規則計算警示條件
        ↓
整理推播訊息格式
        ↓
透過 Discord 發送通知
        ↓
可搭配每日或每週排程自動執行
```

---

## 安裝方式

### 1. 下載專案

```bash
git clone https://github.com/<your-username>/stock-price-bot.git
cd stock-price-bot
```

### 2. 安裝套件

```bash
pip install -r requirements.txt
```

### 3. 建立設定檔

```bash
cp config/.env.example config/.env
cp config/settings.example.json config/settings.json
```

---

## 設定方式

### `.env`

請在 `config/.env` 中填入實際資訊：

| 變數名稱 | 說明 |
|----------|------|
| `FINMIND_TOKEN` | FinMind API token |
| `DISCORD_WEBHOOK_URL` | Discord Webhook 完整網址 |

### `settings.json`

請在 `config/settings.json` 中設定你要追蹤的股票與警示條件，例如：

- 自選股清單
- 技術面警示門檻
- 籌碼面條件
- 期貨部位門檻
- 總經風險監控條件

---

## 執行方式

### 單次執行

```bash
python main.py --mode once
```

立即抓取資料並推送訊息。

### 每日模式

```bash
python main.py --mode daily
```

適合搭配排程工具每日執行，若當日已執行過，則可避免重複推播。

### 基本面模式

```bash
python main.py --mode fundamentals
```

僅更新基本面資料與相關圖表。

---

## 排程方式

### Windows Task Scheduler

1. 開啟「工作排程器」
2. 建立新工作
3. 設定每日執行時間
4. 指定執行 `run_from_task_scheduler.bat`
5. 完成後可透過 `logs/scheduler.log` 查看執行狀況

### Linux / macOS cron

```bash
0 17 * * * cd /path/to/stock-price-bot && python main.py --mode daily >> logs/scheduler.log 2>&1
```

此範例代表每天 17:00 自動執行。

---

## 設定檔範例

<details>
<summary><code>settings.json</code> 範例</summary>

```json
{
  "system_config": {
    "api_delay_seconds": 3,
    "max_retries": 3,
    "enable_macro_alerts": true,
    "enable_market_hot_stocks": false
  },
  "futures_alerts": {
    "foreign_futures_net_oi_alert": -10000
  },
  "watch_list": ["2330", "2317"],
  "technical_alerts": {
    "price_change_pct_threshold": 5.0,
    "volume_breakout_ratio": 2.0,
    "day_trade_ratio_threshold": 0.6,
    "ma_tracking": [10, 20, 60]
  },
  "chip_alerts": {
    "foreign_investor_net_buy_days": 3,
    "investment_trust_net_buy_days": 2,
    "margin_reduction_days": 3,
    "volume_threshold_shares": 1000
  },
  "macro_thresholds": {
    "usd_twd_upper_bound": 32.5,
    "us_10y_yield_upper_bound": 4.5,
    "us_index_drop_alert_pct": -2.0
  }
}
```

</details>

---

## 應用情境

本專案可應用於以下場景：

- 每日自動化市場監控
- 個股觀察清單管理
- 法人與期貨部位異動追蹤
- 總經風險提示
- 以 Discord 建立輕量化市場觀察面板

---

## 未來可優化方向

- 導入 GitHub Actions 或雲端排程，降低本機依賴
- 加入台股休市日判斷，避免非交易日執行
- 支援 LINE、Telegram 等其他通知管道
- 建立歷史訊號查詢介面或簡易 dashboard
- 為訊號判斷模組補上單元測試
- 提高策略邏輯模組化程度，方便未來擴充

---

## 注意事項

本專案公開版本不包含：

- 真實 API token
- Discord Webhook
- 個人設定檔
- 本地快取資料
- 排程執行紀錄

執行前請務必自行建立：

- `config/.env`
- `config/settings.json`

並填入有效的 API 與推播設定。

---
