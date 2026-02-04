# Binance USDT-M 強勢/蓄勢掃描器（Telegram Bot）

這個專案會在 **Binance USDT-M 永續合約**中做「強勢幣血統 + 入場附近觸發」的篩選，並把結果推送到 Telegram，另外也會輸出 TradingView 匯入檔（強勢 / 蓄勢各一份）。

> ✅ 你要求的重點已實作：
> 1. **保持 59 分掃描（每 2 小時一次）**，但針對 `30m/1h/2h/4h` 以 **Binance server time 動態決定 end_shift**（避免使用未收盤 K 線）
> 2. `30m` 在「通過 MTF gate 的候選池」才拉高 `limit`，並且 `get_klines_df()` 的 limit 改成 **由用途推導**
> 3. **血統（橫截面 RS Rank / MTF gate）保留**，但 **入場觸發改由 VCP/Power Play 主導**，MRS 只做確認/結構
> 4. **重用 4H rank 階段抓到的 df_4h**（只保留 Top20% 的 4H df 快取）
> 5. **4H rank pool 只在 4H 收盤後才更新（cache 到下一個 4H boundary）**；1H/2H/30m 也有 TTL cache（到下一根收盤）
> 6. 用 `asyncio.Semaphore(N)` 控制同時 in-flight requests（預設 25）並搭配 retry/backoff
> 7. momentum 不再用 `m_30 > 1`，改成「候選池內 **m_30 前 10%**」
> 8. 每次掃描先比較 BTC/ETH 近 X 天表現，**自動用更強的那個當 Benchmark**
> 9. `c/c_btc` 改成 `log(c) - log(c_benchmark)`（最後仍回到 % 量級的 MRS，方便沿用既有判斷）


## 🌞 Day RS（手動清單，TXT 檔持久化）

你可以把「Day RS」當成一份 **你自己挑選/整理** 的日線強勢清單（TradingView 匯入格式）。

### 如何設定 / 更新 Day RS
1. 直接在 Telegram 對 Bot **傳送一個 `.txt` 檔**（檔名需以 `.txt` 結尾）
2. Bot 會把它存成 `/app/data/day_rs.txt`
3. 之後 **每天 08:00（Asia/Taipei）** 會自動把這個 TXT 檔再傳一次到群組/聊天
4. 當你再次傳送新的 `.txt` 檔時：**會覆蓋舊檔（舊檔會被刪除）**，並以新檔作為 Day RS

### 🌞 標記規則
- 固定排程掃描與 `/now` 的結果中，若某個幣也出現在 Day RS TXT 內，Bot 會在該幣後面加上 **🌞** 標記。

### TXT 格式要求
- 建議使用 TradingView 匯入格式：`BINANCE:XXXUSDT.P` 以逗號分隔
- 可以分多行、加標題（例如 `###強勢族群,...`），Bot 只會抓 `BINANCE:... .P` 的 token 來解析。


---

## 1) 篩選邏輯（由上到下）

### A. Universe（掃描範圍）
- Binance Futures：`USDT` 報價、`PERPETUAL`、`TRADING`
- 排除名單見 `config.EXCLUDED_SYMBOLS`

### B. Benchmark（BTC vs ETH 自動選擇）
- 用 `config.BENCHMARK_LOOKBACK_DAYS`（預設 7 天）
- 比較 `BTCUSDT` vs `ETHUSDT` 在 `config.BENCHMARK_COMPARE_INTERVAL`（預設 4H）的累積報酬
- 誰更強，就用誰作為這次掃描的 benchmark（用來算相對強弱）

### C. 橫截面 RS Rank（血統）
在每個 interval（1D/4H/2H/1H）：
1. 對每個幣計算 **MRS**（見下面 MRS 定義）
2. 取最近一段的 MRS **中位數**做分數（降低最後一根尖刺影響）
3. 全市場橫截面取 **Top `config.RS_RANK_TOP_PCT`**（預設 10%）

> 海選池：**先取 4H Top10%** → 再做 MTF gate

### D. MTF gate（多時框至少命中 3/4）
- 4H Top10% 已經是海選池
- 再要求 1D / 2H / 1H 也要命中 Top10% 的至少 `config.MTF_GATE_MIN_HITS`（預設 3）個  
  （合計視為 1D/4H/2H/1H 四個時框）

### E. 入場觸發（VCP / Power Play 主導）
對候選池的每個幣，在 4H/2H/1H/30m 都會做「趨勢 + 觸發」：

**趨勢條件（Trend OK）**
- `close > SMA_fast > SMA_mid > SMA_slow`
- 在最近 `config.TREND_CHECK_WINDOW` bars 內至少 `config.TREND_MIN_TRUE` bars 成立
- 且前一根 K（prev bar）：`prev_close > prev_sma_fast` 且 `prev_sma_fast > prev_sma_slow * config.SMA60_TOL_RATIO`

**觸發（Trigger）**
- **Power Play**：爆量突破 + 小幅整理（參數見 config 的 `PP_...`）
- **VCP**：用 close 的標準差收斂比（sd5/sd20/sd60 的條件，參數見 config 的 `SD...`）

> **只要 Trend OK 且 (Power Play 或 VCP)** → 視為該時框 tech passed

### F. 強勢（Momentum）
- 不再用固定閾值 `m_30 > 1`
- 改成：在「tech passed 的候選池」內，取 **m_30 前 `config.MOMENTUM_TOP_PCT`**（預設 10%）
- primary_tf 會優先選 2H > 1H > 30m 中「有通過 tech 的」那個

### G. 蓄勢（Rebound / 第二段）
- 結構（MRS Rebound）用來判斷「第一段→盤整/回調→準備第二段」
- 觸發（VCP/PP）仍是必要條件（避免只因 MRS 回落就納入）

Rebound 條件（每個時框各自）：
- lookback 視時框：`config.REBOUND_LOOKBACK_DAYS`
- 曾強：lookback 內 `max(MRS) >= config.REBOUND_PAST_MAX_MIN`（預設 20）
- 冷卻：`current <= max * config.REBOUND_COOLDOWN_RATIO`（預設 0.5）
- 未死：`current >= config.REBOUND_SUPPORT_MIN`（預設 -2）

---

## 2) MRS 定義（你要求的版本）

對齊 benchmark 的 close 後：
- `log_rs = log(c) - log(c_benchmark)`
- `log_rs_ma = SMA(log_rs, length)`
- `MRS = (exp(log_rs - log_rs_ma) - 1) * 100`

`length` 以「時間一致性」固定為 `config.MRS_DAYS`（預設 3 天），各時框自動換算：
- 30m：3天 = 144 bars
- 1h：3天 = 72 bars
- 2h：3天 = 36 bars
- 4h：3天 = 18 bars

---

## 3) end_shift（避免用到未收盤 K 線）

程式會在每次掃描開頭抓一次 Binance `serverTime`（UTC），並對每個 interval 動態判斷：
- 若「現在還在該 interval 的當根 K 線內」→ `end_shift=1`（丟掉最後一根）
- 若已跨過 boundary（並加上安全秒數 `config.CANDLE_CLOSE_SAFETY_SEC`）→ `end_shift=0`

所以你保持「每小時 59 分掃描」也 OK：  
在 `59:00` 時，`30m/1h/2h/4h` 的當根 K 仍未收盤，因此會自動 `end_shift=1`，避免不穩定訊號。

---

## 4) K 線 limit（由用途推導）

`get_klines_df()` 預設會用：

`limit = max(VCP_MIN_BARS, mrs_length + rebound_lookback + KLINE_EXTRA_BARS)`

- 30m 在通過 MTF gate 後才抓，因此 **30m 會自動落在 400~500 附近**（通常約 404）
- 2H/4H 會至少 125（滿足 VCP）

---

## 5) 快取（降低 API 成本、避免被封鎖）

- `futures_exchange_info`：`config.EXCHANGE_INFO_TTL_MIN`（預設 6 小時）
- 每個 `(symbol, interval)` 的 klines：TTL 到「下一根收盤」（boundary）為止
- RS Rank（Top10% set）：TTL 到「下一根收盤」為止  
  → 尤其 4H：只有在 4H 收盤後才更新
- 4H rank 階段會抓到大量 df_4h，為了省 RAM：只保留 Top `config.RS_RANK_KEEP_DF_PCT_4H`（預設 20%）在 cache

---

## 6) 可調參數總覽（config.py）

你最常會想改的：
- `RS_RANK_TOP_PCT`：Top 10% → 例如 0.08/0.12
- `MTF_GATE_MIN_HITS`：至少命中幾個時框
- `MOMENTUM_TOP_PCT`：強勢名單比例
- `BENCHMARK_LOOKBACK_DAYS`：BTC/ETH 強弱比較窗口
- `REBOUND_*`：蓄勢條件（更保守/更積極）
- `BINANCE_MAX_CONCURRENCY`：併發（被限流就調低）
- `BINANCE_RETRIES` / `BINANCE_BASE_DELAY`：重試與退避
- `SCHEDULE_*`：排程時間

---

## 7) Zeabur 部署注意事項（避免崩潰）

### 必要環境變數
- `BINANCE_API_KEY`
- `BINANCE_API_SECRET`
- `TG_TOKEN`
- `TG_CHAT_ID`

### 建議做法
- 用 Volume 掛載 `/app/data`（SQLite 會寫在 `/app/data/trading_log.db`）
- 如果遇到 Binance 429/限流：
  - 先把 `BINANCE_MAX_CONCURRENCY` 從 25 降到 15 或 10
  - 或提高 `BINANCE_BASE_DELAY`
- 若你想要「用到剛收盤的日線」：
  - Binance 日線通常在 **08:00 (GMT+8)** 收盤  
  - 你目前排在 07:59 會自動 end_shift=1，因此會用上一日的日線  
  - 你可以把 `SCHEDULE_DAILY_MINUTE` 改成 1（08:01）更合理

---

## 8) 本地啟動

```bash
pip install -r requirements.txt
export BINANCE_API_KEY=...
export BINANCE_API_SECRET=...
export TG_TOKEN=...
export TG_CHAT_ID=...
python main.py
```

Telegram 指令：
- `/now`：立即掃描一次

---

## 9) 檔案說明
- `main.py`：主程式（掃描、cache、策略、Telegram 推送）
- `config.py`：所有可調參數
- `requirements.txt`：盡量精簡、不鎖版本（會安裝最新）
- `Dockerfile`：Zeabur 用
- `watchlist.txt`：保留原檔（目前不影響核心策略）
## 回溯查詢（API 回推，不存 DB）

你現在可以「回到任意時間點」查詢 **某一顆幣** 在那個時間點是否符合目前這套篩選條件。  
**不需要也不會儲存過去資料**：Bot 會用 Binance API 的 `endTime` 回推到該時間點，並以該時間點之前的 K 線做判斷。

### 指令（查單一幣）
- `/yymmdd hh $SYMBOL`
  - 例：`/260103 16 $CHZ` → 查 **2026-01-03 16:00 (GMT+8)** 當下，`CHZUSDT` 是否會入選  
  - 回覆會逐條列出條件檢查（✅/❌），包含：
    - 4H RS gate（全市場 Top10%）
    - MTF gate（候選池內 1D/4H/2H/1H 命中 ≥3/4）
    - Trigger tech（各時框的 VCP / Power Play + 趨勢確認）
    - Momentum（候選池內 m_30 前 top%）
    - Rebound（各時框 rebound）

### 注意事項（避免誤判）
- **歷史資料不足**：如果該幣在你查詢的時間點之前尚未上市/資料不夠長，會回覆 `⚠️ K 線不足`，並指出哪個階段無法判斷（不會 silent except）。
- **合約清單以「目前」為準**：已下架/更名的幣，可能會抓不到舊資料；Bot 會提示。
- 如果你太頻繁查回溯，可能碰到 Binance 限流，建議調低 `BINANCE_MAX_CONCURRENCY` 或提高 backoff。

### 可調整參數（config.py）
- `LOCAL_UTC_OFFSET_HOURS`：回溯輸入使用的時區（預設 GMT+8）
- `HIST_QUERY_TIMEOUT_SEC`：單次回溯查詢超時秒數（預設 45）
- `HIST_KLINE_CACHE_MAX_ENTRIES` / `HIST_RS_CACHE_MAX_ENTRIES`：回溯查詢的記憶體快取上限