"""config.py
集中管理可調參數，避免散落在 main.py。
- 祕密/金鑰（BINANCE_API_KEY / TG_TOKEN 等）請放 .env 或 Zeabur 的環境變數
- 本檔提供策略參數、快取、併發與排程參數
"""

import os

# ----------------------------
# 基本參數
# ----------------------------
TZ = os.getenv("TZ", "Asia/Taipei")

# 排程：
# - 固定掃描：每 2 小時在 minute=59 執行一次（例如 01:59,03:59,... 對應 02:00,04:00...）
# - 每日：07:59 會先推送 Day RS TXT（等同 08:00），再做一次「Start of Day」掃描
SCHEDULE_SCAN_MINUTE = int(os.getenv("SCHEDULE_SCAN_MINUTE", "59"))
SCHEDULE_SCAN_EVERY_HOURS = int(os.getenv("SCHEDULE_SCAN_EVERY_HOURS", "2"))
SCHEDULE_SCAN_HOUR_OFFSET = int(os.getenv("SCHEDULE_SCAN_HOUR_OFFSET", "1"))  # 1=奇數小時(01,03,05...)

SCHEDULE_DAILY_HOUR = int(os.getenv("SCHEDULE_DAILY_HOUR", "7"))   # 07:59 => 08:00
SCHEDULE_DAILY_MINUTE = int(os.getenv("SCHEDULE_DAILY_MINUTE", "59"))

# Day RS（由你傳送 TXT 檔給 Bot 設定；會持久保存並每日推送）
DAY_RS_FILE_PATH = os.getenv("DAY_RS_FILE_PATH", "/app/data/day_rs.txt")

# ----------------------------
# Binance API / 併發 / 重試
# ----------------------------
BINANCE_MAX_CONCURRENCY = int(os.getenv("BINANCE_MAX_CONCURRENCY", "25"))
BINANCE_RETRIES = int(os.getenv("BINANCE_RETRIES", "5"))
BINANCE_BASE_DELAY = float(os.getenv("BINANCE_BASE_DELAY", "0.5"))

# 用於判定 K 線是否「已收盤」的安全緩衝（秒）
# 目的：避免在剛好切換 K 線時讀到未穩定資料
CANDLE_CLOSE_SAFETY_SEC = int(os.getenv("CANDLE_CLOSE_SAFETY_SEC", "3"))

# ----------------------------
# CPU Executor（只為避免阻塞 event loop，不是為了加速純 CPU）
# ----------------------------
CPU_WORKERS = int(os.getenv("CPU_WORKERS", "4"))

# ----------------------------
# Universe / 排除名單
# ----------------------------
EXCLUDED_SYMBOLS = [
    "BTCDOMUSDT", "ALLUSDT", "XAUUSDT", "XAGUSDT", "USDCUSDT","TSLAUSDT","INTCUSDT","HOODUSDT","MSTRUSDT","AMZNUSDT","CRCLUSDT","COINUSDT","PLTRUSDT","XPTUSDT","XPDUSDT"
]

# ----------------------------
# 強勢篩選（橫截面）
# ----------------------------
RS_RANK_TOP_PCT = float(os.getenv("RS_RANK_TOP_PCT", "0.10"))  # Top10%
# Backward/compat alias: some code paths may reference RS_TOP_PCT.
# Keep this to avoid runtime crashes.
RS_TOP_PCT = RS_RANK_TOP_PCT
RS_RANK_KEEP_DF_PCT_4H = float(os.getenv("RS_RANK_KEEP_DF_PCT_4H", "0.20"))  # 只保留 Top20% 的 4H df 供後段重用

# Multi-timeframe Gate： (1D, 4H, 2H, 1H) 至少命中 N 個
MTF_GATE_INTERVALS = ["1d", "2h", "1h"]  # 4h 已作為海選池
MTF_GATE_MIN_HITS = int(os.getenv("MTF_GATE_MIN_HITS", "2"))

# ----------------------------
# Benchmark
# ----------------------------
# 先比較 BTC 與 ETH，誰在 lookback 期間表現更強，就用誰做 benchmark
BENCHMARK_LOOKBACK_DAYS = int(os.getenv("BENCHMARK_LOOKBACK_DAYS", "7"))
BENCHMARK_COMPARE_INTERVAL = "4h"

# ----------------------------
# MRS 設定
# ----------------------------
MRS_DAYS = int(os.getenv("MRS_DAYS", "3"))  # 以「時間一致性」統一成 3 天

# momentum：不再用 m_30 > 1，而是候選池內的前 X%
MOMENTUM_TOP_PCT = float(os.getenv("MOMENTUM_TOP_PCT", "0.10"))  # Top10%

# ----------------------------
# Rebound（第二波蓄勢）參數
# ----------------------------
REBOUND_LOOKBACK_DAYS = {
    "30m": 5,
    "1h": 5,
    "2h": 5,
    "4h": 14,
}

# 血統認證：過去 lookback 內曾經很強（MRS 曾經 >= 20）
REBOUND_PAST_MAX_MIN = float(os.getenv("REBOUND_PAST_MAX_MIN", "20"))
# 動能冷卻：目前 <= Max * ratio
REBOUND_COOLDOWN_RATIO = float(os.getenv("REBOUND_COOLDOWN_RATIO", "0.5"))
# 趨勢未死：目前 >= support
REBOUND_SUPPORT_MIN = float(os.getenv("REBOUND_SUPPORT_MIN", "-2"))

# ----------------------------
# VCP / 趨勢（技術面確認）
# ----------------------------
VCP_MIN_BARS = int(os.getenv("VCP_MIN_BARS", "125"))
SMA_FAST = int(os.getenv("SMA_FAST", "30"))
SMA_MID = int(os.getenv("SMA_MID", "45"))
SMA_SLOW = int(os.getenv("SMA_SLOW", "60"))

TREND_CHECK_WINDOW = int(os.getenv("TREND_CHECK_WINDOW", "100"))  # 取最近 100 bars 檢查趨勢一致性
TREND_MIN_TRUE = int(os.getenv("TREND_MIN_TRUE", "40"))          # 100 bars 內至少 40 bars 符合 strong_trend_cond
SMA60_TOL_RATIO = float(os.getenv("SMA60_TOL_RATIO", "0.97"))    # prev_sma30 > prev_sma60 * 0.97

# SD Ratio (VCP 收斂) 門檻
SD5_VS_SD20 = float(os.getenv("SD5_VS_SD20", "0.75"))
SD5_VS_SD60 = float(os.getenv("SD5_VS_SD60", "0.60"))
SD20_VS_SD60 = float(os.getenv("SD20_VS_SD60", "0.80"))

# ----------------------------
# Power Play
# ----------------------------
PP_LOOKBACK = int(os.getenv("PP_LOOKBACK", "15"))
PP_VOL_SURGE_MULT = float(os.getenv("PP_VOL_SURGE_MULT", "2.5"))
PP_BREAKOUT_LOOKBACK = int(os.getenv("PP_BREAKOUT_LOOKBACK", "70"))

# ----------------------------
# K 線取得：由用途推導 limit
# limit = max(VCP_MIN_BARS, mrs_length + rebound_lookback + EXTRA_BARS)
# ----------------------------
KLINE_EXTRA_BARS = int(os.getenv("KLINE_EXTRA_BARS", "20"))

# 快取（降低 API 成本、避免被鎖）
EXCHANGE_INFO_TTL_MIN = int(os.getenv("EXCHANGE_INFO_TTL_MIN", "360"))  # 6 小時
ENABLE_KLINE_CACHE = os.getenv("ENABLE_KLINE_CACHE", "1") == "1"
KLINE_CACHE_MAX_ENTRIES = int(os.getenv("KLINE_CACHE_MAX_ENTRIES", "800"))
# ----------------------------
# 歷史回溯查詢（不落地存檔；用 API 回推）
# ----------------------------
# 你可用 /yymmdd hh $SYMBOL 回到指定時間點做「當下」條件判斷（不依賴 DB 快照）。
# 時間輸入預設以 GMT+8（Asia/Taipei）解讀。
LOCAL_UTC_OFFSET_HOURS = int(os.getenv("LOCAL_UTC_OFFSET_HOURS", "8"))

# 避免記憶體無限制長大：歷史查詢用的 in-memory cache 上限
HIST_KLINE_CACHE_MAX_ENTRIES = int(os.getenv("HIST_KLINE_CACHE_MAX_ENTRIES", "1200"))
HIST_RS_CACHE_MAX_ENTRIES = int(os.getenv("HIST_RS_CACHE_MAX_ENTRIES", "50"))

# 避免單次回溯查詢拖太久（秒）。超時會回覆提示，不會 silent except
HIST_QUERY_TIMEOUT_SEC = int(os.getenv("HIST_QUERY_TIMEOUT_SEC", "45"))

# ----------------------------
# 定期掃描入選紀錄（用於 Telegram 顯示「過去 24 小時」入選次數）
# ----------------------------
PICK_HISTORY_WINDOW_HOURS = int(os.getenv("PICK_HISTORY_WINDOW_HOURS", "24"))
PICK_HISTORY_RETENTION_DAYS = int(os.getenv("PICK_HISTORY_RETENTION_DAYS", "7"))
