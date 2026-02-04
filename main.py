import os
import sqlite3
import asyncio
import logging
import math
import re
import functools
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import pandas as pd
import numpy as np
from binance.client import Client
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, ContextTypes, filters

import config

load_dotenv()

# ----------------------------
# Logging
# ----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("scanner")

# ----------------------------
# Env secrets (do NOT hardcode)
# ----------------------------
API_KEY = os.getenv("BINANCE_API_KEY")
API_SECRET = os.getenv("BINANCE_API_SECRET")
TG_TOKEN = os.getenv("TG_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID")
DB_PATH = "/app/data/trading_log.db"

# ----------------------------
# Category tags (used both for output & 24h statistics)
# ----------------------------
CATEGORY_STAR = "star"       # ⭐️ MTF hits>=2 + Rebound + VCP/PP>=1
CATEGORY_BRICK = "brick"     # 🧱 MTF hits>=2 + VCP/PP>=1
CATEGORY_RECYCLE = "recycle" # ♻️ Rebound + VCP/PP>=1
CATEGORY_KNOT = "knot"       # 🪢 4H RS rank + VCP/PP>=2
CATEGORY_MUSHROOM = "mushroom"  # 🍄 4H RS rank + VCP/PP>=1 + Momentum(top%)

CATEGORY_ICON = {
    CATEGORY_STAR: "⭐️",
    CATEGORY_BRICK: "🧱",
    CATEGORY_RECYCLE: "♻️",
    CATEGORY_KNOT: "🪢",
    CATEGORY_MUSHROOM: "🍄",
}

# ----------------------------
# Day RS (user provided TXT) - persisted on disk
# ----------------------------
DAY_RS_SET: set[str] = set()
DAY_RS_LAST_LOADED_AT: datetime | None = None

def parse_day_rs_text(text: str) -> set[str]:
    """Parse Day RS TXT (TradingView style) into a set of symbols like 'SYNUSDT'."""
    if not text:
        return set()
    t = text.upper()
    # Prefer explicit TradingView tokens: BINANCE:XXXUSDT.P
    syms = set(re.findall(r"BINANCE:([A-Z0-9]+)\.P", t))
    if syms:
        return syms
    # Fallback: any token ending with USDT
    parts = re.split(r"[\s,]+", t)
    for p in parts:
        p = p.strip()
        if p.endswith("USDT") and p.replace("_","").isalnum():
            syms.add(p)
    return syms

def load_day_rs_from_disk() -> None:
    """Load Day RS file into memory if exists."""
    global DAY_RS_SET, DAY_RS_LAST_LOADED_AT
    try:
        path = config.DAY_RS_FILE_PATH
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                text = f.read()
            DAY_RS_SET = parse_day_rs_text(text)
            DAY_RS_LAST_LOADED_AT = datetime.now()
            logger.info(f"[DayRS] loaded {len(DAY_RS_SET)} symbols from {path}")
        else:
            DAY_RS_SET = set()
            DAY_RS_LAST_LOADED_AT = None
            logger.info("[DayRS] no file found; Day RS is empty. Send a txt file to bot to set it.")
    except Exception as e:
        logger.error(f"[DayRS] load error: {e}", exc_info=True)
        DAY_RS_SET = set()
        DAY_RS_LAST_LOADED_AT = None

async def send_day_rs_file(bot) -> None:
    """Send current Day RS TXT to telegram (daily push)."""
    try:
        path = config.DAY_RS_FILE_PATH
        if not os.path.exists(path):
            await bot.send_message(
                chat_id=TG_CHAT_ID,
                text="🌞 Day RS 尚未設定。請直接傳送一個 .txt 檔給我（格式同 TradingView 匯入檔）。"
            )
            return
        with open(path, "rb") as f:
            await bot.send_document(
                chat_id=TG_CHAT_ID,
                document=f,
                caption="🌞 Day RS（TV 匯入檔）"
            )
    except Exception as e:
        logger.error(f"[DayRS] send file error: {e}", exc_info=True)

async def day_rs_upload_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Receive a TXT document and set it as Day RS (overwrite old)."""
    try:
        if TG_CHAT_ID and str(update.effective_chat.id) != TG_CHAT_ID:
            return
        doc = update.message.document if update.message else None
        if not doc:
            return

        filename = (doc.file_name or "").lower()
        if not filename.endswith(".txt"):
            await update.message.reply_text("只接受 .txt 檔作為 Day RS。")
            return

        # Ensure data dir exists
        os.makedirs(os.path.dirname(config.DAY_RS_FILE_PATH), exist_ok=True)

        tg_file = await context.bot.get_file(doc.file_id)
        tmp_path = config.DAY_RS_FILE_PATH + ".tmp"
        await tg_file.download_to_drive(custom_path=tmp_path)

        # Validate & parse
        with open(tmp_path, "r", encoding="utf-8", errors="ignore") as f:
            text = f.read()
        new_set = parse_day_rs_text(text)
        if not new_set:
            os.remove(tmp_path)
            await update.message.reply_text("❌ 這個 txt 檔沒有解析到任何 BINANCE:XXXUSDT.P，請確認格式。")
            return

        # Atomic replace (old file is removed)
        os.replace(tmp_path, config.DAY_RS_FILE_PATH)

        # Update memory
        global DAY_RS_SET, DAY_RS_LAST_LOADED_AT
        DAY_RS_SET = new_set
        DAY_RS_LAST_LOADED_AT = datetime.now()

        await update.message.reply_text(
            f"✅ Day RS 已更新：共 {len(DAY_RS_SET)} 檔案已儲存。\n"
            "之後固定排程掃描與 /now 若命中 Day RS 會標記 🌞。"
        )
        logger.info(f"[DayRS] updated by upload: {len(DAY_RS_SET)} symbols")
    except Exception as e:
        logger.error(f"[DayRS] upload handler error: {e}", exc_info=True)
        if update and update.message:
            await update.message.reply_text("❌ 更新 Day RS 失敗（已記錄錯誤 log）。")

if not TG_CHAT_ID:
    logger.warning("TG_CHAT_ID is empty. Telegram send_message will fail until it is set.")

client = Client(API_KEY, API_SECRET)

# ----------------------------
# Global state / caches
# ----------------------------
# 以 UTC 記錄觸發時間，避免 Zeabur 系統時區差異造成的 aware/naive 問題
last_trigger_time = datetime(1970, 1, 1, tzinfo=timezone.utc)
EXCHANGE_INFO_CACHE = {"expires_at": None, "symbols": None}

# KLINE_CACHE: key=(symbol, interval) -> {"df": DataFrame, "limit": int, "expires_at": datetime_utc}
KLINE_CACHE = {}

# RS_RANK_CACHE: key=(interval, benchmark_symbol) -> {"expires_at": datetime_utc, "symbols": set[str]}
RS_RANK_CACHE = {}

# Daily set (for 🌞 tag): key=benchmark_symbol -> set[str]
DAILY_TOP_COINS = {}

# Historical caches (no persistence; used by /yymmdd hh $SYMBOL)
HIST_KLINE_CACHE = {}  # key=(symbol, interval, end_ms, limit) -> DataFrame
HIST_RS_RANK_CACHE = {}  # key=(interval, benchmark_symbol, end_ms) -> dict(top_set, threshold, top_list)
HIST_SCAN_CACHE = {}  # key=(end_ms_1h, benchmark_symbol) -> dict(results, details)


# Concurrency guards
API_SEM = asyncio.Semaphore(config.BINANCE_MAX_CONCURRENCY)

# CPU-bound work offloading (avoid blocking event loop)
CPU_EXECUTOR = ThreadPoolExecutor(
    max_workers=max(1, config.CPU_WORKERS),
    thread_name_prefix="cpu"
)

# ----------------------------
# Helpers: time & intervals
# ----------------------------
def interval_seconds(interval: str) -> int:
    mapping = {
        "30m": 30 * 60,
        "1h": 60 * 60,
        "2h": 2 * 60 * 60,
        "4h": 4 * 60 * 60,
        "1d": 24 * 60 * 60,
    }
    return mapping.get(interval, 60 * 60)

def bars_per_day(interval: str) -> int:
    mapping = {"30m": 48, "1h": 24, "2h": 12, "4h": 6, "1d": 1}
    return mapping.get(interval, 24)

def mrs_length_for_days(interval: str, days: int) -> int:
    return max(1, days * bars_per_day(interval))

def _floor_time_to_interval(ts_utc: datetime, interval: str) -> datetime:
    """Floor ts_utc (timezone-aware UTC) to the opening time of the candle interval."""
    sec = interval_seconds(interval)
    epoch = int(ts_utc.timestamp())
    floored = epoch - (epoch % sec)
    return datetime.fromtimestamp(floored, tz=timezone.utc)

def next_candle_boundary(ts_utc: datetime, interval: str) -> datetime:
    """Return the next candle boundary (open of next candle) in UTC."""
    open_ts = _floor_time_to_interval(ts_utc, interval)
    return open_ts + timedelta(seconds=interval_seconds(interval))

def compute_end_shift(ts_utc: datetime, interval: str) -> int:
    """end_shift=1 -> use last CLOSED candle; end_shift=0 -> allow current candle.

    We dynamically decide based on server time + interval boundary, and apply a small safety window.
    """
    boundary = next_candle_boundary(ts_utc, interval)
    # If we're still before the next boundary (minus safety), current candle is still forming -> shift=1
    if ts_utc < (boundary - timedelta(seconds=config.CANDLE_CLOSE_SAFETY_SEC)):
        return 1
    return 0

async def get_server_time_utc() -> datetime:
    """Fetch Binance server time (UTC). One REST call."""
    payload = await binance_call(client.futures_time)
    if not payload or "serverTime" not in payload:
        # fallback to local UTC
        return datetime.now(timezone.utc)
    return datetime.fromtimestamp(payload["serverTime"] / 1000.0, tz=timezone.utc)
async def get_server_time_utc() -> datetime:
    """Fetch Binance server time (UTC). One REST call."""
    payload = await binance_call(client.futures_time)
    if not payload or "serverTime" not in payload:
        # fallback to local UTC
        return datetime.now(timezone.utc)
    return datetime.fromtimestamp(payload["serverTime"] / 1000.0, tz=timezone.utc)

# ----------------------------
# Historical query time parsing (input in GMT+8 by default)
# ----------------------------
LOCAL_TZ = timezone(timedelta(hours=config.LOCAL_UTC_OFFSET_HOURS))

def parse_yymmdd_hh_to_utc(yymmdd: str, hh: str) -> datetime:
    """Parse yymmdd + hour (0-23) as a *close time* in LOCAL_TZ, return UTC datetime."""
    if not re.fullmatch(r"\d{6}", yymmdd):
        raise ValueError("yymmdd must be 6 digits")
    year = 2000 + int(yymmdd[0:2])
    month = int(yymmdd[2:4])
    day = int(yymmdd[4:6])
    hour = int(hh)
    if not (0 <= hour <= 23):
        raise ValueError("hour must be 0-23")
    dt_local = datetime(year, month, day, hour, 0, 0, tzinfo=LOCAL_TZ)
    return dt_local.astimezone(timezone.utc)

def align_close_boundary_utc(ts_utc: datetime, interval: str) -> datetime:
    """Return the last candle *close boundary* (<= ts_utc) for the given interval, in UTC."""
    if ts_utc.tzinfo is None:
        ts_utc = ts_utc.replace(tzinfo=timezone.utc)
    sec = interval_seconds(interval)
    epoch = int(ts_utc.timestamp())
    floored = epoch - (epoch % sec)
    return datetime.fromtimestamp(floored, tz=timezone.utc)

def kline_end_ms_from_close(close_utc: datetime) -> int:
    """Binance kline endTime expects ms; to avoid look-ahead we use (close_ms - 1)."""
    return int(close_utc.timestamp() * 1000) - 1

def normalize_symbol_input(raw: str) -> str:
    """Accept $CHZ / CHZ / CHZUSDT -> CHZUSDT (USDT-M perpetual)."""
    s = (raw or "").strip().upper()
    s = s.lstrip("$")
    if not s:
        raise ValueError("symbol is empty")
    if s.endswith(".P"):
        s = s.replace(".P", "")
    if s.startswith("BINANCE:"):
        s = s.replace("BINANCE:", "")
    if not s.endswith("USDT"):
        s = s + "USDT"
    return s

def _lru_put(cache: dict, key, value, max_entries: int):
    """A tiny FIFO-LRU-ish cache (dict preserves insertion order in Py3.7+)."""
    try:
        if key in cache:
            cache.pop(key, None)
        cache[key] = value
        while max_entries and len(cache) > max_entries:
            cache.pop(next(iter(cache)))
    except Exception:
        # Never crash on cache maintenance
        logger.debug("[LRU] cache maintenance failed", exc_info=True)

# ----------------------------
# Retry + Exponential Backoff + Semaphore
# ----------------------------
async def binance_call(fn, *args, retries=None, base_delay=None, **kwargs):
    if retries is None:
        retries = config.BINANCE_RETRIES
    if base_delay is None:
        base_delay = config.BINANCE_BASE_DELAY

    for attempt in range(1, retries + 1):
        try:
            async with API_SEM:
                return await asyncio.to_thread(fn, *args, **kwargs)
        except Exception as e:
            delay = base_delay * (2 ** (attempt - 1))
            logger.warning(
                f"[Binance REST Error] {getattr(fn, '__name__', str(fn))} "
                f"attempt={attempt}/{retries} delay={delay:.2f}s err={e}"
            )
            if attempt == retries:
                logger.error(
                    f"[Binance REST Failed] {getattr(fn, '__name__', str(fn))} retries exhausted.",
                    exc_info=True
                )
                return None
            await asyncio.sleep(delay)

# ----------------------------
# Indicators (no pandas_ta dependency)
# ----------------------------
def sma(series: pd.Series, length: int) -> pd.Series:
    return series.rolling(window=length, min_periods=length).mean()

# ----------------------------
# DB
# ----------------------------
def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS scan_history
           (time TEXT, symbol TEXT, price REAL, rs_score REAL, timeframe TEXT)"""
    )

    # 定期掃描入選紀錄：用於統計「過去 24 小時」各標籤入選次數
    # 一個 symbol 在同一次掃描中可能同時屬於多個區塊（例如 🧱 + 🪢），因此以 (time, symbol, tag) 做唯一鍵。
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS pick_history (
               time TEXT NOT NULL,
               symbol TEXT NOT NULL,
               tag TEXT NOT NULL,
               PRIMARY KEY (time, symbol, tag)
           )"""
    )
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_pick_history_time ON pick_history(time)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_pick_history_symbol ON pick_history(symbol)")
    conn.commit()
    conn.close()

    # Load Day RS if exists
    load_day_rs_from_disk()


def _db_connect() -> sqlite3.Connection:
    """Create a sqlite connection with safe defaults."""
    # check_same_thread=False: avoid issues when called from different asyncio tasks
    return sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)


def prune_pick_history(now_utc: datetime) -> None:
    """Keep DB size bounded to avoid Zeabur disk growth."""
    try:
        cutoff = now_utc - timedelta(days=int(getattr(config, "PICK_HISTORY_RETENTION_DAYS", 7)))
        conn = _db_connect()
        with conn:
            conn.execute("DELETE FROM pick_history WHERE time < ?", (cutoff.isoformat(),))
        conn.close()
    except Exception as e:
        logger.error(f"[pick_history] prune error: {e}", exc_info=True)


def save_pick_history(scan_time_utc: datetime, picks: list[tuple[str, str]]) -> None:
    """Persist scheduled scan picks.

    Args:
        scan_time_utc: The scan timestamp in UTC.
        picks: List of (symbol, tag) where tag is one of CATEGORY_*.
    """
    if not picks:
        return
    try:
        prune_pick_history(scan_time_utc)
        conn = _db_connect()
        with conn:
            conn.executemany(
                "INSERT OR IGNORE INTO pick_history(time, symbol, tag) VALUES (?, ?, ?)",
                [(scan_time_utc.isoformat(), s, t) for s, t in picks],
            )
        conn.close()
    except Exception as e:
        logger.error(f"[pick_history] save error: {e}", exc_info=True)


def load_pick_counts_last_window(symbols: list[str], now_utc: datetime) -> dict[str, dict[str, int]]:
    """Return per-symbol counts within last N hours for each tag."""
    if not symbols:
        return {}
    window_h = int(getattr(config, "PICK_HISTORY_WINDOW_HOURS", 24))
    since = now_utc - timedelta(hours=window_h)

    # Pre-fill zeros so formatting is stable
    out: dict[str, dict[str, int]] = {
        s: {
            CATEGORY_STAR: 0,
            CATEGORY_BRICK: 0,
            CATEGORY_RECYCLE: 0,
            CATEGORY_KNOT: 0,
            CATEGORY_MUSHROOM: 0,
        }
        for s in symbols
    }

    try:
        placeholders = ",".join(["?"] * len(symbols))
        sql = (
            "SELECT symbol, tag, COUNT(*) as cnt "
            "FROM pick_history "
            f"WHERE time >= ? AND symbol IN ({placeholders}) "
            "GROUP BY symbol, tag"
        )
        conn = _db_connect()
        cur = conn.cursor()
        cur.execute(sql, [since.isoformat(), *symbols])
        rows = cur.fetchall()
        conn.close()
        for sym, tag, cnt in rows:
            if sym in out and tag in out[sym]:
                out[sym][tag] = int(cnt)
    except Exception as e:
        logger.error(f"[pick_history] query error: {e}", exc_info=True)

    return out

# ----------------------------
# Universe / exchange info (cached)
# ----------------------------
async def get_universe_symbols(ts_utc: datetime) -> list[str]:
    now = ts_utc
    cached = EXCHANGE_INFO_CACHE["symbols"]
    exp = EXCHANGE_INFO_CACHE["expires_at"]
    if cached is not None and exp is not None and now < exp:
        return cached

    info = await binance_call(client.futures_exchange_info)
    if info is None:
        return cached or []

    symbols = [
        s["symbol"]
        for s in info.get("symbols", [])
        if s.get("quoteAsset") == "USDT"
        and s.get("status") == "TRADING"
        and s.get("contractType") == "PERPETUAL"
        and s.get("symbol") not in config.EXCLUDED_SYMBOLS
    ]

    EXCHANGE_INFO_CACHE["symbols"] = symbols
    EXCHANGE_INFO_CACHE["expires_at"] = now + timedelta(minutes=config.EXCHANGE_INFO_TTL_MIN)
    return symbols

# ----------------------------
# Klines with purpose-derived limit + TTL cache
# ----------------------------
def required_kline_limit(interval: str, purpose: str) -> int:
    """Derive klines limit from usage, as requested.

    limit = max(VCP_MIN_BARS, mrs_length + rebound_lookback + EXTRA_BARS)
    """
    # default
    mrs_len = mrs_length_for_days(interval, config.MRS_DAYS)

    lb_days = config.REBOUND_LOOKBACK_DAYS.get(interval)
    if lb_days is None:
        # Not a rebound timeframe; keep it light
        rebound_lb = max(0, bars_per_day(interval) * 3)
    else:
        rebound_lb = int(lb_days * bars_per_day(interval))

    limit = max(
        config.VCP_MIN_BARS if interval in ("30m", "1h", "2h", "4h") else 100,
        mrs_len + rebound_lb + config.KLINE_EXTRA_BARS,
    )

    # Benchmark compare doesn't need so much history
    if purpose == "benchmark":
        need = max(50, int(config.BENCHMARK_LOOKBACK_DAYS * bars_per_day(config.BENCHMARK_COMPARE_INTERVAL)) + 10)
        return need

    # Rank stage: keep consistent so later can reuse, but still bounded
    if purpose == "rank":
        return max(100, min(limit, 200)) if interval != "30m" else max(200, min(limit, 500))

    # Full analysis stage
    if purpose == "analyze":
        # 30m needs ~400-500 after MTF gate; formula already lands in that range
        return min(limit, 500) if interval == "30m" else limit

    return limit

def cache_expiry(ts_utc: datetime, interval: str) -> datetime:
    """Expire at next candle boundary (UTC) + safety buffer."""
    return next_candle_boundary(ts_utc, interval) + timedelta(seconds=config.CANDLE_CLOSE_SAFETY_SEC)

def _prune_kline_cache(ts_utc: datetime):
    if not config.ENABLE_KLINE_CACHE:
        KLINE_CACHE.clear()
        return
    # Drop expired
    expired = [k for k, v in KLINE_CACHE.items() if v.get("expires_at") and ts_utc >= v["expires_at"]]
    for k in expired:
        KLINE_CACHE.pop(k, None)

    # Hard cap by number of entries
    if len(KLINE_CACHE) > config.KLINE_CACHE_MAX_ENTRIES:
        # Remove soonest-expiring first
        items = sorted(KLINE_CACHE.items(), key=lambda kv: kv[1].get("expires_at") or datetime.min.replace(tzinfo=timezone.utc))
        for k, _ in items[: max(0, len(KLINE_CACHE) - config.KLINE_CACHE_MAX_ENTRIES)]:
            KLINE_CACHE.pop(k, None)

async def get_klines_df(symbol: str, interval: str, ts_utc: datetime, purpose: str = "analyze", limit: int | None = None) -> pd.DataFrame | None:
    """Fetch klines as DataFrame with cache + derived limit."""
    if limit is None:
        limit = required_kline_limit(interval, purpose)

    if config.ENABLE_KLINE_CACHE:
        ce = KLINE_CACHE.get((symbol, interval))
        if ce and ce.get("expires_at") and ts_utc < ce["expires_at"] and ce.get("df") is not None:
            df = ce["df"]
            have = int(ce.get("limit") or len(df))
            if have >= limit:
                return df.tail(limit).copy()
            # need more history -> refetch with larger limit

    k = await binance_call(client.futures_klines, symbol=symbol, interval=interval, limit=limit)
    if k is None:
        return None

    df = pd.DataFrame(k).iloc[:, :6]
    df.columns = ["t", "o", "h", "l", "c", "v"]
    df["t"] = pd.to_datetime(df["t"], unit="ms", utc=True)
    df = df.astype({"o": "float", "h": "float", "l": "float", "c": "float", "v": "float"})

    if config.ENABLE_KLINE_CACHE:
        KLINE_CACHE[(symbol, interval)] = {
            "df": df,
            "limit": limit,
            "expires_at": cache_expiry(ts_utc, interval)
        }
    return df

# ----------------------------
# Historical kline fetch (endTime based; does NOT reuse live KLINE_CACHE)
# ----------------------------
async def get_klines_df_at(symbol: str, interval: str, close_utc: datetime, purpose: str = "analyze", limit: int | None = None) -> pd.DataFrame | None:
    """Fetch klines ending at (close_utc) without look-ahead.
    - Uses endTime = close_ms - 1
    - Cached in HIST_KLINE_CACHE by (symbol, interval, end_ms, limit)
    """
    if limit is None:
        limit = required_kline_limit(interval, purpose)

    close_aligned = align_close_boundary_utc(close_utc, interval)
    end_ms = kline_end_ms_from_close(close_aligned)
    key = (symbol, interval, end_ms, limit)

    ce = HIST_KLINE_CACHE.get(key)
    if ce is not None:
        return ce.copy()

    k = await binance_call(client.futures_klines, symbol=symbol, interval=interval, endTime=end_ms, limit=limit)
    if k is None:
        return None
    if not k:
        return None

    df = pd.DataFrame(k).iloc[:, :6]
    df.columns = ["t", "o", "h", "l", "c", "v"]
    df["t"] = pd.to_datetime(df["t"], unit="ms", utc=True)
    df = df.astype({"o": "float", "h": "float", "l": "float", "c": "float", "v": "float"})

    _lru_put(HIST_KLINE_CACHE, key, df, config.HIST_KLINE_CACHE_MAX_ENTRIES)
    return df

# ----------------------------
# MRS (log ratio) aligned to benchmark
# ----------------------------
def calculate_aligned_mrs(df_coin: pd.DataFrame, df_bench: pd.DataFrame, interval: str, ts_utc: datetime, length: int | None = None, end_shift_override: int | None = None) -> tuple[pd.DataFrame | None, float]:
    """Compute MRS based on log(c) - log(c_bench), then compare to its MA.

    mrs = (exp(log_rs - SMA(log_rs)) - 1) * 100
    """
    try:
        if length is None:
            length = mrs_length_for_days(interval, config.MRS_DAYS)

        merged = pd.merge(
            df_coin[["t", "c"]],
            df_bench[["t", "c"]],
            on="t",
            suffixes=("", "_bench"),
            how="inner",
        )

        if len(merged) < length + 5:
            return None, 0.0

        # drop forming candle by dynamic end_shift (or override)
        end_shift = end_shift_override if end_shift_override is not None else compute_end_shift(ts_utc, interval)
        if end_shift and len(merged) > end_shift:
            merged = merged.iloc[:-end_shift].copy()
        elif end_shift and len(merged) <= end_shift:
            return None, 0.0

        c = merged["c"].replace(0, np.nan)
        b = merged["c_bench"].replace(0, np.nan)
        log_rs = np.log(c) - np.log(b)
        log_rs_ma = pd.Series(log_rs).rolling(window=length, min_periods=length).mean()

        merged["rs_log"] = log_rs
        merged["rs_log_ma"] = log_rs_ma
        merged["mrs"] = (np.exp(merged["rs_log"] - merged["rs_log_ma"]) - 1.0) * 100.0

        if merged["mrs"].dropna().empty:
            return None, 0.0

        return merged, float(merged["mrs"].iloc[-1])

    except Exception as e:
        logger.error(f"[calculate_aligned_mrs ERROR] interval={interval} err={e}", exc_info=True)
        return None, 0.0

# ----------------------------
# Rank score (robust: median in recent window)
# ----------------------------
RANK_LOOKBACK_BARS = {
    "1d": 3,
    "4h": 6,
    "2h": 12,
    "1h": 24,
    "30m": 48,
}

def get_rank_mrs_value(df_mrs: pd.DataFrame, interval: str) -> float:
    try:
        if df_mrs is None or "mrs" not in df_mrs.columns:
            return 0.0
        ser = df_mrs["mrs"].dropna()
        if ser.empty:
            return 0.0
        lookback = RANK_LOOKBACK_BARS.get(interval, 12)
        window = ser.tail(min(lookback, len(ser)))
        return float(window.median())
    except Exception as e:
        logger.error(f"[get_rank_mrs_value ERROR] interval={interval} err={e}", exc_info=True)
        return 0.0

# Some historical paths compute rank score from a Series directly.
# Keep a dedicated helper to avoid NameError and keep behavior consistent.
_INVALID_RANK_SCORE = -1e18

def rank_score_from_mrs_series(mrs_series: pd.Series, interval: str) -> float:
    """Compute robust rank score from a raw MRS Series.

    We use the median of the last N bars (interval-specific) to reduce sensitivity
    to the last-bar spike / wick.

    Returns a very negative number when input is invalid.
    """
    try:
        if mrs_series is None:
            return _INVALID_RANK_SCORE
        ser = pd.Series(mrs_series).dropna()
        if ser.empty:
            return _INVALID_RANK_SCORE
        lookback = RANK_LOOKBACK_BARS.get(interval, 12)
        window = ser.tail(min(lookback, len(ser)))
        return float(window.median())
    except Exception as e:
        logger.error(f"[rank_score_from_mrs_series ERROR] interval={interval} err={e}", exc_info=True)
        return _INVALID_RANK_SCORE

# ----------------------------
# Rebound setup
# ----------------------------
def check_mrs_rebound_setup(df_mrs: pd.DataFrame, mode: str) -> bool:
    """Second-leg rebound setup.

    - 曾強：lookback 內 mrs max >= REBOUND_PAST_MAX_MIN
    - 冷卻：current <= max * REBOUND_COOLDOWN_RATIO
    - 未死：current >= REBOUND_SUPPORT_MIN
    """
    try:
        if df_mrs is None or "mrs" not in df_mrs.columns:
            return False

        lb_days = config.REBOUND_LOOKBACK_DAYS.get(mode)
        if lb_days is None:
            return False

        lookback_bars = int(lb_days * bars_per_day(mode))
        recent = df_mrs.tail(lookback_bars)

        min_required = max(30, lookback_bars // 2)
        if len(recent) < min_required:
            return False

        past_max = float(recent["mrs"].max())
        current = float(recent["mrs"].iloc[-1])

        if past_max < config.REBOUND_PAST_MAX_MIN:
            return False
        if current > (past_max * config.REBOUND_COOLDOWN_RATIO):
            return False
        if current < config.REBOUND_SUPPORT_MIN:
            return False
        return True

    except Exception as e:
        logger.error(f"[check_mrs_rebound_setup ERROR] mode={mode} err={e}", exc_info=True)
        return False

# ----------------------------
# Power Play (log exception, avoid silent except)
# ----------------------------
def check_power_play(df: pd.DataFrame) -> bool:
    try:
        close = df["c"]
        volume = df["v"]
        high = df["h"]
        low = df["l"]
        open_ = df["o"]

        if len(close) < 100:
            return False

        lookback = config.PP_LOOKBACK
        climactic_idx = -1
        surge_amplitude = 0.0

        for i in range(2, lookback + 2):
            curr_vol = volume.iloc[-i]
            prev_vol = volume.iloc[-i - 1]
            curr_close = close.iloc[-i]
            curr_open = open_.iloc[-i]
            curr_high = high.iloc[-i]
            curr_low = low.iloc[-i]

            is_vol_surge = curr_vol > (prev_vol * config.PP_VOL_SURGE_MULT)

            past_high = high.iloc[-i - config.PP_BREAKOUT_LOOKBACK: -i].max()
            is_breakout = curr_close > past_high

            if is_vol_surge and is_breakout:
                climactic_idx = i
                range_pct = (curr_high - curr_low) / curr_low * 100.0 if curr_low else 0.0
                body_pct = abs(curr_close - curr_open) / curr_open * 100.0 if curr_open else 0.0
                surge_amplitude = max(range_pct, body_pct)
                break

        if climactic_idx == -1:
            return False

        tolerance_pct = math.ceil(surge_amplitude) / 100.0

        # if breakout bar is very recent, accept
        if climactic_idx <= 2:
            return True

        consolidation_close = close.iloc[-(climactic_idx - 1):]
        con_high = consolidation_close.max()
        con_low = consolidation_close.min()
        if con_low == 0:
            return False

        consolidation_range = (con_high - con_low) / con_low
        return consolidation_range <= tolerance_pct

    except Exception as e:
        logger.error(f"[check_power_play ERROR] err={e}", exc_info=True)
        return False

# ----------------------------
# VCP / trend check (return details)
# ----------------------------
def check_vcp_vol_trend(df: pd.DataFrame, return_details: bool = False):
    try:
        close = df["c"]
        if len(close) < config.VCP_MIN_BARS:
            if return_details:
                return {"passed": False, "trend_ok": False, "power_play": False, "vcp": False}
            return False

        sma_fast = sma(close, config.SMA_FAST)
        sma_mid = sma(close, config.SMA_MID)
        sma_slow = sma(close, config.SMA_SLOW)

        strong_trend_cond = (close > sma_fast) & (sma_fast > sma_mid) & (sma_mid > sma_slow)
        trend_ok = strong_trend_cond.tail(config.TREND_CHECK_WINDOW).sum() >= config.TREND_MIN_TRUE

        if not trend_ok:
            if return_details:
                return {"passed": False, "trend_ok": False, "power_play": False, "vcp": False}
            return False

        prev_close = close.iloc[-2]
        prev_sma_fast = sma_fast.iloc[-2]
        prev_sma_slow = sma_slow.iloc[-2]

        if not (prev_close > prev_sma_fast and prev_sma_fast > (prev_sma_slow * config.SMA60_TOL_RATIO)):
            if return_details:
                return {"passed": False, "trend_ok": True, "power_play": False, "vcp": False}
            return False

        # Path 1: Power Play (dominant trigger)
        power_play = check_power_play(df)
        if power_play:
            if return_details:
                return {"passed": True, "trend_ok": True, "power_play": True, "vcp": False}
            return True

        # Path 2: VCP sd-ratio
        sd5 = close.tail(5).std()
        sd20 = close.tail(20).std()
        sd60 = close.tail(60).std()

        vcp = (
            (sd5 < (sd20 * config.SD5_VS_SD20)) and
            (sd5 < (sd60 * config.SD5_VS_SD60)) and
            (sd20 < (sd60 * config.SD20_VS_SD60))
        )

        if return_details:
            return {"passed": bool(vcp), "trend_ok": True, "power_play": False, "vcp": bool(vcp)}
        return bool(vcp)

    except Exception as e:
        logger.error(f"[check_vcp_vol_trend ERROR] err={e}", exc_info=True)
        if return_details:
            return {"passed": False, "trend_ok": False, "power_play": False, "vcp": False}
        return False

# ----------------------------
# Benchmark selection
# ----------------------------
def _cum_return(df: pd.DataFrame, end_shift: int, bars: int) -> float:
    if df is None or df.empty:
        return -1e9
    if end_shift and len(df) > end_shift:
        df = df.iloc[:-end_shift].copy()
    closes = df["c"].tail(bars)
    if len(closes) < 2:
        return -1e9
    return float((closes.iloc[-1] / closes.iloc[0]) - 1.0)

async def pick_benchmark_symbol(ts_utc: datetime) -> str:
    """Pick BTC or ETH as benchmark by recent strength."""
    interval = config.BENCHMARK_COMPARE_INTERVAL
    bars_need = int(config.BENCHMARK_LOOKBACK_DAYS * bars_per_day(interval)) + 5
    end_shift = compute_end_shift(ts_utc, interval)

    btc = await get_klines_df("BTCUSDT", interval, ts_utc, purpose="benchmark", limit=max(50, bars_need))
    eth = await get_klines_df("ETHUSDT", interval, ts_utc, purpose="benchmark", limit=max(50, bars_need))

    r_btc = _cum_return(btc, end_shift, bars_need)
    r_eth = _cum_return(eth, end_shift, bars_need)

    bench = "ETHUSDT" if r_eth > r_btc else "BTCUSDT"
    logger.info(f"[Benchmark] BTC({r_btc*100:.2f}%) vs ETH({r_eth*100:.2f}%) over ~{config.BENCHMARK_LOOKBACK_DAYS}d -> {bench}")
    return bench

# ----------------------------
# RS Rank candidates (Top10%) with cache per interval
# ----------------------------
async def get_rank90_symbol_set(interval: str, bench_df: pd.DataFrame, symbols: list[str], ts_utc: datetime, benchmark_symbol: str) -> set[str]:
    key = (interval, benchmark_symbol)
    cached = RS_RANK_CACHE.get(key)
    if cached and cached.get("expires_at") and ts_utc < cached["expires_at"]:
        return cached["symbols"]

    # rank all symbols, concurrency controlled by API_SEM
    end_shift = compute_end_shift(ts_utc, interval)

    async def _one(sym: str):
        df = await get_klines_df(sym, interval, ts_utc, purpose="rank")
        if df is None or len(df) < 30:
            return None
        # offload mrs calc to CPU executor
        loop = asyncio.get_running_loop()
        def _calc():
            df_mrs, _ = calculate_aligned_mrs(df, bench_df, interval=interval, ts_utc=ts_utc)
            if df_mrs is None:
                return None
            score = get_rank_mrs_value(df_mrs, interval)
            return (sym, float(score), df)
        return await loop.run_in_executor(CPU_EXECUTOR, _calc)

    tasks = [_one(s) for s in symbols]
    results = await asyncio.gather(*tasks)

    rows = [r for r in results if r is not None]
    if not rows:
        RS_RANK_CACHE[key] = {"expires_at": cache_expiry(ts_utc, interval), "symbols": set(), "top_list": [], "threshold": None}
        return set()

    df_rank = pd.DataFrame([{"symbol": sym, "mrs": score} for sym, score, _df in rows])
    threshold = float(df_rank["mrs"].quantile(1.0 - config.RS_RANK_TOP_PCT))

    top_df = df_rank[df_rank["mrs"] >= threshold].sort_values(by="mrs", ascending=False)
    top_set = set(top_df["symbol"].tolist())

    # 4h: keep df cache only for top20% to reuse later (drop others to reduce memory)
    if interval == "4h" and config.ENABLE_KLINE_CACHE:
        keep_threshold = float(df_rank["mrs"].quantile(1.0 - config.RS_RANK_KEEP_DF_PCT_4H))
        keep_syms = set(df_rank[df_rank["mrs"] >= keep_threshold]["symbol"].tolist())
        # Remove non-kept 4h entries from KLINE_CACHE (only those fetched in this rank run)
        for sym, _score, _df in rows:
            if sym not in keep_syms:
                KLINE_CACHE.pop((sym, interval), None)

    top_list = top_df["symbol"].tolist()
    RS_RANK_CACHE[key] = {"expires_at": cache_expiry(ts_utc, interval), "symbols": top_set, "top_list": top_list, "threshold": threshold}
    logger.info(f"[RS Rank] interval={interval} benchmark={benchmark_symbol} top_set={len(top_set)} threshold={threshold:.4f} end_shift={end_shift}")
    return top_set
# ----------------------------
# Candidate-pool gate (compute Top10% thresholds within the 4H candidate pool)
# This keeps API cost controllable and matches the original design intent.
# ----------------------------
async def compute_candidate_gate_set(interval: str, bench_df: pd.DataFrame, candidate_pool: list[str], ts_utc: datetime) -> tuple[set[str], float]:
    async def _one(sym: str):
        df = await get_klines_df(sym, interval, ts_utc, purpose="rank")
        if df is None or len(df) < 30:
            return None
        loop = asyncio.get_running_loop()

        def _calc():
            df_mrs, _ = calculate_aligned_mrs(df, bench_df, interval=interval, ts_utc=ts_utc)
            if df_mrs is None:
                return None
            score = get_rank_mrs_value(df_mrs, interval)
            return (sym, float(score))

        return await loop.run_in_executor(CPU_EXECUTOR, _calc)

    results = await asyncio.gather(*[_one(s) for s in candidate_pool])
    pairs = [r for r in results if r is not None]
    if not pairs:
        return set(), 0.0

    df_gate = pd.DataFrame(pairs, columns=["symbol", "mrs"])
    threshold = float(df_gate["mrs"].quantile(1.0 - config.RS_RANK_TOP_PCT))
    gate_set = set(df_gate[df_gate["mrs"] >= threshold]["symbol"].tolist())
    return gate_set, threshold


# ----------------------------
# Analysis worker (per symbol features)
# ----------------------------
def _analyze_symbol_worker(
    symbol: str,
    df_4h: pd.DataFrame,
    df_2h: pd.DataFrame,
    df_1h: pd.DataFrame,
    df_30: pd.DataFrame,
    bench_4h: pd.DataFrame,
    bench_2h: pd.DataFrame,
    bench_1h: pd.DataFrame,
    bench_30: pd.DataFrame,
    ts_utc: datetime,
    day_rs_set: set[str],
    end_shift_override: int | None = None,
) -> dict | None:
    try:
        # MRS
        df_mrs_4h, m_4h = calculate_aligned_mrs(df_4h, bench_4h, interval="4h", ts_utc=ts_utc, end_shift_override=end_shift_override)
        df_mrs_2h, m_2h = calculate_aligned_mrs(df_2h, bench_2h, interval="2h", ts_utc=ts_utc, end_shift_override=end_shift_override)
        df_mrs_1h, m_1h = calculate_aligned_mrs(df_1h, bench_1h, interval="1h", ts_utc=ts_utc, end_shift_override=end_shift_override)
        df_mrs_30, m_30 = calculate_aligned_mrs(df_30, bench_30, interval="30m", ts_utc=ts_utc, end_shift_override=end_shift_override)

        if any(x is None for x in [df_mrs_4h, df_mrs_2h, df_mrs_1h, df_mrs_30]):
            return None

        # Rebound flags (MRS tells structure)
        rebound_4h = check_mrs_rebound_setup(df_mrs_4h, mode="4h")
        rebound_2h = check_mrs_rebound_setup(df_mrs_2h, mode="2h")
        rebound_1h = check_mrs_rebound_setup(df_mrs_1h, mode="1h")
        rebound_30 = check_mrs_rebound_setup(df_mrs_30, mode="30m")

        # Tech signals (VCP/PowerPlay dominates trigger)
        tech_4h = check_vcp_vol_trend(df_4h, return_details=True)
        tech_2h = check_vcp_vol_trend(df_2h, return_details=True)
        tech_1h = check_vcp_vol_trend(df_1h, return_details=True)
        tech_30 = check_vcp_vol_trend(df_30, return_details=True)

        is_daily_res = symbol in day_rs_set

        return {
            "symbol": symbol,
            "mrs": {"4h": float(m_4h), "2h": float(m_2h), "1h": float(m_1h), "30m": float(m_30)},
            "rebound": {"4h": rebound_4h, "2h": rebound_2h, "1h": rebound_1h, "30m": rebound_30},
            "tech": {"4h": tech_4h, "2h": tech_2h, "1h": tech_1h, "30m": tech_30},
            "is_daily_res": is_daily_res,
        }

    except Exception as e:
        logger.error(f"[Analyze Worker ERROR] symbol={symbol} err={e}", exc_info=True)
        return None

# ----------------------------
# Complex scan (MTF gate + VCP/PP lead + MRS confirm)
# ----------------------------
async def perform_complex_scan() -> list[dict]:
    try:
        ts_utc = await get_server_time_utc()
        _prune_kline_cache(ts_utc)

        symbols = await get_universe_symbols(ts_utc)
        if not symbols:
            return []

        benchmark_symbol = await pick_benchmark_symbol(ts_utc)

        # Benchmark klines (for alignment)
        bench_4h = await get_klines_df(benchmark_symbol, "4h", ts_utc, purpose="analyze")
        bench_2h = await get_klines_df(benchmark_symbol, "2h", ts_utc, purpose="analyze")
        bench_1h = await get_klines_df(benchmark_symbol, "1h", ts_utc, purpose="analyze")
        bench_30 = await get_klines_df(benchmark_symbol, "30m", ts_utc, purpose="analyze")

        if any(x is None for x in [bench_4h, bench_2h, bench_1h, bench_30]):
            return []

        # 1) 4H RS Rank Top10% (海選池)
        gate_4h_set = await get_rank90_symbol_set("4h", bench_4h, symbols, ts_utc, benchmark_symbol)
        candidate_pool = list(gate_4h_set)

        # Day RS snapshot for 🌞 tag (user-provided file)
        day_rs_set_snapshot = set(DAY_RS_SET)

        # 2) MTF hits (1D,4H,2H,1H) — used for scoring/labeling (no longer a hard pre-filter).
        #    1D gate：以候選池內計算（避免全市場日線額外 API）
        # Day RS 🌞 tag uses user-provided TXT (DAY_RS_SET)
        bench_1d = await get_klines_df(benchmark_symbol, "1d", ts_utc, purpose="rank", limit=160)

        # Candidate-pool gates (Top10% thresholds computed WITHIN candidate pool)
        gate_2h_set, th_2h = await compute_candidate_gate_set("2h", bench_2h, candidate_pool, ts_utc)
        gate_1h_set, th_1h = await compute_candidate_gate_set("1h", bench_1h, candidate_pool, ts_utc)
        gate_1d_gate_set, th_1d = (set(), 0.0)
        if bench_1d is not None:
            gate_1d_gate_set, th_1d = await compute_candidate_gate_set("1d", bench_1d, candidate_pool, ts_utc)

        def _mtf_hits(sym: str) -> int:
            hits = 0
            if sym in gate_1d_gate_set:
                hits += 1
            if sym in gate_4h_set:
                hits += 1
            if sym in gate_2h_set:
                hits += 1
            if sym in gate_1h_set:
                hits += 1
            return hits

        mtf_hits_map = {s: _mtf_hits(s) for s in candidate_pool}
        logger.info(
            f"[MTF Hits] 4h_pool={len(candidate_pool)} (require >= {config.MTF_GATE_MIN_HITS}/4 hits across 1d/4h/2h/1h for MTF condition)")

        if not candidate_pool:
            return []

        # 3) Fetch per-symbol klines (30m uses larger limit; bounded by required_kline_limit)
        # Use gather; API_SEM handles in-flight control.
        async def _fetch_symbol(sym: str):
            df_4h = await get_klines_df(sym, "4h", ts_utc, purpose="analyze")
            df_2h = await get_klines_df(sym, "2h", ts_utc, purpose="analyze")
            df_1h = await get_klines_df(sym, "1h", ts_utc, purpose="analyze")
            df_30 = await get_klines_df(sym, "30m", ts_utc, purpose="analyze")
            if any(x is None for x in [df_4h, df_2h, df_1h, df_30]):
                return None
            # CPU heavy compute in executor
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(
                CPU_EXECUTOR,
                functools.partial(
                    _analyze_symbol_worker,
                    sym, df_4h, df_2h, df_1h, df_30,
                    bench_4h, bench_2h, bench_1h, bench_30,
                    ts_utc,
                    day_rs_set_snapshot,
                )
            )

        feat_list = await asyncio.gather(*[_fetch_symbol(s) for s in candidate_pool])
        feats = [x for x in feat_list if x is not None]
        if not feats:
            return []

        # 4) Momentum: among tech-passed candidates, take top X% by m_30
        eligible = [f for f in feats if f["tech"]["30m"]["passed"] or f["tech"]["1h"]["passed"] or f["tech"]["2h"]["passed"]]
        m30_pairs = [(f["symbol"], f["mrs"]["30m"]) for f in eligible if not math.isnan(f["mrs"]["30m"])]

        momentum_syms = set()
        if m30_pairs:
            m30_pairs.sort(key=lambda x: x[1], reverse=True)
            k = max(1, math.ceil(len(m30_pairs) * config.MOMENTUM_TOP_PCT))
            momentum_syms = set([sym for sym, _ in m30_pairs[:k]])
        logger.info(f"[Momentum] eligible={len(m30_pairs)} top_pct={config.MOMENTUM_TOP_PCT:.2f} selected={len(momentum_syms)}")

        # 5) 新的分類輸出（5 個區塊）
        #
        # (1) ⭐️  MTF hits>=2 + Rebound>=1 + VCP/PP>=1
        # (2) 🧱  MTF hits>=2 + VCP/PP>=1
        # (3) ♻️  Rebound>=1 + VCP/PP>=1
        # (4) 🪢  4H RS rank(候選池) + VCP/PP>=2
        # (5) 🍄  4H RS rank(候選池) + VCP/PP>=1 + Momentum topX%
        #
        # 🚀 Momentum topX% 屬於加分標記：若某幣同時在 ⭐️/🧱/♻️/🪢/🍄，也要附帶 🚀。
        out: list[dict] = []

        def _pick_primary_tf_by_tech(tech: dict) -> str | None:
            for tf in ("4h", "2h", "1h", "30m"):
                if bool(tech.get(tf, {}).get("passed")):
                    return tf
            return None

        for f in feats:
            sym = f["symbol"]
            tech = f["tech"]
            reb = f["rebound"]
            m = f["mrs"]

            mtf_hits = int(mtf_hits_map.get(sym, 0))
            rebound_tfs = [tf for tf in ("4h", "2h", "1h", "30m") if bool(reb.get(tf))]
            rebound_hits = len(rebound_tfs)
            vcp_pp_tfs = [tf for tf in ("4h", "2h", "1h", "30m") if bool(tech.get(tf, {}).get("passed"))]
            vcp_pp_hits = len(vcp_pp_tfs)

            cond_mtf = mtf_hits >= config.MTF_GATE_MIN_HITS
            cond_reb = rebound_hits >= 1
            cond_vcp = vcp_pp_hits >= 1

            primary_tf = _pick_primary_tf_by_tech(tech)
            if primary_tf is None:
                continue

            base = {
                "symbol": sym,
                "rs_score": round((m["2h"] + m["1h"] + m["30m"]) / 3.0, 3),
                "primary_tf": primary_tf,
                "mtf_hits": mtf_hits,
                "mtf_flags": {
                    "1d": sym in gate_1d_gate_set,
                    "4h": sym in gate_4h_set,
                    "2h": sym in gate_2h_set,
                    "1h": sym in gate_1h_set,
                },
                "rebound_tfs": rebound_tfs,
                "vcp_pp_tfs": vcp_pp_tfs,
                "momentum": (sym in momentum_syms),
                "is_daily_res": f["is_daily_res"],
            }

            # 去重 + 依優先序歸類（每個幣只會出現在一個分類）
            category: str | None = None
            if cond_vcp and cond_mtf and cond_reb:
                category = CATEGORY_STAR
            elif cond_vcp and cond_mtf:
                category = CATEGORY_BRICK
            elif cond_vcp and cond_reb:
                category = CATEGORY_RECYCLE
            elif cond_vcp and vcp_pp_hits >= 2:
                category = CATEGORY_KNOT
            elif cond_vcp and (sym in momentum_syms):
                category = CATEGORY_MUSHROOM

            if category is not None:
                out.append({**base, "category": category})

        return out

    except Exception as e:
        logger.error(f"[perform_complex_scan ERROR] {e}", exc_info=True)
        return []

# ----------------------------
# Daily scan (1D RS Rank Top25 report)
# ----------------------------
async def perform_daily_scan_1d() -> tuple[list[str], str]:
    """Return (top25_symbols, benchmark_symbol)."""
    try:
        ts_utc = await get_server_time_utc()
        _prune_kline_cache(ts_utc)

        symbols = await get_universe_symbols(ts_utc)
        if not symbols:
            return [], "BTCUSDT"

        benchmark_symbol = await pick_benchmark_symbol(ts_utc)
        bench_1d = await get_klines_df(benchmark_symbol, "1d", ts_utc, purpose="rank", limit=160)
        if bench_1d is None:
            return [], benchmark_symbol

        gate_1d_set = await get_rank90_symbol_set("1d", bench_1d, symbols, ts_utc, benchmark_symbol)
        DAILY_TOP_COINS[benchmark_symbol] = gate_1d_set

        # For daily report: show Top 25 only (ranked by RS score)
        entry = RS_RANK_CACHE.get(("1d", benchmark_symbol), {})
        top_list = entry.get("top_list") or []
        if not top_list:
            top_list = sorted(list(gate_1d_set))
        top25 = top_list[:25]
        return top25, benchmark_symbol

    except Exception as e:
        logger.error(f"[perform_daily_scan_1d ERROR] {e}", exc_info=True)
        return [], "BTCUSDT"

# ----------------------------
# Report sending (kept compatible)
# ----------------------------
async def send_scan_report(
    bot,
    results: list,
    title: str,
    is_daily_only: bool = False,
    *,
    output_time_local: datetime | None = None,
    record_picks: bool = False,
    include_24h_stats: bool = True,
) -> None:
    """Send scan report to Telegram and attach a TradingView import file.

    Args:
        bot: telegram bot instance
        results: scan results
        title: message title
        is_daily_only: whether this is the daily RS list output
        output_time_local: used for naming the output file (Crypto_RS_mmdd:hh)
        record_picks: when True, write results into sqlite pick_history (for scheduled scans)
        include_24h_stats: when True, add per-symbol counts from last N hours (scheduled scans only)
    """
    try:
        if not results:
            if not is_daily_only:
                await bot.send_message(chat_id=TG_CHAT_ID, text=f"{title}\n❌ 無符合標的。")
            else:
                await bot.send_message(chat_id=TG_CHAT_ID, text=f"{title}\nℹ️ 今日無日線 RS Rank > 90 標的。")
            return

        def tf_icon(tf: str) -> str:
            return "🕓" if tf == "4h" else ("🕑" if tf == "2h" else ("🕐" if tf == "1h" else "⚡"))

        tf_order = {"4h": 0, "2h": 1, "1h": 2, "30m": 3}

        # Pick history recording + last N hours stats are only meaningful for non-daily reports
        now_utc = datetime.now(timezone.utc)
        now_local = output_time_local or datetime.now(LOCAL_TZ)
        if output_time_local is not None:
            scan_time_utc = output_time_local.astimezone(timezone.utc)
        else:
            scan_time_utc = now_utc

        # ----------------------------
        # Daily-only (pure RS Rank list)
        # ----------------------------
        if is_daily_only:
            msg = f"**{title}**\n共 {len(results)} 個標的 (Top 25 Limit)\n(純 RS Rank > 90)\n\n"
            for i, s in enumerate(results):
                s_clean = s.replace("BINANCE:", "").replace(".P", "")
                msg += f"{i+1}. `{s_clean}`\n"
            file_content = ",".join([f"BINANCE:{s}.P" for s in results])

            await bot.send_message(chat_id=TG_CHAT_ID, text=msg, parse_mode="Markdown")

            ts = datetime.now().strftime("%m%d_%H%M")
            file_name = f"Scan_Daily_{ts}.txt"
            with open(file_name, "w", encoding="utf-8") as f:
                f.write(file_content)
            with open(file_name, "rb") as f:
                await bot.send_document(chat_id=TG_CHAT_ID, document=f, caption="📄 TV 匯入檔 (日線)")
            os.remove(file_name)
            return

        # ----------------------------
        # Strategy list (5 categories, dedup + priority)
        # ----------------------------
        cat_order = [CATEGORY_STAR, CATEGORY_BRICK, CATEGORY_RECYCLE, CATEGORY_KNOT, CATEGORY_MUSHROOM]
        cat_items: dict[str, list[dict]] = {c: [] for c in cat_order}
        for r in results:
            c = str(r.get("category") or "")
            if c in cat_items:
                cat_items[c].append(r)

        # Save scheduled picks into sqlite (used by 24h stats)
        if record_picks:
            picks = [(str(r.get("symbol")), str(r.get("category"))) for r in results if r.get("symbol") and r.get("category")]
            save_pick_history(scan_time_utc, picks)

        # 24h counts (scheduled scans only)
        counts_map: dict[str, dict[str, int]] = {}
        if include_24h_stats:
            all_syms = sorted({str(r.get("symbol")) for r in results if r.get("symbol")})
            counts_map = load_pick_counts_last_window(all_syms, now_utc)

        # --- Telegram message (compact layout) ---
        # Keep all strategy logic the same; only change the presentation.
        # - No bold title (user wants a cleaner, log-like header)
        # - Compact multi-line label legend
        # - Per-symbol 24h stats: only show the count of its *current* category (dedup+priority)
        msg = f"{title}\n總計標的: {len(results)}\n"
        msg += (
            f"{CATEGORY_ICON[CATEGORY_STAR]}:{len(cat_items[CATEGORY_STAR])} "
            f"{CATEGORY_ICON[CATEGORY_BRICK]}:{len(cat_items[CATEGORY_BRICK])} "
            f"{CATEGORY_ICON[CATEGORY_RECYCLE]}:{len(cat_items[CATEGORY_RECYCLE])} "
            f"{CATEGORY_ICON[CATEGORY_KNOT]}:{len(cat_items[CATEGORY_KNOT])} "
            f"{CATEGORY_ICON[CATEGORY_MUSHROOM]}:{len(cat_items[CATEGORY_MUSHROOM])}\n"
        )
        msg += (
            "\n標記：\n"
            "⭐️=同時命中 MTF & Rebound 🧱= MTF+VCP/PP\n"
            "♻️= Rebound+VCP/PP\n"
            "🪢= 4H RS rank + VCP/PP>=2 🍄= 4H RS rank + VCP/PP>=1  +🚀\n"
            "🚀=Momentum top10%(加分) 🌞=日線RS清單\n"
        )
        if include_24h_stats:
            window_h = int(getattr(config, "PICK_HISTORY_WINDOW_HOURS", 24))
            msg += f"⏱24h 統計：僅計入『定期掃描』過去 {window_h} 小時的入選次數\n"

        def _stats_str(sym: str, category: str) -> str:
            if not include_24h_stats:
                return ""
            c = counts_map.get(sym)
            if not c:
                return ""
            icon = CATEGORY_ICON.get(category, "")
            return f"⏱24h:{icon}{c.get(category, 0)}" if icon else ""

        def build_msg_section(header: str, items: list[dict], limit: int = 25) -> str:
            if not items:
                return ""
            items_sorted = sorted(
                items,
                key=lambda x: (tf_order.get(str(x.get("primary_tf", "30m")), 99), -float(x.get("rs_score", 0.0))),
            )
            txt = f"\n{header} ({len(items_sorted)})\n"
            for i, r in enumerate(items_sorted[:limit]):
                sym = str(r.get("symbol", ""))
                primary_tf = str(r.get("primary_tf") or "")
                cat = str(r.get("category") or "")
                tags: list[str] = []
                if cat in CATEGORY_ICON:
                    tags.append(CATEGORY_ICON[cat])
                if r.get("momentum"):
                    tags.append("🚀")
                if r.get("is_daily_res"):
                    tags.append("🌞")
                tag_str = "".join(tags)
                vcp_tfs = ",".join(r.get("vcp_pp_tfs") or [])
                reb_tfs = ",".join(r.get("rebound_tfs") or [])
                stats = _stats_str(sym, cat)

                # Compact layout (requested): one line for the main info, one line for 24h stats.
                txt += (
                    f"{i+1}. {sym} | {tf_icon(primary_tf)} {primary_tf} | "
                    f"MTF:{r.get('mtf_hits', 0)}/4 | VCP/PP:{vcp_tfs or '-'} | Rebound:{reb_tfs or '-'} | "
                    f"MRS:{r.get('rs_score')} {tag_str}".rstrip()
                    + "\n"
                )
                if stats:
                    txt += f"{stats}\n"
            if len(items_sorted) > limit:
                txt += f"… 其餘 {len(items_sorted) - limit} 個略\n"
            return txt

        # Section titles: shorter and more scannable
        msg += build_msg_section("⭐️ MTF", cat_items[CATEGORY_STAR])
        msg += build_msg_section("🧱 MTF", cat_items[CATEGORY_BRICK])
        msg += build_msg_section("♻️", cat_items[CATEGORY_RECYCLE])
        msg += build_msg_section("🪢", cat_items[CATEGORY_KNOT])
        msg += build_msg_section("🍄", cat_items[CATEGORY_MUSHROOM])

        msg += "\n\n📎 已輸出為同一份清單（用板塊區隔，可直接匯入 TradingView）。"

        # Split long message
        if len(msg) > 3900:
            chunks = [msg[i : i + 3900] for i in range(0, len(msg), 3900)]
            for ch in chunks:
                await bot.send_message(chat_id=TG_CHAT_ID, text=ch, parse_mode="Markdown")
        else:
            await bot.send_message(chat_id=TG_CHAT_ID, text=msg, parse_mode="Markdown")

        def _tv_list(items: list[dict], tf: str) -> str:
            filtered = sorted(
                [r for r in items if r.get("primary_tf") == tf],
                key=lambda x: float(x.get("rs_score", 0.0)),
                reverse=True,
            )
            return ",".join([f"BINANCE:{r['symbol']}.P" for r in filtered])

        tf_label = {"4h": "4H", "2h": "2H", "1h": "1H", "30m": "30F"}
        block_title = {
            CATEGORY_STAR: "⭐️MTF+Rebound+VCPPP",
            CATEGORY_BRICK: "🧱MTF+VCPPP",
            CATEGORY_RECYCLE: "♻️Rebound+VCPPP",
            CATEGORY_KNOT: "🪢4HRS+VCPPP>=2",
            CATEGORY_MUSHROOM: "🍄4HRS+VCPPP+MOM",
        }

        lines: list[str] = []
        for cat in cat_order:
            items = cat_items.get(cat) or []
            lines.append(f"###{block_title.get(cat, cat)}")
            for tf in ("4h", "2h", "1h", "30m"):
                lines.append(f"###{tf_label[tf]}")
                lines.append(_tv_list(items, tf))

        file_content = "\n".join(lines)

        file_name = f"Crypto_RS_{now_local.strftime('%m%d')}:{now_local.strftime('%H')}.txt"
        with open(file_name, "w", encoding="utf-8") as f:
            f.write(file_content)
        with open(file_name, "rb") as f:
            await bot.send_document(chat_id=TG_CHAT_ID, document=f, caption="📄 TV 匯入檔")
        os.remove(file_name)

    except Exception as e:
        logger.error(f"[send_scan_report ERROR] err={e}", exc_info=True)

# ----------------------------
# BTC crash monitor (kept)
# ----------------------------
async def check_btc_crash():
    try:
        k = await binance_call(client.futures_klines, symbol="BTCUSDT", interval="1h", limit=3)
        if k is None:
            return False, ""
        df = pd.DataFrame(k).iloc[:, 1:5].astype(float)
        df.columns = ["o", "h", "l", "c"]
        drop_1h = (df["c"].iloc[-1] - df["o"].iloc[-1]) / df["o"].iloc[-1] * 100.0
        drop_2h = (df["c"].iloc[-1] - df["o"].iloc[-2]) / df["o"].iloc[-2] * 100.0
        if drop_1h < -2.0:
            return True, f"BTC 1H 急殺 ({drop_1h:.2f}%)"
        if drop_2h < -3.5:
            return True, f"BTC 2H 崩跌 ({drop_2h:.2f}%)"
        return False, ""
    except Exception as e:
        logger.error(f"[check_btc_crash ERROR] err={e}", exc_info=True)
        return False, ""

# ----------------------------
# Historical backtest-style query (no persistence)
# ----------------------------
async def pick_benchmark_symbol_at(ts_query_utc: datetime) -> str:
    """Pick BTC or ETH as benchmark at a historical close time."""
    interval = config.BENCHMARK_COMPARE_INTERVAL
    close_cmp = align_close_boundary_utc(ts_query_utc, interval)
    bars_need = max(1, config.BENCHMARK_LOOKBACK_DAYS * bars_per_day(interval))

    df_btc = await get_klines_df_at("BTCUSDT", interval, close_cmp, purpose="rank")
    df_eth = await get_klines_df_at("ETHUSDT", interval, close_cmp, purpose="rank")

    ret_btc = _cum_return(df_btc, 0, bars_need) if df_btc is not None else -1e18
    ret_eth = _cum_return(df_eth, 0, bars_need) if df_eth is not None else -1e18

    # If one is missing, fall back to the other
    if df_btc is None and df_eth is None:
        return "BTCUSDT"
    if df_btc is None:
        return "ETHUSDT"
    if df_eth is None:
        return "BTCUSDT"
    return "BTCUSDT" if ret_btc >= ret_eth else "ETHUSDT"

async def get_rank90_symbol_set_at(interval: str, bench_df: pd.DataFrame, symbols: list[str], close_utc: datetime, benchmark_symbol: str) -> tuple[set[str], float]:
    """Compute top RS_RANK_TOP_PCT set (RS Rank) at a historical close time."""
    close_aligned = align_close_boundary_utc(close_utc, interval)
    end_ms = kline_end_ms_from_close(close_aligned)
    cache_key = (interval, benchmark_symbol, end_ms)

    ce = HIST_RS_RANK_CACHE.get(cache_key)
    if ce and "top_set" in ce and "threshold" in ce:
        return set(ce["top_set"]), float(ce["threshold"])

    async def _calc(sym: str):
        df = await get_klines_df_at(sym, interval, close_aligned, purpose="rank")
        if df is None:
            return None
        merged, _ = calculate_aligned_mrs(df, bench_df, interval=interval, ts_utc=close_aligned, end_shift_override=0)
        if merged is None or merged.get("mrs") is None:
            return None
        score = rank_score_from_mrs_series(merged["mrs"], interval)
        if score <= -1e17:
            return None
        return (sym, float(score))

    pairs = await asyncio.gather(*[_calc(s) for s in symbols])
    pairs = [p for p in pairs if p is not None]
    if not pairs:
        return set(), 0.0

    scores = np.array([p[1] for p in pairs], dtype=float)
    # threshold for top pct (e.g., 90th percentile for top10%)
    rs_top_pct = getattr(config, "RS_TOP_PCT", getattr(config, "RS_RANK_TOP_PCT", 0.10))
    q = max(0.0, min(1.0, 1.0 - float(rs_top_pct)))
    threshold = float(np.quantile(scores, q))

    top_set = set([s for s, sc in pairs if sc >= threshold])
    # store only minimal to keep memory low
    _lru_put(HIST_RS_RANK_CACHE, cache_key, {"top_set": list(top_set), "threshold": threshold}, config.HIST_RS_CACHE_MAX_ENTRIES)
    return top_set, threshold

async def compute_candidate_gate_set_at(interval: str, bench_df: pd.DataFrame, candidate_pool: list[str], close_utc: datetime) -> tuple[set[str], float]:
    """Within candidate_pool compute top RS_RANK_TOP_PCT threshold set at a historical close."""
    close_aligned = align_close_boundary_utc(close_utc, interval)

    async def _calc(sym: str):
        df = await get_klines_df_at(sym, interval, close_aligned, purpose="rank")
        if df is None:
            return None
        merged, _ = calculate_aligned_mrs(df, bench_df, interval=interval, ts_utc=close_aligned, end_shift_override=0)
        if merged is None or merged.get("mrs") is None:
            return None
        score = rank_score_from_mrs_series(merged["mrs"], interval)
        if score <= -1e17:
            return None
        return (sym, float(score))

    pairs = await asyncio.gather(*[_calc(s) for s in candidate_pool])
    pairs = [p for p in pairs if p is not None]
    if not pairs:
        return set(), 0.0

    scores = np.array([p[1] for p in pairs], dtype=float)
    rs_top_pct = getattr(config, "RS_TOP_PCT", getattr(config, "RS_RANK_TOP_PCT", 0.10))
    q = max(0.0, min(1.0, 1.0 - float(rs_top_pct)))
    threshold = float(np.quantile(scores, q))
    top_set = set([s for s, sc in pairs if sc >= threshold])
    return top_set, threshold


async def perform_complex_scan_at(ts_query_utc: datetime) -> dict:
    """Historical scan at `ts_query_utc` (UTC close time).

    Returns a payload:
      {
        "ts_query_utc": datetime,
        "benchmark": str,
        "results": list[dict],   # passed symbols only
        "details": dict[str, dict],  # per-symbol details for all symbols in 4h top pool
      }
    """
    close_1h = align_close_boundary_utc(ts_query_utc, "1h")
    end_ms_1h = kline_end_ms_from_close(close_1h)

    benchmark_symbol = await pick_benchmark_symbol_at(ts_query_utc)
    cache_key = (end_ms_1h, benchmark_symbol)
    ce = HIST_SCAN_CACHE.get(cache_key)
    if ce and isinstance(ce, dict) and "results" in ce and "details" in ce:
        return ce

    symbols = await get_universe_symbols(ts_query_utc)
    if not symbols:
        payload = {"ts_query_utc": ts_query_utc, "benchmark": benchmark_symbol, "results": [], "details": {}}
        _lru_put(HIST_SCAN_CACHE, cache_key, payload, config.HIST_RS_CACHE_MAX_ENTRIES)
        return payload

    # Bench dfs for ranking/alignment (keep consistent with live scan)
    bench_4h = await get_klines_df_at(benchmark_symbol, "4h", ts_query_utc, purpose="rank", limit=160)
    bench_2h = await get_klines_df_at(benchmark_symbol, "2h", ts_query_utc, purpose="rank", limit=160)
    bench_1h = await get_klines_df_at(benchmark_symbol, "1h", ts_query_utc, purpose="rank", limit=160)
    bench_30 = await get_klines_df_at(benchmark_symbol, "30m", ts_query_utc, purpose="rank", limit=200)
    if any(x is None for x in [bench_4h, bench_2h, bench_1h, bench_30]):
        payload = {"ts_query_utc": ts_query_utc, "benchmark": benchmark_symbol, "results": [], "details": {}}
        _lru_put(HIST_SCAN_CACHE, cache_key, payload, config.HIST_RS_CACHE_MAX_ENTRIES)
        return payload

    # 1) 4H RS Rank Top10% (sea pool)
    gate_4h_set, _ = await get_rank90_symbol_set_at("4h", bench_4h, symbols, ts_query_utc, benchmark_symbol)
    candidate_pool = list(gate_4h_set)
    if not candidate_pool:
        payload = {"ts_query_utc": ts_query_utc, "benchmark": benchmark_symbol, "results": [], "details": {}}
        _lru_put(HIST_SCAN_CACHE, cache_key, payload, config.HIST_RS_CACHE_MAX_ENTRIES)
        return payload

    # 2) MTF hits sets (within candidate pool)
    bench_1d = await get_klines_df_at(benchmark_symbol, "1d", ts_query_utc, purpose="rank", limit=160)
    gate_2h_set, _ = await compute_candidate_gate_set_at("2h", bench_2h, candidate_pool, ts_query_utc)
    gate_1h_set, _ = await compute_candidate_gate_set_at("1h", bench_1h, candidate_pool, ts_query_utc)
    gate_1d_gate_set = set()
    if bench_1d is not None:
        gate_1d_gate_set, _ = await compute_candidate_gate_set_at("1d", bench_1d, candidate_pool, ts_query_utc)

    def _mtf_hits(sym: str) -> int:
        hits = 0
        if sym in gate_1d_gate_set:
            hits += 1
        if sym in gate_4h_set:
            hits += 1
        if sym in gate_2h_set:
            hits += 1
        if sym in gate_1h_set:
            hits += 1
        return hits

    mtf_hits_map = {s: _mtf_hits(s) for s in candidate_pool}
    day_rs_set_snapshot = set(DAY_RS_SET)

    async def _fetch_symbol(sym: str):
        df_4h = await get_klines_df_at(sym, "4h", ts_query_utc, purpose="analyze")
        df_2h = await get_klines_df_at(sym, "2h", ts_query_utc, purpose="analyze")
        df_1h = await get_klines_df_at(sym, "1h", ts_query_utc, purpose="analyze")
        df_30 = await get_klines_df_at(sym, "30m", ts_query_utc, purpose="analyze")
        if any(x is None for x in [df_4h, df_2h, df_1h, df_30]):
            return None
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            CPU_EXECUTOR,
            _analyze_symbol_worker,
            sym, df_4h, df_2h, df_1h, df_30,
            bench_4h, bench_2h, bench_1h, bench_30,
            ts_query_utc, day_rs_set_snapshot,
        )

    feat_list = await asyncio.gather(*[_fetch_symbol(s) for s in candidate_pool])
    feats = [x for x in feat_list if x is not None]
    if not feats:
        payload = {"ts_query_utc": ts_query_utc, "benchmark": benchmark_symbol, "results": [], "details": {}}
        _lru_put(HIST_SCAN_CACHE, cache_key, payload, config.HIST_RS_CACHE_MAX_ENTRIES)
        return payload

    # Momentum tag (bonus)
    eligible = [f for f in feats if f["tech"]["30m"]["passed"] or f["tech"]["1h"]["passed"] or f["tech"]["2h"]["passed"]]
    m30_pairs = [(f["symbol"], f["mrs"]["30m"]) for f in eligible if not math.isnan(f["mrs"]["30m"])]
    momentum_syms = set()
    if m30_pairs:
        m30_pairs.sort(key=lambda x: x[1], reverse=True)
        k = max(1, math.ceil(len(m30_pairs) * config.MOMENTUM_TOP_PCT))
        momentum_syms = set([sym for sym, _ in m30_pairs[:k]])

    def _pick_primary_tf_by_tech(tech: dict) -> str | None:
        for tf in ("4h", "2h", "1h", "30m"):
            if bool(tech.get(tf, {}).get("passed")):
                return tf
        return None

    results: list[dict] = []
    details: dict[str, dict] = {}

    for f in feats:
        sym = f["symbol"]
        tech = f["tech"]
        reb = f["rebound"]
        m = f["mrs"]

        mtf_hits = int(mtf_hits_map.get(sym, 0))
        rebound_tfs = [tf for tf in ("4h", "2h", "1h", "30m") if bool(reb.get(tf))]
        rebound_hits = len(rebound_tfs)
        vcp_pp_tfs = [tf for tf in ("4h", "2h", "1h", "30m") if bool(tech.get(tf, {}).get("passed"))]
        vcp_pp_hits = len(vcp_pp_tfs)

        cond_mtf = mtf_hits >= config.MTF_GATE_MIN_HITS
        cond_reb = rebound_hits >= 1
        cond_vcp = vcp_pp_hits >= 1

        primary_tf = _pick_primary_tf_by_tech(tech)
        rs_score = round((m["2h"] + m["1h"] + m["30m"]) / 3.0, 3)

        # 去重 + 依優先序歸類（每個幣只會出現在一個分類）
        category: str | None = None
        if cond_vcp and cond_mtf and cond_reb:
            category = CATEGORY_STAR
        elif cond_vcp and cond_mtf:
            category = CATEGORY_BRICK
        elif cond_vcp and cond_reb:
            category = CATEGORY_RECYCLE
        elif cond_vcp and vcp_pp_hits >= 2:
            category = CATEGORY_KNOT
        elif cond_vcp and (sym in momentum_syms):
            category = CATEGORY_MUSHROOM

        categories: list[str] = [category] if category is not None else []

        entry = {
            "symbol": sym,
            "rs_score": rs_score,
            "primary_tf": primary_tf,
            "categories": categories,
            "mtf_hits": mtf_hits,
            "mtf_flags": {
                "1d": sym in gate_1d_gate_set,
                "4h": sym in gate_4h_set,
                "2h": sym in gate_2h_set,
                "1h": sym in gate_1h_set,
            },
            "rebound_tfs": rebound_tfs,
            "vcp_pp_tfs": vcp_pp_tfs,
            "momentum": (sym in momentum_syms),
            "is_daily_res": f.get("is_daily_res", False),
            "passed": (category is not None) and (primary_tf is not None),
            "vcp_pp_hits": vcp_pp_hits,
            "rebound_hits": rebound_hits,
        }
        details[sym] = entry

        # Report output rows (dedup): one row per symbol
        if primary_tf is not None and category is not None:
            results.append(
                {
                    "symbol": sym,
                    "rs_score": rs_score,
                    "primary_tf": primary_tf,
                    "category": category,
                    "mtf_hits": mtf_hits,
                    "mtf_flags": entry["mtf_flags"],
                    "rebound_tfs": rebound_tfs,
                    "vcp_pp_tfs": vcp_pp_tfs,
                    "momentum": entry["momentum"],
                    "is_daily_res": entry["is_daily_res"],
                }
            )

    payload = {"ts_query_utc": ts_query_utc, "benchmark": benchmark_symbol, "results": results, "details": details}
    _lru_put(HIST_SCAN_CACHE, cache_key, payload, config.HIST_RS_CACHE_MAX_ENTRIES)
    return payload

async def evaluate_symbol_historical(ts_query_utc: datetime, symbol: str) -> dict:
    """Return a detailed pass/fail report of `symbol` at the given historical close time.

    Updated gate:
      - (MTF hits(1D,4H,2H,1H) >= MTF_GATE_MIN_HITS) OR (Rebound hit(4H,2H,1H,30m) >= 1)
      - AND then require: VCP/PP hit(4H,2H,1H,30m) >= 1

    Momentum topX% is bonus only (label).
    """
    payload = await perform_complex_scan_at(ts_query_utc)
    benchmark_symbol = payload.get("benchmark")
    details = payload.get("details") or {}

    report = {
        "symbol": symbol,
        "ts_query_utc": ts_query_utc,
        "benchmark": benchmark_symbol,
        "passed": False,
        "categories": [],
        "primary_tf": None,
        "rs_score": None,
        "mtf_hits": 0,
        "mtf_flags": {"1d": False, "4h": False, "2h": False, "1h": False},
        "rebound_tfs": [],
        "vcp_pp_tfs": [],
        "vcp_pp_hits": 0,
        "rebound_hits": 0,
        "momentum": False,
        "is_daily_res": False,
        "checks": [],  # list of (name, ok, detail)
        "notes": [],
    }

    d = details.get(symbol)
    if d is None:
        # Not in 4H top pool -> strategy does not proceed to VCP/PP/Rebound stage
        report["checks"].append(("Gate RS Rank 4H (Top10%)", False, "不在 4H Top10% 候選池"))
        report["notes"].append("此策略目前仍以「4H RS Rank Top10%」作為基礎候選池（效率/一致性）。")
        return report

    # Copy computed fields
    for k in (
        "passed",
        "categories",
        "primary_tf",
        "rs_score",
        "mtf_hits",
        "mtf_flags",
        "rebound_tfs",
        "vcp_pp_tfs",
        "vcp_pp_hits",
        "rebound_hits",
        "momentum",
        "is_daily_res",
    ):
        report[k] = d.get(k)

    cond_mtf = report["mtf_hits"] >= config.MTF_GATE_MIN_HITS
    cond_reb = len(report["rebound_tfs"]) >= 1
    cond_vcp = len(report["vcp_pp_tfs"]) >= 1

    report["checks"].append(("Gate RS Rank 4H (Top10%)", bool(report["mtf_flags"].get("4h")), ""))
    report["checks"].append(
        (
            f"MTF hits >= {config.MTF_GATE_MIN_HITS} / 4",
            bool(cond_mtf),
            f"hits={report['mtf_hits']} flags={report['mtf_flags']}",
        )
    )
    report["checks"].append(
        (
            "Rebound hits >= 1 (4H/2H/1H/30m)",
            bool(cond_reb),
            ",".join(report["rebound_tfs"]) or "-",
        )
    )
    report["checks"].append(("(MTF hits OR Rebound)", bool(cond_mtf or cond_reb), ""))
    report["checks"].append(
        (
            "VCP/PP hits >= 1 (4H/2H/1H/30m)",
            bool(cond_vcp),
            ",".join(report["vcp_pp_tfs"]) or "-",
        )
    )
    report["checks"].append(
        (
            "VCP/PP hits >= 2 (for 🪢)",
            bool((report.get("vcp_pp_hits") or 0) >= 2),
            f"hits={report.get('vcp_pp_hits')}",
        )
    )
    report["checks"].append(
        (
            f"Momentum top{int(config.MOMENTUM_TOP_PCT*100)}% (bonus/for 🍄)",
            bool(report["momentum"]),
            "",
        )
    )
    report["checks"].append(
        (
            "Final selected (any of ⭐️🧱♻️🪢🍄)",
            bool(report["passed"]),
            ",".join(report.get("categories") or []) or "-",
        )
    )

    return report

async def format_historical_report(report: dict) -> str:
    symbol = report.get("symbol")
    ts_utc = report.get("ts_query_utc")
    # Display in local time (GMT+8)
    ts_local = ts_utc.astimezone(LOCAL_TZ) if ts_utc else None
    time_str = ts_local.strftime("%Y-%m-%d %H:%M (GMT+8)") if ts_local else "N/A"
    benchmark = report.get("benchmark") or "N/A"

    lines = []
    header = f"🕰️ 回溯查詢：{symbol} @ {time_str}\nBenchmark：{benchmark}"
    lines.append(header)

    if report.get("notes"):
        for n in report["notes"]:
            lines.append(f"• {n}")

    lines.append("\n**條件檢查：**")
    for name, ok, detail in report.get("checks", []):
        if ok is None:
            mark = "⚠️"
        else:
            mark = "✅" if ok else "❌"
        if detail:
            lines.append(f"{mark} {name} — {detail}")
        else:
            lines.append(f"{mark} {name}")

    if report.get("passed"):
        lines.append("\n🎯 **結果：通過篩選**")
        primary_tf = report.get("primary_tf") or ""

        cats = report.get("categories") or []
        cat_icons = "".join([CATEGORY_ICON.get(c, c) for c in cats])

        tags = [cat_icons] if cat_icons else []
        if report.get("momentum"):
            tags.append("🚀")
        if report.get("is_daily_res"):
            tags.append("🌞")
        tag_str = " ".join([t for t in tags if t])

        lines.append(f"分類：{tag_str}  主時框：`{primary_tf}`".rstrip())
        lines.append(f"MTF hits：`{report.get('mtf_hits')}` / 4")
        lines.append(f"VCP/PP：{','.join(report.get('vcp_pp_tfs') or []) or '-' }")
        lines.append(f"Rebound：{','.join(report.get('rebound_tfs') or []) or '-' }")
    else:
        lines.append("\n⛔ **結果：未通過篩選**")
        # quick hint
        mtf_hits = report.get("mtf_hits")
        if isinstance(mtf_hits, int) and mtf_hits > 0:
            lines.append(f"MTF hits：`{mtf_hits}` / 4")
        if report.get("vcp_pp_tfs") is not None:
            lines.append(f"VCP/PP：{','.join(report.get('vcp_pp_tfs') or []) or '-' }")
        if report.get("rebound_tfs") is not None:
            lines.append(f"Rebound：{','.join(report.get('rebound_tfs') or []) or '-' }")

    return "\n".join(lines)

# ----------------------------
# Telegram commands
# ----------------------------
HIST_CMD_RE = re.compile(r"^/(\d{6})\s+(\d{1,2})(?:\s+\$?([A-Za-z0-9]+))?\s*$")

async def historical_query_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Usage:
    /yymmdd hh
    /yymmdd hh $SYMBOL
    e.g. /260103 16
         /260103 16 $CHZ
    """
    if TG_CHAT_ID and str(update.effective_chat.id) != TG_CHAT_ID:
        return

    text = (update.message.text or "").strip()
    m = HIST_CMD_RE.match(text)
    if not m:
        return

    yymmdd, hh, sym_raw = m.groups()
    try:
        ts_utc = parse_yymmdd_hh_to_utc(yymmdd, hh)
        symbol = normalize_symbol_input(sym_raw) if sym_raw else None
    except Exception as e:
        await update.message.reply_text(f"❌ 參數格式錯誤：{e}")
        return

    try:
        if symbol is None:
            payload = await asyncio.wait_for(
                perform_complex_scan_at(ts_utc),
                timeout=config.HIST_QUERY_TIMEOUT_SEC,
            )
            ts_local = ts_utc.astimezone(LOCAL_TZ)
            title = f"🕰️ 回溯掃描 {ts_local.strftime('%Y-%m-%d %H:%M')} (GMT+8)"
            await send_scan_report(
                context.bot,
                payload.get("results") or [],
                title,
                is_daily_only=False,
                output_time_local=ts_local,
                include_24h_stats=False,
            )
        else:
            report = await asyncio.wait_for(
                evaluate_symbol_historical(ts_utc, symbol),
                timeout=config.HIST_QUERY_TIMEOUT_SEC,
            )
            msg = await format_historical_report(report)
            await update.message.reply_text(msg, parse_mode="Markdown")
    except asyncio.TimeoutError:
        await update.message.reply_text("⚠️ 回溯查詢超時：該時間點計算需要較多 API/運算。請改查較近時間或降低查詢頻率。")
    except Exception as e:
        logger.exception("[HIST] query failed")
        await update.message.reply_text(f"❌ 回溯查詢發生錯誤：{type(e).__name__}: {e}")

async def now_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if TG_CHAT_ID and str(update.effective_chat.id) != TG_CHAT_ID:
        return
    await update.message.reply_text(
        "🚀 啟動完整掃描\n"
        "- 主線： (MTF hits>=2 或 Rebound hits>=1) 且 VCP/PP hits>=1 → ⭐️/🧱/♻️\n"
        "- 加碼： 4H RS rank + VCP/PP hits>=2 → 🪢\n"
        "- 加碼： 4H RS rank + VCP/PP hits>=1 且 Momentum top10% → 🍄 (同時標記🚀)\n"
        "- 標記：🚀=Momentum top10%(加分)｜🌞=日線RS清單\n"
        "- 會附上每幣過去 24h『定期掃描』各分類入選次數"
    )
    results = await perform_complex_scan()
    await send_scan_report(context.bot, results, "🎯 即時策略掃描")

# ----------------------------
# Main loop schedule (keep 59-minute scanning; dynamic end_shift handled inside)
# ----------------------------
async def main_loop(app):
    global last_trigger_time
    init_db()

    while True:
        now_local = datetime.now(LOCAL_TZ)
        now_utc = datetime.now(timezone.utc)

        # Daily: 07:59
        if now_local.hour == config.SCHEDULE_DAILY_HOUR and now_local.minute == config.SCHEDULE_DAILY_MINUTE:
            # 08:00 (GMT+8): push Day RS file (user provided), then do a start-of-day scan
            await send_day_rs_file(app.bot)

            results_complex = await perform_complex_scan()
            close_local = now_local.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
            await send_scan_report(
                app.bot,
                results_complex,
                f"🕒 {close_local.strftime('%H')}:00 定時策略掃描 (Start of Day)",
                is_daily_only=False,
                output_time_local=close_local,
                record_picks=True,
            )
            await asyncio.sleep(60)

        # Hourly: minute=59, excluding daily time (already handled)
        elif (now_local.minute == config.SCHEDULE_SCAN_MINUTE
              and (now_local.hour % config.SCHEDULE_SCAN_EVERY_HOURS) == config.SCHEDULE_SCAN_HOUR_OFFSET
              and not (now_local.hour == config.SCHEDULE_DAILY_HOUR and now_local.minute == config.SCHEDULE_DAILY_MINUTE)):
            results_complex = await perform_complex_scan()
            close_local = now_local.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
            await send_scan_report(
                app.bot,
                results_complex,
                f"🕒 {close_local.strftime('%H')}:00 定時策略掃描",
                is_daily_only=False,
                output_time_local=close_local,
                record_picks=True,
            )
            await asyncio.sleep(60)

        # BTC monitor
        if now_local.minute % 10 == 0:
            is_crashing, reason = await check_btc_crash()
            if is_crashing and (now_utc - last_trigger_time) > timedelta(minutes=60):
                await app.bot.send_message(chat_id=TG_CHAT_ID, text=f"⚠️ **{reason}**\n觸發抗跌掃描...")
                results = await perform_complex_scan()
                await send_scan_report(app.bot, results, "⚡ 抗跌掃描報告")
                last_trigger_time = now_utc
            await asyncio.sleep(60)

        await asyncio.sleep(20)

if __name__ == "__main__":
    application = ApplicationBuilder().token(TG_TOKEN).build()
    application.add_handler(MessageHandler(filters.Regex(r"^/\d{6}\s+\d{1,2}(?:\s+\$?[A-Za-z0-9]+)?\s*$"), historical_query_command))
    application.add_handler(CommandHandler("now", now_command))
    # Receive Day RS txt file upload
    application.add_handler(MessageHandler(filters.Document.ALL, day_rs_upload_handler))

    logger.info("🤖 機器人已上線")
    loop = asyncio.get_event_loop()
    loop.create_task(main_loop(application))
    application.run_polling()
