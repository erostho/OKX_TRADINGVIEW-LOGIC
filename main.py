# main.py
# Port logic from "PRO SPOT SMART ASSISTANT V2.6-GOB-STABLE-WITHDASHBOARD" (Pine v6)
# to a Python worker that scans top 300 OKX USDT pairs and writes results to
# Google Sheet (tab "DATA_SPOT") or an Excel fallback, and sends Telegram alerts.
#
# Requirements (add to requirements.txt):
#   requests
#   pandas
#   numpy
#   gspread
#   google-auth
#   openpyxl   (for Excel fallback)
#
# Environment variables (Render -> Environment):
#   TELEGRAM_BOT_TOKEN    - Telegram bot token
#   TELEGRAM_CHAT_ID      - Chat ID (group/channel/user)
#   OKX_INSTTYPE          - default "SPOT" (or "SWAP" if you want futures)
#   BAR                   - default "1H" (OKX bar string: 1m, 5m, 15m, 1H, 4H, 1D, ...)
#   TOP_N                 - default "300"
#   INTERVAL_SEC          - default "180" (scan loop interval)
#   SERVICE_ACCOUNT_FILE  - path to service account JSON (default: /etc/secrets/service_account.json)
#   SHEET_CSV_URL         - any Google Sheets URL of the target spreadsheet; ID is parsed from it
#   SHEET_NAME            - default "DATA_SPOT"
#
# Notes:
# - This file intentionally mirrors the Pine rules (MUA MẠNH = strongBuySignal).
# - "Giá Mua dự kiến" uses the support zone midpoint (≈ low of signal bar).
# - "Giá Bán dự kiến" uses the most recent "ĐỈNH" rule's high if available.
# - Anti-duplicate via SQLite (symbol|bar_time).
# -----------------------------------------------------------------------------

import os
import re
import time
import json
import math
import sqlite3
import logging
from datetime import datetime, timezone, timedelta

import requests
import numpy as np
import pandas as pd

# ======================
# Config & Logging
# ======================
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "").strip()

OKX_INSTTYPE = os.getenv("OKX_INSTTYPE", "SPOT").upper()
BAR          = os.getenv("BAR", "1H")
TOP_N        = int(os.getenv("TOP_N", "300"))
INTERVAL_SEC = int(os.getenv("INTERVAL_SEC", "180"))

# Chỉ giữ các cặp USDT có giá hiện tại < PRICE_MAX_USDT
PRICE_MAX_USDT = float(os.getenv("PRICE_MAX_USDT", "1.0"))

SERVICE_ACCOUNT_FILE = os.getenv("SERVICE_ACCOUNT_FILE", "/etc/secrets/service_account.json")
SHEET_CSV_URL = os.getenv("SHEET_CSV_URL", "")
SHEET_NAME    = os.getenv("SHEET_NAME", "DATA_SPOT")

VN_TZ_OFFSET = 7  # UTC+7

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

DB_PATH = "signals_spot.db"

# ======================
# Helpers
# ======================
def utcnow():
    return datetime.now(timezone.utc)

def now_vn_str():
    return (utcnow() + timedelta(hours=VN_TZ_OFFSET)).strftime("%Y-%m-%d %H:%M:%S")

def parse_sheet_id(url: str) -> str:
    """Extract Google Sheets spreadsheetId from any standard URL."""
    if not url:
        return ""
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", url)
    return m.group(1) if m else ""

def send_telegram(text: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logging.warning("Telegram env not set; skip sending.")
        return
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True
            },
            timeout=15
        )
        if r.status_code != 200:
            logging.error("Telegram error: %s", r.text)
    except Exception as e:
        logging.exception("Telegram exception: %s", e)

# ======================
# DB (anti-duplicate)
# ======================
def db_init():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS sent (
                hash TEXT PRIMARY KEY,
                created_at TEXT
            )
        """)

def already_sent(key: str) -> bool:
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.execute("SELECT 1 FROM sent WHERE hash = ? LIMIT 1", (key,))
        return cur.fetchone() is not None

def mark_sent(key: str):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("INSERT OR IGNORE INTO sent(hash, created_at) VALUES(?, ?)", (key, utcnow().isoformat()))

# ======================
# Google Sheets Writer
# ======================
def clean_old_rows(ws, days=1):
    """Xoá các dòng có Ngày (cột D) cũ hơn days ngày (theo UTC+7)."""
    import datetime, pytz
    from dateutil import parser

    values = ws.get_all_values()
    if not values or len(values) < 2:
        return
    header = values[0]
    rows = values[1:]
    keep = [header]

    # Tính cutoff
    cutoff = datetime.datetime.now(pytz.timezone("Asia/Ho_Chi_Minh")) - datetime.timedelta(days=days)

    for row in rows:
        try:
            dt = parser.parse(row[3])  # cột "Ngày" (index 3)
            if dt >= cutoff:
                keep.append(row)
        except:
            keep.append(row)  # Nếu parse lỗi thì giữ nguyên

    # Clear sheet rồi ghi lại dữ liệu còn giữ
    ws.clear()
    ws.append_rows(keep, value_input_option="USER_ENTERED")
    logging.info("🧹 Đã xoá dữ liệu cũ hơn %d ngày, giữ lại %d dòng", days, len(keep)-1)
    
def write_rows_to_gsheet(rows):
    """Append rows to SHEET_NAME. Each row is a list of columns."""
    sheet_id = parse_sheet_id(SHEET_CSV_URL)
    if not sheet_id:
        raise RuntimeError("Cannot determine spreadsheetId from SHEET_CSV_URL")

    try:
        import gspread
        from google.oauth2.service_account import Credentials

        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scopes)
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(sheet_id)

        try:
            ws = sh.worksheet(SHEET_NAME)
        except gspread.WorksheetNotFound:
            ws = sh.add_worksheet(title=SHEET_NAME, rows=2000, cols=10)
            # Header per sample
            ws.append_row(["Coin","Tín hiệu","Giá","Ngày","Tần suất","Type","Giá Mua dự kiến","Giá Bán dự kiến"])
            # Auto xoá dữ liệu cũ hơn 1 ngày
            clean_old_rows(ws, days=1)
            
        # Ensure header exists
        values = ws.get_all_values()
        if not values:
            ws.append_row(["Coin","Tín hiệu","Giá","Ngày","Tần suất","Type","Giá Mua dự kiến","Giá Bán dự kiến"])

        # Append new rows
        ws.append_rows(rows, value_input_option="USER_ENTERED")
        logging.info("Appended %d rows to Google Sheet '%s'", len(rows), SHEET_NAME)
        return True

    except Exception as e:
        logging.warning("GSheet write failed: %s", e)
        return False
        
    
def write_rows_to_excel(rows, filename="DATA_SPOT.xlsx"):
    try:
        df = pd.DataFrame(rows, columns=["Coin","Tín hiệu","Giá","Ngày","Tần suất","Type","Giá Mua dự kiến","Giá Bán dự kiến"])
        if os.path.exists(filename):
            # Append by reading and concatenating
            old = pd.read_excel(filename, sheet_name="DATA_SPOT")
            df = pd.concat([old, df], ignore_index=True)
        with pd.ExcelWriter(filename, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="DATA_SPOT")
        logging.info("Saved %d rows to Excel %s", len(rows), filename)
        return True
    except Exception as e:
        logging.error("Excel write failed: %s", e)
        return False

# ======================
# OKX API
# ======================
OKX_BASE = "https://www.okx.com"

def okx_top_usdt_symbols(limit=300, price_max=1.0):
    """
    Lấy toàn bộ tickers theo instType (SPOT/SWAP), chỉ giữ cặp *-USDT có last < price_max,
    rồi sort theo volCcy24h giảm dần và cắt limit.
    """
    url = f"{OKX_BASE}/api/v5/market/tickers"
    params = {"instType": OKX_INSTTYPE}
    r = requests.get(url, params=params, timeout=20)
    r.raise_for_status()
    data = r.json().get("data", [])

    items = []
    total_usdt = total_under = 0
    for x in data:
        instId = x.get("instId", "")
        if not instId.endswith("-USDT"):
            continue
        total_usdt += 1

        # Giá hiện tại
        try:
            last = float(x.get("last", "0"))
        except:
            last = 0.0
        if last <= 0 or last >= price_max:
            continue
        total_under += 1

        # Khối lượng (ưu tiên quote volume)
        vol_quote = x.get("volCcy24h") or x.get("volUsd24h") or x.get("vol24h") or "0"
        try:
            v = float(vol_quote)
        except:
            v = 0.0

        items.append((instId, v, last))

    items.sort(key=lambda t: t[1], reverse=True)  # sort theo volume giảm dần
    symbols = [sym for sym, _, _ in items]
    if limit:
        symbols = symbols[:limit]

    logging.info("Filter price<%.4f: USDT pairs=%d → under=%d → selected=%d",
                 price_max, total_usdt, total_under, len(symbols))
    return symbols

def okx_candles(instId: str, bar: str = "1H", limit: int = 300, drop_unconfirmed: bool = True) -> pd.DataFrame | None:
    """
    Lấy OHLCV tăng dần thời gian, cố gắng đủ 'limit' nến.
    - Tự động gọi /market/history-candles để ghép trang (>100 nến).
    - Chuẩn hoá số cột (thiếu 'confirm' thì tự set = '1').
    - Có thể bỏ nến chưa đóng (confirm=='0') nếu drop_unconfirmed=True.
    """
    def _parse_one(resp_json):
        data = resp_json.get("data", [])
        if not data:
            return pd.DataFrame()

        # Một số trường hợp OKX trả 8 cột (không có confirm). Mặc định thêm confirm='1'
        def _norm_row(row):
            row = list(row)
            if len(row) == 9:
                # ts,o,h,l,c,vol,volCcy,volUsd,confirm
                return row
            elif len(row) == 8:
                # thiếu confirm -> thêm '1'
                return row + ["1"]
            elif len(row) >= 5:
                # ít nhất phải có ts,o,h,l,c (bổ sung vol=0, volCcy=0, volUsd=0, confirm=1)
                need = 9 - len(row)
                return row + (["0"] * (need - 1)) + ["1"]
            else:
                return None

        rows = []
        for r in data:
            nr = _norm_row(r)
            if nr:
                rows.append(nr)

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows, columns=["ts","o","h","l","c","vol","volCcy","volUsd","confirm"])
        # Kiểu dữ liệu
        for col in ["o","h","l","c","vol"]:
            df[col] = df[col].astype(float)
        df["ts"] = pd.to_datetime(df["ts"].astype(int), unit="ms", utc=True)
        df["confirm"] = df["confirm"].astype(str)
        return df

    # Lần 1: lấy mới nhất
    base = f"{OKX_BASE}/api/v5/market/candles"
    hist = f"{OKX_BASE}/api/v5/market/history-candles"
    params = {"instId": instId, "bar": bar, "limit": min(limit, 100)}

    try:
        r = requests.get(base, params=params, timeout=20)
        r.raise_for_status()
        df_all = _parse_one(r.json())

        # Ghép thêm nếu cần >100
        while len(df_all) < limit:
            if df_all.empty:
                break
            # oldest ts hiện có (OKX trả mới→cũ)
            oldest_ts = int(df_all["ts"].min().value / 10**6)  # ms
            r2 = requests.get(hist, params={"instId": instId, "bar": bar, "after": oldest_ts, "limit": 100}, timeout=20)
            r2.raise_for_status()
            df_more = _parse_one(r2.json())
            if df_more.empty:
                break
            # chỉ ghép phần cũ hơn
            df_all = pd.concat([df_all, df_more[df_more["ts"] < df_all["ts"].min()]], ignore_index=True)
            # tránh vòng lặp vô hạn
            if len(df_more) < 2:
                break

        if df_all.empty:
            logging.warning("Candles empty %s", instId)
            return None
        # Sắp xếp thời gian tăng dần
        df_all = df_all.sort_values("ts").reset_index(drop=True)

        # Bỏ nến chưa đóng nếu cần
        if drop_unconfirmed and (len(df_all) > 0) and (df_all["confirm"].iloc[-1] != "1"):
            df_all = df_all.iloc[:-1, :]

        return df_all

    except Exception as e:
        logging.warning("Candles failed %s: %s", instId, e)
        return None

# ======================
# TA functions (mirror Pine)
# ======================
def ema(series: pd.Series, span: int):
    return series.ewm(span=span, adjust=False).mean()

def sma(series: pd.Series, n: int):
    return series.rolling(n, min_periods=n).mean()

def rsi(series: pd.Series, n: int = 14):
    delta = series.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)
    avg_gain = gain.ewm(alpha=1/n, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/n, adjust=False).mean()
    rs = avg_gain / (avg_loss.replace(0, np.nan))
    rsi = 100 - (100 / (1 + rs))
    return rsi

def bb(series: pd.Series, n: int = 20, k: float = 2.0):
    mid = sma(series, n)
    std = series.rolling(n, min_periods=n).std()
    upper = mid + k*std
    lower = mid - k*std
    return mid, upper, lower

def macd_line(series: pd.Series):
    ema12 = ema(series, 12)
    ema26 = ema(series, 26)
    macd = ema12 - ema26
    signal = ema(macd, 9)
    return macd, signal

def adx(high: pd.Series, low: pd.Series, close: pd.Series, n: int = 14):
    # True Range
    tr1 = high - low
    tr2 = (high - close.shift()).abs()
    tr3 = (low - close.shift()).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    up_move = high.diff()
    down_move = -low.diff()
    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)
    plus_dm = pd.Series(plus_dm, index=high.index)
    minus_dm = pd.Series(minus_dm, index=high.index)

    tr_sum = tr.rolling(n, min_periods=n).sum()
    plus_sum = plus_dm.rolling(n, min_periods=n).sum()
    minus_sum = minus_dm.rolling(n, min_periods=n).sum()

    plus_di = 100 * (plus_sum / tr_sum.replace(0, np.nan))
    minus_di = 100 * (minus_sum / tr_sum.replace(0, np.nan))
    dx = ( (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan) ) * 100
    adx_val = sma(dx, n)
    return plus_di, minus_di, adx_val

# ======================
# Pine -> Python: MUA MẠNH
# ======================
def pine_like_buy_strong(df: pd.DataFrame):
    """
    Return (is_strong_buy, extras)
    extras: {
      'entry': float, 'support_low': float, 'support_high': float,
      'real_top': float|None
    }
    """
    o, h, l, c, v = df["o"], df["h"], df["l"], df["c"], df["vol"]

    ema20 = ema(c, 20); ema50 = ema(c, 50); ema100 = ema(c, 100)
    trend_up = (ema20 > ema50) & (ema50 > ema100)
    trend_down = (ema20 < ema50) & (ema50 < ema100)

    # RSI, MACD
    rsi14 = rsi(c, 14)
    macd, macd_sig = macd_line(c)
    macd_cross_up = (macd > macd_sig) & (macd.shift(1) <= macd_sig.shift(1))

    vol_avg20 = sma(v, 20)
    vol_break = v > vol_avg20 * 1.5

    # Candle patterns
    body = (c - o).abs()
    prev_body = body.shift(1)

    hammer = (c < o) & ((h - l) > 2 * (o - c).abs()) & (((c - l) / (h - l).replace(0, np.nan)) > 0.6)
    pinbar = (h - pd.concat([c, o], axis=1).max(axis=1)) > 1.5 * (c - o).abs()
    bull_engulf = (c.shift(1) < o.shift(1)) & (c > o) & (c > o.shift(1)) & (o <= c.shift(1))
    strong_bottom = hammer | pinbar | bull_engulf

    momentum_up = (body > prev_body * 1.2) & (c > o) & (c > h.shift(1))

    ema_gap = (ema20 - ema50).abs() / c
    ema_slope = (ema20 - ema20.shift(1)).abs() / c
    mid, upper, lower = bb(c, 20, 2)
    bb_range = (upper - lower) / c
    is_sideway = (ema_gap < 0.0025) & (ema_slope < 0.0015) & (bb_range < 0.02)

    plus_di, minus_di, adx_val = adx(h, l, c, 14)
    is_trending = (adx_val > 15) & (~is_sideway)

    early_bottom = (c < o) & (rsi14 < 50) & (v > vol_avg20 * 0.85) & (strong_bottom)

    score_vol = (v > vol_avg20 * 0.9).astype(int)
    score_rsi = (rsi14 < 55).astype(int)
    score_pattern = strong_bottom.astype(int)
    score_momentum = momentum_up.astype(int)
    buy_score = score_vol + score_rsi + score_pattern + score_momentum

    buy_nearly = (~is_sideway) & (buy_score >= 3) & (macd_cross_up | early_bottom | momentum_up)

    enable_trend_filter = True  # Pine default true
    cond_trend = trend_up if enable_trend_filter else True
    buy_confirmed = buy_nearly & vol_break & cond_trend & is_trending

    strong_buy = buy_confirmed & (ema20 > ema50) & (c > ema20)

    # ---- Real Top (ĐỈNH) ----
    # realTop = highestbars(high,10) == 0 and rsi > 65 and close > ema20 and body > sma(body,20)
    highest10 = h.rolling(10, min_periods=10).apply(lambda x: 1 if x[-1] == np.max(x) else 0, raw=True)
    real_top_mask = (highest10 == 1) & (rsi14 > 65) & (c > ema20) & (body > sma(body, 20))
    recent_real_top_price = None
    idx = df.index[real_top_mask].tolist()
    if idx:
        recent_real_top_price = float(h.loc[idx[-1]])

    # support zone when buyConfirmed: [low*0.99, low*1.01] at the signal bar
    # We'll take midpoint ~ low as "Giá Mua dự kiến"
    extras = {
        "entry": float(c.iloc[-1]) if not math.isnan(c.iloc[-1]) else None,
        "support_low": float((l.iloc[-1] * 0.99) if buy_confirmed.iloc[-1] else np.nan),
        "support_high": float((l.iloc[-1] * 1.01) if buy_confirmed.iloc[-1] else np.nan),
        "support_mid": float(l.iloc[-1]) if buy_confirmed.iloc[-1] else None,
        "real_top": recent_real_top_price
    }
    debug = {
        "close": float(c.iloc[-1]),
        "rsi": float(rsi14.iloc[-1]),
        "adx": float(adx_val.iloc[-1]),
        "vol_break": bool(vol_break.iloc[-1]),
        "buy_score": int(buy_score.iloc[-1]) if not np.isnan(buy_score.iloc[-1]) else 0,
        "buy_nearly": bool(buy_nearly.iloc[-1]),
        "buy_confirmed": bool(buy_confirmed.iloc[-1]),
        "trend_up": bool(trend_up.iloc[-1]),
        "is_trending": bool(is_trending.iloc[-1]),
    }

    return bool(strong_buy.iloc[-1] if len(strong_buy) else False), extras, debug

# ======================
# Core run
# ======================
def build_row(symbol: str, price: float, buy_mid: float | None, real_top: float | None):
    # Sheet columns:
    # Coin | Tín hiệu | Giá | Ngày | Tần suất | Type | Giá Mua dự kiến | Giá Bán dự kiến
    return [
        symbol.replace("/", "-"),
        "MUA MẠNH",
        round(price, 8) if price is not None else "",
        now_vn_str(),
        120,
        "TRADINGVIEW",
        round(buy_mid, 8) if buy_mid is not None else "",
        round(real_top, 8) if real_top is not None else ""
    ]

def scan_once():
    db_init()
    symbols = okx_top_usdt_symbols(TOP_N, PRICE_MAX_USDT)
    logging.info("Scanning %d symbols (bar %s, instType %s, price<%.4f)",
             len(symbols), BAR, OKX_INSTTYPE, PRICE_MAX_USDT)
    sess = requests.Session()

    found_rows = []
    t_round = time.time()
    for i, instId in enumerate(symbols, 1):
        t0 = time.time()
        try:
            df = okx_candles(instId, BAR, 200)
            if df is None or len(df) < 60:
                logging.info(f"[{i}/{len(symbols)}] {instId} - thiếu dữ liệu, skip")
                continue

            is_buy, ex, dbg = pine_like_buy_strong(df)

            # log chi tiết mỗi coin
            logging.info(
                f"[{i}/{len(symbols)}] {instId} | close={dbg['close']:.8f} "
                f"RSI={dbg['rsi']:.1f} ADX={dbg['adx']:.1f} "
                f"volBreak={dbg['vol_break']} buyScore={dbg['buy_score']} "
                f"near={dbg['buy_nearly']} confirm={dbg['buy_confirmed']} "
                f"trendUp={dbg['trend_up']} trending={dbg['is_trending']} "
                f"strongBuy={is_buy} | {time.time()-t0:.2f}s"
            )

            if not is_buy:
                continue

            # Unique key: symbol | last bar timestamp
            last_ts = df["ts"].iloc[-1]
            key = f"{instId}|{BAR}|{int(last_ts.value/10**9)}"
            if already_sent(key):
                logging.info(f"    ↳ duplicate, đã gửi trước đó: {key}")
                continue

            price   = ex.get("entry")
            buy_mid = ex.get("support_mid")
            real_top= ex.get("real_top")

            row = build_row(instId, price, buy_mid, real_top)
            found_rows.append(row)

            send_telegram(
                f"🔥 <b>TRADINGVIEW SIGNAL BUY</b>\n"
                f"{instId} | TF <b>{BAR}</b>\n"
                f"Giá: <code>{price:.8f}</code>\n"
                f"Vùng mua≈low: <code>{'' if buy_mid is None else f'{buy_mid:.8f}'}</code>\n"
                f"Đỉnh gần: <code>{'N/A' if real_top is None else f'{real_top:.8f}'}</code>\n"
                f"⏱ {now_vn_str()}"
            )
            mark_sent(key)

        except Exception as e:
            logging.warning(f"[{i}/{len(symbols)}] {instId} - lỗi: {e}")

        # throttle nhẹ cho OKX
        time.sleep(0.08)

    logging.info(f"Round done: {len(symbols)} symbols, found={len(found_rows)}, "
                 f"elapsed={time.time()-t_round:.2f}s")

    # Write results if any
    if found_rows:
        ok = write_rows_to_gsheet(found_rows)
        if not ok:
            write_rows_to_excel(found_rows)
    else:
        logging.info("No strong BUY signals this round.")

def main_loop():
    while True:
        start = time.time()
        try:
            scan_once()
        except Exception as e:
            logging.exception("scan_once() crashed: %s", e)
        elapsed = time.time() - start
        sleep_s = max(5, INTERVAL_SEC - int(elapsed))
        time.sleep(sleep_s)

if __name__ == "__main__":
    scan_once()
    #main_loop()
