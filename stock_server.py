#!/usr/bin/env python3
"""
每日股市 Web Dashboard — 含即時新聞 & 自動評語
用法: python3 stock_server.py
然後開啟瀏覽器 http://localhost:8888
"""

import csv
import math
import os
import io
import json
import threading
import time
import zipfile
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn

class ThreadingHTTPServer(ThreadingMixIn, HTTPServer):
    """多執行緒 HTTP 伺服器，避免長時間請求（如下載房價 ZIP）卡住其他請求。"""
    daemon_threads = True

import requests
import yfinance as yf

PORT = int(os.environ.get("PORT", 8888))

# NaN/Inf 不是合法 JSON，用此 encoder 全部轉成 null
class SafeEncoder(json.JSONEncoder):
    def iterencode(self, o, _one_shot=False):
        return super().iterencode(self._sanitize(o), _one_shot)
    def _sanitize(self, o):
        if isinstance(o, float):
            return None if (math.isnan(o) or math.isinf(o)) else o
        if isinstance(o, dict):
            return {k: self._sanitize(v) for k, v in o.items()}
        if isinstance(o, (list, tuple)):
            return [self._sanitize(v) for v in o]
        return o

def safe_json(obj):
    return json.dumps(obj, cls=SafeEncoder)
DATA_CACHE_SECONDS  = 60           # 股價快取 60 秒
HOUSE_CACHE_SECONDS = 3600 * 6     # 房價快取 6 小時（內政部每月更新3次）
PING = 3.3058                      # 1 坪 = 3.3058 平方公尺
HOUSE_DATA_URL = "https://plvr.land.moi.gov.tw/opendata/lvr_landAcsv.zip"
HOUSE_CITY_FILE = "d_lvr_land_a.csv"  # 台南市

# ── 追蹤清單 ───────────────────────────────────────────────────────────────────
WATCHLIST = [
    # ── 台股大盤 ──────────────────────────────────────────────────
    ("^TWII",     "台股加權指數",           "台股大盤",     "NTD"),
    ("0050.TW",   "元大台灣50 (0050)",      "台股大盤",     "NTD"),
    ("0056.TW",   "元大高股息 (0056)",      "台股大盤",     "NTD"),
    ("00631L.TW", "台灣50正2 (00631L)",     "台股大盤",     "NTD"),

    # ── 台灣ETF－高息 ─────────────────────────────────────────────
    ("00878.TW",  "國泰永續高股息 00878",   "台灣ETF－高息", "NTD"),
    ("00900.TW",  "富邦特選高股息30 00900", "台灣ETF－高息", "NTD"),
    ("00919.TW",  "群益台灣精選高息 00919", "台灣ETF－高息", "NTD"),
    ("00940.TW",  "元大台灣價值高息 00940", "台灣ETF－高息", "NTD"),
    ("00713.TW",  "元大台灣高息低波 00713", "台灣ETF－高息", "NTD"),
    ("00731.TW",  "復華富時高息低波 00731", "台灣ETF－高息", "NTD"),

    # ── 台灣ETF－主題 ─────────────────────────────────────────────
    ("00881.TW",  "國泰台灣科技龍頭 00881", "台灣ETF－主題", "NTD"),
    ("00850.TW",  "元大臺灣ESG永續 00850",  "台灣ETF－主題", "NTD"),
    ("00687B.TW", "國泰20年美債 00687B",    "台灣ETF－主題", "NTD"),
    ("00635U.TW", "期元大S&P黃金 00635U",   "台灣ETF－主題", "NTD"),
    ("00738U.TW", "期元大道瓊白銀 00738U",  "台灣ETF－主題", "NTD"),

    # ── 台股－半導體／IC ──────────────────────────────────────────
    ("2330.TW",  "台積電 2330",             "台股－半導體",  "NTD"),
    ("2454.TW",  "聯發科 2454",             "台股－半導體",  "NTD"),
    ("2303.TW",  "聯電 2303",               "台股－半導體",  "NTD"),
    ("2344.TW",  "華邦電 2344",             "台股－半導體",  "NTD"),
    ("6643.TW",  "M31 6643",                "台股－半導體",  "NTD"),
    ("3017.TW",  "奇鋐 3017",               "台股－半導體",  "NTD"),

    # ── 台股－電子製造 ────────────────────────────────────────────
    ("2317.TW",  "鴻海 2317",               "台股－電子製造", "NTD"),
    ("2409.TW",  "友達 2409",               "台股－電子製造", "NTD"),
    ("1519.TW",  "華城 1519",               "台股－電子製造", "NTD"),

    # ── 台股－金融 ────────────────────────────────────────────────
    ("2883.TW",  "開發金 2883",             "台股－金融",    "NTD"),
    ("2884.TW",  "玉山金 2884",             "台股－金融",    "NTD"),
    ("2885.TW",  "元大金 2885",             "台股－金融",    "NTD"),
    ("2886.TW",  "兆豐金 2886",             "台股－金融",    "NTD"),
    ("2891.TW",  "中信金 2891",             "台股－金融",    "NTD"),

    # ── 台股－傳產／航運／消費 ────────────────────────────────────
    ("2610.TW",  "華航 2610",               "台股－傳產其他", "NTD"),
    ("2618.TW",  "長榮航 2618",             "台股－傳產其他", "NTD"),
    ("2002.TW",  "中鋼 2002",               "台股－傳產其他", "NTD"),
    ("6505.TW",  "台塑化 6505",             "台股－傳產其他", "NTD"),
    ("5392.TW",  "能率 5392",               "台股－傳產其他", "NTD"),
    ("5519.TW",  "隆大 5519",               "台股－傳產其他", "NTD"),
    ("2727.TW",  "王品 2727",               "台股－傳產其他", "NTD"),

    # ── 美股大盤 ──────────────────────────────────────────────────
    ("^GSPC",   "S&P 500",                  "美股大盤",      "USD"),
    ("^IXIC",   "Nasdaq",                   "美股大盤",      "USD"),
    ("^DJI",    "Dow Jones",                "美股大盤",      "USD"),
    ("VOO",     "VOO (Vanguard S&P500)",    "美股大盤",      "USD"),

    # ── 美股－科技／AI ────────────────────────────────────────────
    ("TSM",     "TSM (台積電ADR)",          "美股－科技AI",  "USD"),
    ("NVDA",    "NVDA (Nvidia)",            "美股－科技AI",  "USD"),
    ("AMD",     "AMD (超微半導體)",          "美股－科技AI",  "USD"),
    ("AAPL",    "AAPL (Apple)",             "美股－科技AI",  "USD"),
    ("MSFT",    "MSFT (Microsoft)",         "美股－科技AI",  "USD"),
    ("GOOG",    "GOOG (Alphabet)",          "美股－科技AI",  "USD"),
    ("AMZN",    "AMZN (Amazon)",            "美股－科技AI",  "USD"),

    # ── 美股－多元 ────────────────────────────────────────────────
    ("TSLA",    "TSLA (Tesla)",             "美股－多元",    "USD"),
    ("COST",    "COST (Costco)",            "美股－多元",    "USD"),
    ("GEV",     "GEV (GE Vernova)",         "美股－多元",    "USD"),
    ("MRNA",    "MRNA (Moderna)",           "美股－多元",    "USD"),
    ("COIN",    "COIN (Coinbase)",          "美股－多元",    "USD"),
    ("UMAC",    "UMAC (Unusual Machines)",  "美股－多元",    "USD"),
]

# ── 未來主題投資（玻璃基板 AI 封裝供應鏈）────────────────────────────────────
THEME_WATCHLIST = [
    ("3149.TW", "正達 3149",   "NTD", "最上游原材料，具切割拋光能力，與康寧合作"),
    ("3583.TW", "辛耘 3583",   "NTD", "修孔／蝕刻設備，設備股較穩健"),
    ("3030.TW", "德律 3030",   "NTD", "AOI 自動光學檢測龍頭"),
    ("3673.TW", "TPK-KY 3673","NTD", "鑽孔／TGV 技術，留意轉型進度"),
]

SECTIONS_ORDER = [
    "台股大盤", "台灣ETF－高息", "台灣ETF－主題",
    "台股－半導體", "台股－電子製造", "台股－金融", "台股－傳產其他",
    "美股大盤", "美股－科技AI", "美股－多元",
]

# ── 快取 ───────────────────────────────────────────────────────────────────────
_data_cache  = {"quotes": [], "news": {}, "ts": 0}
_house_cache = {"data": None, "ts": 0}
_ark_cache   = {"data": None, "ts": 0}
ARK_CACHE_SECONDS = 3600 * 6  # 6小時更新一次（ARK 每天收盤後更新）
_cache_lock  = threading.Lock()

# ── 規則式自動評語 ──────────────────────────────────────────────────────────────
def stock_note(name, pct):
    """根據漲跌幅產生一句繁中評語。"""
    if pct >= 5:
        return f"{name} 大漲 {pct:+.2f}%，強勢突破，短線動能充足。"
    elif pct >= 2:
        return f"{name} 上漲 {pct:+.2f}%，買盤積極，走勢偏強。"
    elif pct >= 0.5:
        return f"{name} 小幅上漲 {pct:+.2f}%，盤勢穩健。"
    elif pct > -0.5:
        return f"{name} 幾乎平盤（{pct:+.2f}%），多空拉鋸，方向待確認。"
    elif pct > -2:
        return f"{name} 小幅下跌 {pct:+.2f}%，賣壓輕微，留意支撐。"
    elif pct > -5:
        return f"{name} 下跌 {pct:+.2f}%，走勢偏弱，謹慎觀望。"
    else:
        return f"{name} 重挫 {pct:+.2f}%，賣壓沉重，注意風險控管。"

def market_summary(quotes):
    """產生整體市場概況（兩句）。"""
    tw = next((q for q in quotes if q["ticker"] == "^TWII"), None)
    sp = next((q for q in quotes if q["ticker"] == "^GSPC"), None)

    def desc(pct):
        if pct >= 1:   return "上漲"
        if pct >= 0:   return "小漲"
        if pct >= -1:  return "小跌"
        return "下跌"

    parts = []
    if tw and not tw["error"]:
        parts.append(f"台股加權指數今日{desc(tw['change_pct'])} {tw['change_pct']:+.2f}%，"
                     f"收 {tw['price']:,.0f} 點。")
    if sp and not sp["error"]:
        parts.append(f"美股 S&P 500 {desc(sp['change_pct'])} {sp['change_pct']:+.2f}%，"
                     f"整體市場氣氛{'偏多' if sp['change_pct'] >= 0 else '偏空'}。")
    return "　".join(parts) if parts else "市場資料載入中…"

def build_commentary(quotes):
    summary = market_summary(quotes)
    stocks  = {q["name"]: stock_note(q["name"], q["change_pct"])
               for q in quotes if not q["error"]}
    return {"market_summary": summary, "stocks": stocks}

# ── 抓取股價 ────────────────────────────────────────────────────────────────────
def fetch_quote(ticker, name, section, currency):
    import math
    try:
        info  = yf.Ticker(ticker).fast_info
        price = info.last_price
        prev  = info.previous_close
        if price is None or prev is None:
            raise ValueError("null price")
        if math.isnan(price) or math.isnan(prev) or prev == 0:
            raise ValueError("nan/zero price")
        change     = price - prev
        change_pct = change / prev * 100
        rp = round(price, 2); rc = round(change, 2); rpc = round(change_pct, 2)
        if math.isnan(rp) or math.isnan(rc) or math.isnan(rpc):
            raise ValueError("nan after round")
        return dict(ticker=ticker, name=name, section=section, currency=currency,
                    price=rp, change=rc, change_pct=rpc, error=False)
    except Exception:
        return dict(ticker=ticker, name=name, section=section, currency=currency,
                    price=0, change=0, change_pct=0, error=True)

# ── 抓取新聞 ────────────────────────────────────────────────────────────────────
def fetch_news(ticker, max_items=3):
    try:
        raw = yf.Ticker(ticker).news or []
        result = []
        for item in raw[:max_items]:
            c     = item.get("content", {})
            title = c.get("title", "")
            url   = (c.get("canonicalUrl") or {}).get("url", "")
            src   = (c.get("provider") or {}).get("displayName", "")
            pub   = c.get("pubDate", "")
            if title and url:
                result.append({"title": title, "url": url, "source": src, "pub": pub})
        return result
    except Exception:
        return []

# ── 買賣訊號 ───────────────────────────────────────────────────────────────────
def _calc_rsi(closes, period=14):
    deltas = [closes[i] - closes[i-1] for i in range(1, len(closes))]
    gains  = [d if d > 0 else 0 for d in deltas]
    losses = [-d if d < 0 else 0 for d in deltas]
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    if avg_loss == 0:
        return 100.0
    return round(100 - (100 / (1 + avg_gain / avg_loss)), 1)

def fetch_signal(ticker):
    """四條件訊號：MA20 + 5日動能 + RSI 50~70 + 放量 1.2x"""
    try:
        raw     = yf.Ticker(ticker).history(period="60d")
        if len(raw) < 22:
            return {"signal": "N/A"}
        closes  = [float(x) for x in raw["Close"]]
        volumes = [float(x) for x in raw["Volume"]]
        price   = closes[-1]
        ma20    = sum(closes[-20:]) / 20
        prev5   = closes[-6]
        rsi     = _calc_rsi(closes) if len(closes) >= 16 else None
        vol_today = volumes[-1]
        vol_ma10  = sum(volumes[-11:-1]) / 10
        above_ma  = price > ma20
        above_5d  = price > prev5
        rsi_ok    = 50 <= rsi <= 70 if rsi is not None else False
        rsi_hot   = rsi is not None and rsi > 70
        vol_ok    = vol_today >= vol_ma10 * 1.2
        if above_ma and above_5d and rsi_hot:
            signal = "HOT"
        elif above_ma and above_5d and rsi_ok and vol_ok:
            signal = "BUY"
        elif above_ma and above_5d:
            signal = "HOLD"
        elif above_ma or above_5d:
            signal = "HOLD"
        else:
            signal = "SELL"
        return {
            "signal": signal, "ma20": round(ma20, 2), "prev5": round(prev5, 2),
            "rsi": rsi, "vol_ratio": round(vol_today / vol_ma10, 2) if vol_ma10 > 0 else None,
            "above_ma": above_ma, "above_5d": above_5d, "rsi_ok": rsi_ok, "vol_ok": vol_ok,
        }
    except Exception:
        return {"signal": "N/A"}

# ── ARK Invest 持股 ────────────────────────────────────────────────────────────
ARK_FUNDS = {
    "ARKK": "ARK Innovation ETF（顛覆式創新）",
    "ARKW": "ARK Next Generation Internet（下一代網路）",
    "ARKG": "ARK Genomic Revolution（基因科技）",
}

def fetch_ark_holdings(symbol="ARKK", top_n=15):
    """從 arkfunds.io 抓取 ARK 每日持股，回傳前 N 大。"""
    try:
        url = f"https://arkfunds.io/api/v2/etf/holdings?symbol={symbol}"
        r = requests.get(url, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        data = r.json()
        holdings = data.get("holdings", [])[:top_n]
        return {
            "symbol":   symbol,
            "name":     ARK_FUNDS.get(symbol, symbol),
            "date":     data.get("date_from", ""),
            "holdings": [
                {
                    "rank":        h["weight_rank"],
                    "ticker":      h["ticker"],
                    "company":     h["company"],
                    "weight":      h["weight"],
                    "share_price": h["share_price"],
                    "market_value": h["market_value"],
                }
                for h in holdings
            ],
        }
    except Exception as e:
        print(f"[ARK] {symbol} 抓取失敗: {e}", flush=True)
        return {"symbol": symbol, "name": ARK_FUNDS.get(symbol, symbol), "date": "", "holdings": []}

def get_ark_data():
    now = time.time()
    with _cache_lock:
        if _ark_cache["data"] and now - _ark_cache["ts"] < ARK_CACHE_SECONDS:
            return _ark_cache["data"]
        data = [fetch_ark_holdings(s) for s in ARK_FUNDS]
        _ark_cache["data"] = data
        _ark_cache["ts"]   = now
    return data

# ── 台南透天厝房價 ─────────────────────────────────────────────────────────────
def roc_to_date(s):
    """民國年月日(如1150311) → '2026-03-11'"""
    try:
        s = str(s).strip().zfill(7)
        y = int(s[:3]) + 1911
        m = int(s[3:5])
        d = int(s[5:7])
        return f"{y}-{m:02d}-{d:02d}"
    except Exception:
        return ""

EXCLUDE_NOTES = ["親友", "特殊關係", "員工", "共有人", "非常規", "無法核實"]

# ── 三大法人籌碼 ────────────────────────────────────────────────────────────────
from datetime import timedelta
import re as _re

TWSE_T86_URL   = "https://www.twse.com.tw/rwd/zh/fund/T86"
CHIPS_CACHE_SECONDS = 3600 * 8  # 收盤後每日更新一次，快取 8 小時

_chips_cache: dict = {"data": {}, "ts": 0}
_chips_lock  = threading.Lock()

def _parse_tw_num(s):
    try:
        return int(str(s).replace(",", "").replace("+", "").strip())
    except Exception:
        return 0

def fetch_chips():
    """從 TWSE T86 API 抓三大法人最新買賣超（自動試最近5個工作日）"""
    today = datetime.now()
    for delta in range(7):
        d = today - timedelta(days=delta)
        if d.weekday() >= 5:        # 跳過六日
            continue
        date_str = d.strftime("%Y%m%d")
        try:
            r = requests.get(TWSE_T86_URL,
                params={"response": "json", "date": date_str, "selectType": "ALLBUT0999"},
                timeout=15, headers={"User-Agent": "Mozilla/5.0"},
                verify=False)
            j = r.json()
            if j.get("stat") != "OK" or not j.get("data"):
                continue
            result = {}
            trade_date = j.get("date", date_str)
            for row in j["data"]:
                code = str(row[0]).strip()
                result[code] = {
                    "date":        trade_date,
                    "foreign_net": _parse_tw_num(row[4]),   # 外資淨買超（股）
                    "trust_net":   _parse_tw_num(row[10]),  # 投信淨買超
                    "dealer_net":  _parse_tw_num(row[13]) + _parse_tw_num(row[16]),  # 自營商
                    "total_net":   _parse_tw_num(row[17]),  # 三大法人合計
                }
            print(f"[Chips] 抓到 {len(result)} 支，日期 {trade_date}", flush=True)
            return result
        except Exception as e:
            print(f"[Chips] {date_str} 失敗: {e}", flush=True)
    return {}

def get_chips():
    now = time.time()
    with _chips_lock:
        if _chips_cache["data"] and now - _chips_cache["ts"] < CHIPS_CACHE_SECONDS:
            return _chips_cache["data"]
    data = fetch_chips()
    with _chips_lock:
        _chips_cache["data"] = data
        _chips_cache["ts"]   = now
    return data

# ── 台股重大訊息（MOPS） ────────────────────────────────────────────────────────
MOPS_URL = "https://mops.twse.com.tw/mops/web/ajax_t05sr01_1"
ANNOUNCE_CACHE_SECONDS = 3600  # 快取 1 小時

_announce_cache: dict = {}          # {tw_code: {"data":[], "ts":0}}
_announce_lock  = threading.Lock()

def fetch_announcements(tw_code):
    """從 MOPS 公開資訊觀測站抓重大訊息"""
    end   = datetime.now()
    start = end - timedelta(days=30)
    try:
        r = requests.post(MOPS_URL, timeout=15,
            headers={"User-Agent": "Mozilla/5.0",
                     "Content-Type": "application/x-www-form-urlencoded",
                     "Referer": "https://mops.twse.com.tw/mops/web/t05sr01_1"},
            data={
                "encodeURIComponent": "1", "step": "1", "firstin": "1",
                "off": "1", "queryName": "co_id", "inpuType": "co_id",
                "TYPEK": "all", "isnew": "false",
                "co_id": tw_code,
                "start_date": start.strftime("%Y%m%d"),
                "end_date":   end.strftime("%Y%m%d"),
            }, verify=False)
        html = r.text
        # 用 regex 解析 HTML 表格
        rows = _re.findall(r'<tr[^>]*>(.*?)</tr>', html, _re.DOTALL)
        results = []
        for row in rows:
            cells = _re.findall(r'<td[^>]*>(.*?)</td>', row, _re.DOTALL)
            if len(cells) < 4:
                continue
            def strip_tags(s):
                return _re.sub(r'<[^>]+>', '', s).strip()
            date_txt  = strip_tags(cells[0])
            time_txt  = strip_tags(cells[1])
            title_txt = strip_tags(cells[3]) if len(cells) > 3 else ""
            # 跳過表頭行
            if not date_txt or not _re.match(r'\d{4}', date_txt):
                continue
            results.append({
                "date":  date_txt,
                "time":  time_txt,
                "title": title_txt,
            })
        return results[:10]   # 最近 10 則
    except Exception as e:
        print(f"[Announce] {tw_code} 失敗: {e}", flush=True)
        return []

def get_announcements(tw_code):
    now = time.time()
    with _announce_lock:
        cached = _announce_cache.get(tw_code)
        if cached and now - cached["ts"] < ANNOUNCE_CACHE_SECONDS:
            return cached["data"]
    data = fetch_announcements(tw_code)
    with _announce_lock:
        _announce_cache[tw_code] = {"data": data, "ts": now}
    return data

def fetch_house_prices():
    """下載內政部實價登錄，回傳台南透天厝分析資料（已過濾異常交易）。"""
    import tempfile, os as _os
    tmp_path = None
    try:
        print("[房價] 開始下載 ZIP...", flush=True)
        r = requests.get(HOUSE_DATA_URL, timeout=120, stream=True,
                         verify=False,
                         headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        # 串流寫入暫存檔，避免記憶體溢出
        with tempfile.NamedTemporaryFile(delete=False, suffix=".zip") as tmp:
            tmp_path = tmp.name
            for chunk in r.iter_content(chunk_size=1024*256):
                if chunk:
                    tmp.write(chunk)
        print(f"[房價] ZIP 下載完成 ({_os.path.getsize(tmp_path)//1024} KB)", flush=True)
        z = zipfile.ZipFile(tmp_path)
        with z.open(HOUSE_CITY_FILE) as f:
            lines = f.read().decode("utf-8-sig").splitlines()
        z.close()
    except Exception as e:
        print(f"[房價] 下載失敗: {e}", flush=True)
        return None
    finally:
        if tmp_path and _os.path.exists(tmp_path):
            try: _os.unlink(tmp_path)
            except: pass

    headers = lines[0].split(",")
    rows = list(csv.DictReader(lines[2:], fieldnames=headers))

    records, excluded = [], []
    for row in rows:
        if "透天" not in row.get("建物型態", ""):
            continue

        note     = row.get("備註", "").strip()
        target   = row.get("交易標的", "").strip()
        transfer = row.get("移轉層次", "").strip()
        purpose  = row.get("主要用途", "").strip()

        # 過濾原因
        skip_reason = ""
        if "土地" == target or "建物" == target:
            skip_reason = "非房地合併交易"
        elif not target.startswith("房地"):
            skip_reason = f"交易標的異常：{target}"
        elif any(kw in note for kw in EXCLUDE_NOTES):
            skip_reason = "親友/特殊關係交易"
        elif "住" not in purpose and purpose:
            skip_reason = f"非住家用途：{purpose}"

        try:
            price   = float(row["總價元"])
            area_m2 = float(row["建物移轉總面積平方公尺"])
            if price <= 0 or area_m2 <= 0:
                skip_reason = skip_reason or "價格或面積為零"
            elif area_m2 < 40:   # 小於 12 坪極可能異常
                skip_reason = skip_reason or f"建物面積過小（{area_m2:.1f}㎡）"
        except (ValueError, TypeError):
            skip_reason = skip_reason or "資料格式錯誤"
            price, area_m2 = 0, 0

        address = row.get("土地位置建物門牌", "").strip()
        date_str = roc_to_date(row.get("交易年月日", ""))

        if skip_reason:
            excluded.append({"address": address, "reason": skip_reason, "price": int(price) if price else 0})
            continue

        area_ping       = area_m2 / PING
        price_per_ping  = price / area_ping
        floor_total     = row.get("總樓層數", "").strip()
        built_roc       = row.get("建築完成年月", "").strip()
        age = ""
        if built_roc and len(built_roc) >= 3:
            try:
                built_ad = int(built_roc[:3]) + 1911
                age = str(datetime.now().year - built_ad)
            except Exception:
                pass

        # 查核連結：Google 搜尋 + 591
        q = f"{address} 實價登錄"
        import urllib.parse
        google_url = "https://www.google.com/search?q=" + urllib.parse.quote(q)
        sale591_url = ("https://sale.591.com.tw/home/house/realPrice/index"
                       f"?regionid=20&keyword={urllib.parse.quote(address[:20])}")

        records.append({
            "district":       row.get("鄉鎮市區", "").strip(),
            "address":        address,
            "date":           date_str,
            "price":          int(price),
            "area_ping":      round(area_ping, 1),
            "price_per_ping": round(price_per_ping),
            "floors":         floor_total,
            "age":            age,
            "note":           note[:30] if note else "",
            "id":             row.get("編號", "").strip(),
            "google_url":     google_url,
            "sale591_url":    sale591_url,
        })

    if not records:
        return None

    records.sort(key=lambda x: x["date"], reverse=True)

    # 各區統計
    by_district = defaultdict(list)
    for rec in records:
        by_district[rec["district"]].append(rec["price_per_ping"])

    district_stats = []
    for dist, prices in sorted(by_district.items()):
        district_stats.append({
            "district": dist,
            "count":    len(prices),
            "avg_ping": round(sum(prices) / len(prices)),
            "min_ping": min(prices),
            "max_ping": max(prices),
        })
    district_stats.sort(key=lambda x: -x["avg_ping"])

    all_prices = [r["price_per_ping"] for r in records]
    overall = {
        "count":         len(records),
        "excluded":      len(excluded),
        "avg_ping":      round(sum(all_prices) / len(all_prices)),
        "min_ping":      min(all_prices),
        "max_ping":      max(all_prices),
        "avg_total":     round(sum(r["price"] for r in records) / len(records)),
    }

    return {
        "updated":       datetime.now().isoformat(),
        "overall":       overall,
        "by_district":   district_stats,
        "recent":        records[:30],
    }

# ── 行事曆 ─────────────────────────────────────────────────────────────────────
FOMC_2026 = [
    ("2026-01-28", "Fed 利率決策", "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"),
    ("2026-03-18", "Fed 利率決策（含經濟預測）", "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"),
    ("2026-04-29", "Fed 利率決策", "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"),
    ("2026-06-17", "Fed 利率決策（含經濟預測）", "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"),
    ("2026-07-29", "Fed 利率決策", "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"),
    ("2026-09-16", "Fed 利率決策（含經濟預測）", "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"),
    ("2026-10-28", "Fed 利率決策", "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"),
    ("2026-12-09", "Fed 利率決策（含經濟預測）", "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"),
]

STATIC_EVENTS = [
    # 重要經濟數據（連結到 BLS / 財政部）
    ("2026-05-08", "美國非農就業（NFP）", "https://www.bls.gov/schedule/news_release/empsit.htm"),
    ("2026-06-10", "美國 CPI 通膨數據", "https://www.bls.gov/schedule/news_release/cpi.htm"),
    # 台股重要資源
    ("9999-12-31", "台股法說會行事曆", "https://mops.twse.com.tw/mops/web/t100sb12"),
]

_calendar_cache = {"data": None, "ts": 0}
CALENDAR_CACHE_SECONDS = 3600 * 4  # 4 小時更新一次財報日

def fetch_calendar():
    today = datetime.now().date()
    events = []

    # 1. Fed 會議（只顯示未來的）
    for date_str, label, url in FOMC_2026:
        from datetime import date
        d = date.fromisoformat(date_str)
        days_left = (d - today).days
        if days_left >= -1:  # 昨天以後都顯示
            events.append({
                "date":      date_str,
                "label":     label,
                "category":  "Fed",
                "days_left": days_left,
                "url":       url,
            })

    # 2. 財報日（從 yfinance 即時抓）
    EARNINGS_TICKERS = {
        "NVDA": "NVDA 財報",
        "AAPL": "AAPL 財報",
        "TSLA": "TSLA 財報",
        "MSFT": "MSFT 財報",
        "2330.TW": "台積電法說會",
        "2454.TW": "聯發科法說會",
    }
    for ticker, label in EARNINGS_TICKERS.items():
        try:
            cal = yf.Ticker(ticker).calendar
            dates = cal.get("Earnings Date", [])
            if not isinstance(dates, list):
                dates = [dates]
            for d in dates:
                if hasattr(d, "date"):
                    d = d.date()
                elif isinstance(d, str):
                    from datetime import date as ddate
                    d = ddate.fromisoformat(d)
                days_left = (d - today).days
                if days_left >= -1:
                    events.append({
                        "date":      d.isoformat(),
                        "label":     label,
                        "category":  "財報",
                        "days_left": days_left,
                        "url": f"https://finance.yahoo.com/quote/{ticker.replace('.TW','')}/financials/",
                    })
        except Exception:
            pass

    # 3. 靜態事件（台股法說會入口等）
    for date_str, label, url in STATIC_EVENTS:
        if date_str == "9999-12-31":
            events.append({
                "date": "—", "label": label,
                "category": "台股", "days_left": 9999, "url": url,
            })

    events.sort(key=lambda x: x["days_left"])
    return events

def get_calendar():
    now = time.time()
    with _cache_lock:
        if _calendar_cache["data"] and now - _calendar_cache["ts"] < CALENDAR_CACHE_SECONDS:
            return _calendar_cache["data"]
        data = fetch_calendar()
        _calendar_cache["data"] = data
        _calendar_cache["ts"]   = now
    return data

def get_house_data():
    now = time.time()
    with _cache_lock:
        if _house_cache["data"] and now - _house_cache["ts"] < HOUSE_CACHE_SECONDS:
            return _house_cache["data"]
        data = fetch_house_prices()
        if data:
            _house_cache["data"] = data
            _house_cache["ts"]   = now
        return _house_cache["data"]

# ── 技術分析圖表資料 ──────────────────────────────────────────────────────────────
_chart_cache: dict = {}
_chart_cache_lock = threading.Lock()
CHART_CACHE_SECONDS = 600  # 10 分鐘快取

def calc_ma(prices, n):
    result = []
    for i in range(len(prices)):
        if i < n - 1:
            result.append(None)
        else:
            result.append(round(sum(prices[i-n+1:i+1]) / n, 2))
    return result

def calc_ema(prices, period):
    result = [None] * len(prices)
    if len(prices) < period:
        return result
    result[period-1] = round(sum(prices[:period]) / period, 6)
    k = 2 / (period + 1)
    for i in range(period, len(prices)):
        result[i] = round(prices[i] * k + result[i-1] * (1 - k), 6)
    return result

def calc_rsi(prices, period=14):
    result = [None] * len(prices)
    if len(prices) <= period:
        return result
    gains, losses = [], []
    for i in range(1, len(prices)):
        diff = prices[i] - prices[i-1]
        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    if avg_loss == 0:
        result[period] = 100.0
    else:
        rs = avg_gain / avg_loss
        result[period] = round(100 - 100 / (1 + rs), 2)
    for i in range(period + 1, len(prices)):
        g = prices[i] - prices[i-1]
        avg_gain = (avg_gain * (period-1) + max(g, 0)) / period
        avg_loss = (avg_loss * (period-1) + max(-g, 0)) / period
        if avg_loss == 0:
            result[i] = 100.0
        else:
            rs = avg_gain / avg_loss
            result[i] = round(100 - 100 / (1 + rs), 2)
    return result

def calc_macd(prices, fast=12, slow=26, signal=9):
    ema_f = calc_ema(prices, fast)
    ema_s = calc_ema(prices, slow)
    dif = [round(ema_f[i] - ema_s[i], 4) if ema_f[i] is not None and ema_s[i] is not None else None
           for i in range(len(prices))]
    first = next((i for i, v in enumerate(dif) if v is not None), None)
    dea = [None] * len(prices)
    if first is not None:
        sub = [dif[i] if dif[i] is not None else 0.0 for i in range(first, len(prices))]
        dea_sub = calc_ema(sub, signal)
        for i, v in enumerate(dea_sub):
            dea[first + i] = round(v, 4) if v is not None else None
    hist = [round((dif[i] - dea[i]) * 2, 4) if dif[i] is not None and dea[i] is not None else None
            for i in range(len(prices))]
    return dif, dea, hist

def _safe_last(lst, offset=0):
    """從尾端取第 offset 個非 None 值"""
    count = 0
    for v in reversed(lst):
        if v is not None:
            if count == offset:
                return v
            count += 1
    return None

def _build_summary(closes, ma5, ma20, ma60, rsi, dif, dea, hist, vols):
    """根據技術指標計算文字摘要"""
    m5 = _safe_last(ma5); m20 = _safe_last(ma20); m60 = _safe_last(ma60)
    m5p = _safe_last(ma5, 5); m20p = _safe_last(ma20, 5)
    rsi_v = _safe_last(rsi)
    dif_v = _safe_last(dif); dea_v = _safe_last(dea); hist_v = _safe_last(hist)
    hist_p = _safe_last(hist, 1)

    # 趨勢
    if m5 and m20 and m60 and m5 > m20 > m60:
        trend, trend_cls = "多頭排列", "bull"
    elif m5 and m20 and m60 and m5 < m20 < m60:
        trend, trend_cls = "空頭排列", "bear"
    else:
        trend, trend_cls = "盤整", "flat"

    # RSI
    if rsi_v is None:
        rsi_txt, rsi_cls = "--", "flat"
    elif rsi_v >= 70:
        rsi_txt, rsi_cls = f"{rsi_v:.1f}　超買", "bear"
    elif rsi_v >= 60:
        rsi_txt, rsi_cls = f"{rsi_v:.1f}　偏強", "bull"
    elif rsi_v >= 40:
        rsi_txt, rsi_cls = f"{rsi_v:.1f}　中性", "flat"
    elif rsi_v >= 30:
        rsi_txt, rsi_cls = f"{rsi_v:.1f}　偏弱", "warn"
    else:
        rsi_txt, rsi_cls = f"{rsi_v:.1f}　超賣", "bull"  # 超賣反而可能反彈

    # MACD
    if dif_v is not None and dea_v is not None and hist_v is not None and hist_p is not None:
        if dif_v > dea_v:
            macd_txt = "多頭擴張" if hist_v > hist_p else "多頭收斂"
            macd_cls = "bull"
        else:
            macd_txt = "空頭擴張" if hist_v < hist_p else "空頭收斂"
            macd_cls = "bear"
    else:
        macd_txt, macd_cls = "--", "flat"

    # 漲跌幅
    def pct(n):
        if len(closes) > n and closes[-n-1]:
            return round((closes[-1] - closes[-n-1]) / closes[-n-1] * 100, 2)
        return None
    c5 = pct(5); c20 = pct(20); c60 = pct(60)

    return {
        "trend": trend, "trend_cls": trend_cls,
        "ma5": round(m5, 2) if m5 else None,
        "ma20": round(m20, 2) if m20 else None,
        "ma60": round(m60, 2) if m60 else None,
        "ma5_dir":  "↑" if m5 and m5p and m5 > m5p else "↓",
        "ma20_dir": "↑" if m20 and m20p and m20 > m20p else "↓",
        "rsi": rsi_txt, "rsi_cls": rsi_cls,
        "macd": macd_txt, "macd_cls": macd_cls,
        "dif": round(dif_v, 3) if dif_v is not None else None,
        "dea": round(dea_v, 3) if dea_v is not None else None,
        "chg5": c5, "chg20": c20, "chg60": c60,
        "high6m": round(max(closes), 2) if closes else None,
        "low6m":  round(min(closes), 2) if closes else None,
        "last_vol": vols[-1] if vols else None,
    }

def fetch_chart_data(ticker, period="6mo"):
    try:
        df = yf.Ticker(ticker).history(period=period, interval="1d", auto_adjust=True)
        if df.empty:
            return None
        dates  = [d.strftime("%Y-%m-%d") for d in df.index]
        opens  = [round(float(v), 2) for v in df["Open"]]
        highs  = [round(float(v), 2) for v in df["High"]]
        lows   = [round(float(v), 2) for v in df["Low"]]
        closes = [round(float(v), 2) for v in df["Close"]]
        vols   = [int(v) for v in df["Volume"]]

        candles = [{"time": dates[i], "open": opens[i], "high": highs[i],
                    "low": lows[i], "close": closes[i]} for i in range(len(dates))]
        volumes = [{"time": dates[i], "value": vols[i],
                    "color": "#26a69a" if closes[i] >= opens[i] else "#ef5350"}
                   for i in range(len(dates))]

        ma5_raw  = calc_ma(closes, 5)
        ma20_raw = calc_ma(closes, 20)
        ma60_raw = calc_ma(closes, 60)
        rsi_raw  = calc_rsi(closes, 14)
        dif_raw, dea_raw, hist_raw = calc_macd(closes)

        def to_series(raw):
            return [{"time": dates[i], "value": raw[i]}
                    for i in range(len(dates)) if raw[i] is not None]

        def to_hist_series(raw):
            return [{"time": dates[i], "value": raw[i],
                     "color": "#ef5350" if raw[i] < 0 else "#26a69a"}
                    for i in range(len(dates)) if raw[i] is not None]

        summary = _build_summary(closes, ma5_raw, ma20_raw, ma60_raw,
                                  rsi_raw, dif_raw, dea_raw, hist_raw, vols)
        return {
            "ticker":  ticker,
            "period":  period,
            "candles": candles,
            "volumes": volumes,
            "ma5":     to_series(ma5_raw),
            "ma20":    to_series(ma20_raw),
            "ma60":    to_series(ma60_raw),
            "rsi":     to_series(rsi_raw),
            "macd_dif":  to_series(dif_raw),
            "macd_dea":  to_series(dea_raw),
            "macd_hist": to_hist_series(hist_raw),
            "summary":   summary,
            "chips":     _get_chips_for(ticker),
        }
    except Exception as e:
        print(f"[Chart] {ticker} 錯誤: {e}", flush=True)
        return None

def _get_chips_for(ticker):
    """若為台股，附上三大法人資料（以千股為單位）"""
    if not ticker.endswith(".TW"):
        return None
    tw_code = ticker.replace(".TW", "")
    all_chips = get_chips()
    c = all_chips.get(tw_code)
    if not c:
        return None
    def to_k(n):   # 轉換為張（1張=1000股）
        return round(n / 1000, 1)
    return {
        "date":        c["date"],
        "foreign_net": to_k(c["foreign_net"]),
        "trust_net":   to_k(c["trust_net"]),
        "dealer_net":  to_k(c["dealer_net"]),
        "total_net":   to_k(c["total_net"]),
    }

def get_chart_data(ticker, period="6mo"):
    key = f"{ticker}_{period}"
    now = time.time()
    with _chart_cache_lock:
        cached = _chart_cache.get(key)
        if cached and now - cached["ts"] < CHART_CACHE_SECONDS:
            return cached["data"]
    # fetch outside lock to avoid blocking other requests
    data = fetch_chart_data(ticker, period)
    with _chart_cache_lock:
        _chart_cache[key] = {"data": data, "ts": now}
    return data

# ── 背景定期更新（HTTP 請求永遠瞬間回傳快取，不等待網路）────────────────────────
NEWS_CACHE_SECONDS = 900
_bg_cache = {
    "quotes":    [],
    "news":      {},
    "signals":   {},
    "theme":     [],
    "updated":   "",
    "ready":     False,
}
_bg_lock = threading.Lock()

def _refresh_quotes():
    """抓最新股價，更新快取。"""
    quotes = []
    try:
        with ThreadPoolExecutor(max_workers=30) as ex:
            futs = {ex.submit(fetch_quote, *row): row[0] for row in WATCHLIST}
            for f in as_completed(futs):
                quotes.append(f.result())
        order = [row[0] for row in WATCHLIST]
        quotes.sort(key=lambda q: order.index(q["ticker"]))
        with _bg_lock:
            _bg_cache["quotes"]  = quotes
            _bg_cache["updated"] = datetime.now().isoformat()
            _bg_cache["ready"]   = True
    except Exception as e:
        print(f"[BG] quotes 更新失敗: {e}", flush=True)

def _refresh_news():
    """抓最新新聞，更新快取。"""
    news = {}
    try:
        with ThreadPoolExecutor(max_workers=30) as ex:
            futs = {ex.submit(fetch_news, row[0]): row[0] for row in WATCHLIST}
            for f in as_completed(futs):
                news[futs[f]] = f.result()
        with _bg_lock:
            _bg_cache["news"] = news
    except Exception as e:
        print(f"[BG] news 更新失敗: {e}", flush=True)

def _refresh_theme():
    """抓未來主題投資股價+訊號。"""
    results = []
    try:
        for ticker, name, currency, desc in THEME_WATCHLIST:
            q = fetch_quote(ticker, name, "未來主題", currency)
            s = fetch_signal(ticker)
            q["desc"] = desc
            q["signal_data"] = s
            results.append(q)
        with _bg_lock:
            _bg_cache["theme"] = results
    except Exception as e:
        print(f"[BG] theme 更新失敗: {e}", flush=True)

def _refresh_signals():
    signals = {}
    try:
        with ThreadPoolExecutor(max_workers=30) as ex:
            futs = {ex.submit(fetch_signal, row[0]): row[0] for row in WATCHLIST}
            for f in as_completed(futs):
                signals[futs[f]] = f.result()
        with _bg_lock:
            _bg_cache["signals"] = signals
    except Exception as e:
        print(f"[BG] signals 更新失敗: {e}", flush=True)

SIGNAL_CACHE_SECONDS = 900

def _background_loop():
    """每隔 60 秒更新股價，每隔 900 秒更新新聞與訊號。"""
    last_news_refresh   = 0
    last_signal_refresh = 0
    while True:
        print("[BG] 更新股價...", flush=True)
        _refresh_quotes()
        print("[BG] 股價更新完成", flush=True)
        now = time.time()
        if now - last_news_refresh > NEWS_CACHE_SECONDS:
            print("[BG] 更新新聞...", flush=True)
            _refresh_news()
            print("[BG] 新聞更新完成", flush=True)
            last_news_refresh = time.time()
        if now - last_signal_refresh > SIGNAL_CACHE_SECONDS:
            print("[BG] 更新訊號...", flush=True)
            _refresh_signals()
            _refresh_theme()
            print("[BG] 訊號+主題更新完成", flush=True)
            last_signal_refresh = time.time()
        time.sleep(DATA_CACHE_SECONDS)

def get_quotes_data():
    """永遠瞬間回傳（從背景快取）。"""
    with _bg_lock:
        quotes = _bg_cache["quotes"]
        news   = _bg_cache["news"]
        ready  = _bg_cache["ready"]
        updated = _bg_cache["updated"]
    if not ready:
        return {"updated": datetime.now().isoformat(), "quotes": [],
                "news": {}, "commentary": {"market_summary": "資料載入中，請稍候…", "stocks": {}},
                "loading": True}
    return {
        "updated":    updated,
        "quotes":     quotes,
        "news":       news,
        "signals":    _bg_cache["signals"],
        "commentary": build_commentary(quotes),
        "loading":    False,
    }

def get_news_only():
    with _bg_lock:
        return _bg_cache["news"]

# ── HTML ────────────────────────────────────────────────────────────────────────
HTML_PAGE = r"""<!DOCTYPE html>
<html lang="zh-TW">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>每日股市 Dashboard</title>
<script src="https://unpkg.com/lightweight-charts@4.1.3/dist/lightweight-charts.standalone.production.js"></script>
<style>
* { box-sizing: border-box; margin: 0; padding: 0; }
body {
  font-family: -apple-system, "Helvetica Neue", Arial, sans-serif;
  background: #0d1117; color: #e2e8f0;
  min-height: 100vh; padding: 24px 20px 48px;
}
header {
  display: flex; align-items: center; justify-content: space-between;
  margin-bottom: 22px; flex-wrap: wrap; gap: 10px;
}
h1 { font-size: 1.45rem; font-weight: 700; color: #f8fafc; }
#meta { display: flex; align-items: center; gap: 10px; }
#dot { width: 8px; height: 8px; border-radius: 50%; background: #22c55e; animation: pulse 2s infinite; }
@keyframes pulse { 0%,100%{opacity:1} 50%{opacity:.3} }
#updated, #countdown { font-size: .75rem; color: #64748b; }

#ai-market {
  background: linear-gradient(135deg,#1a2535,#151e2e);
  border: 1px solid #2d4060;
  border-radius: 14px; padding: 15px 20px;
  margin-bottom: 22px; font-size: .88rem;
  line-height: 1.75; color: #93c5fd;
}
#ai-market strong {
  color: #60a5fa; font-size: .68rem;
  letter-spacing: .1em; text-transform: uppercase;
  display: block; margin-bottom: 6px;
}

.grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(340px, 1fr)); gap: 20px; }
.section { background: #161b27; border: 1px solid #252d42; border-radius: 14px; overflow: hidden; }
.section-title {
  padding: 11px 18px; font-size: .7rem; font-weight: 600;
  letter-spacing: .08em; text-transform: uppercase; color: #94a3b8;
  background: #12172199; border-bottom: 1px solid #252d42;
  display: flex; align-items: center; gap: 8px;
}
.badge { font-size: .6rem; padding: 2px 7px; border-radius: 4px; font-weight: 700; }
.badge-tw { background: #1e3a5f; color: #60a5fa; }
.badge-us { background: #1a3a2a; color: #4ade80; }

table { width: 100%; border-collapse: collapse; }
th { padding: 8px 16px; font-size: .66rem; color: #475569; text-align: right; font-weight: 500; }
th:first-child { text-align: left; }
td { padding: 10px 16px; font-size: .87rem; text-align: right; border-top: 1px solid #1a1f30; vertical-align: top; }
td:first-child { text-align: left; }
tr:hover td { background: #1d2338; }
.name { color: #cbd5e1; font-weight: 500; }
.price { font-variant-numeric: tabular-nums; color: #f1f5f9; font-weight: 600; }
.up   { color: #4ade80; font-weight: 600; }
.down { color: #f87171; font-weight: 600; }
.flat { color: #94a3b8; }
.note { font-size: .7rem; color: #7dd3fc; margin-top: 3px; font-style: italic; line-height: 1.4; }

.news-wrap { border-top: 1px solid #252d42; padding: 12px 16px; background: #0f1420; }
.news-hd { font-size: .65rem; color: #475569; font-weight: 600; letter-spacing: .06em; text-transform: uppercase; margin-bottom: 8px; }
.news-item { margin-bottom: 8px; }
.news-item a { font-size: .75rem; color: #94a3b8; text-decoration: none; line-height: 1.4; display: block; }
.news-item a:hover { color: #60a5fa; }
.news-meta { font-size: .64rem; color: #374151; margin-top: 2px; }
.loading { text-align: center; padding: 40px; color: #475569; font-size: .9rem; }

/* ── ARK 持股 ── */
.ark-wrap { margin-top: 28px; }
.ark-title { font-size:.78rem; font-weight:700; color:#f8fafc; margin-bottom:14px; display:flex; align-items:center; gap:10px; }
.ark-tabs { display:flex; gap:8px; margin-bottom:14px; flex-wrap:wrap; }
.ark-tab { padding:5px 14px; border-radius:20px; font-size:.72rem; font-weight:600; cursor:pointer; border:1px solid #252d42; color:#64748b; background:#161b27; transition:.15s; }
.ark-tab.active { background:#1d4ed8; border-color:#1d4ed8; color:#fff; }
.ark-fund-name { font-size:.72rem; color:#475569; margin-bottom:10px; }
.ark-table { width:100%; border-collapse:collapse; }
.ark-table th { padding:7px 14px; font-size:.65rem; color:#475569; text-align:right; font-weight:500; border-bottom:1px solid #252d42; }
.ark-table th:first-child,.ark-table th:nth-child(2) { text-align:left; }
.ark-table td { padding:9px 14px; font-size:.82rem; text-align:right; border-top:1px solid #1a1f30; }
.ark-table td:first-child { text-align:left; color:#64748b; font-size:.75rem; }
.ark-table td:nth-child(2) { text-align:left; color:#cbd5e1; font-weight:500; }
.ark-table tr:hover td { background:#1d2338; }
.ark-weight { color:#fbbf24; font-weight:700; }
.ark-card { background:#161b27; border:1px solid #252d42; border-radius:14px; overflow:hidden; }
.ark-date { font-size:.65rem; color:#374151; }

/* ── 未來主題投資 ── */
.theme-wrap { margin-top: 28px; }
.theme-title { font-size: .78rem; font-weight: 700; color: #f8fafc; margin-bottom: 6px; display:flex; align-items:center; gap:10px; }
.theme-subtitle { font-size: .7rem; color: #475569; margin-bottom: 14px; }
.theme-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(280px,1fr)); gap: 14px; }
.theme-card {
  background: #161b27; border: 1px solid #252d42; border-radius: 14px;
  padding: 14px 18px; cursor: pointer; transition: border-color .2s;
}
.theme-card:hover { border-color: #3b82f6; }
.theme-card-top { display:flex; justify-content:space-between; align-items:flex-start; margin-bottom:6px; }
.theme-name { font-size:.88rem; font-weight:600; color:#f1f5f9; }
.theme-price { font-size:.88rem; font-weight:700; color:#f1f5f9; font-variant-numeric:tabular-nums; }
.theme-change { font-size:.78rem; margin-top:2px; }
.theme-signal { font-size:.75rem; margin-top:4px; }
.theme-desc { font-size:.7rem; color:#475569; margin-top:8px; border-top:1px solid #1a1f30; padding-top:8px; line-height:1.5; }
.theme-tag { display:inline-block; font-size:.6rem; padding:2px 7px; border-radius:4px; background:#1e3a5f; color:#60a5fa; font-weight:700; margin-bottom:6px; }

/* ── 停損提示 ── */
.sl-wrap{margin-top:28px}.sl-title{font-size:.78rem;font-weight:700;color:#f8fafc;margin-bottom:14px}
.sl-form{display:flex;flex-wrap:wrap;gap:10px;align-items:flex-end;background:#161b27;border:1px solid #252d42;border-radius:14px;padding:16px 20px;margin-bottom:16px}
.sl-field{display:flex;flex-direction:column;gap:5px}.sl-label{font-size:.68rem;color:#64748b;font-weight:600;letter-spacing:.05em}
.sl-input{background:#0d1117;border:1px solid #2d3748;border-radius:8px;color:#f1f5f9;font-size:.85rem;padding:7px 12px;width:130px;outline:none}
.sl-input:focus{border-color:#3b82f6}.sl-btn{background:#1d4ed8;color:#fff;border:none;border-radius:8px;padding:8px 18px;font-size:.82rem;font-weight:600;cursor:pointer}
.sl-btn:hover{background:#2563eb}.sl-list{display:grid;grid-template-columns:repeat(auto-fit,minmax(300px,1fr));gap:14px}
.sl-card{background:#161b27;border:1px solid #252d42;border-radius:14px;padding:14px 18px;position:relative}
.sl-card.danger{border-color:#7f1d1d;background:#1a0a0a}.sl-card.warning{border-color:#78350f;background:#150f00}.sl-card.profit{border-color:#14532d;background:#0a1a0e}
.sl-card-top{display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:8px}
.sl-name{font-size:.9rem;font-weight:600;color:#f1f5f9}.sl-status{font-size:.8rem;font-weight:700}
.sl-status.danger{color:#f87171}.sl-status.warning{color:#fbbf24}.sl-status.profit{color:#4ade80}
.sl-details{font-size:.75rem;color:#64748b;line-height:1.9}.sl-details span{color:#94a3b8}
.sl-del{position:absolute;top:10px;right:14px;background:none;border:none;color:#374151;font-size:.8rem;cursor:pointer}
.sl-del:hover{color:#f87171}.sl-bar-wrap{margin-top:10px;height:5px;background:#1e2438;border-radius:3px;overflow:hidden}
.sl-bar{height:100%;border-radius:3px;transition:width .3s}

/* ── 房價區塊 ── */
.house-wrap { margin-top: 28px; }
.house-title {
  font-size: .78rem; font-weight: 700; color: #f8fafc;
  margin-bottom: 14px; display: flex; align-items: center; gap: 10px;
}
.house-source { font-size: .66rem; color: #475569; font-weight: 400; }
.house-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: 18px; }
.house-card { background: #161b27; border: 1px solid #252d42; border-radius: 14px; overflow: hidden; }
.house-card-title {
  padding: 10px 16px; font-size: .7rem; font-weight: 600;
  letter-spacing: .08em; text-transform: uppercase; color: #94a3b8;
  background: #12172199; border-bottom: 1px solid #252d42;
}
.stat-row { display: flex; justify-content: space-between; align-items: center;
  padding: 10px 16px; border-top: 1px solid #1a1f30; font-size: .86rem; }
.stat-row:first-of-type { border-top: none; }
.stat-label { color: #64748b; font-size: .75rem; }
.stat-val { color: #f1f5f9; font-weight: 600; font-variant-numeric: tabular-nums; }
.stat-val.hl { color: #fbbf24; }
.district-bar {
  padding: 8px 16px; border-top: 1px solid #1a1f30;
  display: flex; align-items: center; gap: 10px;
}
.dist-name { color: #94a3b8; font-size: .8rem; min-width: 60px; }
.bar-wrap { flex: 1; height: 6px; background: #1e2438; border-radius: 3px; overflow: hidden; }
.bar-fill { height: 100%; background: linear-gradient(90deg,#3b82f6,#8b5cf6); border-radius: 3px; }
.dist-price { color: #cbd5e1; font-size: .78rem; min-width: 80px; text-align: right; font-variant-numeric: tabular-nums; }
.recent-row { padding: 10px 16px; border-top: 1px solid #1a1f30; font-size: .78rem; }
.rec-top { display: flex; justify-content: space-between; align-items: flex-start; gap: 8px; margin-bottom: 4px; }
.rec-addr { color: #cbd5e1; font-weight: 500; flex: 1; line-height: 1.3; }
.rec-price { color: #fbbf24; font-weight: 700; white-space: nowrap; font-size: .82rem; }
.rec-meta { color: #475569; font-size: .7rem; margin-bottom: 5px; }
.rec-links { display: flex; gap: 6px; }
.rec-link {
  font-size: .65rem; padding: 2px 8px; border-radius: 4px; text-decoration: none;
  font-weight: 600; transition: opacity .15s;
}
.rec-link:hover { opacity: .75; }
.rec-link-g  { background: #1e3a5f; color: #60a5fa; }
.rec-link-591 { background: #1a3a2a; color: #4ade80; }
.rec-note { font-size: .66rem; color: #78350f; background: #1c1205; border-radius: 3px;
  padding: 1px 6px; display: inline-block; margin-top: 3px; }
.excluded-badge { font-size: .68rem; color: #6b7280; }
.house-note { font-size: .72rem; color: #475569; padding: 8px 16px 12px; }

/* ── 行事曆 ── */
.cal-wrap { margin-top: 28px; }
.cal-title { font-size: .78rem; font-weight: 700; color: #f8fafc; margin-bottom: 14px; }
.cal-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 18px; }
.cal-card { background: #161b27; border: 1px solid #252d42; border-radius: 14px; overflow: hidden; }
.cal-card-title { padding: 10px 16px; font-size: .7rem; font-weight: 600;
  letter-spacing: .08em; text-transform: uppercase; color: #94a3b8;
  background: #12172199; border-bottom: 1px solid #252d42; }
.cal-row { display: flex; align-items: center; gap: 12px;
  padding: 10px 16px; border-top: 1px solid #1a1f30; }
.cal-row:first-of-type { border-top: none; }
.cal-dot { width: 8px; height: 8px; border-radius: 50%; flex-shrink: 0; }
.dot-fed    { background: #f59e0b; }
.dot-report { background: #60a5fa; }
.dot-tw     { background: #4ade80; }
.cal-info { flex: 1; }
.cal-label { font-size: .82rem; color: #cbd5e1; font-weight: 500; }
.cal-date  { font-size: .7rem; color: #475569; margin-top: 2px; }
.cal-badge { font-size: .65rem; padding: 2px 8px; border-radius: 20px;
  font-weight: 700; white-space: nowrap; }
.badge-soon   { background: #7f1d1d; color: #fca5a5; }
.badge-week   { background: #78350f; color: #fcd34d; }
.badge-month  { background: #1e3a5f; color: #93c5fd; }
.badge-later  { background: #1a1a2e; color: #475569; }
.badge-link   { background: #1a3a2a; color: #4ade80; }
.cal-link { font-size: .65rem; color: #475569; text-decoration: none; display: block; margin-top: 3px; }
.cal-link:hover { color: #60a5fa; }
/* ── 技術分析 Modal ────────────────────────────────────────────── */
.chart-modal-overlay {
  display:none; position:fixed; inset:0; background:rgba(0,0,0,.82);
  z-index:1000; align-items:center; justify-content:center; padding:8px;
}
.chart-modal-overlay.open { display:flex; }
.chart-modal {
  background:#0d1117; border:1px solid #21262d;
  border-radius:10px; width:min(1260px,98vw); max-height:95vh;
  overflow:hidden; display:flex; flex-direction:column;
  box-shadow:0 24px 80px rgba(0,0,0,.8);
}
/* Header */
.chart-modal-header {
  display:flex; align-items:center; gap:16px; flex-wrap:wrap;
  padding:10px 16px; border-bottom:1px solid #21262d;
  background:#161b22;
}
.chart-modal-title { font-size:.95rem; font-weight:700; color:#e6edf3; white-space:nowrap; }
.chart-price-bar {
  display:flex; align-items:baseline; gap:10px; flex-wrap:wrap; flex:1;
}
.cph-price { font-size:1.35rem; font-weight:700; color:#e6edf3; font-family:monospace; }
.cph-chg   { font-size:1rem; font-weight:600; font-family:monospace; }
.cph-vol   { font-size:.78rem; color:#6e7681; }
.chart-modal-close {
  background:none; border:none; color:#6e7681; font-size:1.4rem;
  cursor:pointer; padding:2px 8px; border-radius:6px; margin-left:auto;
}
.chart-modal-close:hover { background:#21262d; color:#e6edf3; }
/* Period buttons */
.chart-period-row {
  display:flex; align-items:center; gap:6px; padding:8px 16px 0;
}
.chart-period-btn {
  padding:3px 11px; border-radius:5px; border:1px solid #30363d;
  background:transparent; color:#8b949e; font-size:.77rem; cursor:pointer;
}
.chart-period-btn.active { background:#1f6feb; color:#fff; border-color:#1f6feb; }
.chart-period-btn:hover:not(.active) { background:#21262d; color:#e6edf3; }
/* Body layout */
.chart-modal-body {
  display:flex; flex:1; overflow:hidden; min-height:0;
}
.chart-left {
  flex:1; min-width:0; display:flex; flex-direction:column;
  padding:8px 0 8px 12px; overflow:hidden;
}
.chart-label {
  font-size:.68rem; color:#484f58; font-family:monospace;
  padding:2px 0 2px 4px; letter-spacing:.04em;
}
.chart-legend {
  display:flex; gap:12px; flex-wrap:wrap; font-size:.72rem;
  padding:0 4px 4px; color:#8b949e;
}
.chart-legend span { display:flex; align-items:center; gap:4px; }
.chart-legend i { display:inline-block; width:20px; height:2px; border-radius:1px; }
#chart-container  { flex:0 0 270px; }
#vol-container    { flex:0 0 70px;  margin-top:4px; }
#macd-container   { flex:0 0 110px; margin-top:4px; }
#rsi-container    { flex:0 0 90px;  margin-top:4px; }
/* Right analysis panel */
.chart-right {
  width:240px; flex-shrink:0; border-left:1px solid #21262d;
  overflow-y:auto; padding:12px; display:flex; flex-direction:column; gap:10px;
}
.ap-card {
  background:#161b22; border:1px solid #21262d; border-radius:8px;
  padding:10px 12px;
}
.ap-card-title {
  font-size:.7rem; color:#484f58; font-weight:600; letter-spacing:.08em;
  text-transform:uppercase; margin-bottom:8px;
}
.ap-row {
  display:flex; justify-content:space-between; align-items:center;
  font-size:.78rem; padding:3px 0; border-bottom:1px solid #21262d;
}
.ap-row:last-child { border-bottom:none; }
.ap-label { color:#8b949e; }
.ap-val   { font-family:monospace; font-weight:600; }
.ap-bull  { color:#3fb950; }
.ap-bear  { color:#f85149; }
.ap-warn  { color:#d29922; }
.ap-flat  { color:#8b949e; }
.ap-tag {
  font-size:.7rem; padding:1px 7px; border-radius:12px; font-weight:600;
}
.ap-tag.bull { background:#0d4429; color:#3fb950; }
.ap-tag.bear { background:#3d0f0f; color:#f85149; }
.ap-tag.flat { background:#21262d; color:#8b949e; }
.ap-tag.warn { background:#3d2a00; color:#d29922; }
/* Responsive: collapse right panel on small screens */
@media(max-width:700px){
  .chart-modal-body { flex-direction:column; }
  .chart-right { width:100%; border-left:none; border-top:1px solid #21262d; max-height:220px; }
  .chart-left  { padding:8px; }
}
/* 圖表面板新聞 */
.ap-news-item {
  padding:6px 0; border-bottom:1px solid #21262d; font-size:.73rem; line-height:1.4;
}
.ap-news-item:last-child { border-bottom:none; }
.ap-news-item a { color:#58a6ff; text-decoration:none; display:block; margin-bottom:2px; }
.ap-news-item a:hover { text-decoration:underline; color:#79c0ff; }
.ap-news-meta { color:#484f58; font-size:.67rem; }
.ap-news-loading { color:#484f58; font-size:.75rem; padding:8px 0; text-align:center; }
/* 籌碼指標 */
.chip-bar { display:flex; gap:4px; flex-wrap:wrap; margin-top:3px; }
.chip-tag {
  font-size:.62rem; padding:1px 5px; border-radius:3px; font-family:monospace;
  font-weight:600; white-space:nowrap;
}
.chip-bull { background:#0d4429; color:#3fb950; }
.chip-bear { background:#3d0f0f; color:#f85149; }
.chip-flat { background:#21262d; color:#6e7681; }
/* 籌碼大條 */
.chip-row-bar {
  height:4px; border-radius:2px; transition:width .3s;
}
.name-link { cursor:pointer; }
.name-link:hover { text-decoration:underline; color:#60a5fa; }
</style>
</head>
<body>
<header>
  <h1>📈 每日股市 Dashboard</h1>
  <div id="meta">
    <span id="dot"></span>
    <span id="updated">載入中…</span>
    <span id="countdown"></span>
  </div>
</header>

<div id="ai-market"><strong>📊 今日市場概況</strong><span id="ai-text">分析中…</span></div>
<div class="grid" id="grid"><div class="loading">⏳ 正在抓取最新股價與新聞…</div></div>

<!-- 行事曆區塊 -->
<!-- ARK 持股 -->
<div class="ark-wrap">
  <div class="ark-title">🦅 木頭姐 ARK 每日持股
    <span class="ark-date" id="ark-date"></span>
  </div>
  <div class="ark-tabs" id="ark-tabs"></div>
  <div class="ark-card">
    <div class="ark-fund-name" id="ark-fund-name"></div>
    <table class="ark-table">
      <thead><tr><th>#</th><th>股票</th><th>公司名稱</th><th>占比</th><th>股價</th></tr></thead>
      <tbody id="ark-body"><tr><td colspan="5" style="text-align:center;padding:20px;color:#475569">⏳ 載入中…</td></tr></tbody>
    </table>
  </div>
</div>

<!-- 未來主題投資 -->
<div class="theme-wrap">
  <div class="theme-title">🔭 未來主題投資
    <span style="font-size:.65rem;color:#475569;font-weight:400">玻璃基板 AI 封裝供應鏈</span>
  </div>
  <div class="theme-subtitle">⚠️ 主題股波動較大，適合長期持有、分批佈局，請自行評估風險</div>
  <div class="theme-grid" id="theme-grid"><div class="loading">⏳ 載入主題股資料…</div></div>
</div>

<!-- 停損提示 -->
<!-- 目標買進提示 -->
<div class="sl-wrap" style="margin-bottom:20px">
  <div class="sl-title">🎯 目標買進提示</div>
  <div class="sl-form">
    <div class="sl-field"><span class="sl-label">股票代號</span><input class="sl-input" id="wb-ticker" placeholder="例：MSFT" style="width:110px"></div>
    <div class="sl-field"><span class="sl-label">目標買進價</span><input class="sl-input" id="wb-target" type="number" placeholder="例：400" style="width:110px"></div>
    <div class="sl-field"><span class="sl-label">備註（選填）</span><input class="sl-input" id="wb-note" placeholder="例：等回調" style="width:130px"></div>
    <button class="sl-btn" onclick="wbAdd()">➕ 新增</button>
  </div>
  <div class="sl-list" id="wb-list"></div>
</div>

<div class="sl-wrap">
  <div class="sl-title">🛡️ 停損提示</div>
  <div class="sl-form">
    <div class="sl-field"><span class="sl-label">股票代號</span><input class="sl-input" id="sl-ticker" placeholder="例：2330.TW" style="width:110px"></div>
    <div class="sl-field"><span class="sl-label">買進價格</span><input class="sl-input" id="sl-buy" type="number" placeholder="例：1000" style="width:110px"></div>
    <div class="sl-field"><span class="sl-label">停損幅度 %</span><input class="sl-input" id="sl-pct" type="number" value="7" style="width:80px"></div>
    <div class="sl-field"><span class="sl-label">備註（選填）</span><input class="sl-input" id="sl-note" placeholder="例：定期定額" style="width:130px"></div>
    <button class="sl-btn" onclick="slAdd()">➕ 新增</button>
  </div>
  <div class="sl-list" id="sl-list"></div>
</div>

<div class="cal-wrap">
  <div class="cal-title">📅 重要行事曆</div>
  <div class="cal-grid" id="cal-grid">
    <div class="loading">⏳ 載入行事曆…</div>
  </div>
</div>

<!-- 房價區塊 -->
<div class="house-wrap">
  <div class="house-title">
    🏠 台南市透天厝 實價登錄
    <span class="house-source">資料來源：內政部實價登錄（每月更新）</span>
  </div>
  <div class="house-grid" id="house-grid">
    <div class="loading">⏳ 載入房價資料中…</div>
  </div>
</div>

<!-- 技術分析 Modal -->
<div class="chart-modal-overlay" id="chart-overlay" onclick="if(event.target===this)closeChart()">
  <div class="chart-modal">
    <div class="chart-modal-header">
      <div class="chart-modal-title" id="chart-title">技術分析</div>
      <div class="chart-price-bar" id="chart-price-bar"></div>
      <button class="chart-modal-close" onclick="closeChart()">✕</button>
    </div>
    <div class="chart-period-row">
      <span style="font-size:.72rem;color:#484f58;margin-right:4px;">週期</span>
      <button class="chart-period-btn" onclick="switchPeriod('1mo')">1M</button>
      <button class="chart-period-btn" onclick="switchPeriod('3mo')">3M</button>
      <button class="chart-period-btn active" onclick="switchPeriod('6mo')">6M</button>
      <button class="chart-period-btn" onclick="switchPeriod('1y')">1Y</button>
    </div>
    <div class="chart-modal-body">
      <div class="chart-left">
        <div class="chart-legend">
          <span><i style="background:#26a69a"></i>MA5</span>
          <span><i style="background:#f59e0b"></i>MA20</span>
          <span><i style="background:#a78bfa"></i>MA60</span>
        </div>
        <div id="chart-container"></div>
        <div class="chart-label" style="margin-top:6px;">▌ 成交量</div>
        <div id="vol-container"></div>
        <div class="chart-label" style="margin-top:6px;">▌ MACD (DIF/DEA)</div>
        <div id="macd-container"></div>
        <div class="chart-label" style="margin-top:6px;">▌ RSI(14)　超買&gt;70 超賣&lt;30</div>
        <div id="rsi-container"></div>
      </div>
      <div class="chart-right" id="chart-analysis">
        <div class="loading" style="font-size:.8rem;padding:20px 0;text-align:center">⏳ 載入中…</div>
      </div>
    </div>
  </div>
</div>

<script>
const REFRESH  = 60;
const SECTIONS = ["台股大盤","台灣ETF－高息","台灣ETF－主題","台股－半導體","台股－電子製造","台股－金融","台股－傳產其他","美股大盤","美股－科技AI","美股－多元"];
const CUR      = {NTD:"NT$", USD:"$"};
let timer = REFRESH;
let _allNews  = {};   // 全局新聞快取 {ticker: [{title,url,source,pub}]}
let _allChips = {};   // 全局籌碼快取 {tw_code: {foreign_net,...}}

const fmt = (n,d=2) => n.toLocaleString("en-US",{minimumFractionDigits:d,maximumFractionDigits:d});
const cls  = v => v>0?"up":v<0?"down":"flat";
const arr  = v => v>0?"▲":v<0?"▼":"–";
const sgn  = v => v>=0?"+":"";
function timeAgo(iso){
  if(!iso) return "";
  const s = (Date.now()-new Date(iso).getTime())/1000;
  if(s<3600)  return Math.floor(s/60)+"分前";
  if(s<86400) return Math.floor(s/3600)+"小時前";
  return Math.floor(s/86400)+"天前";
}

function render(data){
  try{
  slUpdatePrices(data.quotes||[]);
  wbUpdatePrices(data.quotes||[]);
  document.getElementById("ai-text").textContent = (data.commentary||{}).market_summary||"";
  const ai      = (data.commentary||{}).stocks||{};
  const news    = data.news||{};
  const signals = data.signals||{};
  const grp   = {};
  for(const q of data.quotes){ (grp[q.section]=grp[q.section]||[]).push(q); }

  let html = "";
  for(const sec of SECTIONS){
    const items = grp[sec]||[];
    const isTW  = sec.startsWith("台");
    const bc    = isTW?"badge-tw":"badge-us";
    const bl    = isTW?"TWD":"USD";

    // 合併本區塊新聞（去重）
    const seen = new Set(), sNews = [];
    for(const q of items) for(const a of (news[q.ticker]||[]))
      if(!seen.has(a.url)){ seen.add(a.url); sNews.push(a); }

    html += `<div class="section">
<div class="section-title">${sec}<span class="badge ${bc}">${bl}</span></div>
<table><thead><tr><th>名稱</th><th>最新價</th><th>漲跌</th><th>漲跌幅</th><th>訊號</th></tr></thead><tbody>`;

    for(const q of items){
      const c = cls(q.change), cu = CUR[q.currency]||"";
      const note = ai[q.name]||"";
      const safeTicker = q.ticker.replace(/'/g,"\\'");
      const safeName   = q.name.replace(/&/g,"&amp;").replace(/'/g,"\\'");
      // 台股籌碼標籤
      let chipTag = "";
      if(q.ticker.endsWith(".TW")){
        const twc = q.ticker.replace(".TW","");
        const ch  = _allChips[twc];
        const chipCls = v => v>0?"chip-bull":v<0?"chip-bear":"chip-flat";
        const chipTxt = v => (v>0?"+":"")+v.toLocaleString()+"張";
        chipTag = `<div class="chip-bar" data-tw-code="${twc}">${ch?`
          <span class="chip-tag ${chipCls(ch.foreign_net)}">外資 ${chipTxt(ch.foreign_net)}</span>
          <span class="chip-tag ${chipCls(ch.trust_net)}">投信 ${chipTxt(ch.trust_net)}</span>`:""
        }</div>`;
      }
      const sig = signals[q.ticker]||{};
      const sigMap = {"BUY":"🟢 買進","HOLD":"🟡 觀望","SELL":"🔴 賣出","HOT":"🔥 過熱","N/A":"⚪"};
      const sigLabel = sigMap[sig.signal||"N/A"]||"⚪";
      const rsiStr = sig.rsi!=null?`RSI:${sig.rsi} ${sig.rsi>70?"⚠️":sig.rsi>=50?"✓":"↓"}`:"";
      const volStr = sig.vol_ratio!=null?`量比:${sig.vol_ratio}x ${sig.vol_ok?"✓":"✗"}`:"";
      const sigTip = sig.signal&&sig.signal!=="N/A"
        ?`MA20:${sig.ma20} ${sig.above_ma?"✓":"✗"}  5日前:${sig.prev5} ${sig.above_5d?"✓":"✗"}  ${rsiStr}  ${volStr}`
        :"資料不足";
      html += `<tr>
<td class="name"><span class="name-link" onclick="openChart('${safeTicker}','${safeName}')">${q.name}</span>${note?`<div class="note">💬 ${note}</div>`:""}${chipTag}</td>
<td class="price">${q.error?"--":cu+fmt(q.price)}</td>
<td class="${c}">${q.error?"--":arr(q.change)+" "+sgn(q.change)+fmt(q.change)}</td>
<td class="${c}">${q.error?"--":sgn(q.change_pct)+fmt(q.change_pct)+"%"}</td>
<td title="${sigTip}" style="font-size:.82rem;text-align:center">${q.error?"--":sigLabel}</td>
</tr>`;
    }
    html += `</tbody></table>`;

    if(sNews.length){
      html += `<div class="news-wrap"><div class="news-hd">📰 最新消息</div>`;
      for(const a of sNews.slice(0,4)){
        html += `<div class="news-item">
<a href="${a.url}" target="_blank" rel="noopener">${a.title}</a>
<div class="news-meta">${a.source}${a.pub?" · "+timeAgo(a.pub):""}</div>
</div>`;
      }
      html += `</div>`;
    }
    html += `</div>`;
  }
  document.getElementById("grid").innerHTML = html;
  document.getElementById("updated").textContent =
    "更新："+new Date().toLocaleString("zh-TW",{hour12:false});
  }catch(e){
    document.getElementById("grid").innerHTML=`<div class="loading">⚠️ render錯誤: ${e.message}<br><small>${(e.stack||'').split('\\n').slice(0,3).join(' | ')}</small></div>`;
    throw e;
  }
}

function renderHouse(h){
  if(!h){ document.getElementById("house-grid").innerHTML=`<div class="loading">⚠️ 房價資料暫無法取得</div>`; return; }
  const o = h.overall;
  const maxAvg = Math.max(...h.by_district.map(d=>d.avg_ping));

  // 卡片1：整體統計
  let html = `<div class="house-card">
<div class="house-card-title">整體統計（有效 ${o.count} 筆，已排除 ${o.excluded} 筆異常）</div>
<div class="stat-row"><span class="stat-label">平均每坪單價</span><span class="stat-val hl">NT$ ${o.avg_ping.toLocaleString()} 元</span></div>
<div class="stat-row"><span class="stat-label">平均總價</span><span class="stat-val">NT$ ${(o.avg_total/10000).toFixed(0)} 萬</span></div>
<div class="stat-row"><span class="stat-label">最低每坪</span><span class="stat-val">NT$ ${o.min_ping.toLocaleString()} 元</span></div>
<div class="stat-row"><span class="stat-label">最高每坪</span><span class="stat-val">NT$ ${o.max_ping.toLocaleString()} 元</span></div>
<div class="house-note">
  資料期間：${h.recent.at(-1)?.date||""} ～ ${h.recent[0]?.date||""}<br>
  ⚠️ 已自動排除：親友交易、非住家用、建物面積過小（&lt;40㎡）等異常資料
</div>
</div>`;

  // 卡片2：各區均價
  html += `<div class="house-card">
<div class="house-card-title">各區每坪均價排行</div>`;
  for(const d of h.by_district){
    const pct = Math.round(d.avg_ping / maxAvg * 100);
    html += `<div class="district-bar">
<span class="dist-name">${d.district}</span>
<div class="bar-wrap"><div class="bar-fill" style="width:${pct}%"></div></div>
<span class="dist-price">${d.avg_ping.toLocaleString()} 元/坪 <span style="color:#475569;font-size:.65rem">(${d.count}筆)</span></span>
</div>`;
  }
  html += `</div>`;

  // 卡片3：最新成交（含連結）
  html += `<div class="house-card">
<div class="house-card-title">最新成交（點連結可查核原始資料）</div>`;
  for(const rec of h.recent.slice(0,20)){
    const noteHtml = rec.note ? `<div class="rec-note">📌 ${rec.note}</div>` : "";
    html += `<div class="recent-row">
<div class="rec-top">
  <span class="rec-addr">${rec.district}　${rec.address}</span>
  <span class="rec-price">${(rec.price/10000).toFixed(0)}萬</span>
</div>
<div class="rec-meta">
  ${rec.date}　${rec.area_ping}坪　${rec.floors||""}　${rec.age?"屋齡"+rec.age+"年":""}
  <strong style="color:#fbbf24">${rec.price_per_ping.toLocaleString()} 元/坪</strong>
</div>
<div class="rec-links">
  <a class="rec-link rec-link-g"  href="${rec.google_url}"  target="_blank" rel="noopener">🔍 Google 查核</a>
  <a class="rec-link rec-link-591" href="${rec.sale591_url}" target="_blank" rel="noopener">🏠 591 搜尋</a>
</div>
${noteHtml}
</div>`;
  }
  html += `</div>`;

  document.getElementById("house-grid").innerHTML = html;
}

async function loadHouse(){
  try{ renderHouse(await (await fetch("/house")).json()); }
  catch{ document.getElementById("house-grid").innerHTML=`<div class="loading">⚠️ 房價資料連線失敗</div>`; }
}

async function load(){
  try{
    const d = await (await fetch("/data")).json();
    if(d.news) _allNews = {..._allNews, ...d.news};  // 先存初始新聞
    render(d);
  } catch(e){
    document.getElementById("grid").innerHTML=`<div class="loading">⚠️ 連線失敗，請稍後<br><small style="color:#888">${e.message||e}</small></div>`;
    return;
  }
  // 背景補載新聞，載完只更新新聞區塊不重抓股價
  try{
    const news = await (await fetch("/news")).json();
    _allNews = news;   // 存進全局，圖表面板可直接使用
    const cached = await (await fetch("/data")).json();  // 此時已有快取，瞬間回傳
    cached.news = news;
    render(cached);
  } catch{ }
}

function tick(){
  clearInterval(window._cd);
  timer = REFRESH;
  window._cd = setInterval(()=>{
    document.getElementById("countdown").textContent=`（${--timer}s 後刷新）`;
    if(timer<=0){ load(); timer=REFRESH; }
  },1000);
}

function renderCal(events){
  if(!events||!events.length){
    document.getElementById("cal-grid").innerHTML=`<div class="loading">⚠️ 無法取得行事曆</div>`;
    return;
  }
  // 分類
  const fed=[], reports=[], tw=[];
  for(const e of events){
    if(e.category==="Fed")    fed.push(e);
    else if(e.category==="財報") reports.push(e);
    else tw.push(e);
  }

  function badge(days){
    if(days<=3)  return `<span class="cal-badge badge-soon">🔴 ${days<=0?"今天/昨天":days+"天後"}</span>`;
    if(days<=7)  return `<span class="cal-badge badge-week">🟡 ${days}天後</span>`;
    if(days<=30) return `<span class="cal-badge badge-month">🔵 ${days}天後</span>`;
    if(days<9000) return `<span class="cal-badge badge-later">${days}天後</span>`;
    return `<span class="cal-badge badge-link">常設連結</span>`;
  }

  function dot(cat){
    if(cat==="Fed")    return "dot-fed";
    if(cat==="財報")   return "dot-report";
    return "dot-tw";
  }

  function rows(list){
    return list.map(e=>`
<div class="cal-row">
  <span class="cal-dot ${dot(e.category)}"></span>
  <div class="cal-info">
    <div class="cal-label">${e.label}</div>
    <div class="cal-date">${e.date}</div>
    <a class="cal-link" href="${e.url}" target="_blank" rel="noopener">🔗 查核來源</a>
  </div>
  ${badge(e.days_left)}
</div>`).join("");
  }

  let html = "";
  if(fed.length) html += `<div class="cal-card"><div class="cal-card-title">🏦 Fed 利率決策</div>${rows(fed)}</div>`;
  if(reports.length) html += `<div class="cal-card"><div class="cal-card-title">📊 美股財報日</div>${rows(reports)}</div>`;
  if(tw.length) html += `<div class="cal-card"><div class="cal-card-title">🇹🇼 台股資源</div>${rows(tw)}</div>`;

  document.getElementById("cal-grid").innerHTML = html;
}

async function loadCal(){
  try{ renderCal(await (await fetch("/calendar")).json()); }
  catch{ document.getElementById("cal-grid").innerHTML=`<div class="loading">⚠️ 行事曆載入失敗</div>`; }
}

// ── 技術分析圖表 ─────────────────────────────────────────────────
let _chartMain=null, _chartVol=null, _chartMacd=null, _chartRsi=null;
let _curTicker="", _curName="", _curPeriod="6mo";

function destroyCharts(){
  [_chartMain,_chartVol,_chartMacd,_chartRsi].forEach(c=>{ if(c){c.remove();} });
  _chartMain=_chartVol=_chartMacd=_chartRsi=null;
}

function closeChart(){
  document.getElementById("chart-overlay").classList.remove("open");
  destroyCharts();
  _curTicker=""; _curName=""; _curPeriod="6mo";
}

async function switchPeriod(p){
  document.querySelectorAll(".chart-period-btn").forEach(b=>{
    b.classList.toggle("active", b.textContent.toLowerCase()===p.replace("mo","m").replace("1y","1y"));
  });
  _curPeriod = p;
  await _loadChart(_curTicker, _curName, p);
}

async function openChart(ticker, name){
  _curTicker = ticker;
  _curName   = name.replace(/&amp;/g,"&");
  _curPeriod = "6mo";
  document.querySelectorAll(".chart-period-btn").forEach(b=>{
    b.classList.toggle("active", b.textContent==="6M");
  });
  document.getElementById("chart-overlay").classList.add("open");
  await _loadChart(ticker, _curName, "6mo");
}

async function _loadChart(ticker, displayName, period){
  destroyCharts();
  document.getElementById("chart-title").textContent = `📈 ${displayName}`;
  document.getElementById("chart-price-bar").innerHTML = "";
  document.getElementById("chart-analysis").innerHTML = '<div class="loading" style="font-size:.8rem;padding:20px 8px;text-align:center">⏳ 載入中…</div>';
  ["chart-container","vol-container","macd-container","rsi-container"]
    .forEach(id=>{ document.getElementById(id).innerHTML=""; });

  let d;
  try{ d = await (await fetch(`/chart-data?ticker=${encodeURIComponent(ticker)}&period=${period}`)).json(); }
  catch(e){ document.getElementById("chart-container").innerHTML=`<div class="loading">⚠️ 載入失敗: ${e.message}</div>`; return; }
  if(!d||!d.candles||!d.candles.length){
    document.getElementById("chart-container").innerHTML='<div class="loading">⚠️ 無圖表資料</div>'; return;
  }

  const LW = window.LightweightCharts;
  const base = {
    layout:{ background:{color:"#0d1117"}, textColor:"#8b949e" },
    grid:  { vertLines:{color:"#161b22"}, horzLines:{color:"#161b22"} },
    timeScale:{ borderColor:"#21262d", timeVisible:true, fixLeftEdge:true, fixRightEdge:true },
    rightPriceScale:{ borderColor:"#21262d" },
    crosshair:{ mode:1 },
    handleScroll:true, handleScale:true,
  };

  // ── 主圖 K線 ──────────────────────────────────────────────────
  _chartMain = LW.createChart(document.getElementById("chart-container"), {...base, height:270});
  const candle = _chartMain.addCandlestickSeries({
    upColor:"#26a69a", downColor:"#ef5350",
    borderVisible:false, wickUpColor:"#26a69a", wickDownColor:"#ef5350"
  });
  candle.setData(d.candles);
  if(d.ma5&&d.ma5.length)  { const s=_chartMain.addLineSeries({color:"#26a69a",lineWidth:1,priceLineVisible:false,lastValueVisible:false}); s.setData(d.ma5); }
  if(d.ma20&&d.ma20.length) { const s=_chartMain.addLineSeries({color:"#f59e0b",lineWidth:1,priceLineVisible:false,lastValueVisible:false}); s.setData(d.ma20); }
  if(d.ma60&&d.ma60.length) { const s=_chartMain.addLineSeries({color:"#a78bfa",lineWidth:1.5,priceLineVisible:false,lastValueVisible:false}); s.setData(d.ma60); }
  _chartMain.timeScale().fitContent();

  // ── 成交量圖 ──────────────────────────────────────────────────
  _chartVol = LW.createChart(document.getElementById("vol-container"), {...base, height:70,
    rightPriceScale:{...base.rightPriceScale, scaleMargins:{top:0.1,bottom:0}}});
  const volS = _chartVol.addHistogramSeries({priceFormat:{type:"volume"}});
  volS.setData(d.volumes||[]);
  _chartVol.timeScale().fitContent();

  // ── MACD 圖 ──────────────────────────────────────────────────
  _chartMacd = LW.createChart(document.getElementById("macd-container"), {...base, height:110});
  if(d.macd_hist&&d.macd_hist.length){
    const histS = _chartMacd.addHistogramSeries({priceLineVisible:false,lastValueVisible:false});
    histS.setData(d.macd_hist);
  }
  if(d.macd_dif&&d.macd_dif.length){
    const difS = _chartMacd.addLineSeries({color:"#60a5fa",lineWidth:1.2,priceLineVisible:false,lastValueVisible:false});
    difS.setData(d.macd_dif);
  }
  if(d.macd_dea&&d.macd_dea.length){
    const deaS = _chartMacd.addLineSeries({color:"#f97316",lineWidth:1.2,priceLineVisible:false,lastValueVisible:false});
    deaS.setData(d.macd_dea);
  }
  _chartMacd.timeScale().fitContent();

  // ── RSI 圖 ───────────────────────────────────────────────────
  _chartRsi = LW.createChart(document.getElementById("rsi-container"), {...base, height:90});
  if(d.rsi&&d.rsi.length){
    const rsiS = _chartRsi.addLineSeries({color:"#c084fc",lineWidth:1.5,priceLineVisible:false,lastValueVisible:false});
    rsiS.setData(d.rsi);
    [[70,"#ef5350"],[30,"#22c55e"]].forEach(([val,col])=>{
      const ref = _chartRsi.addLineSeries({color:col,lineWidth:1,lineStyle:2,priceLineVisible:false,lastValueVisible:false});
      ref.setData(d.rsi.map(p=>({time:p.time,value:val})));
    });
  }
  _chartRsi.timeScale().fitContent();

  // 同步 timescale
  const charts = [_chartMain, _chartVol, _chartMacd, _chartRsi];
  charts.forEach((c,i)=>{
    c.timeScale().subscribeVisibleLogicalRangeChange(range=>{
      if(!range) return;
      charts.forEach((oc,j)=>{ if(i!==j) oc.timeScale().setVisibleLogicalRange(range); });
    });
  });

  // ── 上方價格列 ───────────────────────────────────────────────
  const last = d.candles[d.candles.length-1];
  const prev = d.candles.length>1 ? d.candles[d.candles.length-2].close : last.close;
  const chg  = last.close - prev;
  const pct  = prev ? chg/prev*100 : 0;
  const cls  = chg>=0 ? "ap-bull" : "ap-bear";
  const arr  = chg>=0 ? "▲" : "▼";
  const sm   = d.summary||{};
  const volFmt = v => v>=1e8?`${(v/1e8).toFixed(1)}億`:v>=1e4?`${(v/1e4).toFixed(0)}萬`:v?.toLocaleString()||"--";
  document.getElementById("chart-price-bar").innerHTML = `
    <span class="cph-price">${last.close.toLocaleString()}</span>
    <span class="cph-chg ${cls}">${arr} ${Math.abs(chg).toFixed(2)} (${pct>=0?"+":""}${pct.toFixed(2)}%)</span>
    <span class="cph-vol">量 ${volFmt(sm.last_vol)}</span>`;

  // ── 右側分析面板 ──────────────────────────────────────────────
  renderAnalysis(d, ticker);
}

function renderAnalysis(d, ticker){
  const s = d.summary||{};
  const fv = v => v!=null ? v.toLocaleString() : "--";
  const fp = v => v!=null ? `${v>=0?"+":""}${v.toFixed(2)}%` : "--";
  const pc = v => v==null?"ap-flat":v>=0?"ap-bull":"ap-bear";
  const tagHtml = (txt, cls) => `<span class="ap-tag ${cls}">${txt}</span>`;

  // 新聞 HTML
  const newsItems = _allNews[ticker||""]||[];
  let newsHtml = "";
  if(newsItems.length){
    newsHtml = newsItems.slice(0,6).map(a=>`
<div class="ap-news-item">
  <a href="${a.url}" target="_blank" rel="noopener noreferrer">${a.title}</a>
  <div class="ap-news-meta">${a.source||""}${a.pub?" · "+timeAgo(a.pub):""}</div>
</div>`).join("");
  } else {
    newsHtml = `<div class="ap-news-loading" id="ap-news-spinner">⏳ 新聞載入中…</div>`;
  }

  document.getElementById("chart-analysis").innerHTML = `
  <div class="ap-card">
    <div class="ap-card-title">趨勢分析</div>
    <div class="ap-row">
      <span class="ap-label">趨勢方向</span>
      ${tagHtml(s.trend||"--", s.trend_cls||"flat")}
    </div>
    <div class="ap-row">
      <span class="ap-label">MA5</span>
      <span class="ap-val ap-bull">${fv(s.ma5)} <small style="color:#484f58">${s.ma5_dir||""}</small></span>
    </div>
    <div class="ap-row">
      <span class="ap-label">MA20</span>
      <span class="ap-val ap-warn">${fv(s.ma20)} <small style="color:#484f58">${s.ma20_dir||""}</small></span>
    </div>
    <div class="ap-row">
      <span class="ap-label">MA60</span>
      <span class="ap-val" style="color:#a78bfa">${fv(s.ma60)}</span>
    </div>
  </div>
  <div class="ap-card">
    <div class="ap-card-title">動能指標</div>
    <div class="ap-row">
      <span class="ap-label">RSI(14)</span>
      ${tagHtml(s.rsi||"--", s.rsi_cls||"flat")}
    </div>
    <div class="ap-row">
      <span class="ap-label">MACD</span>
      ${tagHtml(s.macd||"--", s.macd_cls||"flat")}
    </div>
    <div class="ap-row">
      <span class="ap-label">DIF / DEA</span>
      <span class="ap-val ${s.dif!=null&&s.dif>=0?"ap-bull":"ap-bear"}" style="font-size:.72rem">
        ${s.dif!=null?s.dif.toFixed(3):"--"} / ${s.dea!=null?s.dea.toFixed(3):"--"}
      </span>
    </div>
  </div>
  <div class="ap-card">
    <div class="ap-card-title">漲跌表現</div>
    <div class="ap-row">
      <span class="ap-label">近 5 日</span>
      <span class="ap-val ${pc(s.chg5)}">${fp(s.chg5)}</span>
    </div>
    <div class="ap-row">
      <span class="ap-label">近 1 月</span>
      <span class="ap-val ${pc(s.chg20)}">${fp(s.chg20)}</span>
    </div>
    <div class="ap-row">
      <span class="ap-label">近 3 月</span>
      <span class="ap-val ${pc(s.chg60)}">${fp(s.chg60)}</span>
    </div>
    <div class="ap-row">
      <span class="ap-label">高點</span>
      <span class="ap-val ap-bear">${fv(s.high6m)}</span>
    </div>
    <div class="ap-row">
      <span class="ap-label">低點</span>
      <span class="ap-val ap-bull">${fv(s.low6m)}</span>
    </div>
  </div>
  <div class="ap-card" id="ap-chips-card" style="display:none">
    <div class="ap-card-title">🏦 三大法人籌碼</div>
    <div id="ap-chips-body"><div class="ap-news-loading">⏳ 載入中…</div></div>
  </div>
  <div class="ap-card" id="ap-announce-card" style="display:none">
    <div class="ap-card-title">📢 重大訊息公告</div>
    <div id="ap-announce-body"><div class="ap-news-loading">⏳ 載入中…</div></div>
  </div>
  <div class="ap-card">
    <div class="ap-card-title">📰 最新新聞</div>
    <div id="ap-news-body">${newsHtml}</div>
  </div>`;

  // ── 背景補載籌碼 + 重大訊息（台股才抓）──────────────────
  const isTW = ticker && ticker.endsWith(".TW");
  const twCode = isTW ? ticker.replace(".TW","") : null;

  if(isTW){
    document.getElementById("ap-chips-card").style.display = "";
    document.getElementById("ap-announce-card").style.display = "";

    // 三大法人
    fetch("/chips").then(r=>r.json()).then(chips=>{
      const el = document.getElementById("ap-chips-body");
      if(!el) return;
      const c = chips[twCode];
      if(!c){ el.innerHTML=`<div class="ap-news-loading">無籌碼資料</div>`; return; }
      const chip_row = (label, val) => {
        const cls = val>0?"ap-bull":val<0?"ap-bear":"ap-flat";
        const sign = val>0?"+":"";
        const bar_w = Math.min(Math.abs(val)/50*100, 100);
        const bar_c = val>0?"#3fb950":val<0?"#f85149":"#484f58";
        return `<div class="ap-row">
          <span class="ap-label">${label}</span>
          <span class="ap-val ${cls}" style="font-size:.72rem">${sign}${val.toLocaleString()} 張</span>
        </div>
        <div style="height:3px;background:#21262d;border-radius:2px;margin:-4px 0 6px">
          <div class="chip-row-bar" style="width:${bar_w}%;background:${bar_c}"></div>
        </div>`;
      };
      el.innerHTML = `
        <div style="font-size:.65rem;color:#484f58;margin-bottom:6px">日期：${c.date}</div>
        ${chip_row("外資", c.foreign_net)}
        ${chip_row("投信", c.trust_net)}
        ${chip_row("自營商", c.dealer_net)}
        <div style="border-top:1px solid #21262d;margin:4px 0"></div>
        ${chip_row("三大合計", c.total_net)}`;
    }).catch(()=>{
      const el = document.getElementById("ap-chips-body");
      if(el) el.innerHTML=`<div class="ap-news-loading">籌碼載入失敗</div>`;
    });

    // 重大訊息
    fetch(`/announce?code=${twCode}`).then(r=>r.json()).then(items=>{
      const el = document.getElementById("ap-announce-body");
      if(!el) return;
      if(!items.length){ el.innerHTML=`<div class="ap-news-loading">近30日無重大訊息</div>`; return; }
      el.innerHTML = items.map(a=>`
<div class="ap-news-item">
  <div style="color:#e6edf3;font-size:.73rem">${a.title}</div>
  <div class="ap-news-meta">${a.date} ${a.time}</div>
</div>`).join("");
    }).catch(()=>{
      const el = document.getElementById("ap-announce-body");
      if(el) el.innerHTML=`<div class="ap-news-loading">重大訊息載入失敗</div>`;
    });
  }

  // ── 背景補載新聞 ─────────────────────────────────────────
  if(!newsItems.length){
    fetch("/news").then(r=>r.json()).then(news=>{
      _allNews = {..._allNews, ...news};
      const items = news[ticker||""]||[];
      const el = document.getElementById("ap-news-body");
      if(!el) return;
      if(!items.length){
        el.innerHTML = `<div class="ap-news-loading">暫無相關新聞</div>`;
        return;
      }
      el.innerHTML = items.slice(0,6).map(a=>`
<div class="ap-news-item">
  <a href="${a.url}" target="_blank" rel="noopener noreferrer">${a.title}</a>
  <div class="ap-news-meta">${a.source||""}${a.pub?" · "+timeAgo(a.pub):""}</div>
</div>`).join("");
    }).catch(()=>{
      const el = document.getElementById("ap-news-body");
      if(el) el.innerHTML=`<div class="ap-news-loading">新聞載入失敗</div>`;
    });
  }
}

async function loadChips(){
  try{
    const chips = await (await fetch("/chips")).json();
    _allChips = chips;
    // 更新每個 chip-bar（不重抓股價，只補上標籤）
    document.querySelectorAll("[data-tw-code]").forEach(el=>{
      const code = el.dataset.twCode;
      const ch   = chips[code];
      if(!ch) return;
      const chipCls = v => v>0?"chip-bull":v<0?"chip-bear":"chip-flat";
      const chipTxt = v => (v>0?"+":"")+v.toLocaleString()+"張";
      el.innerHTML = `
        <span class="chip-tag ${chipCls(ch.foreign_net)}">外資 ${chipTxt(ch.foreign_net)}</span>
        <span class="chip-tag ${chipCls(ch.trust_net)}">投信 ${chipTxt(ch.trust_net)}</span>`;
    });
  } catch(e){ console.log("chips load failed:", e); }
}

// ── ARK 持股 ──────────────────────────────────────────────────────────────
let _arkData = [], _arkIdx = 0;

function renderArk(idx){
  _arkIdx = idx;
  const fund = _arkData[idx];
  if(!fund) return;
  // tabs
  document.getElementById("ark-tabs").innerHTML = _arkData.map((f,i)=>
    `<span class="ark-tab ${i===idx?"active":""}" onclick="renderArk(${i})">${f.symbol}</span>`
  ).join("");
  document.getElementById("ark-fund-name").textContent = fund.name;
  document.getElementById("ark-date").textContent = fund.date ? `資料日期：${fund.date}` : "";
  document.getElementById("ark-body").innerHTML = fund.holdings.length
    ? fund.holdings.map(h=>`<tr>
<td>${h.rank}</td>
<td><strong>${h.ticker}</strong></td>
<td style="color:#94a3b8;font-size:.75rem">${h.company}</td>
<td class="ark-weight">${h.weight.toFixed(2)}%</td>
<td style="color:#f1f5f9">$${h.share_price.toLocaleString("en-US",{minimumFractionDigits:2,maximumFractionDigits:2})}</td>
</tr>`).join("")
    : `<tr><td colspan="5" style="text-align:center;padding:20px;color:#475569">暫無資料</td></tr>`;
}

async function loadArk(){
  try{
    _arkData = await (await fetch("/ark")).json();
    renderArk(0);
  } catch{
    document.getElementById("ark-body").innerHTML=`<tr><td colspan="5" style="text-align:center;padding:20px;color:#475569">⚠️ ARK 資料載入失敗</td></tr>`;
  }
}

// ── 未來主題投資 ────────────────────────────────────────────────────────────
function renderTheme(stocks){
  const grid = document.getElementById("theme-grid");
  if(!stocks||!stocks.length){
    grid.innerHTML=`<div class="loading">⚠️ 暫無資料</div>`; return;
  }
  const sigMap = {"BUY":"🟢 買進","HOLD":"🟡 觀望","SELL":"🔴 賣出","HOT":"🔥 過熱","N/A":"⚪"};
  grid.innerHTML = stocks.map(q => {
    const c   = q.change_pct > 0 ? "up" : q.change_pct < 0 ? "down" : "flat";
    const arr = q.change_pct > 0 ? "▲" : q.change_pct < 0 ? "▼" : "–";
    const sig = q.signal_data || {};
    const sigLabel = sigMap[sig.signal||"N/A"]||"⚪";
    const rsiStr = sig.rsi!=null ? `　RSI:${sig.rsi}` : "";
    const volStr = sig.vol_ratio!=null ? `　量比:${sig.vol_ratio}x` : "";
    const safeTicker = q.ticker.replace(/'/g,"\\'");
    const safeName   = q.name.replace(/&/g,"&amp;").replace(/'/g,"\\'");
    return `<div class="theme-card" onclick="openChart('${safeTicker}','${safeName}')">
  <span class="theme-tag">玻璃基板供應鏈</span>
  <div class="theme-card-top">
    <div class="theme-name">${q.name}</div>
    <div class="theme-price">${q.error?"--":"NT$"+fmt(q.price)}</div>
  </div>
  <div class="theme-change ${c}">${q.error?"--":arr+" "+( q.change_pct>=0?"+":"")+fmt(q.change_pct)+"%"}</div>
  <div class="theme-signal">${sigLabel}${rsiStr}${volStr}</div>
  <div class="theme-desc">📌 ${q.desc||""}</div>
</div>`;
  }).join("");
}

async function loadTheme(){
  try{ renderTheme(await (await fetch("/theme")).json()); }
  catch{ document.getElementById("theme-grid").innerHTML=`<div class="loading">⚠️ 主題股載入失敗</div>`; }
}

// ── 目標買進提示 ──────────────────────────────────────────────────────────────
let _wbQuotes=[], _wbEntries=JSON.parse(localStorage.getItem("wb_entries")||"[]");

// 預設加入 MSFT 和 COST（若尚未存在）
(function(){
  const defaults=[
    {ticker:"MSFT", target:400, note:"微軟：RSI健康，等回調進場"},
    {ticker:"COST", target:880, note:"好市多：RSI偏弱，接近甜蜜點"},
  ];
  defaults.forEach(d=>{
    if(!_wbEntries.find(e=>e.ticker===d.ticker)){
      _wbEntries.push({...d, addedAt:new Date().toLocaleDateString("zh-TW")});
    }
  });
  localStorage.setItem("wb_entries",JSON.stringify(_wbEntries));
})();

function wbSave(){localStorage.setItem("wb_entries",JSON.stringify(_wbEntries));}
function wbAdd(){
  const ticker=document.getElementById("wb-ticker").value.trim().toUpperCase();
  const target=parseFloat(document.getElementById("wb-target").value);
  const note=document.getElementById("wb-note").value.trim();
  if(!ticker||isNaN(target)||target<=0){alert("請輸入股票代號與目標買進價");return;}
  _wbEntries.push({ticker,target,note,addedAt:new Date().toLocaleDateString("zh-TW")});
  wbSave();
  document.getElementById("wb-ticker").value="";
  document.getElementById("wb-target").value="";
  document.getElementById("wb-note").value="";
  wbRender();
}
function wbDel(i){_wbEntries.splice(i,1);wbSave();wbRender();}
function wbRender(){
  const list=document.getElementById("wb-list");
  if(!_wbEntries.length){
    list.innerHTML=`<div style="color:#475569;font-size:.8rem;padding:8px 0">尚未設定目標買進價。</div>`;return;
  }
  const priceMap={};
  for(const q of _wbQuotes) priceMap[q.ticker]=q;
  list.innerHTML=_wbEntries.map((e,i)=>{
    const q=priceMap[e.ticker];
    const cur=q&&!q.error?q.price:null;
    const nameStr=q?q.name:e.ticker;
    let cardCls="",status="",statusCls="",barW=50,barColor="#3b82f6";
    if(cur===null){
      status="⚪ 等待報價"; statusCls="";
    } else {
      const diff=((cur-e.target)/e.target*100);
      const aboveTarget=cur>e.target;
      if(!aboveTarget){
        status=`🎯 已到達目標價！現價 $${cur}，可考慮進場`;
        statusCls="profit"; cardCls="profit"; barColor="#22c55e"; barW=95;
      } else if(diff<=5){
        status=`🔔 快到了！距目標價還差 ${diff.toFixed(1)}%`;
        statusCls="warning"; cardCls="warning"; barColor="#f59e0b"; barW=80;
      } else if(diff<=15){
        status=`⏳ 持續觀察，距目標 ${diff.toFixed(1)}%`;
        statusCls=""; barColor="#3b82f6"; barW=55;
      } else {
        status=`📈 現價比目標高 ${diff.toFixed(1)}%，繼續等待`;
        statusCls=""; barColor="#475569"; barW=30;
      }
    }
    const curStr=cur!=null?`$${cur.toLocaleString()}`:"載入中";
    return `<div class="sl-card ${cardCls}">
  <button class="sl-del" onclick="wbDel(${i})">✕</button>
  <div class="sl-card-top"><div class="sl-name">${nameStr}</div><div class="sl-status ${statusCls}">${status}</div></div>
  <div class="sl-details">現價 <span>${curStr}</span>　目標買進價 <span>$${e.target.toLocaleString()}</span>${e.note?`　備註 <span>${e.note}</span>`:""}　新增日期 <span>${e.addedAt}</span></div>
  <div class="sl-bar-wrap"><div class="sl-bar" style="width:${barW}%;background:${barColor}"></div></div>
</div>`;
  }).join("");
}
function wbUpdatePrices(quotes){_wbQuotes=quotes;wbRender();}

// ── 停損提示 ────────────────────────────────────────────────────────────────
let _slQuotes=[], _slEntries=JSON.parse(localStorage.getItem("sl_entries")||"[]");
function slSave(){localStorage.setItem("sl_entries",JSON.stringify(_slEntries));}
function slAdd(){
  const ticker=document.getElementById("sl-ticker").value.trim().toUpperCase();
  const buy=parseFloat(document.getElementById("sl-buy").value);
  const pct=parseFloat(document.getElementById("sl-pct").value)||7;
  const note=document.getElementById("sl-note").value.trim();
  if(!ticker||isNaN(buy)||buy<=0){alert("請輸入股票代號與買進價格");return;}
  _slEntries.push({ticker,buy,pct,note,addedAt:new Date().toLocaleDateString("zh-TW")});
  slSave();
  document.getElementById("sl-ticker").value="";
  document.getElementById("sl-buy").value="";
  document.getElementById("sl-note").value="";
  slRender();
}
function slDel(i){_slEntries.splice(i,1);slSave();slRender();}
function slRender(){
  const list=document.getElementById("sl-list");
  if(!_slEntries.length){list.innerHTML=`<div style="color:#475569;font-size:.8rem;padding:8px 0">尚未新增任何持股。輸入買進價格後點「新增」即可追蹤停損。</div>`;return;}
  const priceMap={};
  for(const q of _slQuotes) priceMap[q.ticker]=q;
  list.innerHTML=_slEntries.map((e,i)=>{
    const q=priceMap[e.ticker];
    const cur=q&&!q.error?q.price:null;
    const slPrice=e.buy*(1-e.pct/100);
    const tpPrice=e.buy*1.15;
    let cardCls="",status="",statusCls="",barColor="",barW=50;
    if(cur===null){status="⚪ 等待報價";statusCls="";cardCls="";}
    else{
      const chgFromBuy=(cur-e.buy)/e.buy*100;
      const distToSL=(cur-slPrice)/e.buy*100;
      if(cur<=slPrice){status=`🚨 已跌破停損！現價 ${cur}，虧損 ${chgFromBuy.toFixed(1)}%`;statusCls="danger";cardCls="danger";barColor="#ef4444";barW=5;}
      else if(distToSL<3){status=`⚠️ 接近停損！距停損價還差 ${distToSL.toFixed(1)}%`;statusCls="warning";cardCls="warning";barColor="#f59e0b";barW=20;}
      else if(cur>=tpPrice){status=`🎯 達獲利目標！+${chgFromBuy.toFixed(1)}%`;statusCls="profit";cardCls="profit";barColor="#22c55e";barW=95;}
      else if(chgFromBuy>=0){status=`✅ 獲利中 +${chgFromBuy.toFixed(1)}%`;statusCls="profit";cardCls="profit";barColor="#4ade80";barW=Math.min(90,50+chgFromBuy*3);}
      else{status=`📉 虧損中 ${chgFromBuy.toFixed(1)}%`;statusCls="warning";cardCls="";barColor="#f59e0b";barW=Math.max(10,50+chgFromBuy*3);}
    }
    const nameStr=q?q.name:e.ticker;
    return `<div class="sl-card ${cardCls}">
  <button class="sl-del" onclick="slDel(${i})">✕</button>
  <div class="sl-card-top"><div class="sl-name">${nameStr}</div><div class="sl-status ${statusCls}">${status}</div></div>
  <div class="sl-details">現價 <span>${cur!=null?"NT$ "+cur.toLocaleString():"載入中"}</span>　買進 <span>NT$ ${e.buy.toLocaleString()}</span>　停損價 <span>NT$ ${slPrice.toFixed(0)}（-${e.pct}%）</span>${e.note?`　備註 <span>${e.note}</span>`:""}　新增日期 <span>${e.addedAt}</span></div>
  <div class="sl-bar-wrap"><div class="sl-bar" style="width:${barW}%;background:${barColor}"></div></div>
</div>`;
  }).join("");
}
function slUpdatePrices(quotes){_slQuotes=quotes;slRender();}

load(); loadHouse(); loadCal(); loadTheme(); loadArk(); tick();
wbRender(); slRender();
// 延遲載入籌碼（不阻塞主要股價顯示）
setTimeout(loadChips, 3000);
</script>
</body>
</html>
"""

# ── HTTP 伺服器 ─────────────────────────────────────────────────────────────────
class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a): pass
    def do_GET(self):
        if self.path == "/":
            body = HTML_PAGE.encode()
            self.send_response(200)
            self.send_header("Content-Type","text/html; charset=utf-8")
            self.send_header("Cache-Control","no-cache, no-store, must-revalidate")
            self.end_headers(); self.wfile.write(body)
        elif self.path == "/data":
            payload = safe_json(get_quotes_data()).encode()
            self.send_response(200)
            self.send_header("Content-Type","application/json")
            self.send_header("Access-Control-Allow-Origin","*")
            self.end_headers(); self.wfile.write(payload)
        elif self.path == "/news":
            payload = safe_json(get_news_only()).encode()
            self.send_response(200)
            self.send_header("Content-Type","application/json")
            self.send_header("Access-Control-Allow-Origin","*")
            self.end_headers(); self.wfile.write(payload)
        elif self.path == "/calendar":
            payload = safe_json(get_calendar()).encode()
            self.send_response(200)
            self.send_header("Content-Type","application/json")
            self.send_header("Access-Control-Allow-Origin","*")
            self.end_headers(); self.wfile.write(payload)
        elif self.path == "/ark":
            payload = safe_json(get_ark_data()).encode()
            self.send_response(200)
            self.send_header("Content-Type","application/json")
            self.send_header("Access-Control-Allow-Origin","*")
            self.end_headers(); self.wfile.write(payload)
        elif self.path == "/theme":
            with _bg_lock:
                data = _bg_cache["theme"]
            payload = safe_json(data).encode()
            self.send_response(200)
            self.send_header("Content-Type","application/json")
            self.send_header("Access-Control-Allow-Origin","*")
            self.end_headers(); self.wfile.write(payload)
        elif self.path == "/house":
            payload = safe_json(get_house_data()).encode()
            self.send_response(200)
            self.send_header("Content-Type","application/json")
            self.send_header("Access-Control-Allow-Origin","*")
            self.end_headers(); self.wfile.write(payload)
        elif self.path == "/house-debug":
            # 診斷端點：測試政府 ZIP 下載
            try:
                r = requests.get(HOUSE_DATA_URL, timeout=30, stream=False,
                                 verify=False,
                                 headers={"User-Agent": "Mozilla/5.0"})
                info = {
                    "status_code": r.status_code,
                    "content_type": r.headers.get("Content-Type",""),
                    "content_length": len(r.content),
                    "first_bytes_hex": r.content[:16].hex(),
                    "is_zip": r.content[:2] == b'PK',
                    "error": None
                }
            except Exception as e:
                info = {"error": str(e)}
            payload = json.dumps(info, ensure_ascii=False).encode()
            self.send_response(200)
            self.send_header("Content-Type","application/json")
            self.end_headers(); self.wfile.write(payload)
        elif self.path == "/health":
            self.send_response(200)
            self.send_header("Content-Type","text/plain")
            self.end_headers(); self.wfile.write(b"ok")
        elif self.path.startswith("/chart-data"):
            from urllib.parse import parse_qs, urlparse
            qs = parse_qs(urlparse(self.path).query)
            ticker = qs.get("ticker", [""])[0].strip()
            period = qs.get("period", ["6mo"])[0].strip()
            if period not in ("1mo","3mo","6mo","1y"):
                period = "6mo"
            if ticker:
                payload = safe_json(get_chart_data(ticker, period) or {}).encode()
            else:
                payload = b"{}"
            self.send_response(200)
            self.send_header("Content-Type","application/json")
            self.send_header("Access-Control-Allow-Origin","*")
            self.end_headers(); self.wfile.write(payload)
        elif self.path == "/chips":
            # 傳回全部台股三大法人資料
            payload = safe_json(get_chips()).encode()
            self.send_response(200)
            self.send_header("Content-Type","application/json")
            self.send_header("Access-Control-Allow-Origin","*")
            self.end_headers(); self.wfile.write(payload)
        elif self.path.startswith("/announce"):
            from urllib.parse import parse_qs, urlparse
            qs = parse_qs(urlparse(self.path).query)
            tw_code = qs.get("code", [""])[0].strip()
            if tw_code and _re.match(r'^\d{4,6}$', tw_code):
                data = get_announcements(tw_code)
            else:
                data = []
            payload = safe_json(data).encode()
            self.send_response(200)
            self.send_header("Content-Type","application/json")
            self.send_header("Access-Control-Allow-Origin","*")
            self.end_headers(); self.wfile.write(payload)
        else:
            self.send_response(404); self.end_headers()

def main():
    import sys
    print(f"Python version: {sys.version}", flush=True)
    print(f"🚀  Starting server on port {PORT} ...", flush=True)
    try:
        server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    except Exception as e:
        print(f"❌  Failed to bind port {PORT}: {e}", flush=True)
        sys.exit(1)
    print(f"✅  Server started on 0.0.0.0:{PORT}", flush=True)
    # 啟動背景更新執行緒（永久執行）
    threading.Thread(target=_background_loop, daemon=True).start()
    threading.Thread(target=_refresh_theme, daemon=True).start()
    print("🔄  背景更新執行緒已啟動", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n🛑  Server stopped.")

if __name__ == "__main__":
    main()
