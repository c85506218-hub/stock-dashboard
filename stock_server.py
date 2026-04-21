#!/usr/bin/env python3
"""
每日股市 Web Dashboard — 含即時新聞 & 自動評語
用法: python3 stock_server.py
然後開啟瀏覽器 http://localhost:8888
"""

import csv
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

SECTIONS_ORDER = [
    "台股大盤", "台灣ETF－高息", "台灣ETF－主題",
    "台股－半導體", "台股－電子製造", "台股－金融", "台股－傳產其他",
    "美股大盤", "美股－科技AI", "美股－多元",
]

# ── 快取 ───────────────────────────────────────────────────────────────────────
_data_cache  = {"quotes": [], "news": {}, "ts": 0}
_house_cache = {"data": None, "ts": 0}
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
    try:
        info  = yf.Ticker(ticker).fast_info
        price = info.last_price
        prev  = info.previous_close
        if price is None or prev is None:
            raise ValueError
        change     = price - prev
        change_pct = change / prev * 100
        return dict(ticker=ticker, name=name, section=section, currency=currency,
                    price=round(price, 2), change=round(change, 2),
                    change_pct=round(change_pct, 2), error=False)
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

def fetch_chart_data(ticker):
    try:
        df = yf.Ticker(ticker).history(period="6mo", interval="1d", auto_adjust=True)
        if df.empty:
            return None
        dates = [d.strftime("%Y-%m-%d") for d in df.index]
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

        def to_series(raw):
            return [{"time": dates[i], "value": raw[i]}
                    for i in range(len(dates)) if raw[i] is not None]

        return {
            "ticker":  ticker,
            "candles": candles,
            "volumes": volumes,
            "ma5":     to_series(ma5_raw),
            "ma20":    to_series(ma20_raw),
            "ma60":    to_series(ma60_raw),
            "rsi":     to_series(rsi_raw),
        }
    except Exception as e:
        print(f"[Chart] {ticker} 錯誤: {e}", flush=True)
        return None

def get_chart_data(ticker):
    now = time.time()
    with _chart_cache_lock:
        cached = _chart_cache.get(ticker)
        if cached and now - cached["ts"] < CHART_CACHE_SECONDS:
            return cached["data"]
        data = fetch_chart_data(ticker)
        _chart_cache[ticker] = {"data": data, "ts": now}
        return data

# ── 背景定期更新（HTTP 請求永遠瞬間回傳快取，不等待網路）────────────────────────
NEWS_CACHE_SECONDS = 900
_bg_cache = {
    "quotes":    [],
    "news":      {},
    "updated":   "",
    "ready":     False,   # 第一次抓完才設為 True
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

def _background_loop():
    """每隔 60 秒更新股價，每隔 900 秒更新新聞。"""
    last_news_refresh = 0
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
  display:none; position:fixed; inset:0; background:rgba(0,0,0,.75);
  z-index:1000; align-items:center; justify-content:center;
}
.chart-modal-overlay.open { display:flex; }
.chart-modal {
  background:#131722; border:1px solid #2d3748; border-radius:12px;
  width:min(960px,96vw); max-height:90vh; overflow:hidden;
  display:flex; flex-direction:column;
}
.chart-modal-header {
  display:flex; align-items:center; justify-content:space-between;
  padding:14px 20px; border-bottom:1px solid #2d3748;
}
.chart-modal-title { font-size:1rem; font-weight:700; color:#f8fafc; }
.chart-modal-close {
  background:none; border:none; color:#94a3b8; font-size:1.4rem;
  cursor:pointer; padding:2px 8px; border-radius:6px;
}
.chart-modal-close:hover { background:#2d3748; color:#f8fafc; }
.chart-modal-body { padding:12px 16px 16px; overflow-y:auto; }
.chart-period-btns { display:flex; gap:6px; margin-bottom:10px; }
.chart-period-btn {
  padding:4px 12px; border-radius:6px; border:1px solid #374151;
  background:transparent; color:#94a3b8; font-size:.8rem; cursor:pointer;
}
.chart-period-btn.active, .chart-period-btn:hover {
  background:#2563eb; color:#fff; border-color:#2563eb;
}
.chart-legend {
  display:flex; gap:14px; flex-wrap:wrap; font-size:.75rem; margin-bottom:8px;
}
.chart-legend span { display:flex; align-items:center; gap:5px; }
.chart-legend i { display:inline-block; width:24px; height:3px; border-radius:2px; }
#chart-container { height:340px; }
#rsi-container   { height:120px; margin-top:8px; }
.rsi-label {
  font-size:.72rem; color:#64748b; margin-bottom:3px; margin-top:6px;
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
      <button class="chart-modal-close" onclick="closeChart()">✕</button>
    </div>
    <div class="chart-modal-body">
      <div class="chart-legend">
        <span><i style="background:#26a69a"></i>MA5</span>
        <span><i style="background:#f59e0b"></i>MA20</span>
        <span><i style="background:#a78bfa"></i>MA60</span>
      </div>
      <div id="chart-container"></div>
      <div class="rsi-label">RSI(14) — 超買 &gt;70（紅線）、超賣 &lt;30（綠線）</div>
      <div id="rsi-container"></div>
    </div>
  </div>
</div>

<script>
const REFRESH  = 60;
const SECTIONS = ["台股大盤","台灣ETF－高息","台灣ETF－主題","台股－半導體","台股－電子製造","台股－金融","台股－傳產其他","美股大盤","美股－科技AI","美股－多元"];
const CUR      = {NTD:"NT$", USD:"$"};
let timer = REFRESH;

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
  document.getElementById("ai-text").textContent = (data.commentary||{}).market_summary||"";
  const ai    = (data.commentary||{}).stocks||{};
  const news  = data.news||{};
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
<table><thead><tr><th>名稱</th><th>最新價</th><th>漲跌</th><th>漲跌幅</th></tr></thead><tbody>`;

    for(const q of items){
      const c = cls(q.change), cu = CUR[q.currency]||"";
      const note = ai[q.name]||"";
      const safeTicker = q.ticker.replace(/'/g,"\\'");
      const safeName   = q.name.replace(/&/g,"&amp;").replace(/'/g,"\\'");
      html += `<tr>
<td class="name"><span class="name-link" onclick="openChart('${safeTicker}','${safeName}')">${q.name}</span>${note?`<div class="note">💬 ${note}</div>`:""}</td>
<td class="price">${q.error?"--":cu+fmt(q.price)}</td>
<td class="${c}">${q.error?"--":arr(q.change)+" "+sgn(q.change)+fmt(q.change)}</td>
<td class="${c}">${q.error?"--":sgn(q.change_pct)+fmt(q.change_pct)+"%"}</td>
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
    render(d);
  } catch(e){
    document.getElementById("grid").innerHTML=`<div class="loading">⚠️ 連線失敗，請稍後<br><small style="color:#888">${e.message||e}</small></div>`;
    return;
  }
  // 背景補載新聞，載完只更新新聞區塊不重抓股價
  try{
    const news = await (await fetch("/news")).json();
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
let _chartMain = null, _chartRsi = null;

function closeChart(){
  document.getElementById("chart-overlay").classList.remove("open");
  if(_chartMain){ _chartMain.remove(); _chartMain=null; }
  if(_chartRsi) { _chartRsi.remove();  _chartRsi=null; }
}

async function openChart(ticker, name){
  const displayName = name.replace(/&amp;/g,"&");
  document.getElementById("chart-title").textContent = `📈 ${displayName} — 技術分析`;
  document.getElementById("chart-overlay").classList.add("open");
  document.getElementById("chart-container").innerHTML = '<div class="loading">⏳ 載入圖表中…</div>';
  document.getElementById("rsi-container").innerHTML   = "";

  let d;
  try{ d = await (await fetch(`/chart-data?ticker=${encodeURIComponent(ticker)}`)).json(); }
  catch{ document.getElementById("chart-container").innerHTML='<div class="loading">⚠️ 圖表載入失敗</div>'; return; }
  if(!d||!d.candles||!d.candles.length){
    document.getElementById("chart-container").innerHTML='<div class="loading">⚠️ 無圖表資料</div>'; return;
  }

  // lightweight-charts v4
  document.getElementById("chart-container").innerHTML="";
  document.getElementById("rsi-container").innerHTML="";

  const LW = window.LightweightCharts;
  const chartOpt = {
    layout:{ background:{color:"#131722"}, textColor:"#d1d5db" },
    grid:  { vertLines:{color:"#1f2937"}, horzLines:{color:"#1f2937"} },
    timeScale:{ borderColor:"#374151", timeVisible:true },
    rightPriceScale:{ borderColor:"#374151" },
    crosshair:{ mode:1 },
  };

  // 主圖：K線 + MA + 成交量
  _chartMain = LW.createChart(document.getElementById("chart-container"), {...chartOpt, height:340});
  const candle = _chartMain.addCandlestickSeries({ upColor:"#26a69a", downColor:"#ef5350", borderVisible:false, wickUpColor:"#26a69a", wickDownColor:"#ef5350" });
  candle.setData(d.candles);

  const vol = _chartMain.addHistogramSeries({ priceFormat:{type:"volume"}, priceScaleId:"vol", scaleMargins:{top:0.8,bottom:0} });
  vol.setData(d.volumes||[]);

  if(d.ma5.length)  { const s=_chartMain.addLineSeries({color:"#26a69a",lineWidth:1,priceLineVisible:false}); s.setData(d.ma5); }
  if(d.ma20.length) { const s=_chartMain.addLineSeries({color:"#f59e0b",lineWidth:1,priceLineVisible:false}); s.setData(d.ma20); }
  if(d.ma60.length) { const s=_chartMain.addLineSeries({color:"#a78bfa",lineWidth:1.5,priceLineVisible:false}); s.setData(d.ma60); }

  _chartMain.timeScale().fitContent();

  // RSI 圖
  if(d.rsi&&d.rsi.length){
    _chartRsi = LW.createChart(document.getElementById("rsi-container"), {...chartOpt, height:120});
    const rsiSeries = _chartRsi.addLineSeries({color:"#60a5fa",lineWidth:1.5,priceLineVisible:false});
    rsiSeries.setData(d.rsi);
    // 超買70 / 超賣30 參考線
    const ob = _chartRsi.addLineSeries({color:"#ef5350",lineWidth:1,lineStyle:2,priceLineVisible:false});
    ob.setData(d.rsi.map(p=>({time:p.time,value:70})));
    const os = _chartRsi.addLineSeries({color:"#22c55e",lineWidth:1,lineStyle:2,priceLineVisible:false});
    os.setData(d.rsi.map(p=>({time:p.time,value:30})));
    _chartRsi.timeScale().fitContent();
  }
}

load(); loadHouse(); loadCal(); tick();
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
            payload = json.dumps(get_quotes_data()).encode()
            self.send_response(200)
            self.send_header("Content-Type","application/json")
            self.send_header("Access-Control-Allow-Origin","*")
            self.end_headers(); self.wfile.write(payload)
        elif self.path == "/news":
            payload = json.dumps(get_news_only()).encode()
            self.send_response(200)
            self.send_header("Content-Type","application/json")
            self.send_header("Access-Control-Allow-Origin","*")
            self.end_headers(); self.wfile.write(payload)
        elif self.path == "/calendar":
            payload = json.dumps(get_calendar()).encode()
            self.send_response(200)
            self.send_header("Content-Type","application/json")
            self.send_header("Access-Control-Allow-Origin","*")
            self.end_headers(); self.wfile.write(payload)
        elif self.path == "/house":
            payload = json.dumps(get_house_data()).encode()
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
        elif self.path.startswith("/chart-data?"):
            from urllib.parse import parse_qs, urlparse
            qs = parse_qs(urlparse(self.path).query)
            ticker = qs.get("ticker", [""])[0].strip()
            if ticker:
                payload = json.dumps(get_chart_data(ticker) or {}).encode()
            else:
                payload = b"{}"
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
    print("🔄  背景更新執行緒已啟動", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n🛑  Server stopped.")

if __name__ == "__main__":
    main()
