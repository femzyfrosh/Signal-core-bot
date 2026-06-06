import os, json, time, secrets, requests, threading, hmac, hashlib, urllib.parse
from datetime import datetime, timezone, timedelta
LOCAL_TZ = timezone(timedelta(hours=1))   # UTC+1 (user local time)
from flask import Flask, request, jsonify, make_response
from collections import deque

app = Flask(__name__)

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID", "")
DASHBOARD_PASSWORD = os.environ.get("DASHBOARD_PASSWORD", "")
MEXC_API_KEY    = os.environ.get("MEXC_API_KEY",    "")
MEXC_API_SECRET = os.environ.get("MEXC_API_SECRET", "")
GEMINI_API_KEY  = os.environ.get("GEMINI_API_KEY",  "")

# ── CANDLE CACHE — shared across scanner + visual engine ──────────────
# Keyed by (symbol, interval). Entries expire after TF candle duration.
# Prevents duplicate MEXC fetches when math + visual both need same data.
_candle_cache      = {}
_candle_cache_lock = threading.Lock()
_candle_cache_ts   = {}   # (symbol,interval) → fetch timestamp

# ── VISUAL ANALYSIS RESULT CACHE ─────────────────────────────────────
# Prevents re-analysing a pair whose candles haven't changed since last scan.
# Keyed by symbol → {ts, result}. Valid for 4 minutes (avoids Gemini hammering).
_visual_cache      = {}
_visual_cache_lock = threading.Lock()
VISUAL_CACHE_TTL   = 240   # seconds

MAX_SIGNALS = 500
signals     = deque(maxlen=MAX_SIGNALS)
sessions    = set()

# ── GLOBAL SETTINGS (editable from Settings page) ─────────────────────
scan_settings = {
    "price_interval":        1,
    "scan_interval":         1,
    "cycle_rest":            5,
    "market_refresh_interval": 1,    # seconds between live price updates on market page
    "tg_signals":        True,
    "tg_trades":         True,
    "tg_bot_token":      TELEGRAM_BOT_TOKEN,
    "tg_chat_id":        TELEGRAM_CHAT_ID,
    "model1_enabled":    True,
    "model2_enabled":    True,   # unified scanner for Model #2a + #2b
    "model3_enabled":    True,   # Model #3 (was M4: HTF OB + LTF Sweep→CHoCH→OB)
}
settings_lock = threading.Lock()

trade_config = {
    "enabled":       True,    # auto-trade ON by default
    "api_key":       MEXC_API_KEY,
    "api_secret":    MEXC_API_SECRET,
    "risk_pct":      15.0,    # 15% risk per trade
    "max_trades":    2,       # max 2 simultaneous open trades
    "leverage":      30,      # 30x leverage
    "margin_mode":   2,       # 1 = isolated, 2 = cross margin
}
open_trades  = {}
trade_lock   = threading.Lock()
MEXC_FUTURES = "https://contract.mexc.com/api/v1/private"

scan_state = {
    "running": False, "enabled": True, "current_pair": "",
    "pairs_done": 0, "total_pairs": 0, "scan_count": 0,
    "signals_found": 0, "last_scan": None,
    "log": deque(maxlen=200),
}
scan_lock = threading.Lock()

# ── LIVE PRICE CACHE (background 1-second batch refresh) ─────────────
price_cache      = {}
price_cache_lock = threading.Lock()

TOP_PAIRS = [
    "PENGU_USDT", "GME_USDT",   "MEME_USDT",  "RIVER_USDT", "DRIFT_USDT",
    "FARTCOIN_USDT", "FLOKI_USDT", "BONK_USDT",  "WIF_USDT",   "PEPE_USDT",
    "AVAX_USDT",  "POPCAT_USDT","ONDO_USDT",  "ARB_USDT",   "RENDER_USDT",
    "FET_USDT",   "OPG_USDT",   "APT_USDT",   "LINK_USDT",  "TAO_USDT",
    "INJ_USDT",   "SEI_USDT",   "HBAR_USDT",  "KAS_USDT",   "NEAR_USDT",
    "SUI_USDT",   "HYPE_USDT",  "MAT_USDT", "XRP_USDT",   "BAN_USDT",
]
MEXC_BASE = "https://contract.mexc.com/api/v1/contract"
CRT_TFS   = ["Day1","Hour4","Hour3","Hour2","Min60"]
OB_TFS    = ["Day1","Hour4","Hour3","Hour2","Min60","Min45"]
# LTF per spec: 15,5,4,3,2,1 min
TBS_TFS   = ["Min15","Min5","Min4","Min3","Min2","Min1"]
TBS_TF_MAP = {
    "Min60": ["Min15","Min5","Min4","Min3","Min2","Min1"],
    "Hour2": ["Min15","Min5","Min4","Min3","Min2","Min1"],
    "Hour3": ["Min15","Min5","Min4","Min3","Min2","Min1"],
    "Hour4": ["Min15","Min5","Min4","Min3","Min2","Min1"],
    "Day1":  ["Min15","Min5","Min4","Min3","Min2","Min1"],
}

# Candle duration in seconds per timeframe (used to compute time-left in C2)
TF_SECONDS = {
    "Min1":60,"Min2":120,"Min3":180,"Min4":240,"Min5":300,
    "Min10":600,"Min15":900,"Min30":1800,"Min45":2700,
    "Min60":3600,"Hour2":7200,"Hour3":10800,"Hour4":14400,
    "Hour8":28800,"Day1":86400,"Week1":604800,
}

TF_MINUTES = {
    "Day1": 1440, "Hour4": 240, "Hour3": 180, "Hour2": 120, "Min60": 60,
    "Min45": 45, "Min30": 30, "Min15": 15, "Min10": 10, "Min5": 5,
    "Min4": 4, "Min3": 3, "Min2": 2, "Min1": 1,
}

def get_minutes_remaining(tf_name):
    """How many minutes remain in the CURRENT (still-forming) candle on this timeframe."""
    tf_mins = TF_MINUTES.get(tf_name, 60)
    tf_secs = tf_mins * 60
    now = time.time()
    candle_start = (int(now) // tf_secs) * tf_secs
    candle_end   = candle_start + tf_secs
    return max(0.0, (candle_end - now) / 60.0)

# ── PAPER TRADING ENGINE ──────────────────────────────────────────────
paper_config = {
    "enabled":    True,
    "auto_trade": True,
    "balance":    100.0,
    "risk_pct":   25.0,
    "max_trades": 5,
}
paper_trades  = {}
paper_history = deque(maxlen=50)
paper_lock    = threading.Lock()
paper_stats   = {"total": 0, "wins": 0, "losses": 0, "total_pnl": 0.0}

# ════════ MEXC API ═══════════════════════════════════════════════════

def get_all_pairs():
    try:
        r = requests.get(f"{MEXC_BASE}/detail", timeout=15)
        data = r.json()
        if not data.get("success"): return []
        seen = set()
        pairs = []
        for item in data.get("data", []):
            sym = item.get("symbol","")
            if item.get("state") == 1 and sym.endswith("_USDT") and sym not in seen:
                seen.add(sym)
                pairs.append(sym)
        return sorted(pairs)
    except Exception as e:
        log(f"Pairs error: {e}"); return []

# Intervals MEXC Futures kline endpoint natively supports
_NATIVE_TFS = {"Min1","Min5","Min15","Min30","Min60","Hour4","Hour8","Day1","Week1","Month1"}
# Synthetic TFs: map to (base_interval, candles_to_merge)
_AGG_MAP = {
    "Min2":  ("Min1",  2),
    "Min3":  ("Min1",  3),
    "Min4":  ("Min1",  4),
    "Min45": ("Min15", 3),
    "Hour2": ("Min60", 2),
    "Hour3": ("Min60", 3),
}

def _aggregate_candles(candles, n):
    """Merge every N consecutive candles into one OHLC bar."""
    out = []
    for i in range(0, len(candles), n):
        chunk = candles[i:i+n]
        if not chunk: continue
        out.append({
            "time":  chunk[0]["time"],
            "open":  chunk[0]["open"],
            "high":  max(c["high"] for c in chunk),
            "low":   min(c["low"]  for c in chunk),
            "close": chunk[-1]["close"],
        })
    return out

def get_candles(symbol, interval, limit=150):
    # Synthetic intervals: fetch from a supported base TF and aggregate
    if interval in _AGG_MAP:
        base_tf, n = _AGG_MAP[interval]
        raw = get_candles(symbol, base_tf, limit * n)
        return _aggregate_candles(raw, n)[-limit:]

    # ── Shared candle cache — TTL = half of one candle duration ──────
    # Multiple callers (math scanner + visual engine) share the same fetch.
    cache_key = (symbol, interval, limit)
    tf_secs   = TF_SECONDS.get(interval, 60)
    ttl       = max(15, tf_secs // 2)   # cache for half a candle period
    now_ts    = time.time()
    with _candle_cache_lock:
        if cache_key in _candle_cache:
            age = now_ts - _candle_cache_ts.get(cache_key, 0)
            if age < ttl:
                return _candle_cache[cache_key]

    try:
        r = requests.get(f"{MEXC_BASE}/kline/{symbol}",
                         params={"interval":interval,"limit":limit}, timeout=10)
        data = r.json()
        if not data.get("success") or not data.get("data"): return []
        raw = data["data"]
        out = []
        times=raw.get("time",[]); opens=raw.get("open",[])
        highs=raw.get("high",[]); lows=raw.get("low",[]); closes=raw.get("close",[])
        for i in range(len(times)):
            try:
                out.append({"time":int(times[i]),"open":float(opens[i]),
                            "high":float(highs[i]),"low":float(lows[i]),"close":float(closes[i])})
            except: continue
        with _candle_cache_lock:
            _candle_cache[cache_key]    = out
            _candle_cache_ts[cache_key] = now_ts
        return out
    except: return []

def _parse_mexc_ticker(d):
    """
    Parse a single MEXC futures ticker dict into {price, change, high, low}.
    MEXC futures `riseFallRate` is ALWAYS a decimal fraction per the official API docs
    (e.g. -0.0026 means -0.26%).  We multiply by 100 to get a display percentage.
    """
    def _f(*keys):
        for k in keys:
            v = d.get(k)
            try:
                f = float(v)
                if f > 0:
                    return f
            except (TypeError, ValueError):
                pass
        return 0.0

    price = _f("lastPrice", "last", "price", "indexPrice")
    high  = _f("high24h", "highPrice", "high", "h")
    low   = _f("low24h",  "lowPrice",  "low",  "l")

    # Primary: riseFallRate — guaranteed decimal fraction on MEXC futures
    rfr = d.get("riseFallRate")
    if rfr is not None and rfr != "" and rfr != 0:
        change = float(rfr) * 100
    else:
        # Secondary: other fields — detect format (>2 = already a %, else decimal)
        raw = float(
            d.get("priceChangePercent") or
            d.get("changeRate")         or
            d.get("rate")               or
            d.get("change24h")          or
            0
        )
        change = raw if abs(raw) > 2 else raw * 100

    # Fallback: compute from 24h open when all rate fields are missing/zero
    if change == 0 and price > 0:
        open24 = float(d.get("open24h", d.get("openPrice", d.get("indexPrice", 0))) or 0)
        if open24 > 0:
            change = round((price - open24) / open24 * 100, 2)

    return {
        "price":  round(price,  8),
        "change": round(change, 2),
        "high":   round(high,   8),
        "low":    round(low,    8),
    }


def get_ticker(symbol):
    """
    Fetch live ticker for a symbol.  Returns {price, change, high, low}.
    - Primary: MEXC /contract/ticker (batch or single)
    - Fallback for high/low: use Day1 candle high/low if ticker returns 0
    """
    t = None
    try:
        r = requests.get(f"{MEXC_BASE}/ticker", params={"symbol": symbol}, timeout=6)
        data = r.json()
        if data.get("success") and data.get("data"):
            d = data["data"]
            if isinstance(d, list): d = d[0]
            t = _parse_mexc_ticker(d)
            if t["price"] <= 0:
                t = None
    except:
        pass

    if t is None:
        return None

    # If high or low came back as 0, fill from today's Day1 candle
    if t["high"] <= 0 or t["low"] <= 0:
        try:
            day_candles = get_candles(symbol, "Day1", limit=2)
            if day_candles:
                today = day_candles[-1]
                if t["high"] <= 0:
                    t["high"] = round(float(today.get("high", 0)), 8)
                if t["low"] <= 0:
                    t["low"]  = round(float(today.get("low",  0)), 8)
        except:
            pass

    return t


def _fetch_all_tickers_batch():
    """
    Fetch ALL MEXC futures tickers in one request and update price_cache.
    One HTTP call covers all 30 watchlist pairs simultaneously.
    """
    try:
        r = requests.get(f"{MEXC_BASE}/ticker", timeout=8)
        data = r.json()
        if not (data.get("success") and data.get("data")):
            return
        items = data["data"]
        if not isinstance(items, list):
            items = [items]
        top_set = set(TOP_PAIRS)
        new_entries = {}
        for d in items:
            sym = d.get("symbol", "")
            if sym not in top_set:
                continue
            try:
                t = _parse_mexc_ticker(d)
                if t["price"] > 0:
                    new_entries[sym] = t
            except:
                pass
        if new_entries:
            with price_cache_lock:
                price_cache.update(new_entries)
        # Individual fallback: fetch any watchlist pairs that got no price from batch
        missing = [s for s in top_set if s not in new_entries]
        for sym in missing:
            try:
                r2 = requests.get(f"{MEXC_BASE}/ticker", params={"symbol": sym}, timeout=6)
                d2 = r2.json()
                if d2.get("success") and d2.get("data"):
                    item = d2["data"]
                    if isinstance(item, list): item = item[0]
                    t2 = _parse_mexc_ticker(item)
                    if t2["price"] > 0:
                        with price_cache_lock:
                            price_cache[sym] = t2
            except:
                pass
    except:
        pass


def price_cache_loop():
    """Background thread: keep price_cache fresh — one batch request per second."""
    while True:
        try:
            _fetch_all_tickers_batch()
        except:
            pass
        time.sleep(1)

# ════════ MARKET STRUCTURE ═══════════════════════════════════════════

def _d(direction):
    """Normalize BUY→BULLISH, SELL→BEARISH for helper functions."""
    if direction == "BUY": return "BULLISH"
    if direction == "SELL": return "BEARISH"
    return direction  # already BULLISH/BEARISH

def find_swings(candles, n=2):
    highs=[c["high"] for c in candles]; lows=[c["low"] for c in candles]
    sh=[]; sl=[]
    for i in range(n, len(candles)-n):
        if all(highs[i]>=highs[i-j] and highs[i]>=highs[i+j] for j in range(1,n+1)):
            sh.append((i,highs[i]))
        if all(lows[i]<=lows[i-j] and lows[i]<=lows[i+j] for j in range(1,n+1)):
            sl.append((i,lows[i]))
    return sh, sl

def detect_trend(candles, lookback=80):
    c = candles[-lookback:] if len(candles)>=lookback else candles
    if len(c)<20: return "NEUTRAL",[],[]
    sh,sl = find_swings(c,n=2)
    if len(sh)>=2 and len(sl)>=2:
        hh=sh[-1][1]>sh[-2][1]; hl=sl[-1][1]>sl[-2][1]
        lh=sh[-1][1]<sh[-2][1]; ll=sl[-1][1]<sl[-2][1]
        if hh and hl: return "BULLISH",sh,sl
        if lh and ll: return "BEARISH",sh,sl
    closes=[c["close"] for c in c[-20:]]
    a1=sum(closes[:10])/10; a2=sum(closes[10:])/10
    if a2>a1*1.003: return "BULLISH",sh,sl
    if a2<a1*0.997: return "BEARISH",sh,sl
    return "NEUTRAL",sh,sl

def is_continuous(sh, sl, direction, min_pts=2):
    direction = _d(direction)
    if direction=="BULLISH":
        if len(sh)<min_pts and len(sl)<min_pts: return False
        highs_ok = len(sh)>=min_pts and all(sh[i][1]>sh[i-1][1] for i in range(1,len(sh)))
        lows_ok  = len(sl)>=min_pts and all(sl[i][1]>sl[i-1][1] for i in range(1,len(sl)))
        return highs_ok or lows_ok
    else:
        if len(sh)<min_pts and len(sl)<min_pts: return False
        highs_ok = len(sh)>=min_pts and all(sh[i][1]<sh[i-1][1] for i in range(1,len(sh)))
        lows_ok  = len(sl)>=min_pts and all(sl[i][1]<sl[i-1][1] for i in range(1,len(sl)))
        return highs_ok or lows_ok

# ════════ ORDER BLOCKS ════════════════════════════════════════════════

def find_obs(candles, direction):
    direction = _d(direction)
    obs = []
    if len(candles)<5: return obs
    for i in range(2, len(candles)-2):
        c=candles[i]; cn=candles[i+1]
        if direction=="BULLISH":
            if c["close"]<c["open"] and cn["close"]>c["high"] and cn["close"]>cn["open"]:
                obs.append({"top":c["open"],"bot":c["close"],"high":c["high"],"low":c["low"],"idx":i,"time":c["time"],"type":"BULLISH_OB"})
        else:
            if c["close"]>c["open"] and cn["close"]<c["low"] and cn["close"]<cn["open"]:
                obs.append({"top":c["close"],"bot":c["open"],"high":c["high"],"low":c["low"],"idx":i,"time":c["time"],"type":"BEARISH_OB"})
    return sorted(obs, key=lambda x:x["idx"], reverse=True)

def price_reacted_from_zone(candles, zone_top, zone_bot, direction, lookback=20):
    """
    Check if price RECENTLY reacted from the zone (even if not tapping now).
    Valid if:
    - Price touched the zone within the last `lookback` candles
    - And bounced significantly in the correct direction
    - And no BOS in the opposite direction has occurred since the reaction
    This allows CRTs formed AFTER a zone reaction to still be valid.
    """
    direction = _d(direction)
    if not candles or len(candles) < 5: return False
    recent = candles[-lookback:]
    zone_mid = (zone_top + zone_bot) / 2

    reaction_idx = None
    for i, c in enumerate(recent):
        if direction == "BULLISH":
            # Price tapped into the bullish zone (discount)
            if c["low"] <= zone_top and c["high"] >= zone_bot:
                reaction_idx = i
        else:
            # Price tapped into the bearish zone (premium)
            if c["high"] >= zone_bot and c["low"] <= zone_top:
                reaction_idx = i

    if reaction_idx is None:
        return False   # Never touched the zone recently

    # Check price moved significantly away from zone after reaction
    after = recent[reaction_idx:]
    if not after: return False
    if direction == "BULLISH":
        max_after = max(c["high"] for c in after)
        moved = max_after > zone_top * 1.002  # moved at least 0.2% above zone
    else:
        min_after = min(c["low"] for c in after)
        moved = min_after < zone_bot * 0.998

    return moved


def has_bos_since_reaction(candles, direction, reaction_point, lookback=15):
    """
    Check if a Break of Structure in the OPPOSITE direction has occurred
    since the zone reaction. If yes, the zone setup is invalidated.
    """
    direction = _d(direction)
    if not candles or len(candles) < 4: return False
    recent = candles[-lookback:]
    if direction == "BULLISH":
        # Look for a LL (lower low) printed after the reaction — BOS against bull trend
        lows = [c["low"] for c in recent]
        for i in range(1, len(lows)):
            if lows[i] < lows[i-1] * 0.995:  # significant lower low
                return True
    else:
        highs = [c["high"] for c in recent]
        for i in range(1, len(highs)):
            if highs[i] > highs[i-1] * 1.005:  # significant higher high
                return True
    return False


def ob_at_key_level(ob, direction, sh, sl, tol=0.025):
    direction = _d(direction)
    if direction=="BULLISH" and sl:
        for _, last_hl in sl[-2:]:
            if ob["bot"] <= last_hl*(1+tol) and ob["top"] >= last_hl*(1-tol):
                return True
    elif direction=="BEARISH" and sh:
        for _, last_lh in sh[-2:]:
            if ob["top"] >= last_lh*(1-tol) and ob["bot"] <= last_lh*(1+tol):
                return True
    return False

def ob_in_pd_zone(ob, candles, direction):
    direction = _d(direction)
    if not candles or len(candles)<20: return False,"UNKNOWN"
    recent = candles[-50:]
    swing_high = max(c["high"] for c in recent)
    swing_low  = min(c["low"]  for c in recent)
    full_range = swing_high - swing_low
    if full_range<=0: return False,"UNKNOWN"
    eq = swing_low + full_range*0.5
    ob_mid = (ob["top"]+ob["bot"])/2
    if direction=="BULLISH":
        return ob_mid<eq, ("DISCOUNT" if ob_mid<eq else "PREMIUM")
    else:
        return ob_mid>eq, ("PREMIUM" if ob_mid>eq else "DISCOUNT")

def fvg_in_pd_zone(candles, direction):
    direction = _d(direction)
    if not candles or len(candles)<20: return False, None, None, "UNKNOWN"
    recent = candles[-50:]
    swing_high = max(c["high"] for c in recent)
    swing_low  = min(c["low"]  for c in recent)
    full_range = swing_high - swing_low
    if full_range <= 0: return False, None, None, "UNKNOWN"
    eq = swing_low + full_range * 0.5
    for i in range(len(candles)-3, max(0, len(candles)-40), -1):
        c1 = candles[i]
        c3 = candles[i+2]
        if direction == "BULLISH":
            if c3["low"] > c1["high"]:
                fvg_bot = c1["high"]; fvg_top = c3["low"]
                fvg_mid = (fvg_top + fvg_bot) / 2
                if fvg_mid < eq:
                    return True, fvg_top, fvg_bot, "DISCOUNT (FVG)"
        else:
            if c3["high"] < c1["low"]:
                fvg_top = c1["low"]; fvg_bot = c3["high"]
                fvg_mid = (fvg_top + fvg_bot) / 2
                if fvg_mid > eq:
                    return True, fvg_top, fvg_bot, "PREMIUM (FVG)"
    return False, None, None, "UNKNOWN"

def find_breaker_block(candles, direction):
    direction = _d(direction)
    bbs = []
    if len(candles) < 10: return bbs
    obs = find_obs(candles, "BULLISH" if direction=="BUY" else "BEARISH")
    for ob in obs:
        idx = ob["idx"]
        after = candles[idx+1:]
        if direction == "BULLISH":
            broken = any(c["close"] > ob["top"] for c in after[:8])
            if broken:
                bbs.append({**ob, "type": "BB", "kl_type": "Breaker Block"})
        else:
            broken = any(c["close"] < ob["bot"] for c in after[:8])
            if broken:
                bbs.append({**ob, "type": "BB", "kl_type": "Breaker Block"})
    return bbs

def find_rejection_block(candles, direction):
    direction = _d(direction)
    rjbs = []
    if len(candles) < 5: return rjbs
    for i in range(2, len(candles)-2):
        c = candles[i]
        body = abs(c["close"] - c["open"])
        total = c["high"] - c["low"]
        if total <= 0: continue
        wick_ratio = (total - body) / total
        if direction == "BULLISH":
            lower_wick = c["close"] - c["low"] if c["close"] > c["open"] else c["open"] - c["low"]
            if wick_ratio > 0.65 and lower_wick > body * 2:
                rjbs.append({"top": c["high"], "bot": c["low"],
                              "high": c["high"], "low": c["low"],
                              "idx": i, "time": c["time"],
                              "type": "RJB", "kl_type": "Rejection Block"})
        else:
            upper_wick = c["high"] - c["close"] if c["close"] < c["open"] else c["high"] - c["open"]
            if wick_ratio > 0.65 and upper_wick > body * 2:
                rjbs.append({"top": c["high"], "bot": c["low"],
                              "high": c["high"], "low": c["low"],
                              "idx": i, "time": c["time"],
                              "type": "RJB", "kl_type": "Rejection Block"})
    return sorted(rjbs, key=lambda x: x["idx"], reverse=True)

def is_fvg_unmitigated(fvg_top, fvg_bot, fvg_idx, candles):
    """True if no candle after the FVG has closed back inside the FVG zone."""
    for c in candles[fvg_idx + 3:]:
        if c["low"] <= fvg_top and c["high"] >= fvg_bot:
            return False   # price entered the gap — mitigated
    return True

def ob_is_at_extreme(ob, all_obs, direction):
    """True if no other OB is more extreme (lower for BUY, higher for SELL)."""
    direction = _d(direction)
    if direction == "BULLISH":
        return all(ob["bot"] <= other["bot"] for other in all_obs if other is not ob)
    else:
        return all(ob["top"] >= other["top"] for other in all_obs if other is not ob)

def find_all_key_levels(candles, direction):
    """Only unmitigated FVGs and the single most-extreme OB are valid key levels."""
    direction = _d(direction)
    zones = []
    obs_dir = "BULLISH" if direction == "BUY" else "BEARISH"
    all_obs = find_obs(candles, obs_dir)

    # Add the most-extreme OB only (no other OB below it for BUY / above it for SELL)
    extreme_obs = [ob for ob in all_obs if ob_is_at_extreme(ob, all_obs, direction)]
    for ob in extreme_obs[:1]:               # take only the single best extreme OB
        zones.append({**ob, "kl_type": "OB"})

    # Add unmitigated FVGs only
    for i in range(max(0, len(candles) - 60), len(candles) - 3):
        c1 = candles[i]; c3 = candles[i + 2]
        if direction == "BULLISH" and c3["low"] > c1["high"]:
            fvg_top = c3["low"]; fvg_bot = c1["high"]
            if is_fvg_unmitigated(fvg_top, fvg_bot, i, candles):
                zones.append({"top": fvg_top, "bot": fvg_bot,
                              "high": fvg_top, "low": fvg_bot,
                              "idx": i, "time": c1["time"], "kl_type": "FVG"})
        elif direction == "BEARISH" and c3["high"] < c1["low"]:
            fvg_top = c1["low"]; fvg_bot = c3["high"]
            if is_fvg_unmitigated(fvg_top, fvg_bot, i, candles):
                zones.append({"top": fvg_top, "bot": fvg_bot,
                              "high": fvg_top, "low": fvg_bot,
                              "idx": i, "time": c1["time"], "kl_type": "IFVG"})

    return sorted(zones, key=lambda x: x.get("idx", 0), reverse=True)

def crt_inside_zone(crt, zone_top, zone_bot):
    crh = crt["crh"]; crl = crt["crl"]
    return crl <= zone_top and crh >= zone_bot

def prev_obs_respected(obs, candles, direction, min_resp=1):
    direction = _d(direction)
    if len(obs)<2: return False
    respected=0
    for ob in obs[1:]:
        after = candles[ob["idx"]+1 : ob["idx"]+10]
        if not after: continue
        if direction=="BULLISH":
            tap    = any(c["low"]<=ob["top"] for c in after[:4])
            react  = any(c["close"]>ob["top"]*1.002 for c in after)
            if tap and react: respected+=1
        else:
            tap    = any(c["high"]>=ob["bot"] for c in after[:4])
            react  = any(c["close"]<ob["bot"]*0.998 for c in after)
            if tap and react: respected+=1
    return respected>=min_resp

def liq_sweep_before_ob(candles, ob, direction):
    direction = _d(direction)
    idx = ob["idx"]
    lb  = candles[max(0,idx-20):idx]
    if not lb: return False
    if direction=="BULLISH":
        prev_low = min(c["low"] for c in lb[:-1]) if len(lb)>1 else lb[0]["low"]
        return any(c["low"]<prev_low for c in lb[-8:])
    else:
        prev_high = max(c["high"] for c in lb[:-1]) if len(lb)>1 else lb[0]["high"]
        return any(c["high"]>prev_high for c in lb[-8:])

def price_tapping_ob(candles, ob, direction):
    direction = _d(direction)
    recent = candles[-14:]
    if direction=="BULLISH":
        return any(c["low"]<=ob["top"] and c["high"]>=ob["bot"] for c in recent)
    else:
        return any(c["high"]>=ob["bot"] and c["low"]<=ob["top"] for c in recent)

# ════════ MAD MAN DETECTION ════════════════════════════════════════════

def detect_crt(candles, direction, ob=None):
    direction = _d(direction)
    found = []
    if len(candles)<5: return found
    limit = min(20, len(candles)-2)
    for offset in range(1, limit):
        i3=len(candles)-1-offset; i2=i3-1; i1=i2-1
        if i1<0: break
        c1=candles[i1]; c2=candles[i2]; c3=candles[i3]
        crh=c1["high"]; crl=c1["low"]; cr_range=crh-crl
        if cr_range<=0: continue
        if ob:
            if not (c1["low"]<=ob["top"] and c1["high"]>=ob["bot"]):
                continue
        if direction=="BULLISH":
            # Wick sweeps BELOW CRL; entire body (open AND close) stays inside range
            swept          = c2["low"] < crl
            body_open_in   = crl <= c2["open"]  <= crh
            body_close_in  = crl <= c2["close"] <= crh
            wick_ok        = (c2["close"]-c2["low"]) > cr_range*0.03
            c3_bull        = c3["close"] > c3["open"]
            if swept and body_open_in and body_close_in and wick_ok:
                entry=c2["close"]; sl=c2["low"]; tp=crh
                risk=abs(entry-sl); reward=abs(tp-entry)
                rr=round(reward/risk,2) if risk>0 else 0
                if rr>=3.0:
                    found.append({"direction":"BUY","c1":c1,"c2":c2,"c3":c3,
                                  "crh":crh,"crl":crl,"entry":round(entry,8),
                                  "sl":round(sl,8),"tp":round(tp,8),"rr":rr,
                                  "sweep":round(crl-c2["low"],8),"c3_confirms":c3_bull})
        else:
            # Wick sweeps ABOVE CRH; entire body (open AND close) stays inside range
            swept          = c2["high"] > crh
            body_open_in   = crl <= c2["open"]  <= crh
            body_close_in  = crl <= c2["close"] <= crh
            wick_ok        = (c2["high"]-c2["close"]) > cr_range*0.03
            c3_bear        = c3["close"] < c3["open"]
            if swept and body_open_in and body_close_in and wick_ok:
                entry=c2["close"]; sl=c2["high"]; tp=crl
                risk=abs(sl-entry); reward=abs(entry-tp)
                rr=round(reward/risk,2) if risk>0 else 0
                if rr>=3.0:
                    found.append({"direction":"SELL","c1":c1,"c2":c2,"c3":c3,
                                  "crh":crh,"crl":crl,"entry":round(entry,8),
                                  "sl":round(sl,8),"tp":round(tp,8),"rr":rr,
                                  "sweep":round(c2["high"]-crh,8),"c3_confirms":c3_bear})
    return found

# ════════ TBS ════════════════════════════════════════════════════════

def check_tbs(symbol, direction, crl, crh, crt_tf="Hour4"):
    """
    TBS = Turtle Body Soup — STRICT body-only rules:

      SELL setup: candle OPENS inside the CRT range (crl <= open <= crh)
                  AND CLOSES *above* CRH with its BODY  (close > crh)
                  → the body has fully broken out above the range = valid TBS
                  Invalid: candle that only wicks above CRH but body stays inside
                  Entry  = TBS candle OPEN  (inside the range — limit entry)
                  SL     = TBS candle CLOSE (body close above CRH = sweep extreme)
                  TP     = CRL (opposite CRT level)

      BUY setup:  candle OPENS inside the CRT range (crl <= open <= crh)
                  AND CLOSES *below* CRL with its BODY  (close < crl)
                  → the body has fully broken below the range = valid TBS
                  Invalid: candle that only wicks below CRL but body stays inside
                  Entry  = TBS candle OPEN  (inside the range — limit entry)
                  SL     = TBS candle CLOSE (body close below CRL = sweep extreme)
                  TP     = CRH (opposite CRT level)

    We search the last 80 candles on each LTF mapped to crt_tf,
    returning the MOST RECENT valid TBS.
    """
    direction = _d(direction)
    tfs_to_check = TBS_TF_MAP.get(crt_tf, TBS_TFS)
    cr_range = crh - crl
    if cr_range <= 0:
        return False, None, None, None

    for tf in tfs_to_check:
        candles = get_candles(symbol, tf, limit=150)
        if not candles or len(candles) < 4:
            continue
        recent = candles[-80:]

        # Walk newest → oldest to return the MOST RECENT valid TBS
        for i in range(len(recent) - 1, -1, -1):
            c          = recent[i]
            open_px    = c["open"]
            close_px   = c["close"]
            candle_rng = c["high"] - c["low"]
            if candle_rng <= 0:
                continue
            body_size  = abs(close_px - open_px)
            # Body must be meaningful — at least 20% of the candle's total range
            # This enforces "body soup not wick soup"
            if body_size < candle_rng * 0.20:
                continue

            if direction == "SELL":
                # Open must be INSIDE the CRT range
                open_inside = crl <= open_px <= crh
                # Body (close) must break ABOVE CRH — full body outside, not a wick
                body_breaks_out = close_px > crh
                if open_inside and body_breaks_out:
                    tbs_entry = round(open_px,  8)   # entry at TBS open (inside range)
                    tbs_sl    = round(close_px, 8)   # SL at TBS close (body above CRH)
                    return True, tf, tbs_entry, tbs_sl

            else:  # BUY
                # Open must be INSIDE the CRT range
                open_inside = crl <= open_px <= crh
                # Body (close) must break BELOW CRL — full body outside, not a wick
                body_breaks_out = close_px < crl
                if open_inside and body_breaks_out:
                    tbs_entry = round(open_px,  8)   # entry at TBS open (inside range)
                    tbs_sl    = round(close_px, 8)   # SL at TBS close (body below CRL)
                    return True, tf, tbs_entry, tbs_sl

    return False, None, None, None

# ════════ CHOCH ═══════════════════════════════════════════════════════

def check_choch(symbol, tf, direction):
    direction = _d(direction)
    candles = get_candles(symbol, tf, limit=60)
    if not candles or len(candles)<5: return False, None
    recent = candles[-35:]
    sh=[]; sl=[]
    for i in range(2, len(recent)-2):
        c=recent[i]
        if (c["high"]>recent[i-1]["high"] and c["high"]>recent[i-2]["high"] and
            c["high"]>recent[i+1]["high"] and c["high"]>=recent[i+2]["high"]):
            sh.append((i,c["high"]))
        if (c["low"]<recent[i-1]["low"] and c["low"]<recent[i-2]["low"] and
            c["low"]<recent[i+1]["low"] and c["low"]<=recent[i+2]["low"]):
            sl.append((i,c["low"]))
    if direction=="BUY":
        if not sh:
            for i in range(len(recent)-1,0,-1):
                c=recent[i]; p=recent[i-1]
                if c["close"]>p["high"] and c["close"]>c["open"]:
                    return True, round(p["high"],8)
            return False,None
        last_idx,last_val = sh[-1]
        for i in range(last_idx+1,len(recent)):
            c=recent[i]
            if c["close"]>last_val and c["close"]>c["open"]:
                return True, round(last_val,8)
    else:
        if not sl:
            for i in range(len(recent)-1,0,-1):
                c=recent[i]; p=recent[i-1]
                if c["close"]<p["low"] and c["close"]<c["open"]:
                    return True, round(p["low"],8)
            return False,None
        last_idx,last_val = sl[-1]
        for i in range(last_idx+1,len(recent)):
            c=recent[i]
            if c["close"]<last_val and c["close"]<c["open"]:
                return True, round(last_val,8)
    return False,None

# ════════ FVG + IFVG ══════════════════════════════════════════════════

def find_fvg(symbol, tf, direction):
    """
    M1 scanner FVG finder.
    BUY:  bullish FVG (c3.low > c1.high) — entry at fvg_top (c3.low), first retracement touch
    SELL: bearish FVG (c3.high < c1.low) — entry at fvg_bot (c3.high), first retracement touch
    FVG is fresh (unmitigated) when no later candle CLOSES through the far edge of the gap.
    """
    direction = _d(direction)
    candles = get_candles(symbol, tf, limit=120)
    if not candles or len(candles)<5: return False,None,None,None,None
    last_price = candles[-1]["close"]
    fresh=[]; ifvg=[]
    for i in range(len(candles)-3):
        c1=candles[i]; c3=candles[i+2]
        if direction=="BUY":
            if c3["low"]>c1["high"]:
                zbot=c1["high"]; ztop=c3["low"]
                # Discard only truly stale gaps (>15% away from current price)
                if ztop < last_price * 0.85 or ztop > last_price * 1.15: continue
                # Mitigated if price CLOSED below the bottom of the gap
                mit=any(candles[j]["close"]<zbot for j in range(i+3,len(candles)))
                if not mit:
                    fresh.append({"type":"FVG","entry":round(ztop,8),
                                  "zone_top":round(ztop,8),"zone_bot":round(zbot,8),"idx":i})
                else:
                    ifvg.append({"type":"IFVG","entry":round(ztop,8),
                                 "zone_top":round(ztop,8),"zone_bot":round(zbot,8),"idx":i})
        else:
            if c3["high"]<c1["low"]:
                ztop=c1["low"]; zbot=c3["high"]
                # Discard only truly stale gaps (>15% away from current price)
                if zbot > last_price * 1.15 or zbot < last_price * 0.85: continue
                # Mitigated if price CLOSED above the top of the gap
                mit=any(candles[j]["close"]>ztop for j in range(i+3,len(candles)))
                if not mit:
                    fresh.append({"type":"FVG","entry":round(zbot,8),
                                  "zone_top":round(ztop,8),"zone_bot":round(zbot,8),"idx":i})
                else:
                    ifvg.append({"type":"IFVG","entry":round(zbot,8),
                                 "zone_top":round(ztop,8),"zone_bot":round(zbot,8),"idx":i})
    if fresh:
        b=max(fresh,key=lambda x:x["idx"])
        return True,b["type"],b["entry"],b["zone_top"],b["zone_bot"]
    if ifvg:
        b=max(ifvg,key=lambda x:x["idx"])
        return True,b["type"],b["entry"],b["zone_top"],b["zone_bot"]
    return False,None,None,None,None

def check_choch_multi(symbol, tfs, direction):
    direction = _d(direction)
    for tf in tfs:
        found, level = check_choch(symbol, tf, direction)
        if found and level:
            return True, level
    return False, None

def find_fvg_multi(symbol, tfs, direction):
    direction = _d(direction)
    for tf in tfs:
        found, fvg_type, fvg_entry, fvg_top, fvg_bot = find_fvg(symbol, tf, direction)
        if found and fvg_entry:
            return found, fvg_type, fvg_entry, fvg_top, fvg_bot
    return False, None, None, None, None

# ════════ SIGNAL SCORING ═════════════════════════════════════════════

def score_signal(crt, trend, liq_swept, tbs_found, tbs_tf,
                 fvg_found, fvg_type, choch_found, continuous,
                 is_1d, ob=None, at_key=False, ob_resp=False, ob_zone=None,
                 sh=None, sl=None, direction=None):
    score=0; details=[]
    if direction is None: direction=crt["direction"]
    if sh is None: sh=[]
    if sl is None: sl=[]
    rr=crt["rr"]

    if continuous:
        sh_ok = len(sh)>=2 and ((direction=="BUY" and all(sh[i][1]>sh[i-1][1] for i in range(1,len(sh)))) or (direction=="SELL" and all(sh[i][1]<sh[i-1][1] for i in range(1,len(sh)))))
        sl_ok = len(sl)>=2 and ((direction=="BUY" and all(sl[i][1]>sl[i-1][1] for i in range(1,len(sl)))) or (direction=="SELL" and all(sl[i][1]<sl[i-1][1] for i in range(1,len(sl)))))
        if sh_ok and sl_ok:
            score+=20; details.append("✅ Full HH/HL or LH/LL structure (+20)")
        else:
            score+=12; details.append("⚠️ Partial structure alignment (+12)")
    else:
        details.append("⚠️ Weak structure (+0)")

    if (direction=="BUY" and trend=="BULLISH") or (direction=="SELL" and trend=="BEARISH"):
        score+=10; details.append("✅ Trend aligned (+10)")
    else:
        details.append("❌ Counter-trend (+0)")

    if tbs_found:
        score+=20; details.append(f"✅ TBS body close on {tbs_tf} (+20)")
    else:
        details.append("❌ No TBS — gate failed (+0)")

    if liq_swept:
        score+=15; details.append("✅ Liquidity sweep confirmed (+15)")
    else:
        details.append("⚠️ No liquidity sweep (+0)")

    if rr>=5:   score+=10; details.append(f"✅ Exceptional {rr}R (+10)")
    elif rr>=4: score+=8;  details.append(f"✅ Strong {rr}R (+8)")
    elif rr>=3: score+=6;  details.append(f"⚠️ Minimum {rr}R (+6)")

    if choch_found:
        score+=10; details.append("✅ CHOCH/MSS confirmed (+10)")
    else:
        details.append("⚠️ No CHOCH (+0)")

    if fvg_found:
        score+=10; details.append(f"✅ {fvg_type} entry tip found (+10)")
    else:
        details.append("⚠️ No FVG/IFVG (+0)")

    if not is_1d:
        kl = str(ob_zone) if ob_zone else ""
        if "BB" in kl:    score+=10; details.append("✅ Breaker Block (+10)")
        elif "RJB" in kl: score+=9;  details.append("✅ Rejection Block (+9)")
        elif "OB" in kl:  score+=8;  details.append("✅ Order Block (+8)")
        elif "FVG" in kl: score+=7;  details.append("✅ FVG (+7)")
        elif "IFVG" in kl: score+=6; details.append("✅ IFVG (+6)")
        has_pd = "DISCOUNT" in kl or "PREMIUM" in kl
        if has_pd:
            score+=8; details.append(f"⭐ Premium/Discount zone — A+ eligible (+8)")
        if at_key:
            score+=5; details.append("✅ Key level at swing point (+5)")
        if ob_resp:
            score+=4; details.append("✅ Previous key levels respected (+4)")

    if tbs_found and fvg_found and choch_found:
        score=min(score+8,100); details.append("✅ Triple confluence: TBS+FVG+CHOCH (+8)")

    if crt.get("c3_confirms"):
        score=min(score+5,100); details.append("✅ C3 confirms (+5)")

    has_pd = ob_zone and ("DISCOUNT" in str(ob_zone) or "PREMIUM" in str(ob_zone))
    if tbs_found and score >= 72 and (has_pd or is_1d):
        grade = "A+"
    elif tbs_found and score >= 58:
        grade = "A"
    elif score >= 48:
        grade = "A"
    elif score >= 45:
        grade = "C"
    elif score >= 35:
        grade = "B"
    else:
        grade = "D"
    return min(score,100), grade, details

# ════════ TELEGRAM ════════════════════════════════════════════════════

def send_telegram(msg, kind="signal"):
    tok = scan_settings.get("tg_bot_token","")
    cid = scan_settings.get("tg_chat_id","")
    if not tok or "PASTE" in tok: return False
    if kind == "signal" and not scan_settings.get("tg_signals", True): return False
    if kind == "trade"  and not scan_settings.get("tg_trades",  True): return False
    try:
        r=requests.post(f"https://api.telegram.org/bot{tok}/sendMessage",
                        json={"chat_id":cid,"text":msg,"parse_mode":"HTML"},timeout=10)
        return r.status_code==200
    except: return False

def fmt_tg(sig):
    e    = "🟢" if sig["direction"]=="BUY" else "🔴"
    bars = "█"*(sig["score"]//10)+"░"*(10-sig["score"]//10)
    tbs  = f"✅ {sig.get('tbs_tf','–')}" if sig.get("tbs_found") else "❌"
    fvg  = f"✅ {sig.get('fvg_type','–')}" if sig.get("fvg_found") else "⚠️ None"
    choch= "✅" if sig.get("choch_found") else "⚠️"
    tf_label = {"Day1":"1D","Hour4":"4H","Hour3":"3H","Hour2":"2H","Min60":"1H"}.get(sig.get("tf",""),"–")
    ob_info  = f"\n<b>OB TF:</b>      {sig.get('ob_tf','–')} | {sig.get('ob_zone','–')}" if sig.get("ob_tf") and sig.get("ob_tf") not in ("N/A","N/A (1D)","–") else ""
    return (
        f"{e} <b>MAD MAN MODEL #1 — {sig['direction']}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"<b>Pair:</b>       {sig['symbol']}\n"
        f"<b>Mad Man TF:</b>     {tf_label}{ob_info}\n"
        f"<b>Trend:</b>      {sig['trend']}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"<b>🎯 Entry:</b>    {sig['entry']}\n"
        f"<b>   Type:</b>     Model #1 (TBS Candle Open)\n"
        f"<b>   TBS TF:</b>   {sig.get('tbs_tf','–')}\n"
        f"<b>🛑 SL:</b>       {sig['sl']} (Sweep Extreme)\n"
        f"<b>🎯 TP:</b>       {sig['tp']} ({'CRH' if sig['direction']=='BUY' else 'CRL'})\n"
        f"<b>📊 RR:</b>       {sig['rr']}R\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"<b>CRH:</b>        {sig['crh']}\n"
        f"<b>CRL:</b>        {sig['crl']}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"<b>Score:</b>      {sig['score']}/100 [{bars}] {sig['grade']}\n"
        f"<b>TBS:</b>        {tbs}\n"
        f"<b>FVG:</b>        {fvg} (confluence)\n"
        f"<b>CHOCH:</b>      {choch} (confluence)\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"<i>Mad Man Strategy Scanner • {sig['timestamp']}</i>"
    )

# ════════ LOGGER ══════════════════════════════════════════════════════

def log(msg):
    ts=datetime.now(LOCAL_TZ).strftime("%H:%M:%S")
    line=f"[{ts}] {msg}"; print(line, flush=True)
    with scan_lock: scan_state["log"].appendleft(line)

# ════════ SCAN PAIR ═══════════════════════════════════════════════════

def get_ltf_for_crt(crt_tf):
    return {
        "Day1":  "Min60",
        "Hour4": "Min15",
        "Hour3": "Min15",
        "Hour2": "Min10",
        "Min60": "Min5",
    }.get(crt_tf, "Min15")

# ── MANIPULATION MONITOR ─────────────────────────────────────────────
manip_monitor = {}
manip_lock    = threading.Lock()
MAX_MONITORED = 4

# ── M2 / M3 UNIFIED MONITOR ──────────────────────────────────────────
m2_monitor     = {}
m2_lock        = threading.Lock()
MAX_M2_MONITORED = 6

# ── M4 MONITOR ───────────────────────────────────────────────────────
m4_monitor     = {}
m4_lock        = threading.Lock()
MAX_M4_MONITORED = 6

recent_trades = deque(maxlen=10)

diag = {
    "no_candles":0, "neutral":0, "not_continuous":0,
    "no_obs":0, "not_at_key":0, "not_in_zone":0,
    "no_liq":0, "not_tapping":0, "no_crts":0,
    "no_tbs":0, "rr_low":0, "passed":0,
    "1d_no_crts":0, "1d_no_tbs":0, "1d_rr_low":0,
}

def _zone_was_tapped(candles, zone_top, zone_bot, direction, lookback=120):
    """
    Returns (tapped: bool, tap_idx: int) — the index of the FIRST candle
    that entered the HTF zone (wick OR body).

    SELL zone: any candle whose HIGH reached into the zone (>= zone_bot)
    BUY  zone: any candle whose LOW  reached into the zone (<= zone_top)

    We search the last `lookback` candles, oldest → newest, so we get
    the first (earliest) tap, which anchors when CRTs become valid.
    """
    recent = candles[-lookback:]
    for i, c in enumerate(recent):
        if direction == "SELL" and c["high"] >= zone_bot:
            return True, i
        if direction == "BUY"  and c["low"]  <= zone_top:
            return True, i
    return False, -1


def _price_still_inside_htf_range(candles, zone_top, zone_bot, direction):
    """
    Price is 'still inside the HTF range' when it has NOT closed decisively
    THROUGH the far edge of the zone in the wrong direction.

    SELL zone: price must not have CLOSED above zone_top (full breakout up)
    BUY  zone: price must not have CLOSED below zone_bot (full breakout down)

    We check the last 30 candles; a single close beyond = invalidated.
    """
    recent = candles[-30:]
    for c in recent:
        if direction == "SELL" and c["close"] > zone_top * 1.002:
            return False   # closed above the zone — bullish breakout, bearish setup invalid
        if direction == "BUY"  and c["close"] < zone_bot * 0.998:
            return False   # closed below the zone — bearish breakout, bullish setup invalid
    return True


def _build_m1_signal(symbol, direction, trend, crt, tbs_tf, tbs_entry, tbs_sl,
                     crt_tf, ob_tf, zone_name, zone_type, zone_top, zone_bot,
                     matched_ob, at_key, ob_resp, continuous, sh, sl,
                     liq_swept=False, is_1d=False):
    """
    Build a complete Model #1 signal dict from confirmed TBS + CRT data.
    Entry  = TBS candle OPEN  (inside CRT range)
    SL     = TBS candle CLOSE (body that closed outside CRT range)
    TP     = opposite CRT level: SELL → CRL,  BUY → CRH
    """
    entry  = tbs_entry
    sl_p   = tbs_sl
    tp_p   = round(crt["crl"], 8) if direction == "SELL" else round(crt["crh"], 8)

    risk   = abs(entry - sl_p)
    reward = abs(tp_p - entry)
    rr     = round(reward / risk, 2) if risk > 0 else 0
    if rr < 2.0:
        return None   # caller checks for None

    ltf_map = {"Hour4":["Min15","Min10"],"Hour3":["Min15","Min10"],
               "Hour2":["Min10","Min5"], "Min60":["Min5"],
               "Day1": ["Min60","Min30"]}
    ltfs = ltf_map.get(crt_tf, ["Min15"])

    choch_found, choch_level = check_choch_multi(symbol, ltfs, direction)
    fvg_found, fvg_type, fvg_entry, fvg_top, fvg_bot = find_fvg_multi(symbol, ltfs, direction)

    crt_s = dict(crt); crt_s["entry"] = entry; crt_s["rr"] = rr
    score, grade, details = score_signal(
        crt_s, trend, liq_swept, True, tbs_tf,
        fvg_found, fvg_type, choch_found, continuous,
        is_1d=is_1d, ob=matched_ob, at_key=at_key,
        ob_resp=ob_resp, ob_zone=zone_name,
        sh=sh, sl=sl, direction=direction)

    return {
        "symbol":       symbol,
        "tf":           crt_tf,
        "ob_tf":        ob_tf,
        "ob_zone":      zone_name,
        "zone_type":    zone_type,
        "direction":    direction,
        "trend":        trend,
        "entry":        round(entry, 8),
        "entry_type":   "Model #1 (TBS Open)",
        "sl":           round(sl_p, 8),
        "tp":           round(tp_p, 8),
        "tp1":          round((entry + tp_p) / 2, 8),
        "tp2":          round(tp_p, 8),
        "rr":           rr,
        "crh":          crt["crh"],
        "crl":          crt["crl"],
        "ob_top":       zone_top or "–",
        "ob_bot":       zone_bot or "–",
        "score":        score,
        "grade":        grade,
        "details":      details,
        "tbs_found":    True,
        "tbs_tf":       tbs_tf,
        "tbs_entry":    tbs_entry,
        "tbs_sl":       tbs_sl,
        "fvg_found":    fvg_found,
        "fvg_type":     fvg_type  or "–",
        "fvg_entry":    fvg_entry or "–",
        "fvg_top":      fvg_top   or "–",
        "fvg_bot":      fvg_bot   or "–",
        "choch_found":  choch_found,
        "choch_level":  choch_level or "–",
        "liq_swept":    liq_swept,
        "ob_respected": ob_resp,
        "continuous":   continuous,
        "market_order": True,
        "timestamp":    datetime.now(LOCAL_TZ).strftime("%Y-%m-%d %H:%M UTC+1"),
    }


# ══════════════════════════════════════════════════════════════════════
# GEMINI VISUAL CHART ANALYSIS ENGINE
# Renders candle charts → sends to Gemini Flash Vision → reads structure
# like a trader. Optimised for minimal API calls and rate-limit safety.
#
# Resource strategy:
#   • Candles shared via _candle_cache — no duplicate MEXC fetches
#   • Result cached per symbol for VISUAL_CACHE_TTL seconds (4 min)
#   • Max 3 Gemini calls per pair (bias+CRT combined, lower TF confirm, TBS)
#   • Small PNG: 960×480 px, 80 DPI, 80 candles max — fast upload, less tokens
#   • Rate-limit backoff: 429 → wait 12s and skip pair (not crash)
#   • Skip pair entirely if 4H is NEUTRAL (no calls wasted)
# ══════════════════════════════════════════════════════════════════════

_TF_LABEL = {
    "Day1":"1D","Hour4":"4H","Hour3":"3H","Hour2":"2H","Min60":"1H",
    "Min45":"45m","Min15":"15m","Min5":"5m","Min4":"4m","Min3":"3m",
    "Min2":"2m","Min1":"1m",
}

# Gemini rate-limit state — shared across threads
_gemini_last_429    = 0.0
_gemini_429_lock    = threading.Lock()
GEMINI_BACKOFF_SECS = 12


def _render_chart_b64(candles, symbol, tf, n=80):
    """
    Render up to n candles as a compact PNG and return base64 string.
    Small image = fewer tokens + faster API round-trip.
    """
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.patches as mpatches
    import base64, io

    data = candles[-n:] if len(candles) > n else candles
    if not data:
        return None

    fig, ax = plt.subplots(figsize=(9.6, 4.8))
    fig.patch.set_facecolor("#0f1117")
    ax.set_facecolor("#0f1117")

    for i, c in enumerate(data):
        o, h, l, cl = c["open"], c["high"], c["low"], c["close"]
        col = "#26a69a" if cl >= o else "#ef5350"
        bh  = max(abs(cl - o), (h - l) * 0.008)
        ax.add_patch(mpatches.Rectangle(
            (i - 0.35, min(o, cl)), 0.7, bh, color=col, zorder=2))
        ax.plot([i, i], [l, h], color="#666", linewidth=0.7, zorder=1)

    p_min = min(c["low"]  for c in data)
    p_max = max(c["high"] for c in data)
    pad   = (p_max - p_min) * 0.06
    ax.set_xlim(-1, len(data))
    ax.set_ylim(p_min - pad, p_max + pad)

    step = max(1, len(data) // 8)
    xticks = list(range(0, len(data), step))
    xlabels = []
    for i in xticks:
        ts = data[i].get("time", 0)
        if ts > 1e9:
            dt = datetime.fromtimestamp(ts / 1000 if ts > 1e12 else ts, tz=timezone.utc)
            xlabels.append(dt.strftime("%m/%d %H:%M"))
        else:
            xlabels.append(str(i))
    ax.set_xticks(xticks)
    ax.set_xticklabels(xlabels, rotation=25, ha="right", fontsize=6, color="#aaa")
    ax.tick_params(axis="y", colors="#aaa", labelsize=6)
    for sp in ax.spines.values(): sp.set_edgecolor("#333")
    ax.grid(axis="y", color="#1e1e1e", linewidth=0.4)
    ax.set_title(f"{symbol} {_TF_LABEL.get(tf,tf)} ({len(data)} candles)",
                 color="#ddd", fontsize=8, pad=5)

    buf = io.BytesIO()
    plt.savefig(buf, format="png", bbox_inches="tight",
                facecolor=fig.get_facecolor(), dpi=80)
    plt.close(fig)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode()


def _gemini_vision(b64_image, prompt):
    """
    Single Gemini Flash Vision call.
    Returns parsed JSON dict, or None on failure / rate-limit.
    """
    global _gemini_last_429
    if not GEMINI_API_KEY:
        return None

    # Honour backoff window after a 429
    with _gemini_429_lock:
        since = time.time() - _gemini_last_429
        if since < GEMINI_BACKOFF_SECS:
            return None

    url = (f"https://generativelanguage.googleapis.com/v1beta/models/"
           f"gemini-1.5-flash:generateContent?key={GEMINI_API_KEY}")
    body = {
        "contents": [{
            "parts": [
                {"inline_data": {"mime_type": "image/png", "data": b64_image}},
                {"text": prompt}
            ]
        }],
        "generationConfig": {
            "temperature":    0.1,
            "maxOutputTokens": 600,
        }
    }
    try:
        r = requests.post(url, json=body, timeout=30)
        if r.status_code == 429:
            with _gemini_429_lock:
                _gemini_last_429 = time.time()
            log("⚠️ Gemini 429 — rate limit hit, backing off 12s")
            return None
        if r.status_code != 200:
            log(f"⚠️ Gemini error {r.status_code}: {r.text[:120]}")
            return None
        data = r.json()
        text = (data.get("candidates", [{}])[0]
                    .get("content", {})
                    .get("parts", [{}])[0]
                    .get("text", ""))
        text = text.strip()
        # Strip markdown fences
        if "```" in text:
            parts = text.split("```")
            text  = parts[1] if len(parts) > 1 else parts[0]
            if text.startswith("json"):
                text = text[4:]
        return json.loads(text.strip())
    except json.JSONDecodeError:
        return None
    except Exception as e:
        log(f"⚠️ Gemini call error: {e}")
        return None


def visual_analyse_pair(symbol):
    """
    Top-down visual analysis using Gemini Flash Vision.
    Max 3 API calls per pair. Returns signal dict or None.

    Call 1 — 4H chart: bias + CRT check (combined to save a call)
    Call 2 — Lower TF (2H/1H) only if needed for CRT confirmation
    Call 3 — LTF (15m/5m/3m/1m): TBS candle
    """
    if not GEMINI_API_KEY:
        return None

    # ── Visual result cache — skip if analysed recently ──────────────
    now_ts = time.time()
    with _visual_cache_lock:
        cached = _visual_cache.get(symbol)
        if cached and (now_ts - cached["ts"]) < VISUAL_CACHE_TTL:
            return cached["result"]   # None or signal — both cached

    def _cache(result):
        with _visual_cache_lock:
            _visual_cache[symbol] = {"ts": time.time(), "result": result}
        return result

    # ── Call 1: 4H bias + CRT (combined prompt = 1 call) ─────────────
    h4c = get_candles(symbol, "Hour4", limit=150)
    if not h4c or len(h4c) < 20:
        return _cache(None)

    b64 = _render_chart_b64(h4c, symbol, "Hour4", n=80)
    if not b64:
        return _cache(None)

    call1_prompt = """You are an expert SMC (Smart Money Concepts) trader.

Analyse this 4H candlestick chart and return ONLY a JSON object:
{
  "trend": "BULLISH" or "BEARISH" or "NEUTRAL",
  "bias_notes": "<one sentence confirming HH/HL or LH/LL structure>",
  "premium_top": <price — top of premium/supply zone>,
  "premium_bot": <price — bottom of premium/supply zone>,
  "discount_top": <price — top of discount/demand zone>,
  "discount_bot": <price — bottom of discount/demand zone>,
  "crt_found": true or false,
  "c1_high": <CRT candle high or null>,
  "c1_low": <CRT candle low or null>,
  "c2_confirmed": true or false,
  "key_level_tapped": true or false,
  "key_level_type": "OB" or "FVG" or "BB" or "RB" or null,
  "key_level_top": <price or null>,
  "key_level_bot": <price or null>,
  "zone_name": "<e.g. Bearish OB in 4H Premium>" or null
}

Rules:
- BULLISH = Higher Highs + Higher Lows. BEARISH = Lower Highs + Lower Lows.
- premium zone = upper 50% of last major swing range
- discount zone = lower 50% of last major swing range
- CRT: C1 sets range (CRH/CRL). C2 sweeps C1 high or low with a WICK only —
  C2 body must close BACK INSIDE C1 range. crt_found=true only if C2 confirmed.
- key_level_tapped: price is at or has recently reacted from an OB/FVG/BB/RB
  in the premium zone (if BEARISH) or discount zone (if BULLISH).
Return ONLY valid JSON. No explanation."""

    r1 = _gemini_vision(b64, call1_prompt)
    if not r1 or r1.get("trend") == "NEUTRAL":
        return _cache(None)

    trend_4h  = r1["trend"]
    direction = "BUY" if trend_4h == "BULLISH" else "SELL"
    log(f"👁 Gemini 4H {symbol}: {trend_4h} | {r1.get('bias_notes','')}")

    crt_tf_used = None
    crt_data    = None

    # CRT found on 4H itself?
    if r1.get("crt_found") and r1.get("c2_confirmed"):
        crt_tf_used = "Hour4"
        crt_data    = r1
        log(f"👁 CRT on 4H: {symbol} CRH={r1.get('c1_high')} CRL={r1.get('c1_low')}")
    else:
        # ── Call 2: step down to 2H or 1H for CRT ────────────────────
        pd_side    = "premium (supply)" if direction == "SELL" else "discount (demand)"
        sweep_side = "high" if direction == "SELL" else "low"

        for step_tf in ["Hour2", "Min60"]:
            sc = get_candles(symbol, step_tf, limit=100)
            if not sc or len(sc) < 20:
                continue
            b64s = _render_chart_b64(sc, symbol, step_tf, n=60)
            if not b64s:
                continue

            tfl = _TF_LABEL.get(step_tf, step_tf)
            call2_prompt = f"""You are an expert SMC trader. 4H bias is {trend_4h}.
Only flag {direction} setups — nothing against the 4H bias.

This is the {tfl} chart. Look for a CRT in the {pd_side} zone.
CRT: C1 sets range. C2 sweeps the {sweep_side} of C1 with a wick only —
C2 body must close BACK INSIDE C1 range.

Return ONLY JSON:
{{
  "crt_found": true or false,
  "c1_high": <price or null>,
  "c1_low": <price or null>,
  "c2_confirmed": true or false,
  "aligns_4h": true or false,
  "key_level_tapped": true or false,
  "key_level_type": "OB" or "FVG" or "BB" or "RB" or null,
  "key_level_top": <price or null>,
  "key_level_bot": <price or null>,
  "zone_name": "<description>" or null
}}
Return ONLY valid JSON. No explanation."""

            r2 = _gemini_vision(b64s, call2_prompt)
            if not r2:
                continue
            if not r2.get("aligns_4h", False):
                log(f"⏭ {symbol} {tfl} CRT doesn't align with 4H {trend_4h}")
                continue
            if r2.get("crt_found") and r2.get("c2_confirmed"):
                crt_tf_used = step_tf
                crt_data    = r2
                log(f"👁 CRT on {tfl}: {symbol} CRH={r2.get('c1_high')} CRL={r2.get('c1_low')}")
                break

    if not crt_data or not crt_tf_used:
        return _cache(None)

    crh = crt_data.get("c1_high") or crt_data.get("crh")
    crl = crt_data.get("c1_low")  or crt_data.get("crl")
    if not crh or not crl or float(crh) <= float(crl):
        return _cache(None)
    crh, crl = float(crh), float(crl)

    # ── Call 3: LTF TBS hunt ─────────────────────────────────────────
    tbs_tfs = TBS_TF_MAP.get(crt_tf_used, TBS_TFS)
    tbs_data    = None
    tbs_tf_used = None

    for tbs_tf in tbs_tfs:
        ltfc = get_candles(symbol, tbs_tf, limit=80)
        if not ltfc or len(ltfc) < 10:
            continue
        b64t = _render_chart_b64(ltfc, symbol, tbs_tf, n=60)
        if not b64t:
            continue

        tfl         = _TF_LABEL.get(tbs_tf, tbs_tf)
        close_side  = "above" if direction == "SELL" else "below"
        close_level = f"CRH ({crh})" if direction == "SELL" else f"CRL ({crl})"

        call3_prompt = f"""You are an expert SMC trader. Setup: {direction}, 4H bias {trend_4h}.
CRT range: CRH={crh}, CRL={crl}.

This is the {tfl} chart. Find the most recent TBS (Turtle Body Soup) candle:
1. Opens INSIDE the CRT range (between {crl} and {crh})
2. BODY closes {close_side} {close_level} — body must fully clear the level.
   A wick poking outside does NOT count. Body = open-to-close range.
3. Body is meaningful (not a doji).

Return ONLY JSON:
{{
  "tbs_found": true or false,
  "tbs_open": <price or null>,
  "tbs_close": <price or null>,
  "body_outside": true or false,
  "notes": "<brief>"
}}
Return ONLY valid JSON. No explanation."""

        r3 = _gemini_vision(b64t, call3_prompt)
        if not r3:
            continue
        if r3.get("tbs_found") and r3.get("body_outside"):
            tbs_data    = r3
            tbs_tf_used = tbs_tf
            log(f"🐢 TBS on {tfl}: {symbol} open={r3.get('tbs_open')} close={r3.get('tbs_close')}")
            break

    if not tbs_data or not tbs_tf_used:
        return _cache(None)

    entry = tbs_data.get("tbs_open")
    sl    = tbs_data.get("tbs_close")
    tp    = crl if direction == "SELL" else crh

    if not entry or not sl:
        return _cache(None)

    entry, sl = float(entry), float(sl)
    risk   = abs(entry - sl)
    reward = abs(tp - entry)
    rr     = round(reward / risk, 2) if risk > 0 else 0
    if rr < 2.0:
        log(f"⚠️ Gemini signal RR {rr}R too low — skipping {symbol}")
        return _cache(None)

    zone_name = (crt_data.get("zone_name") or
                 f"{trend_4h} zone · {_TF_LABEL.get(crt_tf_used, crt_tf_used)}")
    kl_type   = crt_data.get("key_level_type") or "KL"
    score     = 90 if rr >= 3.0 else 80 if rr >= 2.5 else 70
    grade     = "A+" if score >= 85 else "A"

    sig = {
        "model":       "1",
        "symbol":      symbol,
        "tf":          crt_tf_used,
        "ob_tf":       crt_tf_used,
        "ob_zone":     zone_name,
        "zone_type":   kl_type,
        "direction":   direction,
        "trend":       trend_4h,
        "entry":       round(entry, 8),
        "entry_type":  "Model #1 Visual · Gemini (TBS Open)",
        "sl":          round(sl, 8),
        "tp":          round(tp, 8),
        "tp1":         round((entry + tp) / 2, 8),
        "tp2":         round(tp, 8),
        "rr":          rr,
        "crh":         crh,
        "crl":         crl,
        "ob_top":      crt_data.get("key_level_top") or crh,
        "ob_bot":      crt_data.get("key_level_bot") or crl,
        "score":       score,
        "grade":       grade,
        "details": [
            f"👁 Gemini visual analysis",
            f"📊 4H bias: {trend_4h} | {r1.get('bias_notes','')}",
            f"✅ CRT on {_TF_LABEL.get(crt_tf_used)}: CRH={crh} CRL={crl}",
            f"   Zone: {zone_name}",
            f"🐢 TBS on {_TF_LABEL.get(tbs_tf_used)}: Entry={entry} SL={sl}",
            f"   TP={tp} | RR:{rr}R | ⚡ Fired at TBS close",
        ],
        "tbs_found":   True,
        "tbs_tf":      tbs_tf_used,
        "tbs_entry":   round(entry, 8),
        "tbs_sl":      round(sl, 8),
        "fvg_found":   False, "fvg_type":"–","fvg_entry":"–",
        "fvg_top":"–","fvg_bot":"–",
        "choch_found": False, "choch_level":"–",
        "liq_swept":   False, "ob_respected": True, "continuous": True,
        "from_visual": True,
        "market_order": True,
        "timestamp":   datetime.now(LOCAL_TZ).strftime("%Y-%m-%d %H:%M UTC+1"),
    }
    return _cache(sig)


def scan_pair(symbol):
    """
    Model #1 scanner — new logic:

    1. Establish HTF bias (trend) on 4H candles.
    2. On each HTF timeframe (Day1, 4H, 3H, 2H, 1H), find every key level
       (OB / BB / FVG) that is in the correct Premium/Discount zone.
    3. A zone is ACTIVE if:
       a) Price has EVER tapped it (wick or body entered the zone) — doesn't
          have to be tapping right now.
       b) Price has NOT closed decisively through the far side (no invalidation).
    4. For every active zone, scan the CRT timeframe for CRTs that formed
       AFTER the initial zone tap — ANY number of them, not just the first.
       Each valid CRT is its own signal opportunity.
    5. For each CRT: check for TBS on the mapped LTF.
       Entry = TBS open, SL = TBS close, TP = opposite CRT level.
    6. All qualifying signals are returned (not just the first one).
    """
    results = []

    # ── Step 1: HTF bias — 4H primary, fallback to 2H/1H ────────────
    trend = "NEUTRAL"
    sh, sl = [], []
    ref_candles = None

    for bias_tf in ["Hour4", "Hour2", "Min60"]:
        c = get_candles(symbol, bias_tf, limit=200)
        if not c or len(c) < 30:
            continue
        t, _sh, _sl = detect_trend(c)
        if ref_candles is None:
            ref_candles = c; sh = _sh; sl = _sl
        if t in ("BULLISH", "BEARISH"):
            trend = t
            ref_candles = c; sh = _sh; sl = _sl
            break

    if ref_candles is None:
        diag["no_candles"] += 1; return results
    if trend == "NEUTRAL":
        diag["neutral"] += 1; return results

    if sh and sl:
        continuous = is_continuous(sh, sl, trend, min_pts=2)
        if not continuous:
            diag["not_continuous"] += 1; return results

    direction = "BUY" if trend == "BULLISH" else "SELL"

    # ── Step 2 & 3: Find active HTF zones across all HTF timeframes ───
    # Each entry: (crt_tf, ob_tf, zone_top, zone_bot, zone_name, zone_type,
    #              matched_ob, at_key, ob_resp, tap_idx, crt_candles)
    active_zones = []

    htf_scan_tfs = [
        ("Day1",  "Day1"),
        ("Hour4", "Hour4"),
        ("Hour4", "Hour3"),
        ("Hour3", "Hour3"),
        ("Hour3", "Hour2"),
        ("Hour2", "Hour2"),
        ("Hour2", "Min60"),
        ("Min60", "Min60"),
    ]

    for crt_tf, ob_tf in htf_scan_tfs:
        crt_candles = get_candles(symbol, crt_tf, limit=200)
        if not crt_candles or len(crt_candles) < 20: continue

        ob_candles = get_candles(symbol, ob_tf, limit=150)
        if not ob_candles or len(ob_candles) < 20: continue

        raw_obs  = find_obs(ob_candles, "BULLISH" if direction == "BUY" else "BEARISH")
        ob_resp  = prev_obs_respected(raw_obs, ob_candles, direction, min_resp=1)

        # Build all candidate key levels: OB + BB + FVG
        valid_kls = []
        for ob in raw_obs[:8]:
            ob["kl_type"] = "OB"
            valid_kls.append(ob)
        for bb in find_breaker_block(ob_candles, direction)[:4]:
            valid_kls.append(bb)
        for i in range(max(0, len(ob_candles) - 60), len(ob_candles) - 3):
            c1x = ob_candles[i]; c3x = ob_candles[i + 2]
            if direction == "BUY" and c3x["low"] > c1x["high"]:
                valid_kls.append({"top": c3x["low"], "bot": c1x["high"],
                                  "high": c3x["low"], "low": c1x["high"],
                                  "idx": i, "time": c1x["time"], "kl_type": "FVG"})
            elif direction == "SELL" and c3x["high"] < c1x["low"]:
                valid_kls.append({"top": c1x["low"], "bot": c3x["high"],
                                  "high": c1x["low"], "low": c3x["high"],
                                  "idx": i, "time": c1x["time"], "kl_type": "FVG"})
        valid_kls.sort(key=lambda x: x.get("idx", 0), reverse=True)

        for kl in valid_kls[:15]:
            zt = kl.get("top", kl.get("high", 0))
            zb = kl.get("bot", kl.get("low",  0))
            if zt <= zb: continue

            # Must be in correct Premium/Discount zone
            in_pd, pd_name = ob_in_pd_zone(kl, ob_candles, direction)
            if not in_pd: continue

            # Zone must have been tapped at some point on the CRT timeframe
            tapped, tap_idx = _zone_was_tapped(crt_candles, zt, zb, direction, lookback=150)
            if not tapped: continue

            # Price must still be inside the HTF range (not broken through the far side)
            if not _price_still_inside_htf_range(crt_candles, zt, zb, direction):
                continue

            zone_type  = kl.get("kl_type", "KL")
            zone_name  = f"{pd_name} · {ob_tf} {zone_type}"
            matched_ob = kl if zone_type in ("OB", "BB") else None
            at_key     = ob_at_key_level(kl, direction, sh, sl)

            active_zones.append({
                "crt_tf":    crt_tf,
                "ob_tf":     ob_tf,
                "zone_top":  zt,
                "zone_bot":  zb,
                "zone_name": zone_name,
                "zone_type": zone_type,
                "matched_ob":matched_ob,
                "at_key":    at_key,
                "ob_resp":   ob_resp,
                "tap_idx":   tap_idx,
                "crt_candles": crt_candles,
                "is_1d":     crt_tf == "Day1",
                "pd_name":   pd_name,
            })

    if not active_zones:
        diag["not_in_zone"] += 1
        return results

    # ── Step 4 & 5: For each active zone, find ALL CRTs formed after ──
    # the zone tap, check TBS on LTF, build signal for each valid one.
    seen_crts = set()   # deduplicate by (crt_tf, crh, crl) so we don't fire twice

    for zone in active_zones:
        crt_tf      = zone["crt_tf"]
        ob_tf       = zone["ob_tf"]
        zone_top    = zone["zone_top"]
        zone_bot    = zone["zone_bot"]
        zone_name   = zone["zone_name"]
        zone_type   = zone["zone_type"]
        matched_ob  = zone["matched_ob"]
        at_key      = zone["at_key"]
        ob_resp     = zone["ob_resp"]
        tap_idx     = zone["tap_idx"]
        crt_candles = zone["crt_candles"]
        is_1d       = zone["is_1d"]

        # Detect ALL CRTs on this timeframe after zone was tapped
        all_crts = detect_crt(crt_candles, direction, ob=None)

        # Keep CRTs that formed AFTER the zone tap AND are moving
        # in the correct direction away from the zone.
        # A CRT is valid if:
        #   - It formed after tap_idx (recent enough)
        #   - Its C1 range is on the correct side of the zone
        #     (SELL: CRT range is at or below the zone — price fell away from supply)
        #     (BUY:  CRT range is at or above the zone — price rose away from demand)
        #   OR the CRT range overlaps the zone (price still near zone)
        # We do NOT require the CRT to overlap the zone — it just needs to be
        # post-tap and aligned with the direction of the reaction.
        valid_crts = []
        for crt in all_crts:
            key = (crt_tf, round(crt["crh"], 8), round(crt["crl"], 8))
            if key in seen_crts:
                continue
            crh_c = crt["crh"]
            crl_c = crt["crl"]
            # SELL: price should be at or below the zone (fell from supply)
            # BUY:  price should be at or above the zone (rose from demand)
            post_tap_aligned = (
                (direction == "SELL" and crh_c <= zone_top * 1.01) or
                (direction == "BUY"  and crl_c >= zone_bot * 0.99)
            )
            if post_tap_aligned:
                valid_crts.append(crt)

        if not valid_crts:
            # No completed CRTs yet — check if manipulation phase is forming
            diag["no_crts"] += 1
            with manip_lock:
                already_monitored = symbol in manip_monitor
                monitor_full      = len(manip_monitor) >= MAX_MONITORED
            if not already_monitored and not monitor_full:
                manip_pending = detect_manip_phase_live(crt_candles, direction, crt_tf)
                if not manip_pending:
                    manip_pending = detect_manip_phase(crt_candles, direction, crt_tf)
                manip_in_zone = [
                    m for m in manip_pending
                    if (direction == "SELL" and m["crh"] <= zone_top * 1.01) or
                       (direction == "BUY"  and m["crl"] >= zone_bot * 0.99)
                ]
                if manip_in_zone:
                    mp        = manip_in_zone[0]
                    mins_info = mp.get("mins_left", "?")
                    with manip_lock:
                        manip_monitor[symbol] = {
                            **mp,
                            "crt_tf":   crt_tf,
                            "ob_tf":    ob_tf,
                            "zone_name":zone_name,
                            "zone_top": zone_top,
                            "zone_bot": zone_bot,
                            "kl_type":  zone_type,
                            "trend":    trend,
                            "added_at": datetime.now(LOCAL_TZ).strftime("%H:%M UTC+1"),
                        }
                    log(f"👁 MONITORING: {symbol} {direction} {crt_tf} — manip phase | "
                        f"{mins_info} min to close | zone:{zone_name}")
            continue

        # ── Step 5: TBS check for each valid CRT ──────────────────────
        for crt in valid_crts:
            tbs_found, tbs_tf, tbs_entry, tbs_sl = check_tbs(
                symbol, direction, crt["crl"], crt["crh"], crt_tf)

            if not tbs_found:
                # TBS not yet confirmed — put into monitor at AWAIT_TBS
                # so it keeps watching for the TBS on LTF
                dedup_key = (crt_tf, round(crt["crh"], 8), round(crt["crl"], 8))
                if dedup_key not in seen_crts:
                    seen_crts.add(dedup_key)
                    with manip_lock:
                        already = symbol in manip_monitor
                        full    = len(manip_monitor) >= MAX_MONITORED
                    if not already and not full:
                        tp_lvl = round(crt["crl"], 8) if direction == "SELL" \
                                 else round(crt["crh"], 8)
                        with manip_lock:
                            manip_monitor[symbol] = {
                                "phase":     "AWAIT_TBS",
                                "direction": direction,
                                "crt_tf":    crt_tf,
                                "ob_tf":     ob_tf,
                                "zone_name": zone_name,
                                "zone_top":  zone_top,
                                "zone_bot":  zone_bot,
                                "kl_type":   zone_type,
                                "trend":     trend,
                                "crh":       crt["crh"],
                                "crl":       crt["crl"],
                                "c1":        crt.get("c1", {}),
                                "c2":        crt.get("c2", {}),
                                "c3":        crt.get("c3", {}),
                                "added_at":  datetime.now(LOCAL_TZ).strftime("%H:%M UTC+1"),
                            }
                        log(f"👁 M1 → AWAIT_TBS: {symbol} {direction} {crt_tf} | "
                            f"CRH:{crt['crh']} CRL:{crt['crl']} | waiting for TBS on LTF")
                diag["no_tbs"] += 1
                continue

            # TBS confirmed — deduplicate
            dedup_key = (crt_tf, round(crt["crh"], 8), round(crt["crl"], 8))
            if dedup_key in seen_crts:
                continue
            seen_crts.add(dedup_key)

            tp_lvl  = round(crt["crl"], 8) if direction == "SELL" \
                      else round(crt["crh"], 8)
            risk    = abs(tbs_entry - tbs_sl)
            rr_chk  = round(abs(tp_lvl - tbs_entry) / risk, 2) if risk > 0 else 0
            if rr_chk < 2.0:
                diag["rr_low"] += 1
                continue

            # Register in manip_monitor at AWAIT_PRICE so the live price
            # trigger is watched — execution fires when price returns to TBS open
            with manip_lock:
                already = symbol in manip_monitor
                full    = len(manip_monitor) >= MAX_MONITORED
            if not already and not full:
                has_pd = "DISCOUNT" in zone_name or "PREMIUM" in zone_name
                score  = 90 if has_pd and rr_chk >= 3.0 else 80 if rr_chk >= 3.0 else 70
                grade  = "A+" if score >= 85 else "A"
                with manip_lock:
                    manip_monitor[symbol] = {
                        "phase":      "AWAIT_PRICE",
                        "direction":  direction,
                        "crt_tf":     crt_tf,
                        "ob_tf":      ob_tf,
                        "zone_name":  zone_name,
                        "zone_top":   zone_top,
                        "zone_bot":   zone_bot,
                        "kl_type":    zone_type,
                        "trend":      trend,
                        "crh":        crt["crh"],
                        "crl":        crt["crl"],
                        "tbs_tf":     tbs_tf,
                        "tbs_entry":  tbs_entry,
                        "tbs_sl":     tbs_sl,
                        "tp":         tp_lvl,
                        "rr":         rr_chk,
                        "score":      score,
                        "grade":      grade,
                        "added_at":   datetime.now(LOCAL_TZ).strftime("%H:%M UTC+1"),
                        "tbs_at":     datetime.now(LOCAL_TZ).strftime("%H:%M UTC+1"),
                    }
                log(f"⏳ M1 → AWAIT_PRICE: {symbol} {direction} | "
                    f"TBS open:{tbs_entry} | waiting for price to return")

            # Also build signal for the signals panel display
            fake_ob = {"idx": len(crt_candles) - 3,
                       "top": crt["crh"], "bot": crt["crl"]}
            liq = liq_sweep_before_ob(crt_candles, fake_ob, direction)

            sig = _build_m1_signal(
                symbol, direction, trend, crt,
                tbs_tf, tbs_entry, tbs_sl,
                crt_tf, ob_tf, zone_name, zone_type,
                zone_top, zone_bot, matched_ob, at_key, ob_resp,
                continuous, sh, sl,
                liq_swept=liq, is_1d=is_1d,
            )

            if sig is None:
                diag["rr_low"] += 1
                continue

            # Mark as pending — awaiting price return to TBS open
            sig["entry_type"]  = "Model #1 (TBS — awaiting price @ {})".format(tbs_entry)
            sig["market_order"] = True
            diag["passed"] += 1
            results.append(sig)

    return results



# ════════ MAD MAN MODEL #2 ════════════════════════════════════════════
M2_LTF_ORDER = ["Min30", "Min15", "Min5", "Min3"]

# ════════ UNIFIED MONITOR HELPERS (M2 + M3) ═════════════════════════

def _find_first_touch_candle(candles, zone_top, zone_bot, direction, lookback=80):
    """
    Find the first candle that taps the HTF zone on LTF.
    SELL: candle wick goes into/above zone_bot
    BUY:  candle wick goes into/below zone_top
    Returns full candle dict.
    """
    for c in candles[-lookback:]:
        if direction == "SELL" and c["high"] >= zone_bot:
            return c
        if direction == "BUY"  and c["low"]  <= zone_top:
            return c
    return None

def _find_tp_origin(candles, ft_candle_idx, direction, lookback=100):
    """
    TP = the low that CREATED the first touch (for SELL)
       = the high that CREATED the first touch (for BUY)
    Walk back before the first touch candle to find the origin swing.
    """
    slice_ = candles[max(0, ft_candle_idx - lookback): ft_candle_idx]
    if not slice_: return None
    if direction == "SELL":
        return round(min(c["low"]  for c in slice_), 8)
    else:
        return round(max(c["high"] for c in slice_), 8)

def _find_swing_point(candles, after_idx, direction, lookback=30):
    """
    After first touch, find the first swing point that forms on retrace.
    SELL: a local low forms (price dipped then came back) = swing low
    BUY:  a local high forms (price popped then came back) = swing high
    Returns (idx, level) or (None, None).
    """
    search = candles[after_idx: after_idx + lookback]
    for i in range(2, len(search) - 2):
        c = search[i]
        if direction == "SELL":   # looking for swing low after bearish first touch
            if (c["low"] < search[i-1]["low"] and c["low"] < search[i-2]["low"] and
                c["low"] < search[i+1]["low"] and c["low"] <= search[i+2]["low"]):
                return after_idx + i, round(c["low"], 8)
        else:                      # looking for swing high after bullish first touch
            if (c["high"] > search[i-1]["high"] and c["high"] > search[i-2]["high"] and
                c["high"] > search[i+1]["high"] and c["high"] >= search[i+2]["high"]):
                return after_idx + i, round(c["high"], 8)
    return None, None

def _valid_single_candle_sweep(candles, sweep_target, direction, search_from=0):
    """
    Find a single candle that sweeps sweep_target and closes back with just a wick.
    Rules:
      SELL sweep: candle high > sweep_target AND candle closes BELOW sweep_target
      BUY  sweep: candle low  < sweep_target AND candle closes ABOVE sweep_target
    Validation: subsequent candles must NOT close beyond the sweep candle's wick extreme.
    Returns (sweep_candle_idx, sweep_candle) or (None, None).
    """
    search = candles[search_from:]
    for i, c in enumerate(search):
        real_i = search_from + i
        swept = False
        if direction == "SELL" and c["high"] > sweep_target and c["close"] < sweep_target:
            swept = True
        elif direction == "BUY" and c["low"] < sweep_target and c["close"] > sweep_target:
            swept = True
        if not swept:
            continue
        # Validate: next candles must not CLOSE beyond the sweep wick
        sweep_extreme = c["high"] if direction == "SELL" else c["low"]
        valid = True
        for j in range(real_i + 1, min(real_i + 6, len(candles))):
            nc = candles[j]
            if direction == "SELL" and nc["close"] > sweep_extreme:
                valid = False; break
            if direction == "BUY"  and nc["close"] < sweep_extreme:
                valid = False; break
        if valid:
            return real_i, c
    return None, None

def _find_choch_after(candles, after_idx, direction):
    """
    Find CHoCH in candles after after_idx.
    SELL direction: CHoCH = price closes below a recent swing low (bearish shift)
    BUY  direction: CHoCH = price closes above a recent swing high (bullish shift)
    Returns (choch_idx, choch_level) or (None, None).
    """
    search = candles[after_idx:]
    sh = []; sl = []
    for i in range(2, len(search) - 2):
        c = search[i]
        if (c["high"] > search[i-1]["high"] and c["high"] > search[i-2]["high"] and
            c["high"] > search[i+1]["high"] and c["high"] >= search[i+2]["high"]):
            sh.append((i, c["high"]))
        if (c["low"] < search[i-1]["low"] and c["low"] < search[i-2]["low"] and
            c["low"] < search[i+1]["low"] and c["low"] <= search[i+2]["low"]):
            sl.append((i, c["low"]))
    if direction == "SELL":
        # Need a swing low then a close below it
        if not sl: return None, None
        for s_idx, s_val in sl:
            for i in range(s_idx + 1, len(search)):
                if search[i]["close"] < s_val and search[i]["close"] < search[i]["open"]:
                    return after_idx + i, round(s_val, 8)
    else:
        if not sh: return None, None
        for s_idx, s_val in sh:
            for i in range(s_idx + 1, len(search)):
                if search[i]["close"] > s_val and search[i]["close"] > search[i]["open"]:
                    return after_idx + i, round(s_val, 8)
    return None, None

def _find_fvg_in_range(candles, from_idx, to_idx, direction):
    """
    Find all unmitigated FVGs between from_idx and to_idx.
    Returns list of {top, bot, tip, idx} sorted newest first.

    SELL FVG (bearish gap-down):  c1.low > c3.high
      fvg_top = c1.low, fvg_bot = c3.high
      tip = fvg_bot  (price retraces UP, first touch at bottom of gap)
      Mitigated when price CLOSES above fvg_top (fully filled the gap)

    BUY FVG (bullish gap-up):  c3.low > c1.high
      fvg_top = c3.low, fvg_bot = c1.high
      tip = fvg_top  (price retraces DOWN, first touch at top of gap)
      Mitigated when price CLOSES below fvg_bot (fully filled the gap)
    """
    fvgs = []
    end = min(to_idx, len(candles) - 2)
    for i in range(from_idx, end):
        c1 = candles[i]; c3 = candles[i + 2]
        if direction == "SELL" and c3["high"] < c1["low"]:
            fvg_top = c1["low"]; fvg_bot = c3["high"]
            # Mitigated only if price CLOSED above the top of the gap
            mit = any(candles[j]["close"] > fvg_top for j in range(i + 3, len(candles)))
            if not mit:
                fvgs.append({"top": round(fvg_top,8), "bot": round(fvg_bot,8),
                             "tip": round(fvg_bot,8), "idx": i})
        elif direction == "BUY" and c3["low"] > c1["high"]:
            fvg_top = c3["low"]; fvg_bot = c1["high"]
            # Mitigated only if price CLOSED below the bottom of the gap
            mit = any(candles[j]["close"] < fvg_bot for j in range(i + 3, len(candles)))
            if not mit:
                fvgs.append({"top": round(fvg_top,8), "bot": round(fvg_bot,8),
                             "tip": round(fvg_top,8), "idx": i})
    return sorted(fvgs, key=lambda x: x["idx"], reverse=True)

def _find_ob_above_fvg(candles, fvg_top, fvg_bot, direction):
    """
    Find the Order Block directly above (SELL) or below (BUY) the FVG.
    OB = last bearish candle before a bullish displacement (for SELL setups above FVG).
    Returns ob_high or None.
    """
    for c in reversed(candles):
        if direction == "SELL":
            # OB above FVG: a bearish candle whose body is above fvg_top
            if c["open"] > fvg_top and c["close"] > fvg_top and c["close"] < c["open"]:
                return round(c["high"], 8)
        else:
            if c["open"] < fvg_bot and c["close"] < fvg_bot and c["close"] > c["open"]:
                return round(c["low"], 8)
    return None

def _swing_before_touch(candles, direction, lookback=80):
    """Liquidity pool on the other side = TP origin."""
    c = candles[-lookback:]
    if direction == "SELL": return round(min(x["low"]  for x in c), 8)
    else:                   return round(max(x["high"] for x in c), 8)


def _ob_is_fresh(ob, candles):
    """
    An OB is fresh if price has NEVER closed inside its body after it formed.
    ob['idx'] is the candle index of the OB. Check all candles after idx+1.
    """
    idx = ob.get("idx", 0)
    ob_top = ob.get("top", ob.get("high", 0))
    ob_bot = ob.get("bot", ob.get("low",  0))
    for c in candles[idx + 1:]:
        if c["close"] >= ob_bot and c["close"] <= ob_top:
            return False   # price closed inside OB — mitigated
    return True

def _displacement_then_choch(candles, direction, lookback=60):
    """
    Check that after the OB formed, price:
    1. Displaced AWAY from the OB (strong impulsive move)
    2. Then printed a CHoCH or BOS confirming trend continuation
    Returns True if both conditions met.
    """
    if len(candles) < 10: return False
    recent = candles[-lookback:]
    # Displacement: at least one candle with body >= 0.5% of price
    displaced = False
    disp_idx  = None
    for i, c in enumerate(recent):
        body = abs(c["close"] - c["open"])
        rng  = c["high"] - c["low"]
        if rng <= 0: continue
        body_pct = body / max(c["open"], 0.0000001)
        # Displacement candle: body > 0.3% of price AND body > 60% of candle range
        if body_pct >= 0.003 and body / rng >= 0.6:
            if direction == "SELL" and c["close"] < c["open"]:
                displaced = True; disp_idx = i; break
            if direction == "BUY"  and c["close"] > c["open"]:
                displaced = True; disp_idx = i; break
    if not displaced or disp_idx is None: return False
    # CHoCH/BOS after displacement
    after = recent[disp_idx:]
    sh = []; sl = []
    for i in range(2, len(after) - 1):
        c = after[i]
        if c["high"] > after[i-1]["high"] and c["high"] > after[i-2]["high"]:
            sh.append((i, c["high"]))
        if c["low"] < after[i-1]["low"] and c["low"] < after[i-2]["low"]:
            sl.append((i, c["low"]))
    if direction == "SELL":
        if sl:
            last_sl_val = sl[-1][1]
            for c in after[sl[-1][0]+1:]:
                if c["close"] < last_sl_val: return True
    else:
        if sh:
            last_sh_val = sh[-1][1]
            for c in after[sh[-1][0]+1:]:
                if c["close"] > last_sh_val: return True
    return False

def scan_pair_model2(symbol):
    """
    Unified M2/M3 scanner.
    Filters:
    - Trend alignment mandatory (BUY only in BULLISH, SELL only in BEARISH)
    - HTF OB must be freshly unmitigated
    - Price must have displaced + printed CHoCH/BOS BEFORE returning to tap OB
    """
    results = []
    with m2_lock:
        if symbol in m2_monitor: return results

    ref = get_candles(symbol, "Hour4", limit=200)
    if not ref or len(ref) < 30: return results
    trend, _, _ = detect_trend(ref)
    if trend == "NEUTRAL": return results
    direction = "BUY" if trend == "BULLISH" else "SELL"

    for htf in ["Hour4", "Min60"]:
        htf_c = get_candles(symbol, htf, limit=150)
        if not htf_c or len(htf_c) < 20: continue
        kls = []
        raw_dir = "BULLISH" if direction == "BUY" else "BEARISH"
        for ob in find_obs(htf_c, raw_dir)[:6]:
            ob["kl_type"] = "OB"; kls.append(ob)
        for bb in find_breaker_block(htf_c, direction)[:3]:
            kls.append(bb)
        kls.sort(key=lambda x: x.get("idx", 0), reverse=True)

        for kl in kls[:8]:
            zone_top = kl.get("top", kl.get("high", 0))
            zone_bot = kl.get("bot", kl.get("low",  0))
            if zone_top <= zone_bot: continue

            # ── FILTER 1: OB must be in correct P/D zone ──
            in_pd, pd_name = ob_in_pd_zone(kl, htf_c, direction)
            if not in_pd: continue

            # ── FILTER 2: OB must be freshly unmitigated ──
            if not _ob_is_fresh(kl, htf_c): continue

            # ── FILTER 3: displacement + CHoCH/BOS must have printed ──
            if not _displacement_then_choch(htf_c, direction): continue

            for ltf in M2_LTF_ORDER:
                ltf_c = get_candles(symbol, ltf, limit=300)
                if not ltf_c or len(ltf_c) < 20: continue

                # Find first touch candle index
                ft_candle = None; ft_idx = None
                for idx, c in enumerate(ltf_c):
                    if direction == "SELL" and c["high"] >= zone_bot:
                        ft_candle = c; ft_idx = idx; break
                    if direction == "BUY"  and c["low"]  <= zone_top:
                        ft_candle = c; ft_idx = idx; break
                if ft_candle is None: continue

                ft_extreme = round(ft_candle["high"] if direction=="SELL" else ft_candle["low"], 8)

                # TP = origin low/high that CREATED the first touch
                tp_origin = _find_tp_origin(ltf_c, ft_idx, direction)
                liq_tgt   = tp_origin if tp_origin else _swing_before_touch(ltf_c, direction)

                with m2_lock:
                    already = symbol in m2_monitor
                    full    = len(m2_monitor) >= MAX_M2_MONITORED
                if not already and not full:
                    with m2_lock:
                        m2_monitor[symbol] = {
                            "phase":       "AWAIT_PATTERN",
                            "model":       None,
                            "symbol":      symbol,
                            "htf":         htf,
                            "ltf":         ltf,
                            "direction":   direction,
                            "trend":       trend,
                            "zone_top":    round(zone_top, 8),
                            "zone_bot":    round(zone_bot, 8),
                            "zone_name":   pd_name + " · " + kl.get("kl_type","KL"),
                            "kl_type":     kl.get("kl_type","KL"),
                            "in_pd":       in_pd,
                            "pd_name":     pd_name,
                            "ft_extreme":  ft_extreme,
                            "ft_idx":      ft_idx,
                            "liq_target":  round(liq_tgt, 8) if liq_tgt else 0,
                            "added_at":    datetime.now(LOCAL_TZ).strftime("%H:%M UTC+1"),
                        }
                    log(f"👁 M2/M3 QUEUED: {symbol} {direction} | {htf} {pd_name} | ft={ft_extreme} | fresh OB ✅ disp+choch ✅")
                break
            if symbol in m2_monitor: break
        if symbol in m2_monitor: break
    return results

def fmt_tg_m2(sig):
    e  = "🟢" if sig["direction"]=="BUY" else "🔴"
    b  = "█"*(sig["score"]//10)+"░"*(10-sig["score"]//10)
    hl = {"Day1":"1D","Hour4":"4H","Hour3":"3H","Hour2":"2H","Min60":"1H"}.get(sig.get("tf",""),"–")
    return (
        f"{e} <b>MAD MAN MODEL #{sig.get('model','2a')} — {sig['direction']}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"<b>Pair:</b>      {sig['symbol']}\n"
        f"<b>HTF Zone:</b>  {hl} · {sig.get('ob_zone','–')}\n"
        f"<b>LTF:</b>       {sig.get('ob_tf','–')} · FVG Tip (1st touch)\n"
        f"<b>Trend:</b>     {sig['trend']}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"<b>🎯 Entry:</b>  {sig['entry']} (FVG tip)\n"
        f"<b>🛑 SL:</b>     {sig['sl']}\n"
        f"<b>🎯 TP1:</b>    {sig.get('tp1','–')} (50% of range)\n"
        f"<b>🏆 TP2:</b>    {sig.get('tp2','–')} (liquidity)\n"
        f"<b>📊 RR:</b>     {sig['rr']}R\n"
        f"<b>Sweep:</b>     {sig.get('sweep_extreme','–')}\n"
        f"<b>FVG:</b>       {sig.get('fvg_top','–')} / {sig.get('fvg_bot','–')}\n"
        f"<b>🔔 Trail SL → TP1 when 70% of range hit</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"<b>Score:</b>     {sig['score']}/100 [{b}] {sig['grade']}\n"
        f"<i>Mad Man Strategy Scanner • {sig['timestamp']}</i>"
    )


def fmt_tg_m3(sig):
    e  = "🟢" if sig["direction"]=="BUY" else "🔴"
    b  = "█"*(sig["score"]//10)+"░"*(10-sig["score"]//10)
    hl = {"Min30":"30m","Min45":"45m"}.get(sig.get("tf",""),"30m")
    return (
        f"{e} <b>MAD MAN MODEL #3 — {sig['direction']}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"<b>Pair:</b>      {sig['symbol']}\n"
        f"<b>HTF Zone:</b>  {hl} · {sig.get('ob_zone','–')}\n"
        f"<b>LTF:</b>       {sig.get('ob_tf','–')} · OB before CHoCH\n"
        f"<b>Trend:</b>     {sig['trend']}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"<b>🎯 Entry:</b>  {sig['entry']} (LTF OB)\n"
        f"<b>🛑 SL:</b>     {sig['sl']} (sweep extreme)\n"
        f"<b>🏆 TP:</b>     {sig['tp']} (liquidity)\n"
        f"<b>📊 RR:</b>     {sig['rr']}R\n"
        f"<b>Sweep:</b>     {sig.get('sweep_extreme','–')}\n"
        f"<b>CHoCH:</b>     {sig.get('choch_level','–')}\n"
        f"<b>HTF OB:</b>    {sig.get('ob_top','–')} / {sig.get('ob_bot','–')}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"<b>Score:</b>     {sig['score']}/100 [{b}] {sig['grade']}\n"
        f"<i>Mad Man Strategy Scanner • {sig['timestamp']}</i>"
    )

# ════════ MAD MAN MODEL #4 ════════════════════════════════════════════
# HTF: 30m or 45m fresh unmitigated OB in correct P/D zone
#      Price displaces away → CHoCH/BOS on HTF → retraces back to OB
# LTF: 3m or 5m
#      Price enters HTF OB → sweep on LTF (single or 2-candle)
#      → CHoCH/BOS on LTF → entry = OB zone before the LTF CHoCH
#      SL = sweep candle extreme, TP = last swing liq, min RR = 1.0

M4_HTF = ["Min30", "Min45"]
M4_LTF = ["Min3",  "Min5"]

def _find_ob_zone_before_choch(candles, choch_idx, direction, lookback=20):
    """
    Find the OB zone (last 1-4 candles before the impulsive move that caused CHoCH).
    For SELL: last 1-4 bullish candles before the bearish impulse that broke structure.
    For BUY:  last 1-4 bearish candles before the bullish impulse that broke structure.
    Returns (ob_top, ob_bot, ob_entry) or (None, None, None).
    """
    start = max(0, choch_idx - lookback)
    search = candles[start:choch_idx]
    if len(search) < 4: return None, None, None

    # Find the impulse candle (biggest body in the direction of CHoCH)
    impulse_idx = None
    best_body = 0
    for i, c in enumerate(search):
        body = abs(c["close"] - c["open"])
        if direction == "SELL" and c["close"] < c["open"] and body > best_body:
            best_body = body; impulse_idx = i
        if direction == "BUY"  and c["close"] > c["open"] and body > best_body:
            best_body = body; impulse_idx = i

    if impulse_idx is None or impulse_idx == 0: return None, None, None

    # OB zone = last 1-4 candles BEFORE the impulse
    ob_start = max(0, impulse_idx - 4)
    ob_candles = search[ob_start:impulse_idx]
    if not ob_candles: return None, None, None

    if direction == "SELL":
        # OB = highest candles before bearish impulse
        ob_top   = max(c["high"]  for c in ob_candles)
        ob_bot   = min(c["low"]   for c in ob_candles)
        ob_entry = round(ob_top, 8)   # entry at TOP of OB — price retests supply from below
    else:
        ob_top   = max(c["high"]  for c in ob_candles)
        ob_bot   = min(c["low"]   for c in ob_candles)
        ob_entry = round(ob_bot, 8)   # entry at BOTTOM of OB — price retests demand from above

    return round(ob_top, 8), round(ob_bot, 8), ob_entry

def _two_candle_sweep_valid(candles, sweep_target, direction, search_from=0):
    """
    2-candle sweep validation:
    SELL: candle A sweeps above sweep_target, candle B closes above A's high
          then ALL remaining candles close back below candle B's close.
    BUY:  candle A sweeps below sweep_target, candle B closes below A's low
          then ALL remaining candles close back above candle B's close.
    Returns (sweep_idx, sweep_extreme) or (None, None).
    """
    search = candles[search_from:]
    for i in range(len(search) - 2):
        a = search[i]; b = search[i + 1]
        if direction == "SELL":
            if a["high"] > sweep_target and b["close"] > a["high"]:
                # All remaining must close below b's close
                remaining = search[i + 2:]
                if all(c["close"] < b["close"] for c in remaining[:6]):
                    return search_from + i, round(b["high"], 8)
        else:
            if a["low"] < sweep_target and b["close"] < a["low"]:
                remaining = search[i + 2:]
                if all(c["close"] > b["close"] for c in remaining[:6]):
                    return search_from + i, round(b["low"], 8)
    return None, None

def _htf_choch_bos(candles, direction, lookback=60):
    """
    Check if HTF has printed a CHoCH or BOS confirming trend direction.
    Returns (True, level) or (False, None).
    """
    recent = candles[-lookback:]
    sh = []; sl = []
    for i in range(2, len(recent) - 1):
        c = recent[i]
        if c["high"] > recent[i-1]["high"] and c["high"] > recent[i-2]["high"]:
            sh.append((i, c["high"]))
        if c["low"] < recent[i-1]["low"] and c["low"] < recent[i-2]["low"]:
            sl.append((i, c["low"]))
    if direction == "SELL" and sl:
        last_val = sl[-1][1]
        for c in recent[sl[-1][0]+1:]:
            if c["close"] < last_val:
                return True, round(last_val, 8)
    if direction == "BUY" and sh:
        last_val = sh[-1][1]
        for c in recent[sh[-1][0]+1:]:
            if c["close"] > last_val:
                return True, round(last_val, 8)
    return False, None

def scan_pair_model4(symbol):
    """
    Model #4 scanner.
    Queues pairs where:
    - 30m or 45m has fresh unmitigated OB in P/D zone
    - Trend aligned
    - Displacement + CHoCH/BOS printed on HTF
    - Price has returned to tap the OB
    LTF (3m/5m) monitor then looks for sweep → CHoCH → OB entry.
    """
    results = []
    with m4_lock:
        if symbol in m4_monitor: return results

    # Trend from Hour4 reference (same as other models)
    ref = get_candles(symbol, "Hour4", limit=200)
    if not ref or len(ref) < 30: return results
    trend, _, _ = detect_trend(ref)
    if trend == "NEUTRAL": return results
    direction = "BUY" if trend == "BULLISH" else "SELL"

    for htf in M4_HTF:
        htf_c = get_candles(symbol, htf, limit=200)
        if not htf_c or len(htf_c) < 30: continue

        # ── Must have HTF CHoCH/BOS first ──
        htf_choch, htf_choch_lvl = _htf_choch_bos(htf_c, direction)
        if not htf_choch: continue

        # ── Find fresh OBs on HTF ──
        raw_dir = "BULLISH" if direction == "BUY" else "BEARISH"
        kls = []
        for ob in find_obs(htf_c, raw_dir)[:8]:
            ob["kl_type"] = "OB"
            if not _ob_is_fresh(ob, htf_c): continue
            kls.append(ob)
        kls.sort(key=lambda x: x.get("idx", 0), reverse=True)

        for kl in kls[:6]:
            zone_top = kl.get("top", kl.get("high", 0))
            zone_bot = kl.get("bot", kl.get("low",  0))
            if zone_top <= zone_bot: continue

            # ── P/D zone check ──
            in_pd, pd_name = ob_in_pd_zone(kl, htf_c, direction)
            if not in_pd: continue

            # ── Price must currently be tapping / inside the OB ──
            tapping = price_tapping_ob(htf_c, kl, direction)
            if not tapping: continue

            # ── Liq target = last swing low/high before the OB formed ──
            liq_tgt = _find_tp_origin(htf_c, kl.get("idx", len(htf_c)-10), direction)
            if not liq_tgt:
                liq_tgt = _swing_before_touch(htf_c, direction)

            with m4_lock:
                already = symbol in m4_monitor
                full    = len(m4_monitor) >= MAX_M4_MONITORED
            if not already and not full:
                with m4_lock:
                    m4_monitor[symbol] = {
                        "phase":         "AWAIT_SWEEP",
                        "model":         "3",
                        "symbol":        symbol,
                        "htf":           htf,
                        "ltf":           M4_LTF[0],   # start with 3m
                        "direction":     direction,
                        "trend":         trend,
                        "zone_top":      round(zone_top, 8),
                        "zone_bot":      round(zone_bot, 8),
                        "zone_name":     pd_name + " · OB",
                        "kl_type":       "OB",
                        "htf_choch_lvl": htf_choch_lvl,
                        "liq_target":    round(liq_tgt, 8) if liq_tgt else 0,
                        "added_at":      datetime.now(LOCAL_TZ).strftime("%H:%M UTC+1"),
                    }
                log(f"👁 M3 QUEUED: {symbol} {direction} | {htf} {pd_name} OB={zone_bot}–{zone_top} | fresh ✅ choch ✅ tapping ✅")
            break
        if symbol in m4_monitor: break
    return results


def m4_monitor_loop():
    """
    Model #4 monitor.
    AWAIT_SWEEP  — watching 3m/5m for sweep of a swing inside the HTF OB
    AWAIT_CHOCH  — sweep confirmed, waiting for LTF CHoCH/BOS
    AWAIT_TAP    — OB before CHoCH identified, waiting for price to enter it
    """
    log("🔍 M3 monitor started")
    while True:
        try:
            with m4_lock:
                symbols = list(m4_monitor.keys())

            for symbol in symbols:
                with m4_lock:
                    if symbol not in m4_monitor: continue
                    mon = dict(m4_monitor[symbol])

                phase     = mon.get("phase", "AWAIT_SWEEP")
                direction = mon.get("direction", "SELL")
                htf       = mon.get("htf", "Min30")
                ltf       = mon.get("ltf", "Min3")
                zone_top  = mon.get("zone_top", 0)
                zone_bot  = mon.get("zone_bot", 0)
                liq_tgt   = mon.get("liq_target", 0)

                ltf_c = get_candles(symbol, ltf, limit=300)
                if not ltf_c or len(ltf_c) < 10: continue

                # ── Expire if price moves 8%+ from zone mid ──
                ticker = get_ticker(symbol)
                if ticker:
                    price    = ticker["price"]
                    zone_mid = (zone_top + zone_bot) / 2
                    if zone_mid > 0 and abs(price - zone_mid) / zone_mid > 0.08:
                        with m4_lock: m4_monitor.pop(symbol, None)
                        log(f"❌ M3 EXPIRED: {symbol} — price 8%+ from zone")
                        continue

                # ════ PHASE: AWAIT_SWEEP ════════════════════════════════
                if phase == "AWAIT_SWEEP":
                    # Look for a swing high/low inside OR just before the HTF OB zone on LTF
                    # Price can sweep a high formed just before tapping the OB — still valid
                    swing_target = None
                    zone_height  = max(zone_top - zone_bot, 0.0001)
                    zone_buffer  = zone_height * 0.5   # 50% of OB height outside is still valid
                    for c in reversed(ltf_c[-100:]):
                        if direction == "SELL":
                            if (zone_bot - zone_buffer) <= c["high"] <= (zone_top + zone_buffer):
                                swing_target = c["high"]; break
                        elif direction == "BUY":
                            if (zone_bot - zone_buffer) <= c["low"] <= (zone_top + zone_buffer):
                                swing_target = c["low"]; break

                    if swing_target is None: continue

                    # Try single candle sweep
                    sweep_idx, sweep_c = _valid_single_candle_sweep(
                        ltf_c, swing_target, direction, search_from=max(0, len(ltf_c)-80))

                    # Try 2-candle sweep if single not found
                    sweep_extreme = None
                    if sweep_idx is not None:
                        sweep_extreme = (sweep_c["high"] if direction=="SELL" else sweep_c["low"])
                    else:
                        s2_idx, s2_ext = _two_candle_sweep_valid(
                            ltf_c, swing_target, direction, search_from=max(0, len(ltf_c)-80))
                        if s2_idx is not None:
                            sweep_idx     = s2_idx
                            sweep_extreme = s2_ext
                            sweep_c       = ltf_c[min(s2_idx+1, len(ltf_c)-1)]

                    if sweep_idx is None: continue

                    log(f"✅ M3 SWEEP: {symbol} {direction} sweep={round(sweep_extreme,8)}")
                    with m4_lock:
                        if symbol in m4_monitor:
                            m4_monitor[symbol].update({
                                "phase":         "AWAIT_CHOCH",
                                "sweep_idx":     sweep_idx,
                                "sweep_extreme": round(sweep_extreme, 8),
                                "sweep_c_high":  round(sweep_c["high"], 8),
                                "sweep_c_low":   round(sweep_c["low"],  8),
                                "swing_target":  round(swing_target, 8),
                                "sweep_time":    datetime.now(LOCAL_TZ).strftime("%H:%M UTC+1"),
                            })
                    continue

                # ════ PHASE: AWAIT_CHOCH ════════════════════════════════
                if phase == "AWAIT_CHOCH":
                    sweep_idx = mon.get("sweep_idx", 0)
                    sweep_c_high = mon.get("sweep_c_high", 0)
                    sweep_c_low  = mon.get("sweep_c_low",  0)

                    # SL = beyond sweep extreme
                    sl = (round(sweep_c_high * 1.001, 8) if direction == "SELL"
                          else round(sweep_c_low  * 0.999, 8))

                    # Look for CHoCH/BOS on LTF after the sweep
                    choch_idx, choch_lvl = _find_choch_after(ltf_c, sweep_idx + 1, direction)
                    if choch_idx is None: 
                        # Timeout 2hr after sweep
                        st = mon.get("sweep_time","")
                        if st:
                            try:
                                t0 = datetime.strptime(st, "%H:%M UTC+1").replace(
                                    year=datetime.now().year, month=datetime.now().month,
                                    day=datetime.now().day, tzinfo=LOCAL_TZ)
                                if (datetime.now(LOCAL_TZ) - t0).total_seconds() > 7200:
                                    with m4_lock: m4_monitor.pop(symbol, None)
                                    log(f"❌ M3 TIMEOUT: {symbol} no CHoCH in 2hr")
                            except: pass
                        continue

                    # Find OB zone before the CHoCH
                    ob_top, ob_bot, ob_entry = _find_ob_zone_before_choch(
                        ltf_c, choch_idx, direction)
                    if ob_top is None: continue

                    # RR check
                    risk   = abs(ob_entry - sl)
                    reward = abs(liq_tgt - ob_entry) if liq_tgt else 0
                    if risk <= 0: continue
                    # Sanity: for SELL sl must be ABOVE entry, for BUY sl must be BELOW
                    if direction == "SELL" and sl <= ob_entry:
                        log(f"⚠️ M3 {symbol} SELL sl={sl} <= entry={ob_entry} — skip bad SL")
                        continue
                    if direction == "BUY"  and sl >= ob_entry:
                        log(f"⚠️ M3 {symbol} BUY sl={sl} >= entry={ob_entry} — skip bad SL")
                        continue
                    rr = round(reward / risk, 2)
                    if rr < 1.0:
                        with m4_lock: m4_monitor.pop(symbol, None)
                        log(f"⚠️ M3 {symbol} RR={rr}R < 1.0 — skip")
                        continue

                    log(f"✅ M3 CHoCH: {symbol} {direction} choch={choch_lvl} ob={ob_bot}–{ob_top} RR={rr}R")
                    with m4_lock:
                        if symbol in m4_monitor:
                            m4_monitor[symbol].update({
                                "phase":      "AWAIT_TAP",
                                "choch_idx":  choch_idx,
                                "choch_lvl":  choch_lvl,
                                "ob_top":     ob_top,
                                "ob_bot":     ob_bot,
                                "ob_entry":   ob_entry,
                                "sl":         sl,
                                "rr":         rr,
                                "choch_time": datetime.now(LOCAL_TZ).strftime("%H:%M UTC+1"),
                            })
                    continue

                # ════ PHASE: AWAIT_TAP ══════════════════════════════════
                if phase == "AWAIT_TAP":
                    ob_top    = mon.get("ob_top", 0)
                    ob_bot    = mon.get("ob_bot", 0)
                    ob_entry  = mon.get("ob_entry", 0)
                    sl        = mon.get("sl", 0)
                    rr        = mon.get("rr", 0)
                    choch_lvl = mon.get("choch_lvl", "–")
                    sweep_ext = mon.get("sweep_extreme", 0)

                    if not ticker: continue
                    price = ticker["price"]

                    # Price enters OB zone = tap
                    tapped = (ob_bot <= price <= ob_top)

                    # Invalidate: price blows clean through OB
                    if direction == "SELL" and price > ob_top * 1.003:
                        with m4_lock: m4_monitor.pop(symbol, None)
                        log(f"❌ M3 INVALID: {symbol} blew through OB top")
                        continue
                    if direction == "BUY" and price < ob_bot * 0.997:
                        with m4_lock: m4_monitor.pop(symbol, None)
                        log(f"❌ M3 INVALID: {symbol} blew through OB bot")
                        continue

                    # 4hr expiry on tap wait
                    ct = mon.get("choch_time","")
                    if not tapped and ct:
                        try:
                            t0 = datetime.strptime(ct, "%H:%M UTC+1").replace(
                                year=datetime.now().year, month=datetime.now().month,
                                day=datetime.now().day, tzinfo=LOCAL_TZ)
                            if (datetime.now(LOCAL_TZ) - t0).total_seconds() > 14400:
                                with m4_lock: m4_monitor.pop(symbol, None)
                                log(f"❌ M3 EXPIRED: {symbol} OB not tapped in 4hr")
                        except: pass
                        continue

                    if not tapped: continue

                    log(f"🚀 M3 OB TAPPED: {symbol} {direction} price={price} ob={ob_bot}–{ob_top} RR={rr}R — SIGNAL!")

                    trend     = mon.get("trend","NEUTRAL")
                    zone_name = mon.get("zone_name","–")
                    in_pd     = mon.get("in_pd", False)
                    score     = 88 if rr >= 3.0 else 78 if rr >= 2.0 else 68
                    grade     = "A+" if score >= 85 else "A"

                    sig = {
                        "model":         "3",
                        "symbol":        symbol,
                        "tf":            htf,
                        "ob_tf":         ltf,
                        "ob_zone":       zone_name,
                        "zone_type":     "OB",
                        "direction":     direction,
                        "trend":         trend,
                        "entry":         round(ob_entry, 8),
                        "entry_type":    "Model #3 (HTF OB + LTF Sweep→CHoCH→OB)",
                        "sl":            sl,
                        "tp":            round(liq_tgt, 8),
                        "tp1":           round((ob_entry + liq_tgt) / 2, 8),
                        "tp2":           round(liq_tgt, 8),
                        "rr":            rr,
                        "crh":           round(zone_top, 8),
                        "crl":           round(zone_bot, 8),
                        "ob_top":        round(zone_top, 8),
                        "ob_bot":        round(zone_bot, 8),
                        "sweep_extreme": sweep_ext,
                        "choch_level":   choch_lvl,
                        "choch_found":   True,
                        "fvg_found":     False,
                        "fvg_type":      "–",
                        "fvg_entry":     "–",
                        "fvg_top":       "–",
                        "fvg_bot":       "–",
                        "tbs_found":     False,
                        "tbs_tf":        "–",
                        "tbs_entry":     "–",
                        "tbs_sl":        "–",
                        "liq_swept":     True,
                        "ob_respected":  True,
                        "continuous":    True,
                        "score":         score,
                        "grade":         grade,
                        "details":       [
                            f"✅ HTF: {htf} fresh OB in {zone_name}",
                            f"✅ HTF CHoCH/BOS: {mon.get('htf_choch_lvl','–')}",
                            f"✅ LTF: {ltf} sweep of {sweep_ext}",
                            f"✅ LTF CHoCH confirmed: {choch_lvl}",
                            f"✅ OB before CHoCH: {ob_bot}–{ob_top}",
                            f"✅ RR:{rr}R | SL: sweep extreme",
                        ],
                        "from_monitor":  True,
                        "market_order":  True,
                        "timestamp":     datetime.now(LOCAL_TZ).strftime("%Y-%m-%d %H:%M UTC+1"),
                    }

                    signals.appendleft(sig)
                    send_telegram(fmt_tg_m3(sig), kind="signal")

                    ready, reason = live_trade_ready()
                    if ready:
                        ok, msg = place_order(sig)
                        log(f"{'✅' if ok else '❌'} M4 trade: {msg}")
                    else:
                        log(f"⚠️ M4 live trade skipped ({symbol}): {reason}")

                    if paper_config["enabled"] and paper_config["auto_trade"]:
                        ok2, msg2 = place_paper_order(sig)
                        if ok2: log(f"📝 M3 paper: {msg2}")

                    with m4_lock:
                        m4_monitor.pop(symbol, None)

        except Exception as e:
            log(f"❌ M3 monitor error: {e}")
        time.sleep(5)

def mexc_sign(api_key, timestamp_ms, request_param, secret):
    """
    MEXC Futures Contract API v1 signature.
    Spec: sign = HmacSHA256(apiKey + timestamp + requestParam, secretKey)
    """
    raw = str(api_key) + str(timestamp_ms) + str(request_param)
    return hmac.new(
        secret.encode("utf-8"),
        raw.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()


def _get_mexc_ts() -> str:
    """
    Fetch the current timestamp DIRECTLY from MEXC's server clock.
    This completely eliminates Railway clock drift — we never use the
    local system clock for signing. MEXC cannot reject our timestamp
    because it came from MEXC itself.

    Endpoint: GET /api/v1/contract/ping  (public, no auth, <10ms)
    Header:   x-mexc-server-time  (milliseconds UTC)
    Fallback: if the header is absent, parse the HTTP Date header.
    Final fallback: local time.time() (old behaviour).
    """
    try:
        r = requests.get(
            "https://contract.mexc.com/api/v1/contract/ping",
            timeout=3)
        # Primary: dedicated ms timestamp header
        srv = r.headers.get("x-mexc-server-time") or r.headers.get("x-server-time")
        if srv and str(srv).strip().isdigit():
            return str(int(srv))
        # Secondary: standard HTTP Date header (second precision)
        date_hdr = r.headers.get("Date")
        if date_hdr:
            from email.utils import parsedate_to_datetime
            dt = parsedate_to_datetime(date_hdr)
            return str(int(dt.timestamp() * 1000))
    except Exception as e:
        log(f"\u26a0\ufe0f  MEXC time fetch failed, using local clock: {e}")
    # Final fallback
    return str(int(time.time() * 1000))


def _run_startup_diagnostics():
    """Run on bot startup: log and Telegram-notify IP, key presence, trading mode."""
    import time as _time
    _time.sleep(3)   # wait for Telegram config to settle

    # ── Outbound IP ─────────────────────────────────────────────────────
    outbound_ip = "unknown"
    try:
        r = requests.get("https://api.ipify.org?format=json", timeout=10)
        outbound_ip = r.json().get("ip", "unknown")
    except Exception as e:
        outbound_ip = f"error ({e})"

    # ── Key info (never log full key/secret) ─────────────────────────────
    api_key    = trade_config.get("api_key", "").strip()
    api_secret = trade_config.get("api_secret", "").strip()
    key_last4  = ("…" + api_key[-4:])  if len(api_key)    >= 4 else ("(not set)" if not api_key    else api_key)
    sec_status = "✅ Present"          if api_secret       else "❌ MISSING"

    # ── MEXC Futures reachability ping ──────────────────────────────────
    mexc_ping = "unknown"
    try:
        rp = requests.get("https://contract.mexc.com/api/v1/contract/ping", timeout=8)
        mexc_ping = f"HTTP {rp.status_code}"
    except Exception as e:
        mexc_ping = f"error ({e})"

    # ── USDT Balance ─────────────────────────────────────────────────────
    startup_bal = "–"
    bal_err     = ""
    if api_key and api_secret and "PASTE" not in api_key:
        bal_v, b_err = get_account_balance()
        if not b_err:
            startup_bal = f"${bal_v:.2f} USDT"
            log(f"💰 Current USDT Balance: ${bal_v:.2f} (usable for trading)")
        else:
            startup_bal = f"Error: {b_err}"
            bal_err     = b_err
            log(f"⚠️  Balance fetch on startup: {b_err}")
    else:
        log("⚠️  API keys not set — skipping startup balance fetch")

    msg_lines = [
        "═══ <b>SIGNALCORE BOT STARTUP</b> ═══",
        f"<b>Outbound IP:</b>     <code>{outbound_ip}</code>",
        f"<b>MEXC Key (last4):</b> <code>{key_last4}</code>",
        f"<b>MEXC Secret:</b>     {sec_status}",
        f"<b>Trading Mode:</b>    {'Cross Margin (openType=2)' if trade_config.get('margin_mode',2)==2 else 'Isolated Margin (openType=1)'}",
        f"<b>MEXC Futures:</b>    {mexc_ping}",
        f"<b>💰 USDT Balance:</b>  {startup_bal}",
        f"<b>Auto-trade:</b>      {'✅ ENABLED' if trade_config.get('enabled') else '⏸ Disabled'}",
        "═══════════════════════════════",
    ]
    diag_msg = "\n".join(msg_lines)

    log(f"🔍 STARTUP DIAGNOSTICS:")
    log(f"   Outbound IP  : {outbound_ip}")
    log(f"   MEXC Key last4: {key_last4}")
    log(f"   MEXC Secret  : {sec_status}")
    log(f"   Trading mode : {'Cross Margin (openType=2)' if trade_config.get('margin_mode',2)==2 else 'Isolated Margin (openType=1)'}")
    log(f"   MEXC ping    : {mexc_ping}")
    log(f"   USDT Balance : {startup_bal}")
    log(f"   Auto-trade   : {'ON' if trade_config.get('enabled') else 'OFF'}")

    send_telegram(diag_msg, kind="trade")


def mexc_request(method, path, params=None, signed=True, _retry=0):
    """
    Authenticated MEXC Futures private API request.

    KEY FIX — signature and body must use the EXACT same bytes:
      - We serialise params to a JSON string ONCE with sorted keys
      - That same string is used for the signature AND sent as the raw body
      - Using requests(json=...) is NOT safe because Python may reorder keys

    HTTP 403 / code=602 root causes fixed here:
      1. Always HTTPS (never HTTP — redirect strips POST body → unsigned request)
      2. Signature built on exact body bytes (sorted keys, no spaces)
      3. Body sent as pre-encoded bytes with Content-Type: application/json

    Retries: up to 3 attempts on network errors or 5xx responses.
    """
    import json as _json

    api_key    = trade_config.get("api_key",    "").strip()
    api_secret = trade_config.get("api_secret", "").strip()
    if not api_key or not api_secret or "PASTE" in api_key or "PASTE" in api_secret:
        return None, "API keys not configured"

    params = params or {}
    ts     = _get_mexc_ts()   # pulled directly from MEXC server clock — zero drift

    # ── Build the exact request_param string for signature ──────────────
    if method.upper() == "GET":
        # GET: alphabetically sorted urlencode (standard REST)
        query_str  = urllib.parse.urlencode(sorted(params.items())) if params else ""
        body_bytes = None
    else:
        # POST: sorted-key JSON, no spaces — sign and send the SAME bytes
        body_str   = _json.dumps(params, sort_keys=True, separators=(",", ":")) if params else ""
        query_str  = body_str
        body_bytes = body_str.encode("utf-8")

    signature = mexc_sign(api_key, ts, query_str, api_secret)

    headers = {
        "Content-Type": "application/json",
        "ApiKey":        api_key,
        "Request-Time":  ts,
        "Signature":     signature,
    }

    # Always HTTPS — never follow redirects on POST (body would be stripped)
    base = "https://contract.mexc.com/api/v1/private"
    url  = f"{base}{path}"

    # ── Detailed pre-request logging ────────────────────────────────────
    key_hint = ("…" + api_key[-4:]) if len(api_key) >= 4 else api_key
    if method.upper() == "POST":
        log(f"📤 MEXC {method} {path} | key=…{key_hint} | ts={ts} | body={body_str}")
    else:
        log(f"📤 MEXC {method} {path} | key=…{key_hint} | ts={ts} | params={query_str[:200]}")

    try:
        if method.upper() == "GET":
            r = requests.get(url, params=params, headers=headers,
                             timeout=15, allow_redirects=False)
        else:
            r = requests.post(url, data=body_bytes, headers=headers,
                              timeout=15, allow_redirects=False)

        # If somehow still redirected, re-fire to HTTPS with same body
        if r.status_code in (301, 302, 307, 308):
            loc = r.headers.get("Location", url).replace("http://", "https://")
            log(f"↩️  MEXC redirect {r.status_code} → {loc}")
            if method.upper() == "GET":
                r = requests.get(loc, params=params, headers=headers, timeout=15)
            else:
                r = requests.post(loc, data=body_bytes, headers=headers, timeout=15)

        raw_text = r.text.strip() if r.text else ""

        # ── Full 403 / non-200 logging ───────────────────────────────────
        if r.status_code == 403:
            current_ip = _get_cached_ip()
            log(f"🚫 MEXC 403 ACCESS DENIED on {path}")
            log(f"   HTTP status  : 403")
            log(f"   Response body: {raw_text[:500]}")
            log(f"   Request URL  : {url}")
            log(f"   ApiKey hint  : {key_hint}")
            log(f"   Timestamp    : {ts}")
            log(f"   Signed body  : {query_str[:300]}")
            log(f"   Possible causes:")
            log(f"     1. IP {current_ip} not whitelisted on MEXC")
            log(f"     2. Futures trading permission not enabled on API key")
            log(f"     3. API key or secret mismatch / copy-paste error")
            log(f"     4. Timestamp drift > 5000ms — check server clock")
            # Auto-alert via Telegram so the user can whitelist the correct IP
            tg_alert = (
                "🚫 <b>MEXC 403 ACCESS DENIED</b>\n\n"
                f"Bot outbound IP: <code>{current_ip}</code>\n\n"
                "<b>How to fix:</b>\n"
                "1. Go to MEXC → API Management\n"
                "2. Edit your API key\n"
                "3. Set IP restriction to <b>No restriction</b> "
                f"OR whitelist: <code>{current_ip}</code>\n"
                "4. Make sure <b>Futures trading</b> permission is enabled\n\n"
                f"Endpoint: <code>{path}</code>"
            )
            send_telegram(tg_alert, kind="trade")
            return None, f"HTTP 403 Access Denied — body: {raw_text[:200]}"

        if r.status_code >= 500:
            log(f"⚠️  MEXC server error {r.status_code} on {path}: {raw_text[:200]}")
            if _retry < 2:
                log(f"   Retrying ({_retry+1}/2) after 2s …")
                time.sleep(2)
                return mexc_request(method, path, params, signed, _retry+1)
            return None, f"MEXC server error (HTTP {r.status_code})"

        if not raw_text:
            return ({}, None) if r.status_code in (200, 201) else \
                   (None, f"Empty response (HTTP {r.status_code})")

        try:
            data = r.json()
        except ValueError:
            return None, f"Non-JSON response (HTTP {r.status_code}): {raw_text[:300]}"

        if data.get("success") is True or str(data.get("code", "")) in ("0", "200"):
            log(f"✅ MEXC {method} {path} → OK | data={str(data.get('data',''))[:120]}")
            return data.get("data") or {}, None

        err_msg = data.get("message") or data.get("msg") or f"code={data.get('code')}"
        log(f"❌ MEXC API error on {path}: {err_msg} | code={data.get('code')} | ts={ts}")
        log(f"   Full response: {raw_text[:400]}")

        # Retry on rate-limit or temporary errors
        if _retry < 2 and str(data.get("code", "")) in ("10007", "429", "1005"):
            log(f"   Rate-limited — retrying ({_retry+1}/2) after 1s …")
            time.sleep(1)
            return mexc_request(method, path, params, signed, _retry+1)

        return None, err_msg

    except requests.exceptions.Timeout:
        log(f"⏱️  MEXC timeout on {path}")
        if _retry < 2:
            log(f"   Retrying ({_retry+1}/2) after 3s …")
            time.sleep(3)
            return mexc_request(method, path, params, signed, _retry+1)
        return None, "Request timed out (15 s)"
    except Exception as e:
        log(f"💥 MEXC exception on {path}: {e}")
        return None, str(e)


# ── Cached outbound IP (set at startup) ──────────────────────────────
_cached_ip = "unknown"

def _get_cached_ip():
    return _cached_ip

def get_account_balance():
    """
    Get available USDT balance from MEXC Futures account.

    MEXC /account/assets returns a LIST of assets — one per currency.
    Each asset dict looks like:
      {
        "currency":        "USDT",
        "positionMargin":  0,
        "availableBalance": 1.10,   ← this is the usable balance
        "cashBalance":      1.10,
        "frozenBalance":    0,
        "equity":           1.10,
        "unrealized":       0,
        "bonus":            0
      }

    The fields `cashBalance` and `availableBalance` on the USDT entry
    both reflect the real usable balance. We prefer (in order):
      equity > availableBalance > cashBalance > walletBalance > available

    Note: other currencies (STETH etc.) in the list will have 0 values —
    we skip them and look specifically for the USDT entry.
    """
    data, err = mexc_request("GET", "/account/assets")
    if err:
        log(f"❌ Balance fetch error: {err}")
        return 0.0, err
    if not data:
        log("❌ Balance fetch: no data returned")
        return 0.0, "No data returned"

    # MEXC returns a list of asset objects
    assets = data if isinstance(data, list) else [data]

    log(f"🔍 Balance raw response ({len(assets)} assets): {str(assets)[:300]}")

    for asset in assets:
        currency = (asset.get("currency") or asset.get("coin") or "").upper()
        if currency != "USDT":
            continue

        # Try fields in priority order — equity is the total account value,
        # availableBalance is what can be used for new positions
        bal = (
            asset.get("equity")            or
            asset.get("availableBalance")  or
            asset.get("cashBalance")       or
            asset.get("walletBalance")     or
            asset.get("available")         or
            0
        )
        bal = float(bal)

        log(f"✅ USDT asset found: equity={asset.get('equity')} "
            f"availableBalance={asset.get('availableBalance')} "
            f"cashBalance={asset.get('cashBalance')} "
            f"→ using {bal:.4f} USDT")
        log(f"💰 Current USDT Balance: ${bal:.2f} (usable for trading)")
        return bal, None

    # Fallback: if data is a single dict (non-list response) try direct fields
    if isinstance(data, dict):
        bal = float(
            data.get("equity") or
            data.get("availableBalance") or
            data.get("cashBalance") or
            data.get("walletBalance") or
            data.get("available") or
            0
        )
        if bal > 0:
            log(f"💰 Current USDT Balance (direct): ${bal:.2f} (usable for trading)")
            return bal, None

    log("⚠️ USDT not found in assets list — full asset dump: " + str(assets)[:500])
    return 0.0, "USDT balance not found in response — check account has USDT in futures wallet"


def get_symbol_info(symbol):
    try:
        r = requests.get(f"{MEXC_BASE}/detail", timeout=10)
        data = r.json()
        for item in data.get("data", []):
            if item.get("symbol") == symbol:
                return {
                    "min_vol":    float(item.get("minVol", 1)),
                    "contract_size": float(item.get("contractSize", 1)),
                    "price_unit": float(item.get("priceUnit", 0.01)),
                }
    except: pass
    return {"min_vol": 1, "contract_size": 1, "price_unit": 0.01}

def calc_position_size(symbol, entry, sl, balance):
    risk_amount = balance * trade_config["risk_pct"] / 100
    sl_distance = abs(entry - sl)
    if sl_distance <= 0: return 0
    info = get_symbol_info(symbol)
    contracts = risk_amount / (sl_distance * info["contract_size"])
    min_vol = info["min_vol"]
    contracts = max(min_vol, round(contracts / min_vol) * min_vol)
    return int(contracts)

def live_trade_ready():
    """Returns (True, '') if live auto-trade can fire, else (False, reason)."""
    if not trade_config.get("enabled", False):
        return False, "auto-trade disabled"
    if not trade_config.get("api_key", ""):
        return False, "no API key"
    with trade_lock:
        open_count = len(open_trades)
    if open_count >= trade_config.get("max_trades", 3):
        return False, f"max trades reached ({open_count})"
    return True, ""

def place_order(sig):
    with trade_lock:
        if not trade_config["enabled"]:
            return False, "Auto-trade disabled"
        if len(open_trades) >= trade_config["max_trades"]:
            return False, f"Max trades ({trade_config['max_trades']}) reached"
        if sig["symbol"] in open_trades:
            return False, f"Already have open trade on {sig['symbol']}"

    balance, err = get_account_balance()
    if err: return False, f"Balance error: {err}"
    if balance < 0.1: return False, "Insufficient balance (min $0.1)"

    total_used = len(open_trades) * (balance * 0.20)
    if total_used >= balance * 0.80:
        return False, "80% balance cap reached across open trades"

    entry = float(sig["entry"])
    sl    = float(sig["sl"])
    tp    = float(sig["tp"])

    # ── STRICT RISK MANAGEMENT ──────────────────────────────────────
    # margin     = 20% of balance (cross margin per trade)
    # max_loss   = 100% of margin (hard cap — SL CANNOT exceed this)
    # leverage   = calculated so that: size * sl_dist * leverage <= max_loss
    # Formula:   leverage = max_loss / (margin * sl_pct)
    #            size (contracts) = margin * leverage / entry / contract_size

    margin    = max(0.1, balance * 0.20)
    max_loss  = margin * 1.00   # SL can cost AT MOST 100% of margin
    info      = get_symbol_info(sig["symbol"])
    cs        = max(info["contract_size"], 1e-8)   # contract size (USDT per contract)

    sl_dist = abs(entry - sl)
    if sl_dist <= 0: return False, "SL distance is zero"
    sl_pct  = sl_dist / entry if entry > 0 else 0.01

    # Step 1: Choose leverage so SL loss = exactly 100% of margin
    # Loss = position_value * sl_pct = (size * cs * entry / leverage) * sl_pct * leverage
    #      = size * cs * entry * sl_pct   (leverage cancels for cross margin loss calc)
    # Wait — for FUTURES: Loss = size * contract_size * sl_dist
    # So: size = max_loss / (sl_dist * cs)
    # Then leverage = size * cs * entry / margin  (= position_value / margin)
    # Cap leverage at 500x and minimum 10x

    size_raw  = max_loss / (sl_dist * cs)
    size      = max(int(info["min_vol"]), int(size_raw))

    # Calculate what leverage that implies and cap it
    position_value = size * cs * entry
    if margin > 0:
        implied_lev = position_value / margin
    else:
        implied_lev = 10

    # Cap leverage: 10x minimum, 500x maximum
    leverage = max(10, min(500, int(implied_lev)))

    # SAFETY CHECK: verify actual max loss with this size+leverage
    # actual_loss = size * cs * sl_dist  (leveraged futures loss)
    actual_max_loss = size * cs * sl_dist
    if actual_max_loss > max_loss * 1.05:   # allow 5% tolerance
        # Scale down size to stay within margin
        size = max(int(info["min_vol"]), int(size * (max_loss / actual_max_loss)))
        actual_max_loss = size * cs * sl_dist

    loss_pct_of_margin = (actual_max_loss / margin * 100) if margin > 0 else 0
    log(f"💰 Risk check: margin=${margin:.2f} | leverage={leverage}x | "
        f"size={size} | SL loss=${actual_max_loss:.2f} ({loss_pct_of_margin:.1f}% of margin)")

    # HARD BLOCK: refuse trade if SL loss would exceed 100% of margin
    if actual_max_loss > max_loss * 1.10:
        return False, f"SL loss ${actual_max_loss:.2f} exceeds margin ${margin:.2f} — trade rejected"

    if size <= 0: return False, "Position size too small"

    # ── MEXC Futures side values ──────────────────────────────────────
    # 1 = open long  (BUY to open),   2 = close long  (SELL to close)
    # 3 = open short (SELL to open),  4 = close short (BUY to close)
    side      = 1 if sig["direction"] == "BUY" else 3   # open long or open short
    open_type = trade_config.get("margin_mode", 2)      # 1=isolated, 2=cross

    # Determine order type: market_order signals use type=5 (market), others use type=1 (limit)
    is_market  = sig.get("market_order", False)
    order_type = 5 if is_market else 1
    order_price = 0 if is_market else round(float(entry), 8)

    # ── Set leverage first (ignore errors — MEXC may reject if already set) ──
    # positionType: 1=long side, 2=short side (must match the order side)
    pos_type = 1 if sig["direction"] == "BUY" else 2
    lev_params = {
        "symbol":       sig["symbol"],
        "leverage":     leverage,
        "openType":     open_type,
        "positionType": pos_type,
    }
    lev_data, lev_err = mexc_request("POST", "/position/change_leverage", lev_params)
    if lev_err:
        log(f"⚠️  Leverage set failed (continuing anyway): {lev_err}")
    else:
        log(f"✅ Leverage set: {leverage}x on {sig['symbol']} ({pos_type=})")

    # ── Place order with SL/TP embedded ───────────────────────────────
    # NOTE: do NOT include "leverage" here — cross-margin accounts reject it.
    # Leverage is already applied via /position/change_leverage above.
    order_params = {
        "symbol":           sig["symbol"],
        "price":            order_price,
        "vol":              size,
        "side":             side,
        "type":             order_type,
        "openType":         open_type,
        "stopLossPrice":    round(float(sl), 8),
        "takeProfitPrice":  round(float(tp), 8),
    }

    log(f"📋 ORDER ATTEMPT: {sig['direction']} {sig['symbol']}")
    log(f"   side={side} type={'MARKET' if is_market else 'LIMIT'} vol={size}")
    log(f"   entry={order_price} SL={sl} TP={tp}")
    log(f"   leverage={leverage}x openType={open_type} ({'Cross' if open_type==2 else 'Isolated'} Margin)")
    log(f"   margin=${margin:.2f} max_loss=${max_loss:.2f}")

    order_data, err = mexc_request("POST", "/order/submit", order_params)
    if err:
        log(f"❌ ORDER FAILED for {sig['symbol']}: {err}")
        return False, f"Order failed: {err}"

    log(f"✅ ORDER SUCCESS: {sig['symbol']} | response={str(order_data)[:200]}")

    order_id = (order_data if isinstance(order_data, (str, int))
                else (order_data.get("orderId") or order_data.get("order_id") or "")
                if isinstance(order_data, dict) else "")

    with trade_lock:
        open_trades[sig["symbol"]] = {
            "order_id":  order_id,
            "symbol":    sig["symbol"],
            "direction": sig["direction"],
            "entry":     entry,
            "sl":        sl,
            "tp":        tp,
            "size":      size,
            "rr":        sig["rr"],
            "score":     sig["score"],
            "grade":     sig["grade"],
            "opened_at": datetime.now(LOCAL_TZ).strftime("%Y-%m-%d %H:%M UTC+1"),
            "status":    "OPEN",
        }

    log(f"🤖 AUTO-TRADE PLACED: {sig['direction']} {sig['symbol']} Entry:{entry} SL:{sl} TP:{tp} Size:{size}")
    tg_msg = (
        "<b>AUTO-TRADE PLACED</b>\n"
        "---\n"
        f"<b>Pair:</b> {sig['symbol']}\n"
        f"<b>Side:</b> {sig['direction']}\n"
        f"<b>Entry:</b> {entry}\n"
        f"<b>SL:</b> {sl}\n"
        f"<b>TP:</b> {tp}\n"
        f"<b>Size:</b> {size} contracts\n"
        f"<b>RR:</b> {sig['rr']}R | Score: {sig['score']}/100 {sig['grade']}\n"
        "<i>Mad Man Model #1 Auto-Trade</i>"
    )
    send_telegram(tg_msg, kind="trade")
    return True, f"Order placed: {order_id}"

def close_trade(symbol, reason="Manual"):
    with trade_lock:
        if symbol not in open_trades:
            return False, "No open trade found"
        trade = open_trades[symbol]

    # Close long = side 2, Close short = side 4
    side = 2 if trade["direction"] == "BUY" else 4
    params = {
        "symbol":    symbol,
        "price":     0,
        "vol":       trade["size"],
        "side":      side,
        "type":      5,          # market close
        "openType":  trade_config.get("margin_mode", 2),
    }
    _, err = mexc_request("POST", "/order/submit", params)
    if err: return False, f"Close failed: {err}"

    with trade_lock:
        completed = dict(open_trades[symbol])
        completed["status"]    = f"CLOSED ({reason})"
        completed["closed_at"] = datetime.now(LOCAL_TZ).strftime("%Y-%m-%d %H:%M UTC+1")
        recent_trades.appendleft(completed)
        del open_trades[symbol]

    log(f"TRADE CLOSED: {symbol} | Reason: {reason}")
    send_telegram(
        f"TRADE CLOSED: {symbol} {completed['direction']}\n"
        f"Entry: {completed['entry']} | Size: {completed['size']}\n"
        f"Reason: {reason}", kind="trade"
    )
    return True, "Position closed"


# ════════ PAPER TRADING ENGINE ════════════════════════════════════════

def place_paper_order(sig):
    """Place a simulated paper trade based on a signal."""
    with paper_lock:
        if not paper_config["enabled"]:
            return False, "Paper trading disabled"
        if not paper_config["auto_trade"]:
            return False, "Paper auto-trade disabled"
        if len(paper_trades) >= paper_config["max_trades"]:
            return False, f"Max paper trades ({paper_config['max_trades']}) reached"
        if sig["symbol"] in paper_trades:
            return False, f"Already have paper trade on {sig['symbol']}"

        balance    = paper_config["balance"]
        entry      = float(sig["entry"])
        sl         = float(sig["sl"])
        tp         = float(sig["tp"])
        risk_amount = balance * paper_config["risk_pct"] / 100
        sl_distance = abs(entry - sl)
        if sl_distance <= 0:
            return False, "SL distance is zero"

        contracts = round(risk_amount / sl_distance, 6)

        paper_trades[sig["symbol"]] = {
            "symbol":       sig["symbol"],
            "direction":    sig["direction"],
            "entry":        entry,
            "current_price":entry,
            "sl":           sl,
            "tp":           tp,
            "size":         contracts,
            "risk_amount":  round(risk_amount, 2),
            "rr":           sig["rr"],
            "score":        sig["score"],
            "grade":        sig["grade"],
            "tf":           sig.get("tf","–"),
            "ob_zone":      sig.get("ob_zone","–"),
            "pnl":          0.0,
            "pnl_pct":      0.0,
            "opened_at":    datetime.now(LOCAL_TZ).strftime("%Y-%m-%d %H:%M UTC+1"),
            "status":       "OPEN",
        }

    log(f"📝 PAPER TRADE: {sig['direction']} {sig['symbol']} Entry:{entry} SL:{sl} TP:{tp} Risk:${risk_amount:.2f}")
    return True, f"Paper trade placed on {sig['symbol']}"


def close_paper_trade(symbol, reason="Manual", close_price=None):
    """Close a paper trade and settle PnL against paper balance."""
    with paper_lock:
        if symbol not in paper_trades:
            return False, "No paper trade found"
        trade = dict(paper_trades[symbol])

    if close_price is None:
        ticker = get_ticker(symbol)
        close_price = ticker["price"] if ticker else trade["entry"]

    entry     = trade["entry"]
    size      = trade["size"]
    direction = trade["direction"]

    if direction == "BUY":
        pnl = (close_price - entry) * size
    else:
        pnl = (entry - close_price) * size

    risk_amount = trade["risk_amount"] if trade["risk_amount"] > 0 else 1.0
    # Hard cap: a single trade can never lose more than the configured risk amount
    if pnl < -risk_amount:
        pnl = -risk_amount
    pnl_pct     = round((pnl / risk_amount) * 100, 2)

    with paper_lock:
        paper_config["balance"] = round(paper_config["balance"] + pnl, 2)
        completed = dict(paper_trades[symbol])
        completed.update({
            "status":      f"CLOSED ({reason})",
            "close_price": round(close_price, 8),
            "pnl":         round(pnl, 2),
            "pnl_pct":     pnl_pct,
            "closed_at":   datetime.now(LOCAL_TZ).strftime("%Y-%m-%d %H:%M UTC+1"),
        })
        paper_history.appendleft(completed)
        del paper_trades[symbol]

        paper_stats["total"]     += 1
        if pnl > 0: paper_stats["wins"]  += 1
        else:       paper_stats["losses"] += 1
        paper_stats["total_pnl"] = round(paper_stats["total_pnl"] + pnl, 2)

    sign = "+" if pnl >= 0 else ""
    log(f"📝 PAPER CLOSED: {symbol} {direction} PnL:{sign}{pnl:.2f} USDT | {reason}")
    return True, f"Paper trade closed. PnL: {sign}{pnl:.2f} USDT"


def paper_monitor_loop():
    """Background thread that watches paper positions for SL/TP hits."""
    log("📝 Paper trading monitor started")
    while True:
        try:
            with paper_lock:
                symbols = list(paper_trades.keys())

            for symbol in symbols:
                with paper_lock:
                    if symbol not in paper_trades: continue
                    trade = dict(paper_trades[symbol])

                ticker = get_ticker(symbol)
                if not ticker: continue
                price = ticker["price"]

                entry     = trade["entry"]
                size      = trade["size"]
                direction = trade["direction"]
                sl        = trade["sl"]
                tp        = trade["tp"]

                if direction == "BUY":
                    pnl = (price - entry) * size
                else:
                    pnl = (entry - price) * size

                risk_amount = max(trade["risk_amount"], 1.0)
                pnl_pct     = round((pnl / risk_amount) * 100, 2)

                with paper_lock:
                    if symbol in paper_trades:
                        paper_trades[symbol]["current_price"] = round(price, 8)
                        paper_trades[symbol]["pnl"]           = round(pnl, 2)
                        paper_trades[symbol]["pnl_pct"]       = pnl_pct

                if direction == "BUY":
                    if price <= sl:
                        close_paper_trade(symbol, "SL Hit", sl)   # close at SL, not market price
                    elif price >= tp:
                        close_paper_trade(symbol, "TP Hit", tp)   # close at TP, not market price
                else:
                    if price >= sl:
                        close_paper_trade(symbol, "SL Hit", sl)   # close at SL, not market price
                    elif price <= tp:
                        close_paper_trade(symbol, "TP Hit", tp)   # close at TP, not market price

        except Exception as e:
            log(f"❌ Paper monitor error: {e}")

        time.sleep(15)


# ════════ MANIPULATION PHASE MONITOR ════════════════════════════════

def detect_manip_phase(candles, direction, crt_tf="Hour4"):
    """
    Detect C2 (manipulation candle) that is CURRENTLY FORMING and:
    - Has already swept below CRL (bull) or above CRH (bear) with its body
    - Has NOT yet closed back inside the CRT range (still in manipulation)
    - Has between 1 second and 59 minutes REMAINING before the candle closes

    Only looks at the LAST candle (index -1) as active C2.
    Uses candle open time + TF duration to compute time remaining.
    """
    direction = _d(direction)
    pending = []
    if len(candles) < 3: return pending
    is_buy = direction in ("BUY", "BULLISH")

    now_ts  = int(time.time())
    tf_secs = TF_SECONDS.get(crt_tf, 3600)

    # Only the last candle is C2 candidate (currently forming)
    c2 = candles[-1]
    c1 = candles[-2]
    crh = c1["high"]; crl = c1["low"]
    cr_range = crh - crl
    if cr_range <= 0: return pending

    # Calculate time remaining in the C2 candle
    c2_open_ts = int(c2["time"])
    if c2_open_ts > 1e10: c2_open_ts //= 1000   # ms → s
    c2_close_ts = c2_open_ts + tf_secs
    secs_left   = c2_close_ts - now_ts
    mins_left   = max(0, secs_left // 60)

    # Only flag if 1 min <= time_left <= 40 min BEFORE close
    if not (1 <= secs_left <= 3540):
        return pending

    if is_buy:
        swept     = c2["low"]   < crl
        still_out = c2["close"] < crl   # body still below CRL
        if swept and still_out:
            pending.append({
                "c1": c1, "c2": c2, "crh": crh, "crl": crl,
                "sweep_low":  round(c2["low"], 8),
                "direction":  "BUY",
                "phase":      "MANIPULATION",
                "mins_left":  mins_left,
            })
    else:
        swept     = c2["high"]  > crh
        still_out = c2["close"] > crh
        if swept and still_out:
            pending.append({
                "c1": c1, "c2": c2, "crh": crh, "crl": crl,
                "sweep_high": round(c2["high"], 8),
                "direction":  "SELL",
                "phase":      "MANIPULATION",
                "mins_left":  mins_left,
            })
    return pending

def detect_manip_phase_live(candles, direction, tf_name, min_mins=0.016, max_mins=59):
    """
    Detect if the CURRENTLY FORMING candle is in manipulation phase
    with 1–40 minutes remaining before it closes.
    The last candle in the series is treated as the live, still-forming C2.
    """
    direction = _d(direction)
    pending = []
    if len(candles) < 3: return pending

    mins_left = get_minutes_remaining(tf_name)
    if mins_left < min_mins or mins_left > max_mins:
        return pending  # Outside the valid window — too early or candle already closed

    c2 = candles[-1]   # Live, still-forming manipulation candle
    c1 = candles[-2]   # Previous completed reference candle (C1)

    crh = c1["high"]; crl = c1["low"]
    cr_range = crh - crl
    if cr_range <= 0: return pending

    is_buy = direction in ("BUY", "BULLISH")
    if is_buy:
        swept      = c2["low"] < crl
        still_below = c2["close"] < crl
        if swept and still_below:
            pending.append({
                "c1": c1, "c2": c2, "crh": crh, "crl": crl,
                "sweep_low": c2["low"], "direction": "BUY",
                "phase": "MANIPULATION", "mins_left": round(mins_left, 1)
            })
    else:
        swept       = c2["high"] > crh
        still_above = c2["close"] > crh
        if swept and still_above:
            pending.append({
                "c1": c1, "c2": c2, "crh": crh, "crl": crl,
                "sweep_high": c2["high"], "direction": "SELL",
                "phase": "MANIPULATION", "mins_left": round(mins_left, 1)
            })
    return pending


def check_manip_completed(symbol, monitor):
    tf = monitor.get("crt_tf", "Hour4")
    candles = get_candles(symbol, tf, limit=50)
    if not candles: return False, []
    crh = monitor["crh"]; crl = monitor["crl"]
    direction = monitor["direction"]
    recent = candles[-5:]
    for c in recent:
        if direction == "BUY":
            if c["close"] > crl and c["close"] <= crh:
                return True, candles
        else:
            if c["close"] < crh and c["close"] >= crl:
                return True, candles
    return False, candles


def _reset_to_watching(symbol, keep_zone=True):
    """
    Reset a symbol's monitor entry back to WATCHING phase so it can hunt
    the next CRT on the same active HTF zone.  All trade-specific state
    is cleared; zone context (zone_name, zone_top, zone_bot, trend, etc.)
    is preserved so we never need to re-qualify the HTF setup.
    Called after TP hit, SL hit, or TBS invalidation.
    """
    with manip_lock:
        if symbol not in manip_monitor:
            return
        if not keep_zone:
            manip_monitor.pop(symbol, None)
            return
        m = manip_monitor[symbol]
        # Preserve zone/trend context, wipe all CRT/TBS/trade state
        manip_monitor[symbol] = {
            "phase":      "WATCHING",
            "direction":  m.get("direction"),
            "crt_tf":     m.get("crt_tf"),
            "ob_tf":      m.get("ob_tf"),
            "zone_name":  m.get("zone_name"),
            "zone_top":   m.get("zone_top"),
            "zone_bot":   m.get("zone_bot"),
            "kl_type":    m.get("kl_type"),
            "trend":      m.get("trend"),
            "added_at":   m.get("added_at"),
            "reset_at":   datetime.now(LOCAL_TZ).strftime("%H:%M UTC+1"),
        }
    log(f"🔄 M1 RESET → WATCHING: {symbol} — hunting next CRT on same zone")


def _zone_still_valid(symbol, monitor):
    """
    Check the HTF zone is still valid:
      - Trend on 4H must still match direction
      - Price must not have closed through the far side of the zone
    If invalid, the monitor entry should be removed entirely.
    Returns True if zone is still good.
    """
    direction = monitor.get("direction", "BUY")
    zone_top  = monitor.get("zone_top", 0)
    zone_bot  = monitor.get("zone_bot", 0)
    crt_tf    = monitor.get("crt_tf", "Hour4")

    # Re-check trend
    ref = get_candles(symbol, "Hour4", limit=100)
    if ref and len(ref) >= 30:
        trend, _, _ = detect_trend(ref)
        expected = "BULLISH" if direction == "BUY" else "BEARISH"
        if trend != expected and trend != "NEUTRAL":
            log(f"🚫 M1 ZONE EXPIRED: {symbol} — trend flipped to {trend}")
            return False

    # Re-check price hasn't broken through the far side
    candles = get_candles(symbol, crt_tf, limit=30)
    if candles:
        if not _price_still_inside_htf_range(candles, zone_top, zone_bot, direction):
            log(f"🚫 M1 ZONE EXPIRED: {symbol} — price broke through far side of zone")
            return False

    return True


def manip_monitor_loop():
    """
    Persistent 4-phase monitor per symbol.  A symbol stays in the monitor
    forever (across multiple trades) as long as the HTF zone is still valid.

    PHASES
    ──────
    WATCHING   — waiting for a fresh CRT to form inside the HTF zone.
                 Scans the CRT timeframe on every tick for a new C1/C2/C3.
                 When a CRT is detected with C2 back inside range → AWAIT_TBS.

    AWAIT_TBS  — CRT confirmed (C2 closed back inside range).
                 Polls LTF every tick for the TBS candle
                 (opens inside CRT range, body closes outside).
                 When TBS found → AWAIT_PRICE.
                 Invalidation: new candle breaks beyond CRH/CRL with body
                 → reset to WATCHING.

    AWAIT_PRICE — TBS confirmed.  Watches live price every tick.
                 When price trades back to (or through) TBS open → fire
                 instant market order → RUNNING.
                 Invalidation: price closes decisively beyond TBS SL
                 → reset to WATCHING (setup voided).

    RUNNING    — trade is live.  Tracks price against TP and SL.
                 TP hit or SL hit → reset to WATCHING (NOT removed —
                 zone still active, hunt next CRT).
                 Zone validity is re-checked every 60 s; if zone expired
                 the entry is fully removed.
    """
    log("🔍 M1 Manipulation monitor started (persistent zone mode)")
    zone_check_counter = {}   # symbol → iteration count for periodic zone recheck

    while True:
        try:
            with manip_lock:
                symbols = list(manip_monitor.keys())

            for symbol in symbols:
                with manip_lock:
                    if symbol not in manip_monitor: continue
                    monitor = dict(manip_monitor[symbol])

                phase     = monitor.get("phase", "WATCHING")
                direction = monitor.get("direction", "BUY")
                crt_tf    = monitor.get("crt_tf", "Hour4")
                zone_top  = monitor.get("zone_top", 0)
                zone_bot  = monitor.get("zone_bot", 0)
                zone_name = monitor.get("zone_name", "–")
                trend     = monitor.get("trend", "NEUTRAL")
                kl_type   = monitor.get("kl_type", "KL")

                # ── Periodic zone validity check (every ~60 s) ───────────
                cnt = zone_check_counter.get(symbol, 0) + 1
                zone_check_counter[symbol] = cnt
                if cnt % 12 == 0:   # 12 × 5 s = 60 s
                    if not _zone_still_valid(symbol, monitor):
                        with manip_lock: manip_monitor.pop(symbol, None)
                        zone_check_counter.pop(symbol, None)
                        continue

                # ════════════════════════════════════════════════════════
                # PHASE: WATCHING
                # Hunt for a fresh CRT inside the active HTF zone.
                # ════════════════════════════════════════════════════════
                if phase == "WATCHING":
                    crt_candles = get_candles(symbol, crt_tf, limit=60)
                    if not crt_candles or len(crt_candles) < 5:
                        continue

                    all_crts  = detect_crt(crt_candles, direction, ob=None)
                    # Any CRT moving away from the zone in the trend direction is valid
                    # Not just CRTs overlapping the zone — once price reacts from the
                    # zone it will form CRTs further away as it moves
                    zone_crts = [
                        c for c in all_crts
                        if (direction == "SELL" and c["crh"] <= zone_top * 1.01) or
                           (direction == "BUY"  and c["crl"] >= zone_bot * 0.99)
                    ]

                    if not zone_crts:
                        # Check if manip phase is forming (pre-CRT)
                        manip_live = detect_manip_phase_live(crt_candles, direction, crt_tf)
                        manip_hist = detect_manip_phase(crt_candles, direction, crt_tf) \
                                     if not manip_live else []
                        candidates = manip_live or manip_hist
                        in_zone    = [
                            m for m in candidates
                            if (direction == "SELL" and m["crh"] <= zone_top * 1.01) or
                               (direction == "BUY"  and m["crl"] >= zone_bot * 0.99)
                        ]
                        if in_zone:
                            mp = in_zone[0]
                            with manip_lock:
                                if symbol in manip_monitor:
                                    manip_monitor[symbol].update({
                                        **mp,
                                        "phase":    "WATCHING",
                                        "crh":      mp["crh"],
                                        "crl":      mp["crl"],
                                        "c2":       mp.get("c2", {}),
                                        "manip_at": datetime.now(LOCAL_TZ).strftime("%H:%M UTC+1"),
                                    })
                            log(f"👁 M1 {symbol} — manip forming | "
                                f"{mp.get('mins_left','?')}m left | "
                                f"CRH:{mp['crh']} CRL:{mp['crl']}")
                        continue

                    # Fresh completed CRT found
                    crt = zone_crts[0]
                    crt_crh = crt["crh"]
                    crt_crl = crt["crl"]
                    log(f"✅ M1 CRT FOUND: {symbol} {direction} on {crt_tf} | "
                        f"CRH:{crt_crh} CRL:{crt_crl}")

                    # Immediately check if TBS is already on the LTF
                    tbs_found, tbs_tf, tbs_entry, tbs_sl = check_tbs(
                        symbol, direction, crt_crl, crt_crh, crt_tf)

                    if tbs_found:
                        tp_lvl = round(crt_crl, 8) if direction == "SELL" \
                                 else round(crt_crh, 8)
                        risk   = abs(tbs_entry - tbs_sl)
                        rr     = round(abs(tp_lvl - tbs_entry) / risk, 2) if risk > 0 else 0
                        has_pd = "DISCOUNT" in zone_name or "PREMIUM" in zone_name
                        score  = 90 if has_pd and rr >= 3.0 else 80 if rr >= 3.0 else 70
                        grade  = "A+" if score >= 85 else "A"
                        log(f"🐢 TBS already confirmed: {symbol} on {tbs_tf} | "
                            f"open:{tbs_entry} SL:{tbs_sl} → AWAIT_PRICE")
                        with manip_lock:
                            if symbol in manip_monitor:
                                manip_monitor[symbol].update({
                                    "phase":     "AWAIT_PRICE",
                                    "crh":       crt_crh,
                                    "crl":       crt_crl,
                                    "tbs_tf":    tbs_tf,
                                    "tbs_entry": tbs_entry,
                                    "tbs_sl":    tbs_sl,
                                    "tp":        tp_lvl,
                                    "rr":        rr,
                                    "score":     score,
                                    "grade":     grade,
                                    "tbs_at":    datetime.now(LOCAL_TZ).strftime("%H:%M UTC+1"),
                                })
                    else:
                        # TBS not yet there — advance to AWAIT_TBS
                        with manip_lock:
                            if symbol in manip_monitor:
                                manip_monitor[symbol].update({
                                    "phase":  "AWAIT_TBS",
                                    "crh":    crt_crh,
                                    "crl":    crt_crl,
                                    "c1":     crt.get("c1", {}),
                                    "c2":     crt.get("c2", {}),
                                    "c3":     crt.get("c3", {}),
                                    "crt_at": datetime.now(LOCAL_TZ).strftime("%H:%M UTC+1"),
                                })
                        log(f"⏳ M1 → AWAIT_TBS: {symbol} | "
                            f"CRH:{crt_crh} CRL:{crt_crl}")
                    continue

                # Read CRT levels stored in monitor for subsequent phases
                crh = monitor.get("crh", 0)
                crl = monitor.get("crl", 0)

                # ════════════════════════════════════════════════════════
                # PHASE: AWAIT_TBS
                # CRT confirmed. Poll LTF every tick for the TBS candle.
                # TBS: opens INSIDE CRT range, body CLOSES OUTSIDE.
                # Signal fires IMMEDIATELY at TBS close — no waiting.
                # ════════════════════════════════════════════════════════
                if phase == "AWAIT_TBS":
                    crt_candles = get_candles(symbol, crt_tf, limit=10)
                    if crt_candles:
                        last = crt_candles[-1]
                        if direction == "SELL" and last["close"] > crh * 1.001:
                            log(f"❌ M1 CRT INVALID: {symbol} — price closed above CRH {crh}")
                            _reset_to_watching(symbol); continue
                        if direction == "BUY"  and last["close"] < crl * 0.999:
                            log(f"❌ M1 CRT INVALID: {symbol} — price closed below CRL {crl}")
                            _reset_to_watching(symbol); continue

                    tbs_found, tbs_tf, tbs_entry, tbs_sl = check_tbs(
                        symbol, direction, crl, crh, crt_tf)

                    if not tbs_found:
                        continue   # Keep polling

                    # ── TBS confirmed — fire signal immediately (per spec) ──
                    tp   = round(crl, 8) if direction == "SELL" else round(crh, 8)
                    risk = abs(tbs_entry - tbs_sl)
                    rr   = round(abs(tp - tbs_entry) / risk, 2) if risk > 0 else 0

                    if rr < 2.0:
                        log(f"⚠️ M1 {symbol} TBS RR={rr}R too low — resetting")
                        _reset_to_watching(symbol); continue

                    has_pd = "DISCOUNT" in zone_name or "PREMIUM" in zone_name
                    score  = 90 if has_pd and rr >= 3.0 else 80 if rr >= 3.0 else 70
                    grade  = "A+" if score >= 85 else "A"

                    log(f"🐢 M1 TBS CONFIRMED — FIRING NOW: {symbol} {tbs_tf} | "
                        f"Entry:{tbs_entry} SL:{tbs_sl} TP:{tp} RR:{rr}R")

                    sig = {
                        "model":       "1",
                        "symbol":      symbol,
                        "tf":          crt_tf,
                        "ob_tf":       monitor.get("ob_tf", "–"),
                        "ob_zone":     zone_name,
                        "zone_type":   kl_type,
                        "direction":   direction,
                        "trend":       trend,
                        "entry":       round(tbs_entry, 8),
                        "entry_type":  "Model #1 (TBS Open — enter now or at open)",
                        "sl":          round(tbs_sl, 8),
                        "tp":          round(tp, 8),
                        "tp1":         round((tbs_entry + tp) / 2, 8),
                        "tp2":         round(tp, 8),
                        "rr":          rr,
                        "crh":         crh,
                        "crl":         crl,
                        "ob_top":      monitor.get("zone_top", "–"),
                        "ob_bot":      monitor.get("zone_bot", "–"),
                        "score":       score,
                        "grade":       grade,
                        "details": [
                            f"✅ HTF zone: {zone_name}",
                            f"✅ CRT on {crt_tf}: CRH={crh} CRL={crl}",
                            f"🐢 TBS on {tbs_tf}: Entry={tbs_entry} SL={tbs_sl}",
                            f"TP: {tp} ({'CRL' if direction=='SELL' else 'CRH'}) | RR:{rr}R",
                            f"⚡ Signal fired at TBS close",
                        ],
                        "tbs_found":   True,
                        "tbs_tf":      tbs_tf,
                        "tbs_entry":   tbs_entry,
                        "tbs_sl":      tbs_sl,
                        "fvg_found":   False, "fvg_type":"–","fvg_entry":"–",
                        "fvg_top":"–","fvg_bot":"–",
                        "choch_found": False, "choch_level":"–",
                        "liq_swept":   False, "ob_respected": False, "continuous": True,
                        "from_monitor":True,
                        "market_order":True,
                        "timestamp":   datetime.now(LOCAL_TZ).strftime("%Y-%m-%d %H:%M UTC+1"),
                    }

                    signals.appendleft(sig)
                    send_telegram(fmt_tg(sig), kind="signal")

                    ready, reason = live_trade_ready()
                    if ready:
                        ok, msg = place_order(sig)
                        log(f"{'✅' if ok else '❌'} M1 order: {msg}")
                        if not ok:
                            send_telegram(
                                f"⚠️ M1 Trade FAILED: {symbol} {direction}\n"
                                f"Reason: {msg}\nEntry:{tbs_entry} SL:{tbs_sl} TP:{tp}",
                                kind="trade")
                    else:
                        log(f"⚠️ M1 live trade skipped ({symbol}): {reason}")

                    if paper_config["enabled"] and paper_config["auto_trade"]:
                        ok2, msg2 = place_paper_order(sig)
                        if ok2: log(f"📝 M1 paper: {msg2}")

                    with manip_lock:
                        if symbol in manip_monitor:
                            manip_monitor[symbol].update({
                                "phase":        "RUNNING",
                                "tbs_tf":       tbs_tf,
                                "tbs_entry":    tbs_entry,
                                "tbs_sl":       tbs_sl,
                                "tp":           tp,
                                "rr":           rr,
                                "score":        score,
                                "grade":        grade,
                                "signal_entry": round(tbs_entry, 8),
                                "signal_sl":    round(tbs_sl, 8),
                                "signal_tp":    round(tp, 8),
                                "signal_rr":    rr,
                                "signal_grade": grade,
                                "signal_time":  datetime.now(LOCAL_TZ).strftime("%H:%M UTC+1"),
                                "last_price":   round(tbs_entry, 8),
                                "tbs_at":       datetime.now(LOCAL_TZ).strftime("%H:%M UTC+1"),
                            })
                    continue

                # ════════════════════════════════════════════════════════
                # PHASE: RUNNING
                # Trade is live. Track TP and SL on every price tick.
                # On close: reset to WATCHING — zone stays alive.
                # ════════════════════════════════════════════════════════
                if phase == "RUNNING":
                    ticker = get_ticker(symbol)
                    if not ticker: continue
                    price  = ticker["price"]
                    tp_p   = monitor.get("signal_tp", 0)
                    sl_p   = monitor.get("signal_sl", 0)

                    # Update last_price
                    with manip_lock:
                        if symbol in manip_monitor:
                            manip_monitor[symbol]["last_price"] = price

                    # TP hit
                    tp_hit = (direction == "BUY"  and price >= tp_p and tp_p > 0) or \
                             (direction == "SELL" and price <= tp_p and tp_p > 0)
                    if tp_hit:
                        log(f"🏆 M1 TP HIT: {symbol} price={price} TP={tp_p} — resetting for next CRT")
                        send_telegram(
                            f"🏆 M1 TP HIT: {symbol} {direction}\n"
                            f"Price: {price} | TP: {tp_p}\n"
                            f"Zone still active — hunting next CRT 🎯",
                            kind="trade")
                        _reset_to_watching(symbol, keep_zone=True)
                        continue

                    # SL hit
                    sl_hit = (direction == "BUY"  and price <= sl_p and sl_p > 0) or \
                             (direction == "SELL" and price >= sl_p and sl_p > 0)
                    if sl_hit:
                        log(f"💀 M1 SL HIT: {symbol} price={price} SL={sl_p} — resetting for next CRT")
                        send_telegram(
                            f"💀 M1 SL HIT: {symbol} {direction}\n"
                            f"Price: {price} | SL: {sl_p}\n"
                            f"Zone still active — hunting next CRT 🎯",
                            kind="trade")
                        _reset_to_watching(symbol, keep_zone=True)
                        continue

        except Exception as e:
            log(f"❌ Manip monitor error: {e}")

        time.sleep(5)


def scanner_loop():
    with scan_lock: scan_state["running"]=True
    log(f"🚀 Mad Man Strategy Scanner started — scanning {len(TOP_PAIRS)} fixed watchlist pairs")
    while True:
        try:
            with scan_lock:
                if not scan_state["enabled"]:
                    scan_state["running"]=False
            if not scan_state["enabled"]:
                time.sleep(5); continue
            with scan_lock: scan_state["running"]=True

            pairs = list(TOP_PAIRS)  # Fixed 30-pair watchlist

            with scan_lock:
                scan_state["total_pairs"]=len(pairs)
                scan_state["pairs_done"]=0
                scan_state["scan_count"]+=1

            log(f"🔄 Scan #{scan_state['scan_count']} — {len(pairs)} watchlist pairs")

            scanned_this_cycle = set()  # Each pair scanned at most once per cycle

            for i,symbol in enumerate(pairs):
                if not scan_state["enabled"]: break

                # Skip pairs already scanned in this cycle
                if symbol in scanned_this_cycle:
                    continue
                scanned_this_cycle.add(symbol)

                # Skip pairs in M2/M4 monitors — they have active phase-based flows
                # M1 (scan_pair) can still run because it independently checks TBS
                with m2_lock:
                    already_in_m2 = symbol in m2_monitor
                with m4_lock:
                    already_in_m4 = symbol in m4_monitor
                if already_in_m2 or already_in_m4:
                    log(f"⏩ {symbol} — in M2/M4 monitor queue, skipping scan")
                    time.sleep(1)
                    continue

                with scan_lock:
                    scan_state["current_pair"]=symbol
                    scan_state["pairs_done"]=i+1
                try:
                    # Visual analysis (Gemini) — primary if key is set
                    # Falls back to math scanner if key missing or no signal
                    visual_sig = None
                    if GEMINI_API_KEY and scan_settings.get("visual_analysis_enabled", True):
                        try:
                            visual_sig = visual_analyse_pair(symbol)
                        except Exception as ve:
                            log(f"⚠️ Visual error {symbol}: {ve}")

                    if visual_sig:
                        m1 = [visual_sig]
                    else:
                        m1 = scan_pair(symbol) if scan_settings.get("model1_enabled", True) else []

                    m2 = scan_pair_model2(symbol) if scan_settings.get("model2_enabled", True) else []
                    m4 = scan_pair_model4(symbol) if scan_settings.get("model3_enabled", True) else []
                    all_res = m1 + m2 + m4
                    for sig in all_res:
                        m = sig.get("model","1")
                        recent_sigs = list(signals)[:100]
                        # Deduplicate by symbol+direction+tf+model+crh+crl
                        # This allows multiple CRTs on the same TF to each fire
                        # but prevents the exact same CRT from firing twice in a row
                        duplicate = any(
                            s.get("symbol")    == sig["symbol"]    and
                            s.get("direction") == sig["direction"] and
                            s.get("tf")        == sig["tf"]        and
                            s.get("model","1") == m                and
                            abs(float(s.get("crh", 0)) - float(sig.get("crh", 0))) < 1e-9 and
                            abs(float(s.get("crl", 0)) - float(sig.get("crl", 0))) < 1e-9
                            for s in recent_sigs
                        )
                        if duplicate:
                            log(f"⏭ SKIP dup M#{m}: {sig['direction']} {symbol} {sig['tf']} CRT:{sig.get('crh','?')}/{sig.get('crl','?')}")
                            continue
                        diag["passed"]+=1
                        signals.appendleft(sig)
                        with scan_lock: scan_state["signals_found"]+=1
                        tf_lbl={"Day1":"1D","Hour4":"4H","Hour3":"3H","Hour2":"2H","Min60":"1H"}.get(sig["tf"],"–")
                        log(f"🎯 M#{m} {sig['direction']} {symbol} | {tf_lbl} | Score:{sig['score']} {sig['grade']} | RR:{sig['rr']}R")
                        send_telegram(fmt_tg(sig), kind="signal")
                        if trade_config["enabled"] and trade_config["api_key"]:
                            ok, msg = place_order(sig)
                            log(f"{'✅' if ok else '❌'} Auto-trade: {msg}")
                        if paper_config["enabled"] and paper_config["auto_trade"]:
                            ok2, msg2 = place_paper_order(sig)
                            if ok2: log(f"📝 Paper auto: {msg2}")
                except Exception as e:
                    log(f"⚠️ Scan error {symbol}: {e}")
                time.sleep(scan_settings["scan_interval"])
                if (i+1) % 50 == 0:
                    log(f"📊 Progress: {i+1}/{scan_state['total_pairs']} pairs scanned")

            with scan_lock: scan_state["last_scan"]=datetime.now(LOCAL_TZ).strftime("%H:%M UTC+1")
            log(f"✅ Scan #{scan_state['scan_count']} complete — {len(pairs)} watchlist pairs | "
                f"scanned={len(scanned_this_cycle)} unique")
            log(f"📊 GATES: neutral={diag.get('neutral',0)} no_cont={diag.get('not_continuous',0)} "
                f"not_zone={diag.get('not_in_zone',0)} no_crt={diag.get('no_crts',0)} "
                f"no_tbs={diag.get('no_tbs',0)} rr_low={diag.get('rr_low',0)} PASSED={diag['passed']}")
            for k in diag: diag[k]=0
            # Rest between cycles so no pair is immediately re-scanned
            log(f"⏸ Cycle rest — {scan_settings['cycle_rest']}s before next scan round...")
            rest_remaining = scan_settings["cycle_rest"]
            while rest_remaining > 0:
                if not scan_state["enabled"]: break
                time.sleep(min(5, rest_remaining))
                rest_remaining -= 5

        except Exception as e:
            log(f"❌ Scanner error: {e}"); time.sleep(15)

# ════════ HTML ════════════════════════════════════════════════════════

LOGIN_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/><meta name="viewport" content="width=device-width,initial-scale=1.0"/>
<title>Mad Man Strategy Scanner 🚀</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/html2canvas/1.4.1/html2canvas.min.js"></script>
<link href="https://fonts.googleapis.com/css2?family=Fredoka+One&family=Nunito:wght@400;600;700;800;900&family=JetBrains+Mono:wght@400;700&display=swap" rel="stylesheet"/>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:'Nunito',sans-serif;background:#0f0e1a;min-height:100vh;display:flex;align-items:center;justify-content:center;overflow:hidden;padding:20px}
.stars{position:fixed;inset:0;z-index:0}
.star{position:absolute;border-radius:50%;background:#fff;animation:twink 3s infinite}
@keyframes twink{0%,100%{opacity:.15;transform:scale(1)}50%{opacity:.9;transform:scale(1.4)}}
.blob{position:fixed;border-radius:50%;filter:blur(70px);opacity:.18;animation:blob-float 10s ease-in-out infinite;z-index:0}
.b1{width:380px;height:380px;background:#7c3aed;top:-120px;left:-80px}
.b2{width:300px;height:300px;background:#db2777;bottom:-80px;right:-60px;animation-delay:-4s}
.b3{width:200px;height:200px;background:#0ea5e9;top:40%;left:40%;animation-delay:-7s}
@keyframes blob-float{0%,100%{transform:translate(0,0) scale(1)}33%{transform:translate(20px,-30px) scale(1.05)}66%{transform:translate(-15px,20px) scale(.95)}}
.card{position:relative;z-index:10;background:rgba(20,18,40,.92);border:2px solid rgba(124,58,237,.4);border-radius:28px;padding:44px 38px 36px;width:100%;max-width:420px;backdrop-filter:blur(24px);box-shadow:0 0 0 1px rgba(124,58,237,.1),0 40px 80px rgba(0,0,0,.7),inset 0 1px 0 rgba(255,255,255,.05)}
.card::before,.card::after{content:'';position:absolute;width:24px;height:24px;border:3px solid rgba(124,58,237,.5);border-radius:6px}
.card::before{top:-3px;left:-3px;border-right:none;border-bottom:none}
.card::after{bottom:-3px;right:-3px;border-left:none;border-top:none}
.head{text-align:center;margin-bottom:30px}
.rocket{font-size:3.6rem;display:block;animation:rocket-bounce 2s ease-in-out infinite;filter:drop-shadow(0 0 20px rgba(124,58,237,.7))}
@keyframes rocket-bounce{0%,100%{transform:translateY(0) rotate(-5deg)}50%{transform:translateY(-14px) rotate(5deg)}}
.title{font-family:'Fredoka One',sans-serif;font-size:2.4rem;letter-spacing:.04em;background:linear-gradient(135deg,#a78bfa,#f472b6,#38bdf8);-webkit-background-clip:text;-webkit-text-fill-color:transparent;margin-bottom:5px}
.sub{font-size:.75rem;color:rgba(200,210,255,.4);letter-spacing:.14em;text-transform:uppercase;font-weight:700}
.lbl{font-size:.7rem;font-weight:800;color:rgba(167,139,250,.7);letter-spacing:.1em;text-transform:uppercase;margin-bottom:7px;display:block}
.inp{width:100%;padding:13px 16px;background:rgba(255,255,255,.05);border:2px solid rgba(124,58,237,.25);border-radius:14px;color:#e2e8f0;font-size:.95rem;font-family:'Nunito',sans-serif;font-weight:700;outline:none;transition:all .2s;margin-bottom:18px}
.inp:focus{border-color:rgba(167,139,250,.6);background:rgba(124,58,237,.08);box-shadow:0 0 0 4px rgba(124,58,237,.1)}
.inp::placeholder{color:rgba(200,210,255,.2)}
.btn{width:100%;padding:14px;background:linear-gradient(135deg,#7c3aed,#db2777);color:#fff;border:none;border-radius:14px;font-family:'Fredoka One',sans-serif;font-size:1.15rem;letter-spacing:.06em;cursor:pointer;transition:all .25s;position:relative;overflow:hidden;box-shadow:0 6px 24px rgba(124,58,237,.4)}
.btn::before{content:'';position:absolute;top:0;left:-100%;width:100%;height:100%;background:linear-gradient(90deg,transparent,rgba(255,255,255,.15),transparent);transition:left .4s}
.btn:hover::before{left:100%}
.btn:hover{transform:translateY(-3px);box-shadow:0 10px 32px rgba(124,58,237,.55)}
.err{background:rgba(239,68,68,.1);border:2px solid rgba(239,68,68,.3);border-radius:12px;padding:10px 14px;font-size:.8rem;color:#f87171;margin-bottom:14px;display:none;font-weight:700}
.err.show{display:block}
.badges{display:flex;gap:6px;margin-top:22px;flex-wrap:wrap;justify-content:center}
.badge{background:rgba(124,58,237,.12);border:1.5px solid rgba(124,58,237,.25);border-radius:20px;padding:4px 11px;font-size:.65rem;color:rgba(167,139,250,.8);font-weight:800;letter-spacing:.04em}
.dot-row{display:flex;align-items:center;justify-content:center;gap:7px;margin-top:16px}
.live-dot{width:7px;height:7px;border-radius:50%;background:#10b981;box-shadow:0 0 8px #10b981;animation:pulse 2s infinite}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.3}}
.live-txt{font-family:'JetBrains Mono',monospace;font-size:.62rem;color:rgba(16,185,129,.7);letter-spacing:.06em;font-weight:700}
</style>
</head>
<body>
<div class="stars" id="stars"></div>
<div class="blob b1"></div><div class="blob b2"></div><div class="blob b3"></div>
<div class="card">
  <div class="head">
    <span class="rocket">🚀</span>
    <div class="title">Mad Man Strategy Scanner</div>
  </div>
  <div class="err" id="err"></div>
  <label class="lbl">Password</label>
  <input class="inp" type="password" id="pw" placeholder="Enter password" autofocus/>
  <button class="btn" id="btn" onclick="login()">Enter</button>
</div>
<script>
const s=document.getElementById('stars');
for(let i=0;i<70;i++){
  const d=document.createElement('div');d.className='star';
  const sz=Math.random()*2.5+.5;
  d.style.cssText=`width:${sz}px;height:${sz}px;top:${Math.random()*100}%;left:${Math.random()*100}%;animation-delay:${Math.random()*3}s;animation-duration:${2+Math.random()*2}s`;
  s.appendChild(d);
}
function login(){
  const pw=document.getElementById('pw').value.trim();
  const err=document.getElementById('err');const btn=document.getElementById('btn');
  if(!pw){err.textContent='🔑 Password required!';err.classList.add('show');return;}
  btn.textContent='🛸 Launching...';btn.disabled=true;err.classList.remove('show');
  fetch('/api/login',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({password:pw})})
    .then(r=>r.json()).then(d=>{
      if(d.ok){localStorage.setItem('crt_tok',d.token||'ok');btn.textContent='✅ Let\'s go!';setTimeout(()=>window.location.href='/dashboard',300);}
      else{err.textContent='❌ Wrong password, try again!';err.classList.add('show');btn.textContent='🔓 Enter Dashboard';btn.disabled=false;document.getElementById('pw').value='';document.getElementById('pw').focus();}
    }).catch(e=>{err.textContent='⚠️ Connection error. Try again.';err.classList.add('show');btn.textContent='🔓 Enter Dashboard';btn.disabled=false;});
}
document.getElementById('pw').addEventListener('keydown',e=>{if(e.key==='Enter')login();});
</script>
</body>
</html>"""

DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/><meta name="viewport" content="width=device-width,initial-scale=1.0"/>
<title>Mad Man Strategy Scanner 🚀</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/html2canvas/1.4.1/html2canvas.min.js"></script>
<link href="https://fonts.googleapis.com/css2?family=Fredoka+One&family=Nunito:wght@400;600;700;800;900&family=JetBrains+Mono:wght@400;700&display=swap" rel="stylesheet"/>
<style>
*{box-sizing:border-box;margin:0;padding:0}
:root{--bg:#0c0b18;--s1:#13122a;--s2:#1a1838;--s3:#201e45;--purple:#7c3aed;--pink:#db2777;--blue:#0ea5e9;--cyan:#06b6d4;--green:#10b981;--red:#ef4444;--yellow:#f59e0b;--orange:#f97316;--text:#e2e8f0;--dim:#94a3b8;--muted:#334155;--border:rgba(124,58,237,.2);--border2:rgba(124,58,237,.45)}
body{font-family:'Nunito',sans-serif;background:var(--bg);color:var(--text);min-height:100vh;padding-bottom:72px}
body::before{content:'';position:fixed;inset:0;background:repeating-linear-gradient(0deg,transparent,transparent 3px,rgba(0,0,0,.025) 3px,rgba(0,0,0,.025) 4px);pointer-events:none;z-index:998}
.bg-glow{position:fixed;inset:0;pointer-events:none;z-index:0}
.bg-glow::before{content:'';position:absolute;width:600px;height:600px;border-radius:50%;background:radial-gradient(circle,rgba(124,58,237,.12),transparent 70%);top:-200px;left:-200px}
.bg-glow::after{content:'';position:absolute;width:500px;height:500px;border-radius:50%;background:radial-gradient(circle,rgba(219,39,119,.1),transparent 70%);bottom:-150px;right:-150px}
/* Market page goes edge-to-edge with no bottom padding */
#page-market.active{padding:0;margin:0}
#page-market{height:calc(100vh - 72px)}
.hdr{background:rgba(12,11,24,.95);border-bottom:2px solid var(--border);backdrop-filter:blur(20px)}
.hdr-glow{position:absolute;bottom:-1px;left:0;right:0;height:1px;background:linear-gradient(90deg,transparent,var(--purple),var(--pink),transparent);opacity:.5}
.hdr-in{padding:0;height:60px;display:flex;align-items:center;justify-content:space-between;gap:14px}
.brand{display:flex;align-items:center;gap:11px}
.brand-icon{font-size:1.7rem;animation:rock 3s ease-in-out infinite;filter:drop-shadow(0 0 8px rgba(124,58,237,.6))}
@keyframes rock{0%,100%{transform:rotate(-8deg)}50%{transform:rotate(8deg)}}
.brand-name{font-family:'Fredoka One',sans-serif;font-size:1.18rem;letter-spacing:.04em;color:#c4b5fd;line-height:1.2}
.brand-sub{font-family:'JetBrains Mono',monospace;font-size:.52rem;color:var(--dim);letter-spacing:.08em}
.scan-pill{display:flex;align-items:center;gap:7px;background:rgba(16,185,129,.08);border:1.5px solid rgba(16,185,129,.22);border-radius:20px;padding:6px 14px}
.pnl-tick{display:flex;align-items:center;gap:6px;border-radius:20px;padding:6px 14px;border:1.5px solid;transition:all .4s}
.pnl-tick.pos{background:rgba(16,185,129,.08);border-color:rgba(16,185,129,.25)}
.pnl-tick.neg{background:rgba(239,68,68,.08);border-color:rgba(239,68,68,.25)}
.pnl-tick.flat{background:rgba(148,163,184,.05);border-color:rgba(148,163,184,.15)}
.ptick-ico{font-size:.85rem}
.ptick-val{font-family:'JetBrains Mono',monospace;font-size:.68rem;font-weight:700;letter-spacing:.04em}
.pnl-tick.pos .ptick-val{color:var(--green)}
.pnl-tick.neg .ptick-val{color:var(--red)}
.pnl-tick.flat .ptick-val{color:var(--dim)}
.ptick-cnt{font-family:'JetBrains Mono',monospace;font-size:.58rem;color:var(--dim);font-weight:700}
.sdot{width:7px;height:7px;border-radius:50%;background:var(--green);animation:sdot 2s infinite}
.sdot.off{background:var(--red);animation:none}
@keyframes sdot{0%,100%{opacity:1;transform:scale(1)}50%{opacity:.3;transform:scale(.6)}}
.stxt{font-family:'JetBrains Mono',monospace;font-size:.62rem;color:var(--green);font-weight:700;letter-spacing:.05em}
.stxt.off{color:var(--red)}
.hdr-right{display:flex;align-items:center;gap:8px;flex-wrap:wrap}
.snum{font-family:'JetBrains Mono',monospace;font-size:.65rem;color:var(--dim);background:var(--s2);border:1.5px solid var(--muted);border-radius:10px;padding:4px 10px}
.tbtn{padding:7px 16px;border:2px solid;border-radius:12px;font-family:'Nunito',sans-serif;font-size:.8rem;font-weight:800;cursor:pointer;transition:all .22s}
.tbtn.on{background:rgba(239,68,68,.1);border-color:rgba(239,68,68,.4);color:var(--red)}
.tbtn.on:hover{background:rgba(239,68,68,.2);transform:scale(1.05)}
.tbtn.off{background:rgba(16,185,129,.1);border-color:rgba(16,185,129,.35);color:var(--green)}
.tbtn.off:hover{background:rgba(16,185,129,.18);transform:scale(1.05)}
.obtn{padding:7px 13px;background:transparent;border:1.5px solid var(--muted);border-radius:10px;color:var(--dim);font-family:'Nunito',sans-serif;font-size:.78rem;font-weight:700;cursor:pointer;transition:all .2s}
.obtn:hover{border-color:var(--red);color:var(--red)}
.pb{background:rgba(239,68,68,.08);border-bottom:2px solid rgba(239,68,68,.25);padding:10px;text-align:center;font-family:'Fredoka One',sans-serif;font-size:.85rem;letter-spacing:.1em;color:var(--red);display:none}
.pb.show{display:block}
.prog{background:rgba(12,11,24,.9);border-bottom:1px solid var(--border);padding:8px 0;position:relative;z-index:10}
.prog-in{display:flex;align-items:center;gap:14px}
.prog-lbl{font-family:'JetBrains Mono',monospace;font-size:.62rem;color:var(--dim);white-space:nowrap;min-width:200px;overflow:hidden;text-overflow:ellipsis}
.prog-track{flex:1;height:6px;background:var(--s3);border-radius:3px;overflow:hidden}
.prog-fill{height:100%;background:linear-gradient(90deg,var(--purple),var(--pink),var(--blue));border-radius:3px;transition:width .5s ease}
.prog-cnt{font-family:'JetBrains Mono',monospace;font-size:.62rem;color:var(--dim);white-space:nowrap}
.sec{margin-top:14px;position:relative;z-index:1}
.sec-hdr{display:flex;align-items:center;gap:10px;margin-bottom:11px}
.sec-ttl{font-family:'Fredoka One',sans-serif;font-size:1rem;letter-spacing:.06em;color:rgba(167,139,250,.8)}
.sec-line{flex:1;height:2px;background:linear-gradient(90deg,rgba(124,58,237,.3),transparent);border-radius:1px}
.sec-note{font-family:'JetBrains Mono',monospace;font-size:.56rem;color:var(--dim)}
.prices-grid{display:grid;grid-template-columns:repeat(5,1fr);gap:10px}
.pc{background:var(--s1);border:2px solid var(--border);border-radius:16px;padding:13px 12px 11px;position:relative;overflow:hidden;transition:all .25s;cursor:default}
.pc::after{content:'';position:absolute;top:0;left:0;right:0;height:3px;border-radius:3px 3px 0 0;background:var(--muted);transition:background .3s}
.pc.up::after{background:linear-gradient(90deg,var(--green),rgba(16,185,129,.3))}
.pc.dn::after{background:linear-gradient(90deg,var(--red),rgba(239,68,68,.3))}
.pc:hover{border-color:var(--border2);transform:translateY(-4px) rotate(.5deg);box-shadow:0 12px 36px rgba(0,0,0,.5)}
.pc-sym{font-family:'Fredoka One',sans-serif;font-size:.75rem;letter-spacing:.06em;color:var(--dim);margin-bottom:5px}
.pc-price{font-family:'JetBrains Mono',monospace;font-size:.86rem;font-weight:700;margin-bottom:5px;line-height:1}
.pc-price.up{color:var(--green)}.pc-price.dn{color:var(--red)}
.pc-chg{font-family:'JetBrains Mono',monospace;font-size:.62rem;font-weight:700;padding:2px 7px;border-radius:8px;display:inline-block}
.pc-chg.up{background:rgba(16,185,129,.12);color:var(--green)}.pc-chg.dn{background:rgba(239,68,68,.12);color:var(--red)}
.stats-grid{display:grid;grid-template-columns:repeat(5,1fr);gap:10px}
.sc{background:var(--s1);border:2px solid var(--border);border-radius:16px;padding:16px 16px 14px;position:relative;overflow:hidden;transition:all .22s}
.sc:hover{border-color:var(--border2);transform:translateY(-3px) rotate(.3deg)}
.sc::before{content:'';position:absolute;top:0;left:0;right:0;height:3px;border-radius:3px 3px 0 0}
.s0::before{background:linear-gradient(90deg,var(--purple),var(--pink))}.s1::before{background:var(--green)}.s2::before{background:var(--red)}.s3::before{background:var(--blue)}.s4::before{background:var(--yellow)}
.sc-lbl{font-family:'JetBrains Mono',monospace;font-size:.54rem;color:var(--dim);letter-spacing:.08em;text-transform:uppercase;margin-bottom:7px;font-weight:700}
.sc-val{font-family:'Fredoka One',sans-serif;font-size:2rem;letter-spacing:.04em;line-height:1;color:#a78bfa}
.sc-sub{font-size:.64rem;color:var(--dim);margin-top:4px;font-weight:600}
.tab-wrap{position:relative;z-index:1}
/* ── PAGE SYSTEM ── */
.page{display:none}.page.active{display:block}
/* Content pages: centered with padding */
#page-home,#page-signals,#page-trade,#page-settings{max-width:1360px;margin:0 auto;padding:16px 20px 20px}
/* Market page: full viewport, no padding, no max-width */
#page-market{width:100%;height:calc(100vh - 72px);padding:0;margin:0;overflow:hidden}
/* ── BOTTOM NAV ── */
.bottom-nav{position:fixed;bottom:0;left:0;right:0;z-index:500;background:rgba(12,11,24,.97);border-top:2px solid var(--border);backdrop-filter:blur(20px);padding:0 16px;padding-bottom:env(safe-area-inset-bottom)}
.bnav-inner{max-width:600px;margin:0 auto;display:flex;align-items:stretch;justify-content:space-around;gap:4px;padding:6px 0}
.bnav-btn{flex:1;display:flex;flex-direction:column;align-items:center;gap:3px;background:none;border:none;cursor:pointer;padding:8px 4px;border-radius:14px;transition:all .2s;color:var(--dim);min-width:0}
.bnav-btn:hover{color:var(--text);background:rgba(124,58,237,.08)}
.bnav-btn.active{color:#a78bfa}
.bnav-btn.active .bnav-ico{background:linear-gradient(135deg,rgba(124,58,237,.25),rgba(219,39,119,.15));border-color:rgba(124,58,237,.4)}
.bnav-ico{font-size:1.25rem;width:40px;height:36px;display:flex;align-items:center;justify-content:center;border-radius:12px;border:1.5px solid transparent;transition:all .2s}
.bnav-lbl{font-family:'Nunito',sans-serif;font-size:.62rem;font-weight:800;letter-spacing:.04em;white-space:nowrap}
/* ── INNER PAGE TABS (Trade page sub-tabs) ── */
.inner-tabs{display:flex;gap:6px;background:var(--s1);border:2px solid var(--border);border-radius:14px;padding:4px;margin-bottom:16px;overflow-x:auto}
.inner-tab{flex:0 0 auto;padding:7px 14px;border:none;border-radius:10px;font-family:'Nunito',sans-serif;font-size:.74rem;font-weight:800;cursor:pointer;transition:all .2s;color:var(--dim);background:transparent;white-space:nowrap}
.inner-tab:hover{color:var(--text)}.inner-tab.active{background:linear-gradient(135deg,var(--purple),var(--pink));color:#fff;box-shadow:0 4px 14px rgba(124,58,237,.35)}
.frow{display:flex;align-items:center;justify-content:space-between;margin-bottom:15px;flex-wrap:wrap;gap:9px}
.ftitle{font-family:'Fredoka One',sans-serif;font-size:1.05rem;letter-spacing:.04em;color:#a78bfa}
.fgrp{display:flex;gap:6px;flex-wrap:wrap}
.fsel{background:var(--s2);border:2px solid var(--border);border-radius:10px;color:var(--text);padding:7px 10px;font-size:.72rem;font-family:'Nunito',sans-serif;font-weight:700;outline:none}
.fsel:focus{border-color:rgba(124,58,237,.5)}
.empty{display:flex;flex-direction:column;align-items:center;justify-content:center;padding:60px 20px;background:var(--s1);border:2px dashed var(--border);border-radius:20px;text-align:center;gap:12px}
.empty-ico{font-size:3rem;animation:wobble 3s ease-in-out infinite}
@keyframes wobble{0%,100%{transform:rotate(-5deg)}50%{transform:rotate(5deg)}}
.empty-t{font-family:'Fredoka One',sans-serif;font-size:1.2rem;letter-spacing:.04em;color:var(--dim)}
.empty-s{font-size:.8rem;color:var(--dim);max-width:380px;line-height:1.7;font-weight:600}
.sig-list{display:flex;flex-direction:column;gap:12px}
.scard{background:var(--s1);border:2px solid var(--border);border-radius:18px;padding:18px 20px;animation:card-pop .35s cubic-bezier(.34,1.56,.64,1);transition:all .22s;position:relative;overflow:hidden}
.scard::before{content:'';position:absolute;top:0;left:0;bottom:0;width:4px;border-radius:4px 0 0 4px}
.scard.buy::before{background:linear-gradient(180deg,var(--green),rgba(16,185,129,.2))}
.scard.sell::before{background:linear-gradient(180deg,var(--red),rgba(239,68,68,.2))}
.scard:hover{border-color:var(--border2);transform:translateY(-4px);box-shadow:0 16px 48px rgba(0,0,0,.55)}
@keyframes card-pop{from{opacity:0;transform:scale(.95) translateY(-12px)}to{opacity:1;transform:scale(1) translateY(0)}}
.card-hdr{display:flex;align-items:center;gap:9px;flex-wrap:wrap;margin-bottom:13px;padding-bottom:11px;border-bottom:1.5px solid var(--border)}
.dtag{font-family:'Fredoka One',sans-serif;font-size:.85rem;letter-spacing:.06em;padding:5px 13px;border-radius:12px;border:2px solid;flex-shrink:0}
.dtag.BUY{background:rgba(16,185,129,.1);border-color:rgba(16,185,129,.35);color:var(--green)}
.dtag.SELL{background:rgba(239,68,68,.1);border-color:rgba(239,68,68,.35);color:var(--red)}
.csym{font-family:'Fredoka One',sans-serif;font-size:1.1rem;letter-spacing:.06em;color:var(--text)}
.chips{display:flex;gap:5px;flex-wrap:wrap;align-items:center}
.chip{font-family:'JetBrains Mono',monospace;font-size:.6rem;padding:3px 8px;border-radius:8px;letter-spacing:.04em;border:1.5px solid;font-weight:700}
.chip-tf{color:var(--cyan);border-color:rgba(6,182,212,.25);background:rgba(6,182,212,.07)}
.chip-ob{color:var(--orange);border-color:rgba(249,115,22,.25);background:rgba(249,115,22,.07)}
.chip-tr.BULLISH{color:var(--green);border-color:rgba(16,185,129,.25);background:rgba(16,185,129,.07)}
.chip-tr.BEARISH{color:var(--red);border-color:rgba(239,68,68,.25);background:rgba(239,68,68,.07)}
.chip-tr.NEUTRAL{color:var(--dim);border-color:var(--muted);background:transparent}
.chip-aplus{color:#fbbf24;border-color:rgba(251,191,36,.4);background:rgba(251,191,36,.1);animation:ap 2s infinite}
@keyframes ap{0%,100%{box-shadow:0 0 0 0 rgba(251,191,36,.3)}50%{box-shadow:0 0 0 4px rgba(251,191,36,0)}}
.gtag{font-family:'Fredoka One',sans-serif;font-size:.9rem;letter-spacing:.06em;padding:4px 11px;border-radius:10px;margin-left:auto;border:2px solid;flex-shrink:0}
.gAp{color:#fbbf24;border-color:rgba(251,191,36,.5);background:rgba(251,191,36,.12);animation:ap 2s infinite}
.gA{color:#a78bfa;border-color:rgba(167,139,250,.4);background:rgba(167,139,250,.08)}
.gB{color:#38bdf8;border-color:rgba(56,189,248,.35);background:rgba(56,189,248,.07)}
.gC{color:var(--orange);border-color:rgba(249,115,22,.3);background:rgba(249,115,22,.06)}
.gD{color:var(--dim);border-color:var(--muted);background:transparent}
.cts{font-family:'JetBrains Mono',monospace;font-size:.57rem;color:var(--dim);white-space:nowrap}
.lvl-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(112px,1fr));gap:8px;margin-bottom:13px}
.lv{background:var(--s2);border:1.5px solid var(--muted);border-radius:12px;padding:10px 12px;transition:all .2s}
.lv:hover{border-color:rgba(124,58,237,.3);transform:translateY(-2px)}
.lv-lbl{font-family:'JetBrains Mono',monospace;font-size:.52rem;color:var(--dim);letter-spacing:.05em;margin-bottom:4px;text-transform:uppercase;font-weight:700}
.lv-val{font-family:'JetBrains Mono',monospace;font-size:.8rem;font-weight:700}
.lv-e .lv-val{color:#f9a8d4}.lv-s .lv-val{color:var(--red)}.lv-t .lv-val{color:var(--green)}.lv-r .lv-val{color:var(--yellow)}.lv-o .lv-val{color:#a78bfa}
.cfms{display:flex;gap:5px;flex-wrap:wrap;margin-bottom:12px}
.cf{font-family:'JetBrains Mono',monospace;font-size:.59rem;padding:3px 9px;border-radius:8px;border:1.5px solid;font-weight:700}
.cf-ok{color:var(--green);border-color:rgba(16,185,129,.25);background:rgba(16,185,129,.07)}
.cf-no{color:var(--dim);border-color:var(--muted);background:transparent}
.cf-w{color:var(--orange);border-color:rgba(249,115,22,.25);background:rgba(249,115,22,.06)}
.cf-g{color:#a78bfa;border-color:rgba(167,139,250,.3);background:rgba(167,139,250,.06)}
.srow{display:flex;align-items:center;gap:12px}
.slbl{font-family:'Fredoka One',sans-serif;font-size:.72rem;color:var(--dim);white-space:nowrap;width:55px}
.strack{flex:1;height:8px;background:var(--s3);border-radius:4px;overflow:hidden}
.sfill{height:100%;border-radius:4px;transition:width .8s cubic-bezier(.34,1.56,.64,1)}
.snum2{font-family:'Fredoka One',sans-serif;font-size:.95rem;white-space:nowrap;width:60px;text-align:right}
.dettog{display:inline-flex;align-items:center;gap:5px;margin-top:10px;font-family:'Nunito',sans-serif;font-size:.68rem;font-weight:800;color:rgba(167,139,250,.5);cursor:pointer;transition:color .18s;border:none;background:transparent;padding:0}
.dettog:hover{color:#a78bfa}
.detbox{display:none;margin-top:10px;background:var(--s2);border:1.5px solid var(--border);border-radius:12px;padding:13px;font-family:'JetBrains Mono',monospace;font-size:.63rem;color:var(--dim);line-height:1.9}
.detbox.open{display:block}
.panel{background:var(--s1);border:2px solid var(--border);border-radius:18px;padding:20px;margin-bottom:14px}
.panel-ttl{font-family:'Fredoka One',sans-serif;font-size:1rem;letter-spacing:.05em;color:#a78bfa;margin-bottom:14px;display:flex;align-items:center;gap:8px;flex-wrap:wrap}
.tbl{width:100%;border-collapse:collapse}
.tbl th{font-family:'JetBrains Mono',monospace;font-size:.56rem;color:var(--dim);letter-spacing:.07em;text-transform:uppercase;padding:7px 9px;text-align:left;border-bottom:1.5px solid var(--border)}
.tbl td{font-family:'JetBrains Mono',monospace;font-size:.68rem;padding:8px 9px;border-bottom:1px solid rgba(124,58,237,.07);vertical-align:middle}
.tbl tr:hover td{background:rgba(124,58,237,.04)}
.buy{color:var(--green);font-weight:800}.sell{color:var(--red);font-weight:800}
.pos-pnl{font-weight:800}.pos-pnl.pos{color:var(--green)}.pos-pnl.neg{color:var(--red)}
.action-btn{padding:4px 9px;border:1.5px solid;border-radius:8px;font-family:'Nunito',sans-serif;font-size:.68rem;font-weight:800;cursor:pointer;transition:all .2s}
.close-btn{background:rgba(239,68,68,.1);border-color:rgba(239,68,68,.3);color:var(--red)}.close-btn:hover{background:rgba(239,68,68,.2)}
.share-btn{background:rgba(124,58,237,.1);border-color:rgba(124,58,237,.3);color:#a78bfa}.share-btn:hover{background:rgba(124,58,237,.2)}
.monitor-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(210px,1fr));gap:10px}
.mon-card{background:var(--s2);border:2px solid var(--border);border-radius:14px;padding:14px;position:relative;overflow:hidden;animation:card-pop .3s ease}
.mon-card::before{content:'';position:absolute;top:0;left:0;right:0;height:3px}
.mon-card.buy::before{background:var(--green)}.mon-card.sell::before{background:var(--red)}
.mon-card:hover{border-color:var(--border2);transform:translateY(-3px)}
.mon-sym{font-family:'Fredoka One',sans-serif;font-size:1rem;margin-bottom:6px}
.mon-row{display:flex;justify-content:space-between;font-family:'JetBrains Mono',monospace;font-size:.62rem;color:var(--dim);margin-bottom:3px}
.mon-row span:last-child{color:var(--text);font-weight:700}
.mon-status{margin-top:8px;padding:4px 10px;border-radius:8px;font-family:'JetBrains Mono',monospace;font-size:.6rem;font-weight:700;text-align:center;background:rgba(245,158,11,.12);color:var(--yellow);border:1px solid rgba(245,158,11,.3);animation:pulse-y 2s infinite}
@keyframes pulse-y{0%,100%{opacity:1}50%{opacity:.45}}
.trade-form{display:grid;grid-template-columns:repeat(auto-fill,minmax(190px,1fr));gap:12px;margin-bottom:16px}
.tf-group{display:flex;flex-direction:column;gap:6px}
.tf-lbl{font-family:'JetBrains Mono',monospace;font-size:.6rem;color:var(--dim);letter-spacing:.08em;text-transform:uppercase;font-weight:700}
.tf-inp{background:var(--s2);border:1.5px solid var(--muted);border-radius:10px;color:var(--text);padding:9px 12px;font-size:.82rem;font-family:'Nunito',sans-serif;font-weight:700;outline:none;transition:border-color .2s;width:100%}
.tf-inp:focus{border-color:rgba(124,58,237,.5)}
.trade-actions{display:flex;gap:10px;flex-wrap:wrap;align-items:center;margin-bottom:12px}
.trade-btn{padding:10px 20px;border:none;border-radius:12px;font-family:'Nunito',sans-serif;font-size:.82rem;font-weight:800;cursor:pointer;transition:all .2s}
.tb-save{background:linear-gradient(135deg,var(--purple),var(--pink));color:#fff;box-shadow:0 4px 16px rgba(124,58,237,.35)}.tb-save:hover{transform:translateY(-2px)}
.tb-on{background:rgba(16,185,129,.12);border:2px solid rgba(16,185,129,.35);color:var(--green)}
.tb-off{background:rgba(239,68,68,.1);border:2px solid rgba(239,68,68,.3);color:var(--red)}
.tb-chk{background:rgba(56,189,248,.1);border:2px solid rgba(56,189,248,.3);color:var(--blue)}
.bal-chip{display:flex;align-items:center;gap:7px;background:rgba(16,185,129,.07);border:1.5px solid rgba(16,185,129,.2);border-radius:10px;padding:8px 14px;font-family:'JetBrains Mono',monospace;font-size:.72rem;color:var(--green);font-weight:700}
.t-status{font-family:'JetBrains Mono',monospace;font-size:.7rem;padding:8px 14px;border-radius:10px;font-weight:700;margin-top:8px;display:none}
.t-status.ok{background:rgba(16,185,129,.1);border:1.5px solid rgba(16,185,129,.3);color:var(--green);display:block}
.t-status.err{background:rgba(239,68,68,.1);border:1.5px solid rgba(239,68,68,.3);color:var(--red);display:block}
.info-box{border-radius:12px;padding:12px 14px;font-size:.73rem;font-weight:700;line-height:1.6;margin-bottom:12px}
.info-blue{background:rgba(14,165,233,.07);border:1.5px solid rgba(14,165,233,.2);color:rgba(56,189,248,.8)}
.info-red{background:rgba(239,68,68,.06);border:1.5px solid rgba(239,68,68,.2);color:rgba(239,68,68,.8)}
.info-green{background:rgba(16,185,129,.06);border:1.5px solid rgba(16,185,129,.2);color:rgba(16,185,129,.85)}
.tc-modal{position:fixed;inset:0;background:rgba(0,0,0,.85);z-index:999;display:none;align-items:center;justify-content:center;backdrop-filter:blur(8px);padding:16px}
.tc-modal.show{display:flex}
.tc-wrap{display:flex;flex-direction:column;align-items:center;gap:12px;width:340px;max-width:96vw}
.tc-card{width:100%;border-radius:22px;overflow:hidden;position:relative;box-shadow:0 30px 80px rgba(0,0,0,.8);border:2px solid rgba(56,189,248,.25)}
.tc-bg{position:absolute;inset:0;background-image:url('/logo');background-size:cover;background-position:center top;filter:brightness(.22) saturate(1.4)}
.tc-glass{position:relative;z-index:2;padding:20px 18px 16px}
.tc-header{text-align:center;margin-bottom:14px;padding-bottom:12px;border-bottom:1px solid rgba(56,189,248,.2)}
.tc-brand{font-family:'Fredoka One',sans-serif;font-size:1.35rem;letter-spacing:.12em;background:linear-gradient(135deg,#38bdf8,#a78bfa);-webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text}
.tc-tagline{font-family:'JetBrains Mono',monospace;font-size:.52rem;color:rgba(56,189,248,.6);letter-spacing:.15em;margin-top:2px}
.tc-dir-row{display:flex;align-items:center;gap:8px;margin-bottom:10px}
.tc-dir-badge{font-family:'Fredoka One',sans-serif;font-size:.9rem;padding:5px 14px;border-radius:8px;letter-spacing:.08em}
.tc-dir-badge.buy{background:rgba(16,185,129,.2);border:1.5px solid rgba(16,185,129,.5);color:#10b981}
.tc-dir-badge.sell{background:rgba(239,68,68,.18);border:1.5px solid rgba(239,68,68,.45);color:#ef4444}
.tc-pair-name{font-family:'Fredoka One',sans-serif;font-size:1.05rem;color:#f1f5f9;flex:1}
.tc-grade-badge{font-family:'Fredoka One',sans-serif;font-size:.75rem;padding:3px 9px;border-radius:7px;background:rgba(251,191,36,.12);border:1.5px solid rgba(251,191,36,.35);color:#fbbf24}
.tc-status{text-align:center;font-family:'JetBrains Mono',monospace;font-size:.68rem;font-weight:700;letter-spacing:.1em;padding:5px 12px;border-radius:8px;display:inline-block;margin:0 auto 12px}
.tc-status.running{background:rgba(56,189,248,.12);border:1.5px solid rgba(56,189,248,.35);color:#38bdf8}
.tc-status.win{background:rgba(16,185,129,.12);border:1.5px solid rgba(16,185,129,.4);color:#10b981}
.tc-status.loss{background:rgba(239,68,68,.12);border:1.5px solid rgba(239,68,68,.4);color:#ef4444}
.tc-prices{display:grid;grid-template-columns:1fr 1fr;gap:6px;margin-bottom:10px}
.tc-price-box{background:rgba(255,255,255,.04);border:1px solid rgba(255,255,255,.08);border-radius:10px;padding:7px 10px}
.tc-price-box.highlight{border-color:rgba(56,189,248,.3);background:rgba(56,189,248,.07)}
.tc-price-box.sl-box{border-color:rgba(239,68,68,.25);background:rgba(239,68,68,.05)}
.tc-price-box.tp-box{border-color:rgba(16,185,129,.25);background:rgba(16,185,129,.05)}
.tc-price-lbl{font-family:'Nunito',sans-serif;font-size:.55rem;font-weight:700;color:rgba(148,163,184,.7);text-transform:uppercase;letter-spacing:.08em;margin-bottom:2px}
.tc-price-val{font-family:'JetBrains Mono',monospace;font-size:.78rem;font-weight:700;color:#f1f5f9}
.tc-price-box.highlight .tc-price-val{color:#38bdf8}
.tc-price-box.sl-box .tc-price-val{color:#ef4444}
.tc-price-box.tp-box .tc-price-val{color:#10b981}
.tc-pnl-row{background:rgba(255,255,255,.04);border:1px solid rgba(255,255,255,.1);border-radius:12px;padding:10px 14px;display:flex;align-items:center;justify-content:space-between;margin-bottom:10px}
.tc-pnl-label{font-family:'Nunito',sans-serif;font-size:.65rem;font-weight:800;color:rgba(148,163,184,.7);text-transform:uppercase;letter-spacing:.1em}
.tc-pnl-val{font-family:'Fredoka One',sans-serif;font-size:1.2rem}
.tc-pnl-pct{font-family:'JetBrains Mono',monospace;font-size:.72rem;font-weight:700}
.tc-pnl-row.pos .tc-pnl-val,.tc-pnl-row.pos .tc-pnl-pct{color:#10b981}
.tc-pnl-row.neg .tc-pnl-val,.tc-pnl-row.neg .tc-pnl-pct{color:#ef4444}
.tc-pnl-row.neutral .tc-pnl-val,.tc-pnl-row.neutral .tc-pnl-pct{color:#38bdf8}
.tc-type-row{display:flex;justify-content:space-between;align-items:center;padding-top:10px;border-top:1px solid rgba(56,189,248,.12)}
.tc-type-lbl{font-family:'JetBrains Mono',monospace;font-size:.56rem;color:rgba(148,163,184,.5);letter-spacing:.1em}
.tc-rr-lbl{font-family:'Fredoka One',sans-serif;font-size:.85rem;color:#a78bfa}
.tc-close-btn{background:rgba(255,255,255,.07);border:1.5px solid rgba(255,255,255,.12);border-radius:12px;color:rgba(148,163,184,.8);font-family:'Nunito',sans-serif;font-size:.82rem;font-weight:700;cursor:pointer;padding:10px 28px;transition:all .2s}
.tc-close-btn:hover{background:rgba(255,255,255,.12);color:#f1f5f9}
.log-wrap{background:var(--s1);border:2px solid var(--border);border-radius:18px;overflow:hidden}
.log-hdr{padding:13px 18px;border-bottom:1.5px solid var(--border);display:flex;align-items:center;justify-content:space-between}
.log-ttl{font-family:'Fredoka One',sans-serif;font-size:.9rem;letter-spacing:.05em;color:#a78bfa}
.log-sub{font-family:'JetBrains Mono',monospace;font-size:.58rem;color:var(--dim);font-weight:700}
.log-body{padding:13px 18px;max-height:500px;overflow-y:auto;font-family:'JetBrains Mono',monospace;font-size:.67rem;line-height:1.95;color:var(--dim)}
.log-body::-webkit-scrollbar{width:4px}.log-body::-webkit-scrollbar-thumb{background:var(--muted);border-radius:2px}
.ll-s{color:var(--green)}.ll-e{color:var(--red)}.ll-i{color:rgba(56,189,248,.7)}.ll-t{color:#f9a8d4}.ll-m{color:var(--yellow)}.ll-p{color:rgba(167,139,250,.9)}
.diag-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(145px,1fr));gap:8px;margin-bottom:14px}
.dg{background:var(--s2);border:1.5px solid var(--border);border-radius:12px;padding:10px 12px;transition:all .2s}
.dg:hover{border-color:var(--border2);transform:translateY(-2px)}
.dg-lbl{font-family:'JetBrains Mono',monospace;font-size:.52rem;color:var(--dim);letter-spacing:.06em;margin-bottom:5px;text-transform:uppercase;font-weight:700}
.dg-val{font-family:'Fredoka One',sans-serif;font-size:1.6rem;line-height:1}
.paper-stats{display:grid;grid-template-columns:repeat(auto-fill,minmax(150px,1fr));gap:10px;margin-bottom:16px}
.pstat{background:var(--s2);border:1.5px solid var(--border);border-radius:14px;padding:14px}
.pstat-lbl{font-family:'JetBrains Mono',monospace;font-size:.52rem;color:var(--dim);text-transform:uppercase;letter-spacing:.07em;margin-bottom:5px;font-weight:700}
.pstat-val{font-family:'Fredoka One',sans-serif;font-size:1.7rem;line-height:1}
.toast{position:fixed;bottom:80px;left:50%;transform:translateX(-50%) translateY(100px);background:var(--s2);border:2px solid var(--border2);border-radius:16px;padding:12px 22px;font-family:'Nunito',sans-serif;font-size:.85rem;font-weight:800;box-shadow:0 18px 50px rgba(0,0,0,.6);opacity:0;transition:all .4s cubic-bezier(.34,1.56,.64,1);pointer-events:none;z-index:9999;white-space:nowrap}
.toast.show{transform:translateX(-50%) translateY(0);opacity:1}
.toast.bt{border-color:rgba(16,185,129,.4);color:var(--green)}.toast.st{border-color:rgba(239,68,68,.4);color:var(--red)}.toast.tt{border-color:rgba(249,115,22,.4);color:var(--orange)}.toast.pt{border-color:rgba(167,139,250,.4);color:#a78bfa}
@media(max-width:820px){.stats-grid{grid-template-columns:1fr 1fr 1fr}.prices-grid{grid-template-columns:repeat(3,1fr)}.hdr-in,.sec,.tab-wrap{padding:0 13px}.prog{padding:7px 13px}.snum{display:none}.lvl-grid{grid-template-columns:1fr 1fr}.trade-form{grid-template-columns:1fr}}
@media(max-width:480px){.stats-grid{grid-template-columns:1fr 1fr}.prices-grid{grid-template-columns:repeat(2,1fr)}}
</style>
</head>
<body>
<div class="bg-glow"></div>
<div class="tab-wrap">
  <!-- ══ PAGE: HOME ══ -->
  <div id="page-home" class="page active">
    <div class="pb" id="pb">⏸ SCANNER PAUSED — HIT RESUME! 🚀</div>
    <header class="hdr">
      <div class="hdr-glow"></div>
      <div class="hdr-in">
        <div class="brand"><span class="brand-icon">📡</span><div><div class="brand-name">Mad Man Strategy Scanner</div><div class="brand-sub">MEXC USDT PERP · CROSS MARGIN · UP TO 500X</div></div></div>
        <div class="scan-pill"><div class="sdot" id="sdot"></div><span class="stxt" id="stxt">SCANNING...</span></div>
        <div class="pnl-tick flat" id="pnl-tick" style="display:none"><span class="ptick-ico">💹</span><span class="ptick-val" id="ptick-val">–</span><span class="ptick-cnt" id="ptick-cnt"></span></div>
        <div class="hdr-right">
          <span class="snum" id="snum">SCAN #0</span>
          <button class="tbtn on" id="tbtn" onclick="toggleScanner()">⏹ Stop</button>
          <button class="obtn" onclick="logout()">👋 Exit</button>
        </div>
      </div>
    </header>
    <div class="prog"><div class="prog-in">
      <span class="prog-lbl" id="cpair">🔍 Initialising...</span>
      <div class="prog-track"><div class="prog-fill" id="pfill" style="width:0%"></div></div>
      <span class="prog-cnt" id="pcnt">0/0</span>
    </div></div>
    <div class="sec">
      <div class="sec-hdr"><span class="sec-ttl">📈 Live Prices</span><div class="sec-line"></div><span class="sec-note" id="pupd">–</span></div>
      <div class="prices-grid" id="pgrid"><div class="pc" style="min-height:70px"></div><div class="pc" style="min-height:70px"></div><div class="pc" style="min-height:70px"></div><div class="pc" style="min-height:70px"></div><div class="pc" style="min-height:70px"></div></div>
    </div>
    <div class="sec" style="margin-top:12px">
      <div class="stats-grid">
        <div class="sc s0"><div class="sc-lbl">Signals</div><div class="sc-val" id="st">0</div><div class="sc-sub">All time</div></div>
        <div class="sc s1"><div class="sc-lbl">🟢 Buy</div><div class="sc-val" style="color:var(--green)" id="sb">0</div></div>
        <div class="sc s2"><div class="sc-lbl">🔴 Sell</div><div class="sc-val" style="color:var(--red)" id="ss">0</div></div>
        <div class="sc s3"><div class="sc-lbl">Scans</div><div class="sc-val" style="color:var(--blue)" id="sc2">0</div><div class="sc-sub" id="sl2">–</div></div>
        <div class="sc s4"><div class="sc-lbl">👁 Monitoring</div><div class="sc-val" style="color:var(--yellow)" id="smon">0</div><div class="sc-sub">manip phase</div></div>
      </div>
    </div>
  </div>

  <!-- ══ PAGE: SIGNALS ══ -->
  <div id="page-signals" class="page">
    <div class="frow">
      <div class="ftitle">🎯 Mad Man Signals</div>
      <div class="fgrp">
        <select class="fsel" id="fd" onchange="renderSigs()"><option value="">All</option><option value="BUY">🟢 BUY</option><option value="SELL">🔴 SELL</option></select>
        <select class="fsel" id="fg" onchange="renderSigs()"><option value="">All Grades</option><option value="A+">⭐ A+</option><option value="A">A</option><option value="B">B+</option></select>
        <select class="fsel" id="ftf" onchange="renderSigs()"><option value="">All TFs</option><option value="Day1">1D</option><option value="Hour4">4H</option><option value="Hour3">3H</option><option value="Hour2">2H</option><option value="Min60">1H</option></select>
        <button class="action-btn close-btn" onclick="clearAllSignals()" style="font-size:.7rem;padding:5px 12px">🗑 Clear All</button>
      </div>
    </div>
    <div class="sig-list" id="slist"><div class="empty"><div class="empty-ico">🔭</div><div class="empty-t">Scanning the galaxy...</div><div class="empty-s">Hunting Mad Man Model #1 setups. TBS body close mandatory. Min 2R. 🎯</div></div></div>

    <!-- Monitor inside Signals page -->
    <div style="margin-top:20px">
      <div class="panel-ttl" style="font-family:'Fredoka One',sans-serif;font-size:1rem;letter-spacing:.06em;color:rgba(167,139,250,.8);margin-bottom:10px">👁 Monitor</div>
      <div class="panel">
        <div class="panel-ttl">🕯 Model #1 — Manipulation Monitor <span id="mon-count" style="font-family:'JetBrains Mono',monospace;font-size:.72rem;color:var(--dim)">(0/4)</span></div>
        <div style="font-family:'JetBrains Mono',monospace;font-size:.62rem;color:var(--dim);margin-bottom:12px;line-height:1.6">Pairs where C2 (manipulation candle) is actively forming — awaiting body close back inside CRT range to confirm TBS entry.</div>
        <div id="monitor-wrap"><div class="empty" style="padding:40px"><div class="empty-ico">🌙</div><div class="empty-t">Nothing monitored yet</div><div class="empty-s">Pairs in manipulation phase appear here automatically</div></div></div>
      </div>
      <div class="panel" style="margin-top:14px">
        <div class="panel-ttl" style="color:#38bdf8">👁 Model #2a — Sweep→FVG Monitor <span id="m2a-mon-count" style="font-family:'JetBrains Mono',monospace;font-size:.72rem;color:var(--dim)">(0)</span></div>
        <div style="font-family:'JetBrains Mono',monospace;font-size:.62rem;color:var(--dim);margin-bottom:12px;line-height:1.6">HTF OB/BB first touch → LTF swing forms → single candle sweeps the first-touch extreme → FVG forms → price retests FVG for entry.</div>
        <div id="m2a-monitor-wrap"><div class="empty" style="padding:40px"><div class="empty-ico">🔭</div><div class="empty-t">No Model #2a pairs queued</div><div class="empty-s">Pairs appear here when price first touches an HTF OB/BB in a P/D zone.</div></div></div>
      </div>
      <div class="panel" style="margin-top:14px">
        <div class="panel-ttl" style="color:#a78bfa">👁 Model #2b — Sweep→CHoCH→FVG Monitor <span id="m2b-mon-count" style="font-family:'JetBrains Mono',monospace;font-size:.72rem;color:var(--dim)">(0)</span></div>
        <div style="font-family:'JetBrains Mono',monospace;font-size:.62rem;color:var(--dim);margin-bottom:12px;line-height:1.6">HTF OB/BB first touch → single candle sweeps extreme → LTF CHoCH/BOS confirms → FVG forms → price retests FVG for entry.</div>
        <div id="m2b-monitor-wrap"><div class="empty" style="padding:40px"><div class="empty-ico">🔭</div><div class="empty-t">No Model #2b pairs queued</div><div class="empty-s">Pairs appear here once sweep is confirmed on LTF, waiting for CHoCH → FVG retest.</div></div></div>
      </div>
      <div class="panel" style="margin-top:14px">
        <div class="panel-ttl" style="color:#f97316">💎 Model #3 — HTF OB + LTF Sweep→CHoCH→OB Monitor <span id="m3-mon-count" style="font-family:'JetBrains Mono',monospace;font-size:.72rem;color:var(--dim)">(0)</span></div>
        <div style="font-family:'JetBrains Mono',monospace;font-size:.62rem;color:var(--dim);margin-bottom:12px;line-height:1.6">4H+ bearish/bullish trend → price taps HTF OB → 30m LTF: sweep of a high near/before OB → LTF CHoCH → entry at LTF OB before CHoCH. SL = sweep extreme.</div>
        <div id="m3-monitor-wrap"><div class="empty" style="padding:40px"><div class="empty-ico">💎</div><div class="empty-t">No Model #3 pairs queued</div><div class="empty-s">Pairs enter here when price taps a fresh 30m/45m OB with prior HTF CHoCH confirmation.</div></div></div>
      </div>
    </div>
  </div>

  <!-- ══ PAGE: TRADE ══ -->
  <div id="page-trade" class="page">
    <div class="inner-tabs">
      <button class="inner-tab active" onclick="swInner('trade-live',this)">🤖 Live Auto-Trade</button>
      <button class="inner-tab" onclick="swInner('trade-paper',this)">📝 Paper Trade</button>
      <button class="inner-tab" onclick="swInner('trade-open',this)">💹 Open Trades</button>
      <button class="inner-tab" onclick="swInner('trade-history',this)">📜 History</button>
    </div>

    <!-- Live Auto-Trade sub-tab -->
    <div id="itab-trade-live">
      <div class="panel">
        <div class="panel-ttl">🤖 Live Auto-Trade Settings <span id="trade-badge" style="font-size:.7rem;padding:3px 10px;border-radius:8px;background:rgba(239,68,68,.1);border:1.5px solid rgba(239,68,68,.3);color:var(--red);font-family:'JetBrains Mono',monospace;font-weight:700">DISABLED</span></div>
        <div style="font-family:'JetBrains Mono',monospace;font-size:.62rem;color:var(--dim);margin-bottom:14px">
          🔬 <b style="color:var(--text)">Model Scan Controls</b> — toggle each model's scanner independently
        </div>
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:18px">
          <button id="m1-toggle-btn" onclick="toggleModelScan(1)" style="padding:10px 8px;border-radius:10px;border:2px solid rgba(16,185,129,.4);background:rgba(16,185,129,.1);color:var(--green);font-family:'JetBrains Mono',monospace;font-size:.68rem;font-weight:700;cursor:pointer">🎯 Model #1: ON</button>
          <button id="m2-toggle-btn" onclick="toggleModelScan(2)" style="padding:10px 8px;border-radius:10px;border:2px solid rgba(16,185,129,.4);background:rgba(16,185,129,.1);color:var(--green);font-family:'JetBrains Mono',monospace;font-size:.68rem;font-weight:700;cursor:pointer">👁 Model #2a/2b: ON</button>
          <button id="m3-toggle-btn" onclick="toggleModelScan(3)" style="padding:10px 8px;border-radius:10px;border:2px solid rgba(16,185,129,.4);background:rgba(16,185,129,.1);color:var(--green);font-family:'JetBrains Mono',monospace;font-size:.68rem;font-weight:700;cursor:pointer">💎 Model #3: ON</button>
        </div>
        <div class="info-box info-green" style="margin-bottom:14px">🔑 <b>API Keys:</b> Loaded from Railway environment variables — no need to enter them here.</div>
        <div class="trade-form">
          <div class="tf-group"><div class="tf-lbl">Risk per Trade (%)</div><input class="tf-inp" type="number" id="t-risk" value="15" min="0.1" max="100" step="0.1"/></div>
          <div class="tf-group"><div class="tf-lbl">Leverage (10–500x) <span style="font-size:.7rem;color:var(--dim)">0 = auto-calculate</span></div><input class="tf-inp" type="number" id="t-leverage" value="30" min="0" max="500" step="1" placeholder="0 = auto"/><div style="font-size:.68rem;color:var(--dim);margin-top:4px;font-family:'JetBrains Mono',monospace">Auto: bot calculates leverage to risk exactly your % per trade · Manual: overrides auto</div></div>
          <div class="tf-group"><div class="tf-lbl">Max Simultaneous Open Trades</div><input class="tf-inp" type="number" id="t-max" value="2" min="1" max="10" step="1"/></div>
          <div class="tf-group"><div class="tf-lbl">Account Balance</div><div style="display:flex;gap:8px;align-items:center"><div class="bal-chip" style="flex:1">💰 $<span id="bal-val">–</span> USDT</div><button class="trade-btn tb-chk" style="padding:9px 14px" onclick="fetchBalance()">🔄 Fetch Balance</button></div></div>
        </div>
        <div class="info-box info-blue">ℹ️ <b>Risk model:</b> Cross margin · Auto-leverage 10x–500x · SL capped at 100% of margin per trade</div>
        <!-- ── Margin Mode Toggle ── -->
        <div style="margin:14px 0 0;padding:14px;background:var(--s2);border:2px solid var(--border);border-radius:14px">
          <div style="font-family:'Fredoka One',sans-serif;font-size:.9rem;letter-spacing:.04em;color:#a78bfa;margin-bottom:10px">⚖️ Margin Mode</div>
          <div style="font-family:'JetBrains Mono',monospace;font-size:.6rem;color:var(--dim);margin-bottom:12px;line-height:1.6">
            <b>Cross Margin:</b> All positions share your full account balance. Higher risk tolerance but losses can exceed margin.<br>
            <b>Isolated Margin:</b> Each position uses only the allocated margin. Loss capped at that position's margin.
          </div>
          <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap">
            <button id="margin-cross-btn" onclick="setMarginMode(2)"
              style="flex:1;padding:12px 10px;border-radius:12px;border:2px solid rgba(56,189,248,.5);background:rgba(56,189,248,.12);color:#38bdf8;font-family:'JetBrains Mono',monospace;font-size:.72rem;font-weight:700;cursor:pointer;transition:all .22s;min-width:120px">
              🔗 Cross Margin
            </button>
            <button id="margin-iso-btn" onclick="setMarginMode(1)"
              style="flex:1;padding:12px 10px;border-radius:12px;border:2px solid var(--muted);background:transparent;color:var(--dim);font-family:'JetBrains Mono',monospace;font-size:.72rem;font-weight:700;cursor:pointer;transition:all .22s;min-width:120px">
              🔒 Isolated Margin
            </button>
          </div>
          <div id="margin-mode-status" style="margin-top:10px;font-family:'JetBrains Mono',monospace;font-size:.62rem;color:var(--dim);text-align:center"></div>
        </div>
        <div class="trade-actions">
          <button class="trade-btn tb-save" onclick="saveTradeConfig()">💾 Save</button>
          <button class="trade-btn tb-on" id="t-enable-btn" onclick="enableTrade(true)">▶ Enable</button>
          <button class="trade-btn tb-off" id="t-disable-btn" onclick="enableTrade(false)" style="display:none">⏹ Disable</button>
        </div>
        <div class="t-status" id="trade-msg"></div>
        <div class="info-box info-red">⚠️ Real money risk. The bot uses cross margin with auto-calculated leverage (up to 500x). Start with 0.5–1% risk setting and monitor closely.</div>
      </div>
    </div>

    <!-- Paper Trade sub-tab -->
    <div id="itab-trade-paper" style="display:none">
      <div class="panel">
        <div class="panel-ttl">📝 Paper Trading Engine
          <span id="paper-badge" style="font-size:.7rem;padding:3px 10px;border-radius:8px;background:rgba(239,68,68,.1);border:1.5px solid rgba(239,68,68,.3);color:var(--red);font-family:'JetBrains Mono',monospace;font-weight:700">DISABLED</span>
          <span id="paper-auto-badge" style="display:none;font-size:.7rem;padding:3px 10px;border-radius:8px;background:rgba(167,139,250,.1);border:1.5px solid rgba(167,139,250,.35);color:#a78bfa;font-family:'JetBrains Mono',monospace;font-weight:700">AUTO ON</span>
        </div>
        <div class="info-box info-green">📝 Paper trading mirrors the live engine exactly — same entry, SL, TP, and risk % — but uses a virtual balance. Perfect for testing before going live.</div>
        <div class="trade-form">
          <div class="tf-group">
            <div class="tf-lbl">Virtual Balance (USDT)</div>
            <div style="display:flex;gap:8px">
              <input class="tf-inp" type="number" id="p-balance" placeholder="10000" min="100" step="100" style="flex:1"/>
              <button class="trade-btn tb-save" style="padding:9px 16px;white-space:nowrap" onclick="setPaperBalance()">Set</button>
            </div>
          </div>
          <div class="tf-group">
            <div class="tf-lbl">Risk per Trade (%)</div>
            <input class="tf-inp" type="number" id="p-risk" value="1" min="0.1" max="10" step="0.1"/>
          </div>
          <div class="tf-group">
            <div class="tf-lbl">Max Simultaneous Trades</div>
            <input class="tf-inp" type="number" id="p-max" value="4" min="1" max="10" step="1"/>
          </div>
          <div class="tf-group">
            <div class="tf-lbl">Current Balance</div>
            <div class="bal-chip" id="p-bal-chip">💰 $<span id="p-bal-val">10,000.00</span> USDT</div>
          </div>
        </div>
        <div class="trade-actions">
          <button class="trade-btn tb-save" onclick="savePaperConfig()">💾 Save Settings</button>
          <button class="trade-btn tb-on" id="p-enable-btn" onclick="enablePaper(true)">▶ Enable Paper</button>
          <button class="trade-btn tb-off" id="p-disable-btn" onclick="enablePaper(false)" style="display:none">⏹ Disable Paper</button>
          <button class="trade-btn" id="p-auto-btn" onclick="togglePaperAuto()" style="background:rgba(167,139,250,.1);border:2px solid rgba(167,139,250,.3);color:#a78bfa">🤖 Auto-Trade: OFF</button>
          <button class="trade-btn tb-chk" onclick="resetPaperStats()">🔄 Reset Stats</button>
        </div>
        <div class="t-status" id="paper-msg"></div>
      </div>
      <div class="panel">
        <div class="panel-ttl">📊 Paper Performance</div>
        <div class="paper-stats">
          <div class="pstat"><div class="pstat-lbl">Total Trades</div><div class="pstat-val" id="ps-total" style="color:#a78bfa">0</div></div>
          <div class="pstat"><div class="pstat-lbl">Wins</div><div class="pstat-val" id="ps-wins" style="color:var(--green)">0</div></div>
          <div class="pstat"><div class="pstat-lbl">Losses</div><div class="pstat-val" id="ps-losses" style="color:var(--red)">0</div></div>
          <div class="pstat"><div class="pstat-lbl">Win Rate</div><div class="pstat-val" id="ps-wr" style="color:var(--yellow)">0%</div></div>
          <div class="pstat"><div class="pstat-lbl">Total PnL</div><div class="pstat-val" id="ps-pnl" style="color:var(--green)">$0</div></div>
          <div class="pstat"><div class="pstat-lbl">Open Trades</div><div class="pstat-val" id="ps-open" style="color:var(--cyan)">0</div></div>
        </div>
      </div>
      <div class="panel">
        <div class="panel-ttl">📂 Open Paper Positions <span id="paper-trades-count" style="font-family:'JetBrains Mono',monospace;font-size:.72rem;color:var(--dim)">(0)</span></div>
        <div id="paper-trades-wrap"><div class="empty" style="padding:40px"><div class="empty-ico">📝</div><div class="empty-t">No open paper trades</div><div class="empty-s">Enable paper trading and turn on auto-trade to place trades from signals automatically</div></div></div>
      </div>
      <div class="panel">
        <div class="panel-ttl">📜 Paper Trade History</div>
        <div id="paper-history-wrap"><div class="empty" style="padding:30px"><div class="empty-ico">📭</div><div class="empty-t">No paper trades yet</div></div></div>
      </div>
    </div>

    <!-- Open Trades sub-tab -->
    <div id="itab-trade-open" style="display:none">
      <div class="panel">
        <div class="panel-ttl">💹 Running Trades <span id="trades-count" style="font-family:'JetBrains Mono',monospace;font-size:.72rem;color:var(--dim)">(0)</span><button class="action-btn tb-chk" style="margin-left:auto;border:none;padding:6px 14px" onclick="fetchPnl()">🔄 Refresh</button></div>
        <div id="live-trades-wrap"><div class="empty" style="padding:40px"><div class="empty-ico">😴</div><div class="empty-t">No open trades</div></div></div>
      </div>
      <div class="panel" style="margin-top:14px">
        <div class="panel-ttl">💹 Open Positions <span id="live-pos-count" style="font-family:'JetBrains Mono',monospace;font-size:.72rem;color:var(--dim)">(0)</span><button class="action-btn tb-chk" style="margin-left:auto;border:none;padding:6px 14px" onclick="window.fetchPnl()">🔄 Refresh</button></div>
        <div id="live-trades-wrap2"><div class="empty" style="padding:40px"><div class="empty-ico">😴</div><div class="empty-t">No open trades</div></div></div>
      </div>
    </div>

    <!-- History sub-tab -->
    <div id="itab-trade-history" style="display:none">
      <div class="panel">
        <div class="panel-ttl">📜 Recent Trades (Last 10)</div>
        <div id="history-wrap"><div class="empty" style="padding:40px"><div class="empty-ico">📭</div><div class="empty-t">No completed trades yet</div></div></div>
      </div>
      <div class="panel" style="margin-top:14px">
        <div class="panel-ttl">📜 Trade History</div>
        <div id="history-wrap2"><div class="empty" style="padding:40px"><div class="empty-ico">📭</div><div class="empty-t">No completed trades yet</div></div></div>
      </div>
    </div>
  </div>

  <!-- ══ PAGE: MARKET ══ -->
  <div id="page-market" class="page" style="height:calc(100vh - 72px);padding:0;margin:0">
    <iframe id="market-iframe" src="" style="width:100%;height:100%;border:none;background:var(--s1);display:block" loading="lazy"></iframe>
  </div>

  <!-- ══ PAGE: SETTINGS ══ -->
  <div id="page-settings" class="page">
    <div class="panel">
      <div class="panel-ttl">⚙️ General Settings</div>
      <div class="info-box info-green">🔑 <b>API Keys:</b> MEXC_API_KEY and MEXC_API_SECRET are loaded from Railway environment variables automatically.</div>
      <div class="trade-form" style="margin-top:14px">
        <div class="tf-group">
          <div class="tf-lbl">Risk per Trade (%)</div>
          <input class="tf-inp" type="number" id="s-risk" min="0.1" max="100" step="0.1" value="1"/>
        </div>
        <div class="tf-group">
          <div class="tf-lbl">Account Balance</div>
          <div style="display:flex;gap:8px;align-items:center">
            <div class="bal-chip" style="flex:1">💰 $<span id="s-bal-val">–</span> USDT</div>
            <button class="trade-btn tb-chk" style="padding:9px 14px" onclick="loadSettingsBal()">🔄 Fetch</button>
          </div>
        </div>
      </div>
    </div>
    <div class="panel">
      <div class="panel-ttl">👁 Visual Chart Analysis (Gemini AI · Free)</div>
      <div class="trade-form">
        <div class="tf-group">
          <div class="tf-lbl">Gemini API Key</div>
          <input class="tf-inp" type="password" id="s-gemini-key" placeholder="AIza…" autocomplete="off"/>
          <div style="font-size:.68rem;color:var(--dim);margin-top:4px;font-family:'JetBrains Mono',monospace">
            Free key at <b>aistudio.google.com</b> → Get API key. Bot renders charts
            &amp; sends to Gemini Vision — reads structure like a trader.
            Max 3 calls/pair. 1500 free requests/day.
          </div>
        </div>
        <div class="tf-group" style="margin-top:10px">
          <label style="display:flex;align-items:center;gap:8px;color:var(--text);font-size:.85rem;cursor:pointer">
            <input type="checkbox" id="s-visual-mode" checked style="width:16px;height:16px;accent-color:#a78bfa"/>
            🤖 Enable visual chart analysis
          </label>
          <div style="font-size:.68rem;color:var(--dim);margin-top:4px;font-family:'JetBrains Mono',monospace">
            ON: Gemini reads 4H→2H→1H→LTF charts visually. OFF: math-based OHLC only.
          </div>
        </div>
      </div>
    </div>
    <div class="panel">
      <div class="panel-ttl">📢 Telegram Notifications</div>
      <div class="trade-form">
        <div class="tf-group">
          <div class="tf-lbl">Bot Token</div>
          <input class="tf-inp" type="text" id="s-tg-token" placeholder="123456:ABCdef..."/>
        </div>
        <div class="tf-group">
          <div class="tf-lbl">Chat ID</div>
          <input class="tf-inp" type="text" id="s-tg-chat" placeholder="Your Telegram chat ID"/>
        </div>
        <div class="tf-group">
          <div class="tf-lbl">Notify on</div>
          <div style="display:flex;gap:20px;margin-top:8px">
            <label style="display:flex;align-items:center;gap:8px;color:var(--text);font-size:.85rem;cursor:pointer">
              <input type="checkbox" id="s-tg-signals" style="width:18px;height:18px;accent-color:#a78bfa"/> 📊 Signal alerts
            </label>
            <label style="display:flex;align-items:center;gap:8px;color:var(--text);font-size:.85rem;cursor:pointer">
              <input type="checkbox" id="s-tg-trades" style="width:18px;height:18px;accent-color:#a78bfa"/> 🤖 Trade alerts
            </label>
          </div>
        </div>
      </div>
    </div>
    <div class="panel">
      <div class="panel-ttl">⚡ Update Intervals</div>
      <div class="trade-form">
        <div class="tf-group">
          <div class="tf-lbl">Live Price Refresh (seconds)</div>
          <input class="tf-inp" type="number" id="s-price-int" min="1" max="60" step="1" value="1"/>
        </div>
        <div class="tf-group">
          <div class="tf-lbl">Scan Interval — delay between each pair (seconds)</div>
          <input class="tf-inp" type="number" id="s-scan-int" min="1" max="30" step="1" value="1"/>
        </div>
        <div class="tf-group">
          <div class="tf-lbl">Cycle Rest — pause after full scan round (seconds)</div>
          <input class="tf-inp" type="number" id="s-cycle-rest" min="1" max="3600" step="1" value="5"/>
        </div>
        <div class="tf-group">
          <div class="tf-lbl">Market Page — Pairs Price Refresh (seconds)</div>
          <input class="tf-inp" type="number" id="s-market-refresh" min="1" max="300" step="1" value="1"/>
          <div style="font-size:.68rem;color:var(--dim);margin-top:4px;font-family:'JetBrains Mono',monospace">How often the Market/Watchlist page auto-refreshes live pair prices (default 10s)</div>
        </div>
      </div>
    </div>
    <div style="padding:0 4px 24px">
      <button class="trade-btn tb-save" style="width:100%;padding:14px;font-size:1rem;border-radius:14px" onclick="saveAllSettings()">💾 Save All Settings</button>
      <div class="t-status" id="settings-msg" style="margin-top:12px;text-align:center;font-size:.85rem"></div>
    </div>
    <!-- Log section inside Settings -->
    <div style="background:var(--s1);border:2px solid var(--border);border-radius:18px;padding:18px 20px;margin-bottom:14px">
      <div style="font-family:'Fredoka One',sans-serif;font-size:.9rem;letter-spacing:.05em;color:#a78bfa;margin-bottom:14px">🔬 Gate Diagnostics</div>
      <div class="diag-grid" id="diag-grid"></div>
    </div>
    <div class="log-wrap">
      <div class="log-hdr">
        <span class="log-ttl">🖥️ Live Log</span>
        <div style="display:flex;align-items:center;gap:10px">
          <span class="log-sub">UPDATES EVERY 3S</span>
          <button class="action-btn tb-chk" onclick="fetchLog()" style="border:none;padding:4px 10px">🔄 Refresh</button>
        </div>
      </div>
      <div class="log-body" id="lbody"><div style="color:rgba(56,189,248,.5);font-style:italic">Waiting for log entries... Scanner logs appear here in real-time.</div></div>
    </div>
  </div>
</div>
<!-- ══ BOTTOM NAV ══ -->
<nav class="bottom-nav">
  <div class="bnav-inner">
    <button class="bnav-btn active" id="bnav-home" onclick="swPage('home',this)">
      <div class="bnav-ico">🏠</div>
      <span class="bnav-lbl">Home</span>
    </button>
    <button class="bnav-btn" id="bnav-signals" onclick="swPage('signals',this)">
      <div class="bnav-ico">📊</div>
      <span class="bnav-lbl">Signals</span>
    </button>
    <button class="bnav-btn" id="bnav-trade" onclick="swPage('trade',this)">
      <div class="bnav-ico">💹</div>
      <span class="bnav-lbl">Trade</span>
    </button>
    <button class="bnav-btn" id="bnav-market" onclick="swPage('market',this)">
      <div class="bnav-ico">📈</div>
      <span class="bnav-lbl">Market</span>
    </button>
    <button class="bnav-btn" id="bnav-settings" onclick="swPage('settings',this)">
      <div class="bnav-ico">⚙️</div>
      <span class="bnav-lbl">Settings</span>
    </button>
  </div>
</nav>

<div class="tc-modal" id="tc-modal">
  <div class="tc-wrap">
    <div class="tc-card" id="tc-card">
      <div class="tc-bg"></div>
      <div class="tc-glass">
        <div class="tc-header">
          <div class="tc-brand" style="font-family:'Fredoka One',sans-serif;font-size:1.6rem;letter-spacing:.12em;background:linear-gradient(135deg,#a78bfa,#38bdf8,#10b981);-webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text;margin-bottom:2px">SIGNALCORE</div>
          <div class="tc-tagline">MAD MAN STRATEGY · SMARTER SIGNALS · BETTER TRADES</div>
        </div>
        <div class="tc-dir-row">
          <span class="tc-dir-badge" id="tc-dir-badge">LONG</span>
          <span class="tc-pair-name" id="tc-pair-name">BTC_USDT</span>
          <span class="tc-grade-badge" id="tc-grade-badge">A+</span>
        </div>
        <div style="text-align:center">
          <span class="tc-status" id="tc-status-badge">🔄 RUNNING</span>
        </div>
        <div class="tc-prices" id="tc-prices"></div>
        <div class="tc-pnl-row" id="tc-pnl-row">
          <span class="tc-pnl-label">PnL</span>
          <span class="tc-pnl-val" id="tc-pnl-val">–</span>
          <span class="tc-pnl-pct" id="tc-pnl-pct">–</span>
        </div>
        <div class="tc-type-row">
          <span class="tc-type-lbl" id="tc-type-lbl">MAD MAN MODEL #1</span>
          <span class="tc-rr-lbl" id="tc-rr-lbl">–</span>
        </div>
      </div>
    </div>
    <div style="display:flex;gap:10px;justify-content:center">
      <button class="tc-close-btn" onclick="document.getElementById('tc-modal').classList.remove('show')">✕ Close</button>
      <button class="tc-close-btn" onclick="saveTcCard()" style="background:linear-gradient(135deg,#7c3aed,#db2777);color:#fff;border:none">📸 Save Card</button>
    </div>
  </div>
</div>
<div class="toast" id="toast"></div>


<script>
(function(){
'use strict';
let allSigs=[],toastT,activePage='home',activeInnerTrade='trade-live',tick=0,lastCount=0,paperAutoOn=false;
const $=id=>document.getElementById(id);
function toast(m,t,d=3500){const el=$("toast");el.textContent=m;el.className="toast show"+(t==="buy"?" bt":t==="sell"?" st":t==="trade"?" tt":t==="paper"?" pt":"");clearTimeout(toastT);toastT=setTimeout(()=>el.classList.remove("show"),d);}
window.toast=toast;
function scoreColor(s){return s>=88?"#fbbf24":s>=75?"#a78bfa":s>=60?"var(--blue)":s>=45?"var(--orange)":"var(--dim)";}
function fmt(v){if(v===null||v===undefined||v==="–")return "–";const n=parseFloat(v);if(isNaN(n))return String(v).slice(0,12);if(n>=1000)return n.toLocaleString("en",{maximumFractionDigits:2});if(n>=100)return n.toFixed(2);if(n>=1)return n.toFixed(4);if(n>=0.1)return n.toFixed(5);if(n>=0.01)return n.toFixed(6);if(n>=0.001)return n.toFixed(7);return n.toFixed(8);}
function fmtP(v){const n=Number(v);if(!n)return"–";if(n>=10000)return"$"+n.toLocaleString(undefined,{maximumFractionDigits:2});if(n>=1)return"$"+n.toFixed(4);return"$"+n.toFixed(6);}
const TFM={"Day1":"1D","Hour4":"4H","Hour3":"3H","Hour2":"2H","Min60":"1H","Min45":"45m","Min30":"30m","Min15":"15m","Min10":"10m","Min5":"5m","Min4":"4m","Min3":"3m","Min2":"2m","Min1":"1m"};
const TOP=["PENGU_USDT","GME_USDT","MEME_USDT","RIVER_USDT","DRIFT_USDT","FARTCOIN_USDT","FLOKI_USDT","BONK_USDT","WIF_USDT","PEPE_USDT","AVAX_USDT","POPCAT_USDT","ONDO_USDT","ARB_USDT","RENDER_USDT","FET_USDT","OPG_USDT","APT_USDT","LINK_USDT","TAO_USDT","INJ_USDT","SEI_USDT","HBAR_USDT","KAS_USDT","NEAR_USDT","SUI_USDT","HYPE_USDT","MAT_USDT","XRP_USDT","BAN_USDT"];
async function fetchPrices(){try{const r=await fetch("/api/prices");const data=await r.json();$("pupd").textContent="Updated "+new Date().toLocaleTimeString();const withData=TOP.map(sym=>({sym,d:data[sym]})).filter(x=>x.d);withData.sort((a,b)=>Math.abs(b.d.change)-Math.abs(a.d.change));const top5=withData.slice(0,5);$("pgrid").innerHTML=top5.map(({sym,d})=>{const name=sym.replace("_USDT","");const up=d.change>=0;return`<div class="pc ${up?"up":"dn"}"><div class="pc-sym">${name}/USDT</div><div class="pc-price ${up?"up":"dn"}">${fmtP(d.price)}</div><span class="pc-chg ${up?"up":"dn"}">${up?"▲":"▼"} ${Math.abs(d.change).toFixed(2)}%</span></div>`;}).join("");}catch{}}
function buildCard(s,idx){const dir=(s.direction||"BUY").toUpperCase();const sc=s.score||0,gr=s.grade||"–";const gc={"A+":"gAp","A":"gA","B":"gB","C":"gC","D":"gD"}[gr]||"gD";const crtTF=TFM[s.tf]||s.tf||"–";const obTF=TFM[s.ob_tf]||s.ob_tf||"–";const isND=s.tf==="Day1";const zt=s.zone_type||s.ob_zone||"–";const isAplus=gr==="A+";const details=(s.details||[]).join("\n");const cf=(ok,l)=>`<span class="cf ${ok?"cf-ok":"cf-no"}">${ok?"✓":"✗"} ${l}</span>`;const cfw=(ok,l)=>`<span class="cf ${ok?"cf-ok":"cf-w"}">${ok?"✓":"⚠"} ${l}</span>`;const cfg=(ok,l)=>`<span class="cf ${ok?"cf-g":"cf-no"}">${ok?"💎":"◇"} ${l}</span>`;
const barFill=Math.round(sc/100*100);const barColor=sc>=88?"var(--yellow)":sc>=75?"#a78bfa":sc>=60?"var(--blue)":"var(--orange)";
return`<div class="scard ${dir.toLowerCase()}"><div class="card-hdr"><span class="dtag ${dir}">${dir}</span><span class="csym">${s.symbol||"–"}</span><div class="chips"><span class="chip chip-tf">${crtTF} Mad Man</span>${!isND&&s.ob_tf&&s.ob_tf!=="N/A"?`<span class="chip chip-ob">${zt} ${obTF}</span>`:""}<span class="chip chip-tr ${s.trend}">${s.trend}</span>${isAplus?'<span class="chip chip-aplus">⭐ A+</span>':""} ${s.from_monitor?'<span class="chip" style="color:#fbbf24;border-color:rgba(251,191,36,.3);background:rgba(251,191,36,.07)">👁 Monitored</span>':""}</div><span class="gtag ${gc}">${gr}</span><button onclick="discardSignal(${idx})" style="background:rgba(239,68,68,.08);border:1.5px solid rgba(239,68,68,.25);border-radius:8px;color:var(--red);font-size:.65rem;padding:3px 8px;cursor:pointer;font-family:'Nunito',sans-serif;font-weight:800;flex-shrink:0;margin-left:4px" title="Discard signal">🗑</button><span class="cts">${s.timestamp||""}</span></div>
<div class="lvl-grid"><div class="lv lv-e"><div class="lv-lbl">🎯 Entry (TBS Open)</div><div class="lv-val">${fmt(s.entry)}</div></div><div class="lv lv-e" style="border-color:rgba(167,139,250,.2)"><div class="lv-lbl">TBS TF</div><div class="lv-val" style="color:#a78bfa">${TFM[s.tbs_tf]||s.tbs_tf||"–"}</div></div><div class="lv lv-s"><div class="lv-lbl">🛑 Stop Loss</div><div class="lv-val">${fmt(s.sl)}</div></div><div class="lv lv-t"><div class="lv-lbl">🎯 Take Profit</div><div class="lv-val">${fmt(s.tp)}</div></div><div class="lv lv-r"><div class="lv-lbl">📊 RR</div><div class="lv-val">${s.rr}R</div></div><div class="lv"><div class="lv-lbl">CRH</div><div class="lv-val" style="color:#f9a8d4">${fmt(s.crh)}</div></div><div class="lv"><div class="lv-lbl">CRL</div><div class="lv-val" style="color:#6ee7b7">${fmt(s.crl)}</div></div></div>
<div class="cfms">${cf(s.tbs_found,`TBS ${TFM[s.tbs_tf]||s.tbs_tf||"?"}`)}${cfw(s.fvg_found,s.fvg_type||"FVG")}${cfw(s.choch_found,"CHOCH")}${cfw(s.liq_swept,"Liq Sweep")}${cfw(s.ob_respected,"OB Resp")}${cfg(isAplus,"A+")}</div>
<div class="srow"><span class="slbl">Score</span><div class="strack"><div class="sfill" style="width:${barFill}%;background:${barColor}"></div></div><span class="snum2" style="color:${barColor}">${sc}/100</span></div>
<button class="dettog" onclick="toggleDet(${idx})">▶ Score Breakdown</button><div class="detbox" id="det-${idx}">${details}</div></div>`;}
window.toggleDet=function(i){const b=$("det-"+i);if(!b)return;b.classList.toggle("open");const t=b.previousElementSibling;if(t)t.textContent=b.classList.contains("open")?"▼ Score Breakdown":"▶ Score Breakdown";};
window.renderSigs=function(){const dF=$("fd").value,gF=$("fg").value,tfF=$("ftf").value;let f=allSigs.filter(s=>{if(dF&&s.direction!==dF)return false;if(tfF&&s.tf!==tfF)return false;if(gF){if(gF==="A+"&&s.grade!=="A+")return false;if(gF==="A"&&s.grade!=="A")return false;if(gF==="B"&&!["A+","A","B"].includes(s.grade))return false;}return true;});const list=$("slist");if(!f.length){list.innerHTML='<div class="empty"><div class="empty-ico">🔭</div><div class="empty-t">Scanning the galaxy...</div><div class="empty-s">Hunting Mad Man Model #1 setups. TBS body close mandatory. Min 2R.</div></div>';return;}list.innerHTML=f.slice(0,100).map((s,i)=>buildCard(s,i)).join("");};
async function fetchSigs(){try{const r=await fetch("/api/signals?limit=200");const data=await r.json();allSigs=data;if(data.length>lastCount&&lastCount>0){const n=data[0];toast(`🎯 ${n.direction} ${n.symbol} · ${n.score}/100 ${n.grade} · ${n.rr}R`,n.direction==="BUY"?"buy":"sell");}lastCount=data.length;renderSigs();}catch{}}
async function fetchStats(){try{const r=await fetch("/api/stats");const d=await r.json();$("st").textContent=d.total||0;$("sb").textContent=d.buys||0;$("ss").textContent=d.sells||0;}catch{}}
async function fetchState(){try{const r=await fetch("/api/scan-state");const d=await r.json();const pct=d.total_pairs>0?Math.round(d.pairs_done/d.total_pairs*100):0;$("pfill").style.width=pct+"%";$("pcnt").textContent=`${d.pairs_done}/${d.total_pairs}`;$("cpair").textContent=d.current_pair?`🔍 ${d.current_pair}`:"⏳ Waiting...";$("sc2").textContent=d.scan_count||0;$("sl2").textContent=d.last_scan?`Last: ${d.last_scan}`:"–";$("snum").textContent=`Scan #${d.scan_count||0}`;const en=d.enabled!==false;$("tbtn").textContent=en?"⏹ Stop":"▶ Resume";$("tbtn").className="tbtn "+(en?"on":"off");$("sdot").className="sdot"+(en?"":" off");$("stxt").textContent=en?"SCANNING...":"PAUSED";$("stxt").className="stxt"+(en?"":" off");$("pb").className="pb"+(en?"":" show");}catch{}}
async function fetchMonitor(){if(activePage!=="signals")return;try{
  // ── M1 Manipulation Monitor ──
  const r1=await fetch("/api/monitor");
  const d1=await r1.json();
  $("smon").textContent=d1.length;
  $("mon-count").textContent=`(${d1.length}/4)`;
  const w1=$("monitor-wrap");
  if(w1){
    if(!d1.length){
      w1.innerHTML='<div class="empty" style="padding:40px"><div class="empty-ico">🌙</div><div class="empty-t">Nothing monitored</div><div class="empty-s">Pairs in manipulation phase appear here automatically</div></div>';
    }else{
      w1.innerHTML=`<div class="monitor-grid">${d1.map(m=>{
        const dir=(m.direction||"BUY").toUpperCase();
        const tf=TFM[m.crt_tf]||m.crt_tf||"–";
        const phase=m.phase||"WATCHING";
        const isRunning=phase==="RUNNING";
        const phaseBadge=isRunning
          ?`<div class="mon-status" style="background:rgba(16,185,129,.15);border-color:rgba(16,185,129,.4);color:var(--green);font-size:.65rem">🟢 RUNNING — LIMIT PLACED</div>`
          :`<div class="mon-status">⏳ AWAITING TBS CLOSE</div>`;
        const runningRows=isRunning?`
        <div class="mon-row"><span>Entry</span><span>${fmt(m.signal_entry)}</span></div>
        <div class="mon-row"><span>SL</span><span style="color:var(--red)">${fmt(m.signal_sl)}</span></div>
        <div class="mon-row"><span>TP</span><span style="color:var(--green)">${fmt(m.signal_tp)}</span></div>
        <div class="mon-row"><span>RR</span><span style="color:var(--yellow)">${m.signal_rr}R</span></div>
        <div class="mon-row"><span>Live Price</span><span style="color:var(--yellow)">${fmt(m.last_price)}</span></div>`:"";
        return`<div class="mon-card ${dir.toLowerCase()}"><div class="mon-sym">${dir==="BUY"?"🟢":"🔴"} ${m.symbol||"–"}</div>
        <div class="mon-row"><span>Mad Man TF</span><span>${tf}</span></div>
        <div class="mon-row"><span>Key Level</span><span>${m.kl_type||"–"}</span></div>
        <div class="mon-row"><span>Trend</span><span>${m.trend||"–"}</span></div>
        <div class="mon-row"><span>CRH</span><span>${fmt(m.crh)}</span></div>
        <div class="mon-row"><span>CRL</span><span>${fmt(m.crl)}</span></div>
        <div class="mon-row"><span>Zone</span><span>${(m.zone_name||"–").slice(0,22)}</span></div>
        <div class="mon-row"><span>Added</span><span>${m.added_at||"–"}</span></div>
        ${runningRows}${phaseBadge}</div>`;
      }).join("")}</div>`;
    }
  }
  // ── M2a / M2b Monitor ──
  const r2=await fetch("/api/m2-monitor");
  const d2=await r2.json();
  const d2a=d2.filter(m=>!m.model||m.model==="2a");
  const d2b=d2.filter(m=>!m.model||m.model==="2b");
  const setCount=(id,n)=>{const el=$(id);if(el)el.textContent=`(${n})`;};
  setCount("m2a-mon-count",d2a.length);setCount("m2b-mon-count",d2b.length);
  const phaseLbl2={"AWAIT_PATTERN":"👀 Awaiting Pattern","AWAIT_FVG":"🔍 Awaiting FVG","AWAIT_TAP":"🎯 Awaiting FVG Tap"};
  const phaseClr2={"AWAIT_PATTERN":"var(--yellow)","AWAIT_FVG":"var(--orange)","AWAIT_TAP":"var(--green)"};
  function renderM2Cards(data,wrapId,accentColor){
    const w=$(wrapId);if(!w)return;
    if(!data.length){w.innerHTML='<div class="empty" style="padding:30px"><div class="empty-ico">🔭</div><div class="empty-t">No pairs queued</div></div>';return;}
    w.innerHTML=`<div class="monitor-grid">${data.map(m=>{
      const dir=(m.direction||"BUY").toUpperCase();
      const phase=m.phase||"AWAIT_PATTERN";
      const model=m.model?`Model #${m.model}`:"Queued";
      const htf=TFM[m.htf]||m.htf||"–";const ltf=TFM[m.ltf]||m.ltf||"–";
      const sweepRow=m.sweep_extreme?`<div class="mon-row"><span>Sweep</span><span style="color:var(--orange)">${fmt(m.sweep_extreme)}</span></div>`:"";
      const chochRow=m.choch_level?`<div class="mon-row"><span>CHoCH</span><span style="color:var(--cyan)">${fmt(m.choch_level)}</span></div>`:"";
      const fvgRow=(m.fvg_top&&m.fvg_bot)?`<div class="mon-row"><span>FVG Zone</span><span style="color:var(--green)">${fmt(m.fvg_bot)}–${fmt(m.fvg_top)}</span></div>`:"";
      return`<div class="mon-card ${dir.toLowerCase()}" style="border-color:${accentColor}40">
      <div class="mon-sym">${dir==="BUY"?"🟢":"🔴"} ${m.symbol||"–"} <span style="font-size:.6rem;color:${accentColor};font-family:'JetBrains Mono',monospace">${model}</span></div>
      <div class="mon-row"><span>HTF Zone</span><span>${htf}</span></div>
      <div class="mon-row"><span>LTF Watch</span><span>${ltf}</span></div>
      <div class="mon-row"><span>Direction</span><span>${dir}</span></div>
      <div class="mon-row"><span>Zone</span><span>${(m.zone_name||"–").slice(0,22)}</span></div>
      <div class="mon-row"><span>FT Extreme</span><span>${fmt(m.ft_extreme)}</span></div>
      ${sweepRow}${chochRow}${fvgRow}
      <div class="mon-row"><span>Liq Target</span><span>${fmt(m.liq_target)}</span></div>
      <div class="mon-row"><span>Added</span><span>${m.added_at||"–"}</span></div>
      <div class="mon-status" style="background:${accentColor}18;border-color:${accentColor}50;color:${phaseLbl2[phase]?phaseClr2[phase]:"var(--yellow)"}">
        ${phaseLbl2[phase]||phase}
      </div></div>`;
    }).join("")}</div>`;
  }
  renderM2Cards(d2a,"m2a-monitor-wrap","#38bdf8");
  renderM2Cards(d2b,"m2b-monitor-wrap","#a78bfa");
  // ── Model #3 Monitor (was M4) ──
  const r3=await fetch("/api/m4-monitor");
  const d3=await r3.json();
  setCount("m3-mon-count",d3.length);
  const w3=$("m3-monitor-wrap");
  if(w3){
    if(!d3.length){
      w3.innerHTML='<div class="empty" style="padding:40px"><div class="empty-ico">💎</div><div class="empty-t">No Model #3 pairs queued</div><div class="empty-s">Pairs enter here when price taps a fresh 30m/45m OB with HTF CHoCH confirmed.</div></div>';
    }else{
      const phaseLbl3={"AWAIT_SWEEP":"⚡ Awaiting LTF Sweep","AWAIT_CHOCH":"🔄 Awaiting LTF CHoCH","AWAIT_TAP":"🎯 Awaiting OB Tap"};
      const phaseClr3={"AWAIT_SWEEP":"var(--yellow)","AWAIT_CHOCH":"var(--orange)","AWAIT_TAP":"var(--green)"};
      w3.innerHTML=`<div class="monitor-grid">${d3.map(m=>{
        const dir=(m.direction||"SELL").toUpperCase();
        const phase=m.phase||"AWAIT_SWEEP";
        const htf=TFM[m.htf]||m.htf||"–";const ltf=TFM[m.ltf]||m.ltf||"–";
        const sweepRow=m.sweep_extreme?`<div class="mon-row"><span>Sweep</span><span style="color:var(--orange)">${fmt(m.sweep_extreme)}</span></div>`:"";
        const chochRow=m.choch_lvl?`<div class="mon-row"><span>CHoCH</span><span style="color:var(--cyan)">${fmt(m.choch_lvl)}</span></div>`:"";
        const obRow=(m.ob_top&&m.ob_bot)?`<div class="mon-row"><span>LTF OB</span><span style="color:#f97316">${fmt(m.ob_bot)}–${fmt(m.ob_top)}</span></div>`:"";
        const entryRow=m.ob_entry?`<div class="mon-row"><span>Potential Entry</span><span style="color:var(--green);font-weight:700">${fmt(m.ob_entry)}</span></div>`:"";
        const rrRow=m.rr?`<div class="mon-row"><span>Est. RR</span><span style="color:var(--yellow)">${m.rr}R</span></div>`:"";
        const slRow=m.sl?`<div class="mon-row"><span>SL</span><span style="color:var(--red)">${fmt(m.sl)}</span></div>`:"";
        return`<div class="mon-card ${dir.toLowerCase()}" style="border-color:rgba(249,115,22,.3)">
        <div class="mon-sym">${dir==="BUY"?"🟢":"🔴"} ${m.symbol||"–"} <span style="font-size:.6rem;color:#f97316;font-family:'JetBrains Mono',monospace">Model #3</span></div>
        <div class="mon-row"><span>HTF Zone</span><span>${htf}</span></div>
        <div class="mon-row"><span>LTF Watch</span><span>${ltf}</span></div>
        <div class="mon-row"><span>Direction</span><span>${dir}</span></div>
        <div class="mon-row"><span>HTF OB</span><span>${fmt(m.zone_bot||0)}–${fmt(m.zone_top||0)}</span></div>
        ${sweepRow}${chochRow}${obRow}${entryRow}${rrRow}${slRow}
        <div class="mon-row"><span>Liq Target</span><span>${fmt(m.liq_target)}</span></div>
        <div class="mon-row"><span>Added</span><span>${m.added_at||"–"}</span></div>
        <div class="mon-status" style="background:rgba(249,115,22,.12);border-color:rgba(249,115,22,.4);color:${phaseClr3[phase]||"var(--yellow)"}">
          ${phaseLbl3[phase]||phase}
        </div></div>`;
      }).join("")}</div>`;
    }
  }
}catch(e){console.error("fetchMonitor",e);}}
window.fetchPnl=async function(){const onTrades=activeTab==="trades",onCfg=activeTab==="trade-cfg";if(!onTrades&&!onCfg)return;try{const[tr,pnl,hist]=await Promise.all([fetch("/api/trades").then(r=>r.json()),fetch("/api/pnl").then(r=>r.json()),fetch("/api/recent-trades").then(r=>r.json())]);const pnlMap={};(pnl.positions||[]).forEach(p=>pnlMap[p.symbol]=p);const buildOpenTable=function(trades,wrapId,countId){const wrap=$(wrapId);if(!wrap)return;if(countId&&$(countId))$(countId).textContent=`(${trades.length})`;if(!trades.length){wrap.innerHTML='<div class="empty" style="padding:40px"><div class="empty-ico">😴</div><div class="empty-t">No open trades</div></div>';return;}window._liveTradesData=trades;wrap.innerHTML=`<div style="overflow-x:auto"><table class="tbl"><thead><tr><th>Pair</th><th>Dir</th><th>Entry Price</th><th>Current Price</th><th>SL</th><th>TP</th><th>RR</th><th>Lev</th><th>Margin</th><th>Live PnL</th><th>ROI%</th><th>Grade</th><th>Card</th><th>Action</th></tr></thead><tbody>${trades.map((t,i)=>{const live=pnlMap[t.symbol]||{};const pv=live.pnl||0;const roi=live.roi_pct||0;const cur=live.current||0;const lev=live.leverage||t.leverage||"–";const margin=(live.margin||0).toFixed(2);return`<tr><td style="font-weight:800;color:var(--text)">${t.symbol}</td><td class="${t.direction==="BUY"?"buy":"sell"}">${t.direction}</td><td>${fmt(t.entry)}</td><td style="color:var(--yellow)">${cur?fmt(cur):"–"}</td><td style="color:var(--red)">${fmt(t.sl)}</td><td style="color:var(--green)">${fmt(t.tp)}</td><td style="color:var(--yellow)">${t.rr}R</td><td style="color:#a78bfa">${lev}x</td><td>$${margin}</td><td class="pos-pnl ${pv>=0?"pos":"neg"}">${pv>=0?"+":""}${pv.toFixed(2)}</td><td class="pos-pnl ${roi>=0?"pos":"neg"}">${roi>=0?"+":""}${roi.toFixed(2)}%</td><td style="color:${scoreColor(t.score||0)};font-family:'Fredoka One',sans-serif">${t.grade||"–"}</td><td><button class="action-btn share-btn" onclick="showTradeCard({symbol:'${t.symbol}',direction:'${t.direction}',entry:${t.entry},sl:${t.sl},tp:${t.tp},rr:'${t.rr}',grade:'${t.grade||'–'}',score:${t.score||0},pnl:${pv.toFixed(2)},pnl_pct:${roi.toFixed(2)},market_price:${cur},status:'RUNNING'},true,'LIVE')">📸</button></td><td><button class="action-btn close-btn" onclick="closeTrade('${t.symbol}')">✕</button></td></tr>`;}).join("")}</tbody></table></div>`;};const buildHistTable=function(data,wrapId){const wrap=$(wrapId);if(!wrap)return;if(!data.length){wrap.innerHTML='<div class="empty" style="padding:40px"><div class="empty-ico">📭</div><div class="empty-t">No completed trades yet</div></div>';return;}window._histData=data;wrap.innerHTML=`<div style="overflow-x:auto"><table class="tbl"><thead><tr><th>Pair</th><th>Dir</th><th>Entry Price</th><th>Exit Price</th><th>SL</th><th>TP</th><th>RR</th><th>PnL $</th><th>PnL %</th><th>Grade</th><th>Status</th><th>Opened</th><th>Card</th></tr></thead><tbody>${data.map((t,i)=>{const pv=t.pnl||0;const pp=t.pnl_pct||0;const isW=(t.status||"").toLowerCase().includes("tp");return`<tr><td style="font-weight:800;color:var(--text)">${t.symbol}</td><td class="${t.direction==="BUY"?"buy":"sell"}">${t.direction}</td><td>${fmt(t.entry)}</td><td style="color:var(--yellow)">${fmt(t.close_price||t.exit_price||"–")}</td><td style="color:var(--red)">${fmt(t.sl)}</td><td style="color:var(--green)">${fmt(t.tp)}</td><td style="color:var(--yellow)">${t.rr}R</td><td class="pos-pnl ${pv>=0?"pos":"neg"}">${pv>=0?"+":""}${pv.toFixed(2)}</td><td class="pos-pnl ${pp>=0?"pos":"neg"}">${pp>=0?"+":""}${pp.toFixed(2)}%</td><td style="color:${scoreColor(t.score||0)};font-family:'Fredoka One',sans-serif">${t.grade||"–"}</td><td style="color:${isW?"var(--green)":"var(--red)"};font-size:.62rem">${t.status||"–"}</td><td style="color:var(--dim)">${(t.opened_at||"").replace(" UTC","")}</td><td><button class="action-btn share-btn" onclick="showTradeCard(window._histData[${i}],false,'LIVE')">📸</button></td></tr>`;}).join("")}</tbody></table></div>`;};buildOpenTable(tr,"live-trades-wrap","trades-count");buildOpenTable(tr,"live-trades-wrap2","live-pos-count");buildHistTable(hist,"history-wrap2");}catch{}};
async function fetchHistory(){if(activeTab!=="history")return;try{const r=await fetch("/api/recent-trades");const data=await r.json();const wrap=$("history-wrap");if(!wrap)return;if(!data.length){wrap.innerHTML='<div class="empty" style="padding:40px"><div class="empty-ico">📭</div><div class="empty-t">No completed trades yet</div></div>';return;}window._histData=data;wrap.innerHTML=`<div style="overflow-x:auto"><table class="tbl"><thead><tr><th>Pair</th><th>Dir</th><th>Entry Price</th><th>Exit Price</th><th>SL</th><th>TP</th><th>RR</th><th>PnL $</th><th>PnL %</th><th>Grade</th><th>Status</th><th>Opened</th><th>Card</th></tr></thead><tbody>${data.map((t,i)=>{const pv=t.pnl||0;const pp=t.pnl_pct||0;const isW=(t.status||"").toLowerCase().includes("tp");return`<tr><td style="font-weight:800;color:var(--text)">${t.symbol}</td><td class="${t.direction==="BUY"?"buy":"sell"}">${t.direction}</td><td>${fmt(t.entry)}</td><td style="color:var(--yellow)">${fmt(t.close_price||t.exit_price||"–")}</td><td style="color:var(--red)">${fmt(t.sl)}</td><td style="color:var(--green)">${fmt(t.tp)}</td><td style="color:var(--yellow)">${t.rr}R</td><td class="pos-pnl ${pv>=0?"pos":"neg"}">${pv>=0?"+":""}${pv.toFixed(2)}</td><td class="pos-pnl ${pp>=0?"pos":"neg"}">${pp>=0?"+":""}${pp.toFixed(2)}%</td><td style="color:${scoreColor(t.score||0)};font-family:'Fredoka One',sans-serif">${t.grade||"–"}</td><td style="color:${isW?"var(--green)":"var(--red)"};font-size:.62rem">${t.status||"–"}</td><td style="color:var(--dim)">${(t.opened_at||"").replace(" UTC","")}</td><td><button class="action-btn share-btn" onclick="showTradeCard(window._histData[${i}],false,'LIVE')">📸</button></td></tr>`;}).join("")}</tbody></table></div>`;}catch{}}
window.showTradeCard=function(t,isOpen,tradingType){
  if(!t)return;
  const dir=t.direction==="BUY"?"BUY":"SELL";
  const isBuy=dir==="BUY";

  // Direction badge
  const dirBadge=$("tc-dir-badge");
  dirBadge.textContent=isBuy?"🟢 LONG":"🔴 SHORT";
  dirBadge.className="tc-dir-badge "+(isBuy?"buy":"sell");

  // Pair & grade
  $("tc-pair-name").textContent=(t.symbol||"–").replace("_USDT","") + "/USDT";
  $("tc-grade-badge").textContent=t.grade||"–";

  // Status badge
  const sb=$("tc-status-badge");
  const status=(t.status||"RUNNING").toUpperCase();
  if(isOpen){sb.textContent="🔄 RUNNING";sb.className="tc-status running";}
  else if(status.includes("TP")){sb.textContent="✅ TAKE PROFIT HIT";sb.className="tc-status win";}
  else if(status.includes("SL")||status.includes("STOP")){sb.textContent="❌ STOP LOSS HIT";sb.className="tc-status loss";}
  else if(status.includes("MANUAL")||status.includes("CLOSE")){sb.textContent="🔒 CLOSED";sb.className="tc-status loss";}
  else{sb.textContent=status;sb.className="tc-status running";}

  // Price boxes — Entry Price / Current Price
  const entryVal=t.entry||0;
  const curVal=isOpen?(t.market_price||t.current_price||0):(t.close_price||t.exit_price||0);
  const slVal=t.sl||0;const tpVal=t.tp||0;
  $("tc-prices").innerHTML=`
    <div class="tc-price-box">
      <div class="tc-price-lbl">ENTRY PRICE</div>
      <div class="tc-price-val">${fmt(entryVal)}</div>
    </div>
    <div class="tc-price-box highlight">
      <div class="tc-price-lbl">${isOpen?"CURRENT PRICE":"EXIT PRICE"}</div>
      <div class="tc-price-val">${fmt(curVal)||"–"}</div>
    </div>
    <div class="tc-price-box sl-box">
      <div class="tc-price-lbl">STOP LOSS</div>
      <div class="tc-price-val">${fmt(slVal)}</div>
    </div>
    <div class="tc-price-box tp-box">
      <div class="tc-price-lbl">TAKE PROFIT</div>
      <div class="tc-price-val">${fmt(tpVal)}</div>
    </div>`;

  // PnL
  const pnlRow=$("tc-pnl-row");
  const pnlV=parseFloat(t.pnl||t.live_pnl||0);
  const pnlP=parseFloat(t.pnl_pct||t.roi_pct||0);
  const pnlCls=pnlV>0?"pos":pnlV<0?"neg":"neutral";
  pnlRow.className="tc-pnl-row "+pnlCls;
  $("tc-pnl-val").textContent=(pnlV>=0?"+":"")+pnlV.toFixed(2)+" USDT";
  $("tc-pnl-pct").textContent=(pnlP>=0?"+":"")+pnlP.toFixed(2)+"%";

  // Footer — model number #1 / #2 / #3
  const modelNum=t.model||"1";
  $("tc-type-lbl").textContent=(tradingType==="PAPER"?"📝 PAPER TRADE":"🤖 LIVE TRADE")+" · MAD MAN MODEL #"+modelNum;
  $("tc-rr-lbl").textContent=(t.rr||"–")+"R";

  $("tc-modal").classList.add("show");
};
async function fetchLog(){if(activePage!=="home"&&activePage!=="settings")return;try{const r=await fetch("/api/log");const d=await r.json();const body=$("lbody");if(!d.log||!d.log.length){body.innerHTML='<div style="color:rgba(56,189,248,.5);font-style:italic">Waiting for log entries... The scanner logs appear here automatically.</div>';return;}body.innerHTML=d.log.map(l=>{const cls=l.includes("🎯")||l.includes("SIGNAL")?"ll-s":l.includes("📝")||l.includes("PAPER")?"ll-p":l.includes("🤖")||l.includes("TRADE")?"ll-t":l.includes("❌")||l.includes("Error")||l.includes("error")?"ll-e":l.includes("👁")||l.includes("MONITOR")||l.includes("MANIP")?"ll-m":"ll-i";return`<div class="${cls}">${l}</div>`;}).join("");}catch(e){const body=$("lbody");if(body)body.innerHTML=`<div class="ll-e">Log fetch error: ${e}</div>`;}}
async function loadTradeConfig(){try{const r=await fetch("/api/trade-config");const d=await r.json();$("t-risk").value=d.risk_pct||1;if($("t-leverage"))$("t-leverage").value=d.leverage||0;if($("t-max"))$("t-max").value=d.max_trades||3;updateTradeBadge(d.enabled);updateMarginModeUI(d.margin_mode||2);}catch{}}
function updateMarginModeUI(mode){
  const crossBtn=$("margin-cross-btn"),isoBtn=$("margin-iso-btn"),status=$("margin-mode-status");
  if(!crossBtn||!isoBtn)return;
  if(mode===2||mode==="2"){
    crossBtn.style.cssText="flex:1;padding:12px 10px;border-radius:12px;border:2px solid rgba(56,189,248,.8);background:rgba(56,189,248,.18);color:#38bdf8;font-family:'JetBrains Mono',monospace;font-size:.72rem;font-weight:700;cursor:pointer;transition:all .22s;min-width:120px";
    isoBtn.style.cssText="flex:1;padding:12px 10px;border-radius:12px;border:2px solid var(--muted);background:transparent;color:var(--dim);font-family:'JetBrains Mono',monospace;font-size:.72rem;font-weight:700;cursor:pointer;transition:all .22s;min-width:120px";
    if(status)status.textContent="Active: 🔗 Cross Margin (openType=2)";
  }else{
    isoBtn.style.cssText="flex:1;padding:12px 10px;border-radius:12px;border:2px solid rgba(249,115,22,.8);background:rgba(249,115,22,.15);color:#f97316;font-family:'JetBrains Mono',monospace;font-size:.72rem;font-weight:700;cursor:pointer;transition:all .22s;min-width:120px";
    crossBtn.style.cssText="flex:1;padding:12px 10px;border-radius:12px;border:2px solid var(--muted);background:transparent;color:var(--dim);font-family:'JetBrains Mono',monospace;font-size:.72rem;font-weight:700;cursor:pointer;transition:all .22s;min-width:120px";
    if(status)status.textContent="Active: 🔒 Isolated Margin (openType=1)";
  }
}
window.setMarginMode=async function(mode){
  const label=mode===2?"Cross":"Isolated";
  if(!confirm(`Switch to ${label} Margin?\n\nThis will update your MEXC account for all watchlist pairs and apply to all future auto-trades.`))return;
  const status=$("margin-mode-status");
  if(status)status.textContent="⏳ Applying to MEXC...";
  try{
    const r=await fetch("/api/margin-mode",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({mode})});
    const d=await r.json();
    if(d.ok){
      updateMarginModeUI(mode);
      toast(`⚖️ ${label} Margin activated on MEXC!`,"trade");
      if(status)status.textContent=`✅ ${d.message}`;
      setTimeout(()=>{if(status)status.textContent=`Active: ${mode===2?"🔗 Cross":"🔒 Isolated"} Margin (openType=${mode})`;},5000);
    }else{
      if(status)status.textContent="❌ Failed: "+(d.error||"unknown error");
      toast("❌ Margin mode change failed","sell");
    }
  }catch(e){
    if(status)status.textContent="❌ Error: "+e.message;
    toast("❌ Error: "+e.message,"sell");
  }
};
async function syncModelToggles(){try{const r=await fetch("/api/settings");const d=await r.json();[1,2,3].forEach(n=>{const key=`model${n}_enabled`;const on=d[key]!==false;const btn=$(`m${n}-toggle-btn`);if(btn){const labels={1:"🎯 Model #1",2:"👁 Model #2a/2b",3:"💎 Model #3"};btn.textContent=`${labels[n]||"Model #"+n}: ${on?"ON":"OFF"}`;btn.style.borderColor=on?"rgba(16,185,129,.4)":"rgba(239,68,68,.3)";btn.style.background=on?"rgba(16,185,129,.1)":"rgba(239,68,68,.08)";btn.style.color=on?"var(--green)":"var(--red)";}});}catch{}}
window.toggleModelScan=async function(n){try{const r=await fetch("/api/settings");const d=await r.json();const key=`model${n}_enabled`;const newVal=!(d[key]!==false);const patch={};patch[key]=newVal;await fetch("/api/settings",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(patch)});await syncModelToggles();toast(`Model #${n} scan ${newVal?"ON":"OFF"}`,newVal?"trade":"");}catch(e){toast("Error: "+e.message,"");}};

function updateTradeBadge(en){const b=$("trade-badge"),eb=$("t-enable-btn"),db=$("t-disable-btn");if(en){b.textContent="ENABLED";b.style.cssText="font-size:.7rem;padding:3px 10px;border-radius:8px;background:rgba(16,185,129,.12);border:1.5px solid rgba(16,185,129,.35);color:var(--green);font-family:'JetBrains Mono',monospace;font-weight:700";eb.style.display="none";db.style.display="";}else{b.textContent="DISABLED";b.style.cssText="font-size:.7rem;padding:3px 10px;border-radius:8px;background:rgba(239,68,68,.1);border:1.5px solid rgba(239,68,68,.3);color:var(--red);font-family:'JetBrains Mono',monospace;font-weight:700";eb.style.display="";db.style.display="none";}}
function showTradeMsg(msg,ok){const el=$("trade-msg");el.textContent=msg;el.className="t-status "+(ok?"ok":"err");setTimeout(()=>el.className="t-status",4000);}
function showPaperMsg(msg,ok){const el=$("paper-msg");el.textContent=msg;el.className="t-status "+(ok?"ok":"err");setTimeout(()=>el.className="t-status",4000);}
window.saveTradeConfig=async function(){const cfg={risk_pct:parseFloat($("t-risk").value)||1,leverage:parseInt(($("t-leverage")||{}).value||0)||0,max_trades:parseInt(($("t-max")||{}).value||3)||3};try{const r=await fetch("/api/trade-config",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(cfg)});const d=await r.json();if(d.ok){showTradeMsg("✅ Saved!",true);toast("💾 Settings saved!","trade");checkMarginWarning(cfg.risk_pct);}else showTradeMsg("❌ Save failed",false);}catch{showTradeMsg("❌ Error",false);}};
window.enableTrade=async function(en){try{const r=await fetch("/api/trade-config",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({enabled:en})});const d=await r.json();if(d.ok){updateTradeBadge(en);showTradeMsg(en?"✅ Auto-trade ENABLED!":"✅ Disabled",true);toast(en?"🤖 Auto-trade ON!":"⏹ Off","trade");}}catch{showTradeMsg("❌ Error",false);}};
window.fetchBalance=async function(){
  const formKey=($("t-apikey")||{value:""}).value.trim();
  const formSec=($("t-secret")||{value:""}).value.trim();
  if(formKey||formSec){const patch={};if(formKey)patch.api_key=formKey;if(formSec)patch.api_secret=formSec;try{await fetch("/api/trade-config",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(patch)});}catch{}}
  try{const r=await fetch("/api/balance");const d=await r.json();if(d.error)showTradeMsg("❌ "+d.error,false);else{const bal=Number(d.balance);$("bal-val").textContent=bal.toFixed(2);showTradeMsg("✅ Balance loaded",true);const riskPct=parseFloat($("t-risk").value)||1;checkMarginWarning(riskPct,bal);}}catch{showTradeMsg("❌ Check API keys",false);}};
function checkMarginWarning(riskPct,bal){const balVal=bal!=null?bal:(parseFloat($("bal-val").textContent)||0);if(balVal<=0)return;const margin=balVal*riskPct/100;if(margin<0.1){showTradeMsg("⚠️ Minimum margin per trade is $0.10 — your current risk ("+riskPct+"% of $"+balVal.toFixed(2)+" = $"+margin.toFixed(4)+") is below this. Increase risk % or top up balance.",false);}}
window.closeTrade=async function(sym){if(!confirm("Close "+sym+"?"))return;try{const r=await fetch("/api/trade-close",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({symbol:sym})});const d=await r.json();toast(d.ok?"✅ "+sym+" closed":"❌ "+d.message,"trade");await window.fetchPnl();}catch{toast("❌ Close failed","trade");}};
window.toggleScanner=async function(){try{const r=await fetch("/api/toggle-scanner",{method:"POST"});const d=await r.json();toast(d.enabled?"▶ Scanner on! 🚀":"⏹ Paused",d.enabled?"buy":"sell");await fetchState();}catch{}};

/* ─── PAPER TRADING ─────────────────────────── */
async function loadPaperConfig(){try{const r=await fetch("/api/paper-config");const d=await r.json();$("p-balance").value=d.balance||100;$("p-risk").value=d.risk_pct||25;$("p-max").value=d.max_trades||5;$("p-bal-val").textContent=Number(d.balance||100).toLocaleString(undefined,{minimumFractionDigits:2,maximumFractionDigits:2});paperAutoOn=d.auto_trade!=null?d.auto_trade:true;updatePaperBadge(d.enabled,d.auto_trade);}catch{}}
function updatePaperBadge(en,auto){const b=$("paper-badge"),ab=$("paper-auto-badge"),eb=$("p-enable-btn"),db=$("p-disable-btn"),autobtn=$("p-auto-btn");if(en){b.textContent="ENABLED";b.style.cssText="font-size:.7rem;padding:3px 10px;border-radius:8px;background:rgba(16,185,129,.12);border:1.5px solid rgba(16,185,129,.35);color:var(--green);font-family:'JetBrains Mono',monospace;font-weight:700";eb.style.display="none";db.style.display="";}else{b.textContent="DISABLED";b.style.cssText="font-size:.7rem;padding:3px 10px;border-radius:8px;background:rgba(239,68,68,.1);border:1.5px solid rgba(239,68,68,.3);color:var(--red);font-family:'JetBrains Mono',monospace;font-weight:700";eb.style.display="";db.style.display="none";}if(ab){ab.style.display=auto?"":"none";}if(autobtn){autobtn.textContent=`🤖 Auto-Trade: ${auto?"ON":"OFF"}`;autobtn.style.background=auto?"rgba(167,139,250,.2)":"rgba(167,139,250,.1)";autobtn.style.borderColor=auto?"rgba(167,139,250,.6)":"rgba(167,139,250,.3)";}}
window.savePaperConfig=async function(){const cfg={risk_pct:parseFloat($("p-risk").value)||1,max_trades:parseInt($("p-max").value)||4};try{const r=await fetch("/api/paper-config",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(cfg)});const d=await r.json();if(d.ok){showPaperMsg("✅ Settings saved!",true);toast("💾 Paper settings saved","paper");}else showPaperMsg("❌ Save failed",false);}catch{showPaperMsg("❌ Error",false);}};
window.setPaperBalance=async function(){const bal=parseFloat($("p-balance").value);if(!bal||bal<100){showPaperMsg("❌ Minimum balance $100",false);return;}try{const r=await fetch("/api/paper-config",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({balance:bal})});const d=await r.json();if(d.ok){$("p-bal-val").textContent=bal.toLocaleString(undefined,{minimumFractionDigits:2,maximumFractionDigits:2});showPaperMsg(`✅ Balance set to $${bal.toLocaleString()}`,true);toast(`💰 Paper balance: $${bal.toLocaleString()}`,"paper");}else showPaperMsg("❌ Failed",false);}catch{showPaperMsg("❌ Error",false);}};
window.enablePaper=async function(en){try{const r=await fetch("/api/paper-config",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({enabled:en})});const d=await r.json();if(d.ok){updatePaperBadge(en,paperAutoOn);showPaperMsg(en?"✅ Paper trading ENABLED!":"✅ Paper trading disabled",true);toast(en?"📝 Paper ON!":"⏹ Paper off","paper");}else showPaperMsg("❌ Error",false);}catch{showPaperMsg("❌ Error",false);}};
window.togglePaperAuto=async function(){paperAutoOn=!paperAutoOn;try{const r=await fetch("/api/paper-config",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({auto_trade:paperAutoOn})});const d=await r.json();if(d.ok){updatePaperBadge(d.config.enabled,paperAutoOn);showPaperMsg(paperAutoOn?"✅ Auto-trade ON — signals will auto paper-trade!":"✅ Auto-trade OFF",true);toast(paperAutoOn?"🤖 Paper auto-trade ON!":"⏹ Auto off","paper");}else showPaperMsg("❌ Error",false);}catch{showPaperMsg("❌ Error",false);}};
window.closePaperTrade=async function(sym){if(!confirm("Close paper trade on "+sym+"?"))return;try{const r=await fetch("/api/paper-close",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({symbol:sym})});const d=await r.json();if(d.ok){toast(`📝 Paper closed: ${sym} · ${d.message}`,"paper");await fetchPaperData();}else toast("❌ "+d.message,"sell");}catch{toast("❌ Error","sell");}};
window.resetPaperStats=async function(){if(!confirm("Reset all paper trading stats and history?"))return;try{const r=await fetch("/api/paper-reset",{method:"POST"});const d=await r.json();if(d.ok){toast("📝 Paper stats reset","paper");await fetchPaperData();}else toast("❌ Reset failed","sell");}catch{toast("❌ Error","sell");}};
async function fetchPaperData(){if(activePage!=="trade")return;try{const[cfg,trades,hist,stats]=await Promise.all([fetch("/api/paper-config").then(r=>r.json()),fetch("/api/paper-trades").then(r=>r.json()),fetch("/api/paper-history").then(r=>r.json()),fetch("/api/paper-stats").then(r=>r.json())]);
// Update balance display
$("p-bal-val").textContent=Number(cfg.balance||0).toLocaleString(undefined,{minimumFractionDigits:2,maximumFractionDigits:2});
paperAutoOn=cfg.auto_trade||false;updatePaperBadge(cfg.enabled,cfg.auto_trade);
// Stats
$("ps-total").textContent=stats.total||0;$("ps-wins").textContent=stats.wins||0;$("ps-losses").textContent=stats.losses||0;
const wr=stats.total>0?Math.round(stats.wins/stats.total*100):0;$("ps-wr").textContent=wr+"%";
const pnl=stats.total_pnl||0;const pnlEl=$("ps-pnl");pnlEl.textContent=(pnl>=0?"+":"")+pnl.toFixed(2);pnlEl.style.color=pnl>=0?"var(--green)":"var(--red)";
$("ps-open").textContent=trades.length;$("paper-trades-count").textContent=`(${trades.length})`;
// Open positions
const ptWrap=$("paper-trades-wrap");if(ptWrap){if(!trades.length){ptWrap.innerHTML='<div class="empty" style="padding:40px"><div class="empty-ico">📝</div><div class="empty-t">No open paper trades</div><div class="empty-s">Enable paper trading and turn on auto-trade to place trades from signals automatically</div></div>';}else{window._paperTradesData=trades;ptWrap.innerHTML=`<div style="overflow-x:auto"><table class="tbl"><thead><tr><th>Pair</th><th>Dir</th><th>Entry Price</th><th>Current Price</th><th>SL</th><th>TP</th><th>RR</th><th>Risk $</th><th>Live PnL</th><th>PnL %</th><th>Grade</th><th>Card</th><th>Action</th></tr></thead><tbody>${trades.map((t,i)=>{const pv=t.pnl||0;const pp=t.pnl_pct||0;return`<tr><td style="font-weight:800">${t.symbol}</td><td class="${t.direction==="BUY"?"buy":"sell"}">${t.direction}</td><td>${fmt(t.entry)}</td><td style="color:var(--yellow)">${fmt(t.current_price)}</td><td style="color:var(--red)">${fmt(t.sl)}</td><td style="color:var(--green)">${fmt(t.tp)}</td><td style="color:var(--yellow)">${t.rr}R</td><td>$${t.risk_amount}</td><td class="pos-pnl ${pv>=0?"pos":"neg"}">${pv>=0?"+":""}${pv.toFixed(2)}</td><td class="pos-pnl ${pp>=0?"pos":"neg"}">${pp>=0?"+":""}${pp.toFixed(2)}%</td><td style="color:${scoreColor(t.score||0)};font-family:'Fredoka One',sans-serif">${t.grade||"–"}</td><td><button class="action-btn share-btn" onclick="showTradeCard(window._paperTradesData[${i}],true,'PAPER')">📸</button></td><td><button class="action-btn close-btn" onclick="closePaperTrade('${t.symbol}')">✕</button></td></tr>`;}).join("")}</tbody></table></div>`;}}
// History
const phWrap=$("paper-history-wrap");if(phWrap){if(!hist.length){phWrap.innerHTML='<div class="empty" style="padding:30px"><div class="empty-ico">📭</div><div class="empty-t">No paper trades yet</div></div>';}else{window._paperHistData=hist;phWrap.innerHTML=`<div style="overflow-x:auto"><table class="tbl"><thead><tr><th>Pair</th><th>Dir</th><th>Entry Price</th><th>Exit Price</th><th>SL</th><th>TP</th><th>RR</th><th>PnL $</th><th>PnL %</th><th>Grade</th><th>Status</th><th>Opened</th><th>Card</th></tr></thead><tbody>${hist.map((t,i)=>{const pv=t.pnl||0;const pp=t.pnl_pct||0;const isW=(t.status||"").toLowerCase().includes("tp");return`<tr><td style="font-weight:800">${t.symbol}</td><td class="${t.direction==="BUY"?"buy":"sell"}">${t.direction}</td><td>${fmt(t.entry)}</td><td style="color:var(--yellow)">${fmt(t.close_price||"–")}</td><td style="color:var(--red)">${fmt(t.sl)}</td><td style="color:var(--green)">${fmt(t.tp)}</td><td style="color:var(--yellow)">${t.rr}R</td><td class="pos-pnl ${pv>=0?"pos":"neg"}">${pv>=0?"+":""}${pv.toFixed(2)}</td><td class="pos-pnl ${pp>=0?"pos":"neg"}">${pp>=0?"+":""}${pp.toFixed(2)}%</td><td style="color:${scoreColor(t.score||0)};font-family:'Fredoka One',sans-serif">${t.grade||"–"}</td><td style="color:${isW?"var(--green)":"var(--red)"};font-size:.62rem">${t.status||"–"}</td><td style="color:var(--dim)">${(t.opened_at||"").replace(" UTC","")}</td><td><button class="action-btn share-btn" onclick="showTradeCard(window._paperHistData[${i}],false,'PAPER')">📸</button></td></tr>`;}).join("")}</tbody></table></div>`;}}
}catch(e){console.error("Paper data error:",e);}}

async function fetchDiag(){try{const r=await fetch("/api/diag");const d=await r.json();const labels={neutral:"😴 Neutral",not_continuous:"📉 Structure",no_obs:"📦 No OBs",not_at_key:"🎯 Not Key",not_in_zone:"📍 Zone",not_tapping:"👆 Tapping",no_crts:"🕯 No Setup",no_tbs:"🐢 No TBS",rr_low:"📊 Low RR","1d_no_crts":"1D No Setup","1d_no_tbs":"1D NoTBS","1d_rr_low":"1D LowRR",passed:"✅ PASSED",outbound_ip:"🌐 IP",key_last4:"🔑 Key",secret_ok:"🔒 Secret",trade_mode:"⚙️ Mode",auto_trade:"🤖 Auto",mexc_ping:"📡 MEXC Ping",auth_test:"🔐 Auth",balance_usdt:"💰 Balance"};const colors={neutral:"var(--dim)",not_continuous:"var(--dim)",no_obs:"var(--orange)",not_at_key:"var(--orange)",not_in_zone:"var(--orange)",not_tapping:"var(--red)",no_crts:"var(--red)",no_tbs:"var(--red)",rr_low:"var(--orange)","1d_no_crts":"var(--dim)","1d_no_tbs":"var(--dim)","1d_rr_low":"var(--dim)",passed:"var(--green)",outbound_ip:"var(--cyan)",key_last4:"var(--cyan)",secret_ok:"var(--cyan)",trade_mode:"var(--cyan)",auto_trade:"var(--cyan)",mexc_ping:"var(--cyan)",auth_test:"var(--cyan)",balance_usdt:"var(--green)"};const grid=$("diag-grid");if(grid)grid.innerHTML=Object.entries(d).map(([k,v])=>{const isTrading=["outbound_ip","key_last4","secret_ok","trade_mode","auto_trade","mexc_ping","auth_test","balance_usdt"].includes(k);const style=isTrading?"border-color:rgba(6,182,212,.3);background:rgba(6,182,212,.04)":"";const valColor=colors[k]||"var(--text)";const dispVal=typeof v==="object"?"[object]":String(v);const fontSize=dispVal.length>12?"0.75rem":dispVal.length>8?"0.95rem":"1.6rem";return`<div class="dg" style="${style}"><div class="dg-lbl">${labels[k]||k}</div><div class="dg-val" style="color:${valColor};font-size:${fontSize}">${dispVal}</div></div>`;}).join("");}catch{}}

// ── Inner trade sub-tabs ──────────────────────────────────────────────
window.swInner=function(tab,btn){
  activeInnerTrade=tab;
  document.querySelectorAll(".inner-tab").forEach(b=>b.classList.remove("active"));
  if(btn)btn.classList.add("active");
  ["trade-live","trade-paper","trade-open","trade-history"].forEach(t=>{
    const el=$("itab-"+t);if(el)el.style.display=t===tab?"block":"none";
  });
  if(tab==="trade-open")window.fetchPnl();
  if(tab==="trade-history"){fetchHistory();}
  if(tab==="trade-live"){loadTradeConfig();syncModelToggles();}
  if(tab==="trade-paper"){loadPaperConfig();fetchPaperData();}
};

// ── Page navigation ──────────────────────────────────────────────────
window.swPage=function(page,btn){
  activePage=page;
  document.querySelectorAll(".bnav-btn").forEach(b=>b.classList.remove("active"));
  if(btn)btn.classList.add("active");
  ["home","signals","trade","market","settings"].forEach(p=>{
    const el=$("page-"+p);if(el)el.classList.toggle("active",p===page);
  });
  if(page==="market"){const fr=$("market-iframe");if(fr&&!fr.getAttribute("src"))fr.src="/market";}
  if(page==="signals"){fetchMonitor();}
  if(page==="trade"){loadTradeConfig();syncModelToggles();window.fetchPnl();}
  if(page==="settings"){loadSettings();fetchLog();fetchDiag();}
};

// ── Signal discard/clear ──────────────────────────────────────────────
window.discardSignal=function(idx){
  allSigs.splice(idx,1);
  lastCount=allSigs.length;
  renderSigs();
  toast("🗑 Signal discarded","sell",2000);
};
window.clearAllSignals=async function(){
  if(!confirm("Discard all signals from view?"))return;
  try{
    await fetch("/api/signals/clear",{method:"POST"});
    allSigs=[];lastCount=0;renderSigs();
    toast("🗑 All signals cleared","sell",2000);
  }catch{allSigs=[];lastCount=0;renderSigs();toast("🗑 Cleared (local)","sell",2000);}
};

// Legacy sw() alias — keep for any code that still calls it
window.sw=function(tab,btn){
  const pageMap={signals:"signals",market:"market",trades:"trade","trade-cfg":"trade",paper:"trade",log:"settings",settings:"settings",monitor:"signals",history:"trade"};
  const page=pageMap[tab]||"home";
  const navBtn=document.getElementById("bnav-"+page);
  swPage(page,navBtn);
};

async function loadSettings(){
  try{
    const r=await fetch("/api/settings");
    const d=await r.json();
    const g=id=>document.getElementById(id);
    if(g("s-risk"))      g("s-risk").value      =d.risk_pct||1;
    if(g("s-tg-token"))  g("s-tg-token").value  =d.tg_bot_token||"";
    if(g("s-tg-chat"))   g("s-tg-chat").value   =d.tg_chat_id||"";
    if(g("s-tg-signals"))g("s-tg-signals").checked=!!d.tg_signals;
    if(g("s-tg-trades")) g("s-tg-trades").checked =!!d.tg_trades;
    if(g("s-price-int")) g("s-price-int").value  =d.price_interval||5;priceRefreshSecs=Math.max(1,d.price_interval||5);
    if(g("s-scan-int"))  g("s-scan-int").value   =d.scan_interval||1;
    if(g("s-cycle-rest"))g("s-cycle-rest").value  =d.cycle_rest||5;
    if(g("s-market-refresh"))g("s-market-refresh").value=d.market_refresh_interval||1;
    if(g("s-gemini-key"))g("s-gemini-key").placeholder=d.gemini_api_key_set?"Key set (paste to update)":"AIza…";
    if(g("s-visual-mode"))g("s-visual-mode").checked=d.visual_analysis_enabled!==false;
  }catch(e){console.error("loadSettings",e);}
}

window.loadSettingsBal=async function(){
  const el=document.getElementById("s-bal-val");
  if(el)el.textContent="…";
  try{
    const r=await fetch("/api/balance");
    const d=await r.json();
    if(d.error){if(el)el.textContent="❌ "+d.error;}
    else{if(el)el.textContent=d.balance!=null?("$"+Number(d.balance).toFixed(2)+" USDT"):"–";}
  }catch(e){if(el)el.textContent="❌ error";}
}

function showSettingsMsg(text,ok){const el=document.getElementById("settings-msg");if(!el)return;el.textContent=text;el.className="t-status "+(ok?"ok":"err");clearTimeout(window._smTimer);window._smTimer=setTimeout(()=>{el.className="t-status";el.textContent="";},4000);}
window.saveAllSettings=async function(){
  const g=id=>document.getElementById(id);
  showSettingsMsg("💾 Saving…",true);
  const payload={
    tg_bot_token:   (g("s-tg-token")||{}).value||"",
    tg_chat_id:     (g("s-tg-chat")||{}).value||"",
    tg_signals:     !!(g("s-tg-signals")||{}).checked,
    tg_trades:      !!(g("s-tg-trades")||{}).checked,
    price_interval: parseInt((g("s-price-int")||{}).value)||1,
    scan_interval:  parseInt((g("s-scan-int")||{}).value)||1,
    cycle_rest:     parseInt((g("s-cycle-rest")||{}).value)||5,
    market_refresh_interval: parseInt((g("s-market-refresh")||{}).value)||10,
    visual_analysis_enabled: !!(g("s-visual-mode")||{}).checked,
  };
  // Only send Gemini key if user typed a new value
  const gk=(g("s-gemini-key")||{}).value||"";
  if(gk && gk.startsWith("AIza")) payload.gemini_api_key=gk;
  const riskVal=parseFloat((g("s-risk")||{}).value);
  if(!isNaN(riskVal)&&riskVal>0) payload.risk_pct=riskVal;
  try{
    const r=await fetch("/api/settings",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(payload)});
    if(!r.ok){showSettingsMsg("❌ Server error "+r.status,false);return;}
    const d=await r.json();
    if(d.ok){
      showSettingsMsg("✅ All settings saved!",true);
      toast("💾 All settings saved!","trade");
      loadSettings();
      priceRefreshSecs=Math.max(1,payload.price_interval||5);
    }else{showSettingsMsg("❌ Save failed: "+(d.error||"unknown"),false);}
  }catch(e){showSettingsMsg("❌ Network error: "+e.message,false);}
}
window.logout=function(){fetch("/api/logout",{method:"POST"}).finally(()=>window.location.href="/");};
window.showShare = function(i) {
  var t = (window._histData || [])[i];
  if (!t) return;
  var modal = document.getElementById('share-modal');
  var content = document.getElementById('sh-content');
  var dir = t.direction === 'BUY' ? '🟢 LONG' : '🔴 SHORT';
  var rows = [
    ['Pair',        t.symbol || '-'],
    ['Direction',   dir],
    ['Entry',       fmt(t.entry)],
    ['Stop Loss',   fmt(t.sl)],
    ['Take Profit', fmt(t.tp)],
    ['Risk:Reward', (t.rr || '-') + 'R'],
    ['Grade',       t.grade || '-'],
    ['Strategy',    'Mad Man Strategy'],
    ['Status',      t.status || '-'],
    ['Opened',      (t.opened_at || '').replace(' UTC','')],
  ];
  var html = '';
  rows.forEach(function(r) {
    html += '<div style="display:flex;justify-content:space-between;padding:6px 0;border-bottom:1px solid var(--border);font-family:JetBrains Mono,monospace;font-size:.72rem">'
          + '<span style="color:var(--dim)">' + r[0] + '</span>'
          + '<span style="color:var(--text);font-weight:700">' + r[1] + '</span>'
          + '</div>';
  });
  content.innerHTML = html;
  window._shareData = t;
  modal.style.display = 'flex';
};

window.copyShareCard = function() {
  var t = window._shareData;
  if (!t) return;
  var lines = [
    'Mad Man Strategy Scanner',
    '========================',
    (t.direction === 'BUY' ? 'LONG' : 'SHORT') + ' ' + (t.symbol || ''),
    'Entry:  ' + fmt(t.entry),
    'SL:     ' + fmt(t.sl),
    'TP:     ' + fmt(t.tp),
    'RR:     ' + t.rr + 'R',
    'Grade:  ' + (t.grade || '-'),
    'Status: ' + (t.status || '-'),
    'Strategy: Mad Man Model #1'
  ];
  var text = lines.join('\n');
  if (navigator.clipboard) {
    navigator.clipboard.writeText(text).then(function() {
      toast('Copied!', 'trade');
    });
  }
};

window.saveTcCard = function() {
  var card = document.getElementById('tc-card');
  if (!card) { toast('Card not found',''); return; }
  // Try html2canvas
  if (typeof html2canvas !== 'undefined') {
    html2canvas(card, {
      backgroundColor: null,
      scale: 2,
      useCORS: true,
      allowTaint: true
    }).then(function(canvas) {
      var link = document.createElement('a');
      var sym = (document.getElementById('tc-pair-name')||{}).textContent || 'trade';
      link.download = 'madman-' + sym.replace('/','') + '-' + Date.now() + '.png';
      link.href = canvas.toDataURL('image/png');
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      toast('📸 Card saved!', 'trade');
    }).catch(function(e) {
      toast('Save failed — try screenshot instead', '');
    });
  } else {
    // html2canvas not loaded — copy text fallback
    var dir  = (document.getElementById('tc-dir-badge')||{}).textContent || '';
    var pair = (document.getElementById('tc-pair-name')||{}).textContent || '';
    var pnl  = (document.getElementById('tc-pnl-val')||{}).textContent || '';
    var pct  = (document.getElementById('tc-pnl-pct')||{}).textContent || '';
    var rr   = (document.getElementById('tc-rr-lbl')||{}).textContent || '';
    var txt  = 'Mad Man Strategy Scanner\n' + dir + ' ' + pair + '\nPnL: ' + pnl + ' (' + pct + ')\nRR: ' + rr;
    if (navigator.clipboard) {
      navigator.clipboard.writeText(txt).then(function(){ toast('📋 Copied!','trade'); });
    } else {
      toast('Take a screenshot manually', '');
    }
  }
};

let priceRefreshSecs=1;

async function fetchPnlTicker(){
  try{
    const r=await fetch("/api/trades");
    const trades=await r.json();
    const tick=$("pnl-tick"),val=$("ptick-val"),cnt=$("ptick-cnt");
    if(!tick)return;
    if(!trades||!trades.length){tick.style.display="none";return;}
    // Fetch live PnL from MEXC
    const pr=await fetch("/api/pnl");
    const pd=await pr.json();
    const positions=pd.positions||[];
    const total=positions.reduce((s,p)=>s+(p.pnl||0),0);
    const n=positions.length||trades.length;
    tick.style.display="flex";
    const pos=total>0,neg=total<0;
    tick.className="pnl-tick "+(pos?"pos":neg?"neg":"flat");
    val.textContent=(pos?"+":"")+total.toFixed(2)+" USDT";
    cnt.textContent=` · ${n} open`;
  }catch{}
}

async function poll(){tick++;const ps=[fetchSigs(),fetchStats(),fetchState()];if(tick%(priceRefreshSecs)===0)ps.push(fetchPrices());if(tick%3===0)ps.push(fetchPnlTicker());if(activePage==="home")ps.push(fetchLog());if(activePage==="settings"&&tick%3===0)ps.push(fetchDiag());if(activePage==="signals"&&tick%2===0)ps.push(fetchMonitor());if(activePage==="trade"&&tick%3===0)ps.push(window.fetchPnl());if(activePage==="trade"&&tick%5===0)ps.push(fetchHistory());if(activePage==="trade"&&tick%2===0)ps.push(fetchPaperData());await Promise.all(ps);setTimeout(poll,1000);}
fetchPrices();loadTradeConfig();poll();
})();
</script>
</body>
</html>"""


# ════════ FLASK ROUTES ════════════════════════════════════════════════

@app.route("/logo")
def serve_logo():
    from flask import send_file as _sf
    p = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                     "attached_assets",
                     "file_000000001c80722fbd20e5efaf017c2d_1779284086479.png")
    if os.path.exists(p):
        return _sf(p, mimetype="image/png", max_age=86400)
    return "", 404

@app.route("/")
def root():
    token=request.cookies.get("session")
    if token and token in sessions:
        return make_response(DASHBOARD_HTML,200,{"Content-Type":"text/html"})
    return make_response(LOGIN_HTML,200,{"Content-Type":"text/html"})

@app.route("/dashboard")
def dashboard():
    return make_response(DASHBOARD_HTML,200,{"Content-Type":"text/html"})

@app.route("/api/login",methods=["POST"])
def api_login():
    data=request.get_json(silent=True) or {}
    if data.get("password")==DASHBOARD_PASSWORD:
        token=secrets.token_hex(32); sessions.add(token)
        resp=make_response(jsonify({"ok":True,"token":token}))
        resp.set_cookie("session",token,max_age=86400*7,httponly=True,samesite="Lax")
        return resp
    return jsonify({"ok":False}),401

@app.route("/api/logout",methods=["POST"])
def api_logout():
    token=request.cookies.get("session"); sessions.discard(token)
    resp=make_response(jsonify({"ok":True})); resp.delete_cookie("session")
    return resp

@app.route("/api/toggle-scanner",methods=["POST"])
def api_toggle():
    with scan_lock:
        scan_state["enabled"]=not scan_state["enabled"]
        en=scan_state["enabled"]
    log(f"{'▶ RESUMED' if en else '⏸ PAUSED'} by user")
    return jsonify({"enabled":en})

@app.route("/api/signals")
def api_signals():
    limit=min(int(request.args.get("limit",200)),MAX_SIGNALS)
    return jsonify(list(signals)[:limit])

@app.route("/api/signals/clear",methods=["POST"])
def api_signals_clear():
    signals.clear()
    log("🗑 All signals cleared by user")
    return jsonify({"ok":True})

@app.route("/api/stats")
def api_stats():
    all_s=list(signals)
    return jsonify({"total":len(all_s),
                    "buys": sum(1 for s in all_s if s.get("direction")=="BUY"),
                    "sells":sum(1 for s in all_s if s.get("direction")=="SELL")})

@app.route("/api/scan-state")
def api_scan_state():
    with scan_lock:
        state = {k:v for k,v in scan_state.items() if k!="log"}
    state["diag"] = dict(diag)
    return jsonify(state)

@app.route("/api/log")
def api_log():
    with scan_lock: return jsonify({"log":list(scan_state["log"])})

@app.route("/api/signal-detail/<symbol>")
def api_signal_detail(symbol):
    """Return the latest signal levels for a symbol — used to draw analysis lines on chart."""
    latest = None
    for s in signals:
        if s.get("symbol") == symbol:
            latest = s
            break
    if not latest:
        return jsonify({"found": False})

    def safe(v):
        try:
            f = float(v)
            return f if f > 0 else None
        except Exception:
            return None

    return jsonify({
        "found":      True,
        "direction":  latest.get("direction"),
        "entry":      safe(latest.get("entry")),
        "sl":         safe(latest.get("sl")),
        "tp":         safe(latest.get("tp")),
        "crh":        safe(latest.get("crh")),
        "crl":        safe(latest.get("crl")),
        "ob_top":     safe(latest.get("ob_top")),
        "ob_bot":     safe(latest.get("ob_bot")),
        "tbs_entry":  safe(latest.get("tbs_entry")),
        "grade":      latest.get("grade"),
        "rr":         latest.get("rr"),
        "model":      latest.get("model"),
    })


@app.route("/api/prices")
def api_prices():
    with price_cache_lock:
        out = dict(price_cache)
    # Cold-start: cache empty on first request — do a synchronous batch fetch
    if not out:
        _fetch_all_tickers_batch()
        with price_cache_lock:
            out = dict(price_cache)
    return jsonify(out)

@app.route("/api/ticker/<symbol>")
def api_ticker(symbol):
    """
    Returns live price + today's Day1 candle high/low for the chart header.
    High and low always come from the current Day1 candle — guaranteed real values.
    """
    # Day1 candle gives today's exact high and low
    high = 0.0
    low  = 0.0
    try:
        day_candles = get_candles(symbol, "Day1", limit=2)
        if day_candles:
            today = day_candles[-1]
            high  = round(float(today.get("high", 0)), 8)
            low   = round(float(today.get("low",  0)), 8)
    except:
        pass

    # Live price + change still come from ticker/cache
    price  = 0.0
    change = 0.0
    with price_cache_lock:
        cached = price_cache.get(symbol, {})
    if cached.get("price", 0) > 0:
        price  = cached["price"]
        change = cached.get("change", 0.0)
    else:
        t = get_ticker(symbol)
        if t and t.get("price", 0) > 0:
            price  = t["price"]
            change = t.get("change", 0.0)

    return jsonify({
        "ok":     True,
        "price":  price,
        "change": change,
        "high":   high,
        "low":    low,
    })

@app.route("/api/candles/<symbol>")
def api_candles(symbol):
    interval = request.args.get("interval", "Min5")
    limit    = request.args.get("limit", 300, type=int)
    candles  = get_candles(symbol, interval, limit=limit)
    return jsonify(candles)

@app.route("/api/trade-config", methods=["GET","POST"])
def api_trade_config():
    if request.method == "POST":
        data = request.get_json(silent=True) or {}
        with trade_lock:
            if "api_key"     in data: trade_config["api_key"]    = data["api_key"]
            if "api_secret"  in data: trade_config["api_secret"] = data["api_secret"]
            if "risk_pct"    in data: trade_config["risk_pct"]   = float(data["risk_pct"])
            if "max_trades"  in data: trade_config["max_trades"] = int(data["max_trades"])
            if "leverage"    in data: trade_config["leverage"]   = int(data["leverage"])
            if "enabled"     in data: trade_config["enabled"]    = bool(data["enabled"])
            if "margin_mode" in data: trade_config["margin_mode"] = int(data["margin_mode"])
        log(f"⚙️ Trade config updated. Auto-trade: {'ON' if trade_config['enabled'] else 'OFF'}")
        return jsonify({"ok": True, "config": {k:v for k,v in trade_config.items() if k!="api_secret"}})
    cfg = {k: ("***" if k=="api_secret" and v else v) for k,v in trade_config.items()}
    return jsonify(cfg)


@app.route("/api/margin-mode", methods=["POST"])
def api_set_margin_mode():
    """
    Switch margin mode on MEXC for ALL watchlist pairs (or a specific symbol).
    openType: 1 = isolated, 2 = cross
    MEXC endpoint: POST /position/change_margin_type
    Body: {"symbol": "BTC_USDT", "openType": 2}
    """
    data = request.get_json(silent=True) or {}
    mode = int(data.get("mode", 2))  # 1=isolated, 2=cross
    if mode not in (1, 2):
        return jsonify({"ok": False, "error": "mode must be 1 (isolated) or 2 (cross)"}), 400

    with trade_lock:
        trade_config["margin_mode"] = mode

    mode_label = "Cross" if mode == 2 else "Isolated"
    log(f"⚙️ Margin mode changed to {mode_label} (openType={mode}) — applies to all new orders")

    # Push the change to MEXC for every watchlist pair using change_leverage.
    # MEXC sets margin mode (openType) via the change_leverage endpoint —
    # there is no separate change_margin_type endpoint in the futures API.
    # We send both positionType 1 (long) and 2 (short) for each pair.
    # Errors are expected for pairs with no open position — that is normal.
    errors = []
    successes = 0

    # Get current leverage setting to keep it unchanged
    current_leverage = trade_config.get("leverage", 20) or 20
    if current_leverage <= 0:
        current_leverage = 20

    for symbol in TOP_PAIRS:
        for pos_type in (1, 2):
            params = {
                "symbol":       symbol,
                "leverage":     current_leverage,
                "openType":     mode,
                "positionType": pos_type,
            }
            _, err = mexc_request("POST", "/position/change_leverage", params)
            if err:
                errors.append(f"{symbol} pos{pos_type}: {err}")
            else:
                successes += 1

    msg = f"Margin mode set to {mode_label} on MEXC. {successes} slots updated."
    if errors:
        msg += f" {len(errors)} errors (expected for pairs with no open position)."
        log(f"⚠️ Margin mode switch — some errors (normal): {errors[:2]}")

    send_telegram(
        f"⚙️ <b>Margin Mode Changed</b>\n"
        f"New mode: <b>{mode_label} Margin</b> (openType={mode})\n"
        f"Applies to all future orders.\n"
        f"{successes} position slots updated.",
        kind="trade"
    )
    return jsonify({"ok": True, "mode": mode, "label": mode_label, "message": msg})

@app.route("/api/trades")
def api_trades():
    with trade_lock:
        return jsonify(list(open_trades.values()))

@app.route("/api/trade-close", methods=["POST"])
def api_trade_close():
    data   = request.get_json(silent=True) or {}
    symbol = data.get("symbol","")
    if not symbol: return jsonify({"ok":False,"error":"symbol required"}),400
    ok, msg = close_trade(symbol, reason="Manual (Dashboard)")
    return jsonify({"ok":ok,"message":msg})

@app.route("/api/balance")
def api_balance():
    bal, err = get_account_balance()
    return jsonify({"balance":bal,"error":err})

@app.route("/api/settings", methods=["GET","POST"])
def api_settings():
    if request.method == "POST":
        data = request.get_json(silent=True) or {}
        with settings_lock:
            for k in ["price_interval","scan_interval","cycle_rest","market_refresh_interval"]:
                if k in data:
                    try: scan_settings[k] = max(1, int(data[k]))
                    except: pass
            for k in ["tg_signals","tg_trades","model1_enabled","model2_enabled","model3_enabled"]:
                if k in data: scan_settings[k] = bool(data[k])
            if "tg_bot_token" in data:
                scan_settings["tg_bot_token"] = str(data["tg_bot_token"]).strip()
            if "tg_chat_id" in data:
                scan_settings["tg_chat_id"] = str(data["tg_chat_id"]).strip()
            if "visual_analysis_enabled" in data:
                scan_settings["visual_analysis_enabled"] = bool(data["visual_analysis_enabled"])
            if "gemini_api_key" in data and str(data["gemini_api_key"]).strip():
                new_key = str(data["gemini_api_key"]).strip()
                scan_settings["gemini_api_key"] = new_key
                global GEMINI_API_KEY
                GEMINI_API_KEY = new_key
                log("🔑 Gemini API key updated — visual chart analysis active")
        with trade_lock:
            if "api_key" in data and str(data["api_key"]).strip():
                trade_config["api_key"] = str(data["api_key"]).strip()
            if "api_secret" in data and str(data["api_secret"]).strip():
                trade_config["api_secret"] = str(data["api_secret"]).strip()
            if "risk_pct" in data:
                try: trade_config["risk_pct"] = max(0.1, min(100.0, float(data["risk_pct"])))
                except: pass
        log("⚙️ Settings updated")
        return jsonify({"ok": True})
    with settings_lock:
        out = dict(scan_settings)
    with trade_lock:
        out["api_key"]  = trade_config.get("api_key","")
        out["risk_pct"] = trade_config.get("risk_pct", 1.0)
        sec = trade_config.get("api_secret","")
    out["model1_enabled"] = scan_settings.get("model1_enabled", True)
    out["model2_enabled"] = scan_settings.get("model2_enabled", True)
    out["model3_enabled"] = scan_settings.get("model3_enabled", True)
    out["visual_analysis_enabled"] = scan_settings.get("visual_analysis_enabled", True)
    out["gemini_api_key_set"] = bool(GEMINI_API_KEY)
    out["api_secret_masked"] = (sec[:4] + "●●●●●●●●●●●●●●●●") if len(sec) > 4 else ("●" * len(sec))
    out["api_secret_set"] = bool(sec)
    return jsonify(out)

@app.route("/health")
def health():
    return jsonify({"status":"healthy","signals":len(signals),"scanning":scan_state["running"]}),200

@app.route("/api/diag")
def api_diag():
    # ── Scanner gate counters (flat dict — rendered as diag grid) ──────
    scanner_diag = dict(diag)

    # ── Live MEXC connectivity + key check ───────────────────────────
    api_key    = trade_config.get("api_key", "").strip()
    api_secret = trade_config.get("api_secret", "").strip()
    key_last4  = ("…" + api_key[-4:]) if len(api_key) >= 4 else ("(not set)" if not api_key else api_key)

    mexc_ping = "unknown"
    try:
        rp = requests.get("https://contract.mexc.com/api/v1/contract/ping", timeout=8)
        mexc_ping = f"HTTP {rp.status_code}"
    except Exception as e:
        mexc_ping = f"error: {e}"

    # Test auth + fetch balance in one call
    auth_test = "not tested"
    live_balance = None
    if api_key and api_secret and "PASTE" not in api_key:
        bal, auth_err = get_account_balance()
        if not auth_err:
            auth_test = f"✅ OK — ${bal:.2f} USDT"
            live_balance = bal
        else:
            auth_test = f"❌ {auth_err}"

    # Return flat structure: scanner gates + trading fields merged
    # Frontend renders ALL keys as diag cards, so we keep them at top level
    # but prefix trading keys so they're visually grouped
    return jsonify({
        # ── Scanner gates ──
        "neutral":        scanner_diag.get("neutral", 0),
        "not_continuous": scanner_diag.get("not_continuous", 0),
        "no_obs":         scanner_diag.get("no_obs", 0),
        "not_at_key":     scanner_diag.get("not_at_key", 0),
        "not_in_zone":    scanner_diag.get("not_in_zone", 0),
        "not_tapping":    scanner_diag.get("not_tapping", 0),
        "no_crts":        scanner_diag.get("no_crts", 0),
        "no_tbs":         scanner_diag.get("no_tbs", 0),
        "rr_low":         scanner_diag.get("rr_low", 0),
        "1d_no_crts":     scanner_diag.get("1d_no_crts", 0),
        "1d_no_tbs":      scanner_diag.get("1d_no_tbs", 0),
        "1d_rr_low":      scanner_diag.get("1d_rr_low", 0),
        "passed":         scanner_diag.get("passed", 0),
        # ── Trading diagnostics ──
        "outbound_ip":    _cached_ip,
        "key_last4":      key_last4,
        "secret_ok":      "✅ Set" if api_secret else "❌ Missing",
        "trade_mode":     "Cross Margin",
        "auto_trade":     "✅ ON" if trade_config.get("enabled", False) else "⏸ OFF",
        "mexc_ping":      mexc_ping,
        "auth_test":      auth_test,
        "balance_usdt":   f"${live_balance:.2f}" if live_balance is not None else "–",
    })

@app.route("/api/monitor")
def api_monitor():
    with manip_lock:
        return jsonify(list(manip_monitor.values()))

@app.route("/api/m2-monitor")
def api_m2_monitor():
    with m2_lock:
        return jsonify(list(m2_monitor.values()))

@app.route("/api/m4-monitor")
def api_m4_monitor():
    with m4_lock:
        return jsonify(list(m4_monitor.values()))

@app.route("/api/recent-trades")
def api_recent_trades():
    return jsonify(list(recent_trades))

@app.route("/api/pnl")
def api_pnl():
    data, err = mexc_request("GET", "/position/open_positions")
    if err or not data:
        return jsonify({"error": err or "No positions", "positions": []})
    positions = []
    for p in (data if isinstance(data, list) else []):
        positions.append({
            "symbol":    p.get("symbol",""),
            "direction": "BUY" if p.get("positionType")==1 else "SELL",
            "entry":     float(p.get("openAvgPrice",0)),
            "current":   float(p.get("closeAvgPrice",0) or p.get("currentPrice",0)),
            "size":      float(p.get("vol",0)),
            "leverage":  int(p.get("leverage",1)),
            "margin":    float(p.get("im",0)),
            "pnl":       float(p.get("unrealisedPnl",0)),
            "roi_pct":   round(float(p.get("unrealisedPnl",0)) /
                               max(float(p.get("im",1)),1) * 100, 2),
        })
    return jsonify({"positions": positions})

# ════════ PAPER TRADING ROUTES ═══════════════════════════════════════

@app.route("/api/paper-config", methods=["GET","POST"])
def api_paper_config():
    if request.method == "POST":
        data = request.get_json(silent=True) or {}
        with paper_lock:
            if "enabled"     in data: paper_config["enabled"]    = bool(data["enabled"])
            if "auto_trade"  in data: paper_config["auto_trade"] = bool(data["auto_trade"])
            if "balance"     in data: paper_config["balance"]    = float(data["balance"])
            if "risk_pct"    in data: paper_config["risk_pct"]   = float(data["risk_pct"])
            if "max_trades"  in data: paper_config["max_trades"] = int(data["max_trades"])
        log(f"📝 Paper config: enabled={paper_config['enabled']} auto={paper_config['auto_trade']} bal=${paper_config['balance']:.2f}")
        return jsonify({"ok": True, "config": dict(paper_config)})
    with paper_lock:
        return jsonify(dict(paper_config))

@app.route("/api/paper-trades")
def api_paper_trades():
    with paper_lock:
        return jsonify(list(paper_trades.values()))

@app.route("/api/paper-history")
def api_paper_history():
    return jsonify(list(paper_history))

@app.route("/api/paper-stats")
def api_paper_stats():
    return jsonify(dict(paper_stats))

@app.route("/api/paper-close", methods=["POST"])
def api_paper_close():
    data   = request.get_json(silent=True) or {}
    symbol = data.get("symbol","")
    if not symbol: return jsonify({"ok":False,"message":"symbol required"}),400
    ok, msg = close_paper_trade(symbol, reason="Manual (Dashboard)")
    return jsonify({"ok":ok,"message":msg})

@app.route("/api/paper-reset", methods=["POST"])
def api_paper_reset():
    with paper_lock:
        paper_trades.clear()
        paper_history.clear()
        paper_stats["total"]     = 0
        paper_stats["wins"]      = 0
        paper_stats["losses"]    = 0
        paper_stats["total_pnl"] = 0.0
    log("📝 Paper trading stats reset")
    return jsonify({"ok": True})

# ════════ STARTUP ═════════════════════════════════════════════════════

def _detect_sharp_sweep(candles, zone_top, zone_bot, direction, lookback=30):
    """
    Detect a sharp sweep: a candle that aggressively pierces through the zone
    with a wick or full body, showing strong momentum through the key level.
    Returns (True, sweep_extreme) or (False, None).
    """
    recent = candles[-lookback:]
    for c in reversed(recent):
        if direction == "BUY":
            # Bullish sweep: wick punches below zone_bot then closes above it
            swept = c["low"] < zone_bot
            closed_above = c["close"] > zone_bot
            # Sharp = wick is at least 60% of candle range
            candle_range = c["high"] - c["low"]
            wick_size = c["close"] - c["low"] if c["close"] > c["open"] else c["open"] - c["low"]
            sharp = candle_range > 0 and (wick_size / candle_range) >= 0.4
            if swept and closed_above and sharp:
                return True, round(c["low"], 8)
        else:
            # Bearish sweep: wick punches above zone_top then closes below it
            swept = c["high"] > zone_top
            closed_below = c["close"] < zone_top
            candle_range = c["high"] - c["low"]
            wick_size = c["high"] - c["close"] if c["close"] < c["open"] else c["high"] - c["open"]
            sharp = candle_range > 0 and (wick_size / candle_range) >= 0.4
            if swept and closed_below and sharp:
                return True, round(c["high"], 8)
    return False, None


def _find_fvg_after_sweep(candles, sweep_idx_from_end, direction):
    """
    After a sweep, look for a displacement FVG in the candles that follow.
    Returns (fvg_top, fvg_bot, fvg_tip) or (None, None, None).
    """
    start = max(0, len(candles) - sweep_idx_from_end)
    for i in range(start, len(candles) - 2):
        c1 = candles[i]
        c3 = candles[i + 2]
        if direction == "BUY":
            if c3["low"] > c1["high"]:   # bullish FVG
                fvg_top = c3["low"]
                fvg_bot = c1["high"]
                return round(fvg_top, 8), round(fvg_bot, 8), round(fvg_top, 8)  # entry at top (price pulls back into FVG)
        else:
            if c3["high"] < c1["low"]:   # bearish FVG
                fvg_top = c1["low"]
                fvg_bot = c3["high"]
                return round(fvg_top, 8), round(fvg_bot, 8), round(fvg_bot, 8)  # entry at bot
    return None, None, None


def m2_monitor_loop():
    """
    Unified M2 / M3 monitor.

    AWAIT_PATTERN  — watching LTF for whichever pattern forms first:
        M2: swing point forms → single candle sweeps ft_extreme (no CHoCH needed)
        M3: single candle sweeps ft_extreme → CHoCH forms after

    Once pattern identified → AWAIT_FVG → AWAIT_TAP → market order

    M2 SL: above sweep candle high/low
    M3 SL: above sweep candle IF fvg was before CHoCH
            above OB above FVG if fvg was after CHoCH
    TP:  the origin low/high that created the first touch
    """
    log("🔍 Unified M2a/M2b monitor started")
    while True:
        try:
            with m2_lock:
                symbols = list(m2_monitor.keys())

            for symbol in symbols:
                with m2_lock:
                    if symbol not in m2_monitor: continue
                    mon = dict(m2_monitor[symbol])

                phase      = mon.get("phase", "AWAIT_PATTERN")
                direction  = mon.get("direction", "BUY")
                ltf        = mon.get("ltf", "Min15")
                zone_top   = mon.get("zone_top", 0)
                zone_bot   = mon.get("zone_bot", 0)
                ft_extreme = mon.get("ft_extreme", 0)
                ft_idx     = mon.get("ft_idx", 0)
                liq_target = mon.get("liq_target", 0)
                model_tag  = mon.get("model")

                ltf_c = get_candles(symbol, ltf, limit=300)
                if not ltf_c or len(ltf_c) < 10: continue

                # ── PHASE: AWAIT_PATTERN ──────────────────────────────────────
                # Watch for M2 or M3 pattern to form after first touch
                if phase == "AWAIT_PATTERN":

                    # Expire if price moves too far from zone
                    ticker = get_ticker(symbol)
                    if ticker:
                        price    = ticker["price"]
                        zone_mid = (zone_top + zone_bot) / 2
                        if zone_mid > 0 and abs(price - zone_mid) / zone_mid > 0.08:
                            with m2_lock: m2_monitor.pop(symbol, None)
                            log(f"❌ EXPIRED: {symbol} — price moved 8%+ from zone")
                            continue

                    # ── Try M2: swing point forms after ft, then single candle sweeps ft_extreme ──
                    swing_idx, swing_val = _find_swing_point(ltf_c, ft_idx + 1, direction)
                    m2_sweep_idx = m2_sweep_c = None
                    if swing_idx is not None:
                        m2_sweep_idx, m2_sweep_c = _valid_single_candle_sweep(
                            ltf_c, ft_extreme, direction, search_from=swing_idx + 1)

                    # ── Try M3: single candle sweeps ft_extreme, then CHoCH forms ──
                    m3_sweep_idx, m3_sweep_c = _valid_single_candle_sweep(
                        ltf_c, ft_extreme, direction, search_from=ft_idx + 1)
                    m3_choch_idx = m3_choch_lvl = None
                    if m3_sweep_idx is not None:
                        m3_choch_idx, m3_choch_lvl = _find_choch_after(
                            ltf_c, m3_sweep_idx + 1, direction)

                    # ── Decide which model fired ──
                    # M3 takes priority if both found (sweep→choch is stronger confirmation)
                    confirmed_model = None
                    sweep_idx_used = sweep_c_used = None
                    choch_idx_used = choch_lvl_used = None

                    if m3_sweep_idx is not None and m3_choch_idx is not None:
                        confirmed_model  = "2b"
                        sweep_idx_used   = m3_sweep_idx
                        sweep_c_used     = m3_sweep_c
                        choch_idx_used   = m3_choch_idx
                        choch_lvl_used   = m3_choch_lvl
                    elif m2_sweep_idx is not None:
                        confirmed_model  = "2a"
                        sweep_idx_used   = m2_sweep_idx
                        sweep_c_used     = m2_sweep_c

                    if confirmed_model is None:
                        continue   # neither pattern ready yet

                    sweep_extreme = (sweep_c_used["high"] if direction == "SELL"
                                     else sweep_c_used["low"])

                    log(f"✅ MODEL #{confirmed_model} PATTERN: {symbol} {direction} "
                        f"sweep={round(sweep_extreme,8)}"
                        + (f" choch={choch_lvl_used}" if confirmed_model=="3" else ""))

                    with m2_lock:
                        if symbol in m2_monitor:
                            m2_monitor[symbol].update({
                                "phase":        "AWAIT_FVG",
                                "model":        confirmed_model,
                                "sweep_idx":    sweep_idx_used,
                                "sweep_extreme":round(sweep_extreme, 8),
                                "sweep_c_high": round(sweep_c_used["high"], 8),
                                "sweep_c_low":  round(sweep_c_used["low"],  8),
                                "choch_idx":    choch_idx_used,
                                "choch_level":  choch_lvl_used,
                                "pattern_time": datetime.now(LOCAL_TZ).strftime("%H:%M UTC+1"),
                            })
                    continue

                # ── PHASE: AWAIT_FVG ─────────────────────────────────────────
                if phase == "AWAIT_FVG":
                    sweep_idx     = mon.get("sweep_idx", 0)
                    choch_idx     = mon.get("choch_idx")
                    sweep_extreme = mon.get("sweep_extreme", 0)
                    sweep_c_high  = mon.get("sweep_c_high", 0)
                    sweep_c_low   = mon.get("sweep_c_low",  0)

                    # Determine SL now (needed for RR calc)
                    sl_sweep = (round(sweep_c_high * 1.001, 8) if direction == "SELL"
                                else round(sweep_c_low  * 0.999, 8))

                    fvg_found_data = None
                    fvg_source     = None   # "displacement" | "pre_choch" | "post_choch" | "post_sweep"
                    sl_final       = sl_sweep

                    def _fvg_on_correct_side(fvg, sw_ext, dirn):
                        """FVG zone must be on the CORRECT side of the sweep extreme.
                        BUY: the gap-up zone must be ABOVE sweep low (not in the dropped area).
                        SELL: the gap-down zone must be BELOW sweep high."""
                        if dirn == "BUY":
                            return fvg["bot"] >= sw_ext * 0.995
                        return fvg["top"] <= sw_ext * 1.005

                    if model_tag == "2b" and choch_idx is not None:
                        # ── Priority 1: Displacement FVG — the gap created BY the CHoCH impulse ──
                        # The 3-candle imbalance from the candle that broke structure
                        disp_start = max(sweep_idx, choch_idx - 5)
                        disp_end   = min(choch_idx + 6, len(ltf_c))
                        disp_fvgs  = [f for f in _find_fvg_in_range(ltf_c, disp_start, disp_end, direction)
                                      if _fvg_on_correct_side(f, sweep_extreme, direction)]
                        if disp_fvgs:
                            fvg_found_data = disp_fvgs[0]
                            fvg_source     = "displacement"
                            sl_final       = sl_sweep
                        else:
                            # ── Priority 2: Any FVG from sweep → choch, on correct side ──
                            pre_fvgs = [f for f in _find_fvg_in_range(ltf_c, sweep_idx, choch_idx + 3, direction)
                                        if _fvg_on_correct_side(f, sweep_extreme, direction)]
                            if pre_fvgs:
                                fvg_found_data = pre_fvgs[0]
                                fvg_source     = "pre_choch"
                                sl_final       = sl_sweep
                            else:
                                # ── Priority 3: Post-CHoCH FVG (price displaced, waiting for retest) ──
                                post_fvgs = [f for f in _find_fvg_in_range(ltf_c, choch_idx, len(ltf_c), direction)
                                             if _fvg_on_correct_side(f, sweep_extreme, direction)]
                                if post_fvgs:
                                    fvg_found_data = post_fvgs[0]
                                    fvg_source     = "post_choch"
                                    ob_extreme = _find_ob_above_fvg(
                                        ltf_c, fvg_found_data["top"], fvg_found_data["bot"], direction)
                                    sl_final = (round(ob_extreme * 1.001, 8) if ob_extreme
                                                else sl_sweep)
                    else:
                        # ── M2a: FVG must form AFTER the sweep — search from sweep onward ──
                        post_fvgs = [f for f in _find_fvg_in_range(ltf_c, sweep_idx, len(ltf_c), direction)
                                     if _fvg_on_correct_side(f, sweep_extreme, direction)]
                        if post_fvgs:
                            fvg_found_data = post_fvgs[0]
                            fvg_source     = "post_sweep"
                            sl_final       = sl_sweep

                    if fvg_found_data is None:
                        # Timeout 3hr after pattern confirmed
                        pt = mon.get("pattern_time","")
                        if pt:
                            try:
                                t0 = datetime.strptime(pt, "%H:%M UTC+1").replace(
                                    year=datetime.now().year, month=datetime.now().month,
                                    day=datetime.now().day, tzinfo=LOCAL_TZ)
                                if (datetime.now(LOCAL_TZ) - t0).total_seconds() > 10800:
                                    with m2_lock: m2_monitor.pop(symbol, None)
                                    log(f"❌ TIMEOUT: {symbol} M{model_tag} — no FVG in 3hr")
                            except: pass
                        continue

                    # RR check
                    fvg_tip = fvg_found_data["tip"]
                    risk    = abs(fvg_tip - sl_final)
                    reward  = abs(liq_target - fvg_tip) if liq_target else 0
                    if risk <= 0: continue
                    rr = round(reward / risk, 2)
                    if rr < 2.0:
                        with m2_lock: m2_monitor.pop(symbol, None)
                        log(f"⚠️ {symbol} M{model_tag} RR={rr}R < 2.0 — skip")
                        continue

                    log(f"✅ M{model_tag} FVG: {symbol} {direction} "
                        f"fvg={fvg_found_data['bot']}–{fvg_found_data['top']} "
                        f"src={fvg_source} RR={rr}R — watching for tap")

                    with m2_lock:
                        if symbol in m2_monitor:
                            m2_monitor[symbol].update({
                                "phase":      "AWAIT_TAP",
                                "fvg_top":    fvg_found_data["top"],
                                "fvg_bot":    fvg_found_data["bot"],
                                "fvg_tip":    fvg_tip,
                                "fvg_source": fvg_source,
                                "sl":         sl_final,
                                "tp":         round(liq_target, 8),
                                "rr":         rr,
                                "fvg_time":   datetime.now(LOCAL_TZ).strftime("%H:%M UTC+1"),
                            })
                    continue

                # ── PHASE: AWAIT_TAP ─────────────────────────────────────────
                if phase == "AWAIT_TAP":
                    fvg_top    = mon.get("fvg_top", 0)
                    fvg_bot    = mon.get("fvg_bot", 0)
                    fvg_tip    = mon.get("fvg_tip", 0)
                    sl_p       = mon.get("sl", 0)
                    tp_p       = mon.get("tp", 0)
                    rr         = mon.get("rr", 0)
                    fvg_source = mon.get("fvg_source","")
                    sweep_ext  = mon.get("sweep_extreme", 0)
                    choch_lvl  = mon.get("choch_level","–")

                    ticker = get_ticker(symbol)
                    if not ticker: continue
                    price = ticker["price"]

                    # Tap = price enters FVG zone
                    tapped = (fvg_bot <= price <= fvg_top)

                    # Invalidate: price blows clean through FVG (0.3% buffer)
                    if direction == "SELL" and price > fvg_top * 1.006:
                        with m2_lock: m2_monitor.pop(symbol, None)
                        log(f"❌ M{model_tag} INVALID: {symbol} blew through FVG top")
                        continue
                    if direction == "BUY"  and price < fvg_bot * 0.994:
                        with m2_lock: m2_monitor.pop(symbol, None)
                        log(f"❌ M{model_tag} INVALID: {symbol} blew through FVG bot")
                        continue

                    # 8hr expiry on FVG tap wait
                    fvg_time_str = mon.get("fvg_time","")
                    if not tapped and fvg_time_str:
                        try:
                            ft_ = datetime.strptime(fvg_time_str, "%H:%M UTC+1").replace(
                                year=datetime.now().year, month=datetime.now().month,
                                day=datetime.now().day, tzinfo=LOCAL_TZ)
                            if (datetime.now(LOCAL_TZ) - ft_).total_seconds() > 28800:
                                with m2_lock: m2_monitor.pop(symbol, None)
                                log(f"❌ M{model_tag} EXPIRED: {symbol} FVG not tapped in 8hr")
                        except: pass
                        continue

                    if not tapped: continue

                    log(f"🚀 M{model_tag} FVG TAPPED: {symbol} {direction} "
                        f"price={price} fvg={fvg_bot}–{fvg_top} RR={rr}R — MARKET ORDER")

                    zone_name = mon.get("zone_name","–")
                    trend     = mon.get("trend","NEUTRAL")
                    htf       = mon.get("htf","Hour4")
                    in_pd     = mon.get("in_pd", False)
                    has_pd    = in_pd or "DISCOUNT" in zone_name or "PREMIUM" in zone_name
                    score     = 92 if has_pd and rr >= 3.0 else 82 if rr >= 3.0 else 72
                    grade     = "A+" if score >= 85 else "A"

                    m_label = f"Model #{model_tag}"
                    if model_tag == "2":
                        entry_type = "Model #2a (Sweep→FVG Retest)"
                        details = [
                            "✅ HTF Key Level (P/D zone)",
                            f"✅ Swing point + single candle sweep of first touch",
                            f"✅ Sweep candle closes with wick only",
                            f"✅ Displacement FVG → retest",
                            f"✅ RR:{rr}R | SL: above sweep candle",
                        ]
                    elif model_tag == "2b":
                        src_lbl = "Displacement" if fvg_source == "displacement" else ("Pre-CHoCH" if fvg_source == "pre_choch" else "Post-CHoCH")
                        entry_type = f"Model #2b (Sweep→CHoCH→{src_lbl} FVG)"
                        sl_note = "SL above OB above FVG" if fvg_source=="post_choch" else "SL above sweep candle"
                        details = [
                            "✅ HTF Key Level (P/D zone)",
                            f"✅ Single candle sweep of first touch extreme",
                            f"✅ CHoCH confirmed after sweep ({choch_lvl})",
                            f"✅ {'Pre-CHoCH' if fvg_source=='pre_choch' else 'Post-CHoCH'} FVG → retest",
                            f"✅ RR:{rr}R | {sl_note}",
                        ]

                    sig = {
                        "model":         model_tag,
                        "symbol":        symbol,
                        "tf":            htf,
                        "ob_tf":         ltf,
                        "ob_zone":       zone_name,
                        "zone_type":     mon.get("kl_type","KL"),
                        "direction":     direction,
                        "trend":         trend,
                        "entry":         round(price, 8),
                        "entry_type":    entry_type,
                        "sl":            sl_p,
                        "tp":            tp_p,
                        "tp1":           round((price + tp_p) / 2, 8),
                        "tp2":           tp_p,
                        "rr":            rr,
                        "crh":           round(zone_top, 8),
                        "crl":           round(zone_bot, 8),
                        "ob_top":        round(zone_top, 8),
                        "ob_bot":        round(zone_bot, 8),
                        "fvg_found":     True,
                        "fvg_type":      f"M{model_tag}-FVG ({fvg_source})",
                        "fvg_entry":     fvg_tip,
                        "fvg_top":       fvg_top,
                        "fvg_bot":       fvg_bot,
                        "sweep_extreme": sweep_ext,
                        "choch_found":   model_tag == "2b",
                        "choch_level":   choch_lvl if model_tag=="2b" else "–",
                        "tbs_found":     False,"tbs_tf":"–","tbs_entry":"–","tbs_sl":"–",
                        "liq_swept":     True,"ob_respected":False,"continuous":True,
                        "score":         score,"grade":grade,
                        "details":       details,
                        "from_monitor":  True,
                        "market_order":  True,
                        "timestamp":     datetime.now(LOCAL_TZ).strftime("%Y-%m-%d %H:%M UTC+1"),
                    }

                    signals.appendleft(sig)
                    send_telegram(fmt_tg_m2(sig), kind="signal")

                    ready, reason = live_trade_ready()
                    if ready:
                        ok, msg = place_order(sig)
                        log(f"{'✅' if ok else '❌'} M{model_tag} market order: {msg}")
                        if not ok:
                            log(f"[REJECTED] {symbol} {direction}: {msg}")
                            send_telegram(
                                f"⚠️ Trade REJECTED: {symbol} {direction}\n"
                                f"Reason: {msg}\n"
                                f"Entry: {sig.get('entry','–')} | SL: {sig.get('sl','–')} | TP: {sig.get('tp','–')}\n"
                                f"RR: {sig.get('rr','–')}R | Score: {sig.get('score','–')}/100",
                                kind="trade"
                            )
                    else:
                        log(f"⚠️ M{model_tag} live trade skipped ({symbol}): {reason}")

                    if paper_config["enabled"] and paper_config["auto_trade"]:
                        ok2, msg2 = place_paper_order(sig)
                        if ok2: log(f"📝 M{model_tag} paper: {msg2}")

                    with m2_lock:
                        m2_monitor.pop(symbol, None)

        except Exception as e:
            log(f"❌ M2a/M2b monitor error: {e}")
        time.sleep(5)



def start_scanner():
    t=threading.Thread(target=scanner_loop,       daemon=True,name="scanner"); t.start()
    m=threading.Thread(target=manip_monitor_loop,  daemon=True,name="manip");   m.start()
    m2=threading.Thread(target=m2_monitor_loop,    daemon=True,name="m2mon");   m2.start()
    m4=threading.Thread(target=m4_monitor_loop,    daemon=True,name="m4mon");   m4.start()
    p=threading.Thread(target=paper_monitor_loop,  daemon=True,name="paper");   p.start()
    pc=threading.Thread(target=price_cache_loop,   daemon=True,name="price_cache"); pc.start()
    log("🚀 Scanner + M1 + M2a/M2b + M3 + paper monitor + price-cache threads launched.")

def _delayed_start():
    global _cached_ip
    time.sleep(2)
    # ── Cache outbound IP immediately so 403 logs show real IP ──────────
    try:
        r = requests.get("https://api.ipify.org?format=json", timeout=10)
        _cached_ip = r.json().get("ip", "unknown")
    except Exception:
        _cached_ip = "unknown"
    log(f"🌐 Outbound IP: {_cached_ip}")
    start_scanner()
    # Run startup diagnostics (logs + Telegram) after scanner is up
    threading.Thread(target=_run_startup_diagnostics, daemon=True, name="diag").start()

# ── STARTUP ──────────────────────────────────────────────────────
# Works with Railway (Gunicorn) — starts scanner thread on module load
_scanner_started = False

def _ensure_started():
    global _scanner_started
    if not _scanner_started:
        _scanner_started = True
        t = threading.Thread(target=_delayed_start, daemon=True)
        t.start()
        log("🚀 Mad Man Strategy Scanner threads launched")


# ════════ MARKET PAGE ════════════════════════════════════════════════

MARKET_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1.0,maximum-scale=1.0,user-scalable=no"/>
<title>SignalCore · Market</title>
<script src="https://unpkg.com/lightweight-charts@3.8.0/dist/lightweight-charts.standalone.production.js"></script>
<link href="https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500;700&family=Syne:wght@700;800&family=Fredoka+One&display=swap" rel="stylesheet"/>
<style>
*{box-sizing:border-box;margin:0;padding:0;-webkit-tap-highlight-color:transparent}
:root{
  --bg:#070810;--s1:#0d0f1e;--s2:#12152a;--s3:#181c35;
  --accent:#4f6ef7;--accent2:#7c3aed;--green:#00d4aa;--red:#ff4d6a;
  --yellow:#ffc93c;--text:#e8eaf6;--dim:#6b7299;--muted:#2a2f52;
  --border:rgba(79,110,247,.18);--border2:rgba(79,110,247,.4);
  --font:'Space Grotesk',sans-serif;--mono:'JetBrains Mono',monospace;--display:'Syne',sans-serif
}
html,body{height:100%;background:var(--bg);color:var(--text);font-family:var(--font);overflow:hidden}
.app{display:flex;flex-direction:column;height:100vh;height:100dvh}
/* TOP BAR */
.topbar{background:rgba(7,8,16,.95);border-bottom:1px solid var(--border);padding:0 16px;height:52px;display:flex;align-items:center;justify-content:space-between;flex-shrink:0;backdrop-filter:blur(20px);position:relative;z-index:10}
.topbar-left{display:flex;align-items:center;gap:10px}
.logo{font-family:var(--display);font-size:1.05rem;font-weight:800;letter-spacing:.06em;background:linear-gradient(135deg,#4f6ef7,#7c3aed,#00d4aa);-webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text}
.scanner-dot{width:6px;height:6px;border-radius:50%;background:var(--green);box-shadow:0 0 8px var(--green);animation:blink 2s infinite}
@keyframes blink{0%,100%{opacity:1}50%{opacity:.3}}
.pair-label{font-family:var(--mono);font-size:.7rem;color:var(--dim);letter-spacing:.05em;display:none}
.pair-label.show{display:block}
.back-btn{background:var(--s2);border:1px solid var(--border);border-radius:8px;color:var(--dim);padding:5px 10px;font-family:var(--font);font-size:.75rem;font-weight:600;cursor:pointer;transition:all .2s;display:none;align-items:center;gap:5px}
.back-btn.show{display:flex}
.back-btn:hover{border-color:var(--accent);color:var(--text)}
.topbar-right{display:flex;align-items:center;gap:8px}
.dash-link{background:var(--s2);border:1px solid var(--border);border-radius:8px;color:var(--dim);padding:5px 10px;font-family:var(--font);font-size:.75rem;font-weight:600;cursor:pointer;text-decoration:none;transition:all .2s}
.dash-link:hover{border-color:var(--accent);color:var(--text)}
/* MAIN */
.main{flex:1;overflow:hidden;position:relative}
/* PAGE SYSTEM */
.page{position:absolute;inset:0;display:flex;flex-direction:column;transition:transform .32s cubic-bezier(.4,0,.2,1),opacity .32s;will-change:transform,opacity}
.page.hidden-left{transform:translateX(-100%);opacity:0;pointer-events:none}
.page.hidden-right{transform:translateX(100%);opacity:0;pointer-events:none}
.page.visible{transform:translateX(0);opacity:1}
/* WATCHLIST */
.wl-header{padding:16px 16px 12px;flex-shrink:0;border-bottom:1px solid var(--border)}
.wl-title{font-family:'Fredoka One',var(--display),sans-serif;font-size:1.15rem;font-weight:900;letter-spacing:.08em;background:linear-gradient(135deg,var(--accent),var(--accent2),var(--green));-webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text;text-transform:uppercase;margin-bottom:10px}
.wl-search{position:relative}
.wl-search input{width:100%;background:var(--s2);border:1.5px solid var(--border);border-radius:12px;padding:10px 12px 10px 36px;color:var(--text);font-family:var(--font);font-size:.84rem;font-weight:500;outline:none;transition:border-color .2s}
.wl-search input:focus{border-color:var(--accent);box-shadow:0 0 0 3px rgba(79,110,247,.12)}
.wl-search input::placeholder{color:var(--dim)}
.search-ico{position:absolute;left:11px;top:50%;transform:translateY(-50%);color:var(--dim);font-size:.82rem;pointer-events:none}
.wl-list{flex:1;overflow-y:auto;padding:0 0 70px;scrollbar-width:thin;scrollbar-color:var(--muted) transparent}
.wl-list::-webkit-scrollbar{width:3px}
.wl-list::-webkit-scrollbar-thumb{background:var(--muted);border-radius:2px}
.wl-row{display:flex;align-items:center;padding:14px 16px;border-bottom:1px solid rgba(79,110,247,.06);cursor:pointer;transition:all .15s;position:relative;gap:13px}
.wl-row:active{background:rgba(79,110,247,.07)}
.wl-row::before{content:'';position:absolute;left:0;top:6px;bottom:6px;width:3px;background:transparent;border-radius:0 2px 2px 0;transition:all .2s}
.wl-row.has-signal.buy-sig::before{background:var(--green);box-shadow:0 0 6px var(--green)}
.wl-row.has-signal.sell-sig::before{background:var(--red);box-shadow:0 0 6px var(--red)}
.sym-icon{width:46px;height:46px;border-radius:13px;display:flex;align-items:center;justify-content:center;flex-shrink:0;font-weight:800;font-family:var(--mono);border:1.5px solid;position:relative;font-size:.72rem;letter-spacing:0}
.sym-icon .signal-badge{position:absolute;top:-4px;right:-4px;width:11px;height:11px;border-radius:50%;border:2px solid var(--bg);animation:blink 1.5s infinite}
.wl-info{flex:1;min-width:0}
.wl-sym{font-weight:800;font-size:1.05rem;letter-spacing:.01em;font-family:'Fredoka One',var(--display),sans-serif;line-height:1.1;color:var(--text)}
.wl-sym span{font-family:var(--mono);font-size:.7rem;font-weight:400;color:var(--dim);letter-spacing:0}
.wl-base{font-size:.62rem;color:var(--dim);font-family:var(--mono);margin-top:3px;letter-spacing:.02em}
.wl-right{text-align:right;flex-shrink:0;min-width:90px}
.wl-price{font-family:var(--mono);font-size:.95rem;font-weight:800;letter-spacing:-.01em;line-height:1.2}
.wl-chg{font-family:var(--mono);font-size:.68rem;font-weight:700;padding:3px 8px;border-radius:6px;margin-top:4px;display:inline-block;letter-spacing:.01em}
.wl-chg.up{background:rgba(0,212,170,.12);color:var(--green);border:1px solid rgba(0,212,170,.2)}
.wl-chg.dn{background:rgba(255,77,106,.12);color:var(--red);border:1px solid rgba(255,77,106,.2)}
.wl-chg.flat{background:rgba(107,114,153,.1);color:var(--dim);border:1px solid rgba(107,114,153,.15)}
/* CHART PAGE */
.chart-header{padding:12px 16px 8px;flex-shrink:0;border-bottom:1px solid var(--border)}
.ch-row1{display:flex;align-items:center;justify-content:space-between;margin-bottom:6px}
.ch-sym{font-family:var(--display);font-size:1.1rem;font-weight:800;letter-spacing:.04em}
.ch-badge{font-family:var(--mono);font-size:.6rem;padding:2px 7px;border-radius:5px;background:rgba(79,110,247,.12);border:1px solid var(--border);color:var(--accent);letter-spacing:.06em}
.ch-price-row{display:flex;align-items:baseline;gap:10px;margin-bottom:4px}
.ch-price{font-family:var(--mono);font-size:1.5rem;font-weight:700}
.ch-chg{font-family:var(--mono);font-size:.72rem;font-weight:700;padding:3px 8px;border-radius:6px}
.ch-chg.up{background:rgba(0,212,170,.1);color:var(--green)}
.ch-chg.dn{background:rgba(255,77,106,.1);color:var(--red)}
.ch-stats{display:flex;gap:14px}
.ch-stat{display:flex;flex-direction:column;gap:1px}
.ch-stat-lbl{font-size:.55rem;color:var(--dim);text-transform:uppercase;letter-spacing:.07em;font-weight:600}
.ch-stat-val{font-family:var(--mono);font-size:.7rem;font-weight:700;color:var(--text)}
.tf-bar{display:flex;gap:4px;padding:8px 16px;flex-shrink:0;border-bottom:1px solid var(--border);overflow-x:auto;scrollbar-width:none}
.tf-bar::-webkit-scrollbar{display:none}
.tf-btn{padding:5px 11px;border-radius:7px;border:1px solid var(--border);background:transparent;color:var(--dim);font-family:var(--mono);font-size:.65rem;font-weight:700;cursor:pointer;transition:all .18s;white-space:nowrap;flex-shrink:0}
.tf-btn.active{background:var(--accent);border-color:var(--accent);color:#fff;box-shadow:0 0 12px rgba(79,110,247,.4)}
.tf-btn:hover:not(.active){border-color:var(--accent);color:var(--text)}
/* DRAWING TOOLBAR */
.draw-toolbar{display:flex;align-items:center;gap:6px;padding:6px 12px;flex-shrink:0;border-bottom:1px solid var(--border);background:rgba(7,8,16,.6);overflow-x:auto;scrollbar-width:none}
.draw-toolbar::-webkit-scrollbar{display:none}
.draw-btn{display:flex;align-items:center;gap:5px;padding:5px 10px;border-radius:7px;border:1px solid var(--border);background:transparent;color:var(--dim);font-family:var(--mono);font-size:.62rem;font-weight:700;cursor:pointer;transition:all .18s;white-space:nowrap;flex-shrink:0;letter-spacing:.02em}
.draw-btn:hover:not(.active){border-color:var(--accent);color:var(--text)}
.draw-btn.active{background:rgba(79,110,247,.18);border-color:var(--accent2);color:#a78bfa;box-shadow:0 0 8px rgba(124,58,237,.25)}
.draw-btn.danger{border-color:rgba(255,77,106,.3);color:var(--red)}
.draw-btn.danger:hover{background:rgba(255,77,106,.1)}
.draw-sep{width:1px;height:20px;background:var(--border);flex-shrink:0;margin:0 2px}
.tool-hint{font-family:var(--mono);font-size:.58rem;color:var(--dim);padding:0 4px;white-space:nowrap;flex-shrink:0}
/* SIGNAL BAR */
.signal-bar{margin:6px 16px;padding:8px 12px;border-radius:10px;border:1px solid;font-size:.72rem;font-weight:600;display:none;align-items:center;gap:8px;animation:slide-in .3s ease}
@keyframes slide-in{from{transform:translateY(-8px);opacity:0}to{transform:translateY(0);opacity:1}}
.signal-bar.show{display:flex}
.signal-bar.buy{background:rgba(0,212,170,.08);border-color:rgba(0,212,170,.3);color:var(--green)}
.signal-bar.sell{background:rgba(255,77,106,.08);border-color:rgba(255,77,106,.3);color:var(--red)}
.signal-bar-ico{font-size:.9rem}
.signal-bar-info{flex:1;min-width:0}
.signal-bar-title{font-weight:700;font-size:.72rem}
.signal-bar-sub{font-size:.6rem;color:inherit;opacity:.75;margin-top:1px}
.signal-bar-rr{font-family:var(--mono);font-weight:700;font-size:.72rem;flex-shrink:0;padding:3px 8px;border-radius:5px;background:rgba(255,255,255,.07)}
/* CHART WRAP */
.chart-wrap{flex:1;position:relative;min-height:0;overflow:hidden}
#chart-container{width:100%;height:100%;position:absolute;inset:0}
/* DRAWING CANVAS — sits above chart, pointer-events managed by JS */
#drawing-canvas{position:absolute;inset:0;z-index:5;pointer-events:none;touch-action:none}
.chart-loading{position:absolute;inset:0;display:flex;align-items:center;justify-content:center;background:var(--bg);z-index:20;flex-direction:column;gap:10px}
.chart-loading.hidden{display:none}
.loading-ring{width:32px;height:32px;border:3px solid var(--muted);border-top-color:var(--accent);border-radius:50%;animation:spin .7s linear infinite}
@keyframes spin{to{transform:rotate(360deg)}}
.loading-txt{font-family:var(--mono);font-size:.65rem;color:var(--dim);letter-spacing:.05em}
/* EMPTY STATE */
.empty-chart{position:absolute;inset:0;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:12px;text-align:center;padding:24px;z-index:10}
.empty-chart-ico{font-size:3rem;opacity:.4}
.empty-chart-t{font-family:var(--display);font-size:1rem;font-weight:700;color:var(--dim)}
.empty-chart-s{font-size:.75rem;color:var(--dim);max-width:240px;line-height:1.6;opacity:.7}
/* SETTINGS PANEL (floating) */
.settings-panel{position:fixed;z-index:200;background:rgba(13,15,30,.97);border:1.5px solid var(--border2);border-radius:16px;padding:14px 16px;min-width:240px;max-width:290px;box-shadow:0 8px 32px rgba(0,0,0,.6);backdrop-filter:blur(20px);display:none;touch-action:none}
.settings-panel.show{display:block}
.sp-title{font-family:var(--display);font-size:.8rem;font-weight:700;color:var(--accent);letter-spacing:.06em;margin-bottom:12px;display:flex;align-items:center;justify-content:space-between}
.sp-close{background:none;border:none;color:var(--dim);cursor:pointer;font-size:.85rem;padding:2px 5px;border-radius:5px;transition:color .2s}
.sp-close:hover{color:var(--red)}
.sp-row{display:flex;align-items:center;justify-content:space-between;margin-bottom:10px;gap:10px}
.sp-lbl{font-family:var(--mono);font-size:.62rem;color:var(--dim);letter-spacing:.04em;flex-shrink:0}
.sp-val{font-family:var(--mono);font-size:.68rem;color:var(--text);font-weight:600}
.sp-inp{background:var(--s2);border:1px solid var(--border);border-radius:7px;color:var(--text);font-family:var(--mono);font-size:.7rem;padding:4px 8px;outline:none;width:70px;transition:border-color .2s}
.sp-inp:focus{border-color:var(--accent)}
.sp-inp-wide{width:100%;flex:1}
.sp-color{width:36px;height:26px;border-radius:6px;border:1px solid var(--border);cursor:pointer;padding:0;background:none}
.sp-select{background:var(--s2);border:1px solid var(--border);border-radius:7px;color:var(--text);font-family:var(--mono);font-size:.65rem;padding:4px 6px;outline:none;cursor:pointer;transition:border-color .2s}
.sp-select:focus{border-color:var(--accent)}
.sp-btn{background:rgba(79,110,247,.12);border:1px solid var(--border);border-radius:7px;color:var(--accent);font-family:var(--mono);font-size:.62rem;font-weight:700;padding:5px 10px;cursor:pointer;transition:all .18s;letter-spacing:.03em}
.sp-btn:hover{background:rgba(79,110,247,.25);border-color:var(--accent)}
.sp-btn.danger{background:rgba(255,77,106,.1);border-color:rgba(255,77,106,.3);color:var(--red)}
.sp-btn.danger:hover{background:rgba(255,77,106,.2)}
.sp-divider{border:none;border-top:1px solid var(--border);margin:10px 0}
.sp-info-grid{display:grid;grid-template-columns:auto 1fr;gap:3px 10px;font-size:.6rem}
.sp-info-grid span:nth-child(odd){color:var(--dim);font-family:var(--mono)}
.sp-info-grid span:nth-child(even){color:var(--text);font-family:var(--mono);font-weight:600;text-align:right}
.sp-toggle{display:flex;align-items:center;gap:6px;cursor:pointer}
.sp-toggle input[type=checkbox]{width:15px;height:15px;accent-color:var(--accent);cursor:pointer}
/* BOTTOM NAV */
.bottom-nav{position:fixed;bottom:0;left:0;right:0;background:rgba(7,8,16,.97);border-top:1px solid var(--border);display:flex;height:60px;z-index:100;backdrop-filter:blur(20px)}
.nav-btn{flex:1;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:3px;border:none;background:transparent;cursor:pointer;transition:all .2s;position:relative;padding:0}
.nav-ico{font-size:1.1rem;transition:transform .2s;line-height:1}
.nav-lbl{font-family:var(--font);font-size:.6rem;font-weight:600;letter-spacing:.03em;transition:color .2s;color:var(--dim)}
.nav-btn.active .nav-lbl{color:var(--accent)}
.nav-btn.active .nav-ico{transform:scale(1.15)}
.nav-btn::before{content:'';position:absolute;top:0;left:25%;right:25%;height:2px;background:var(--accent);border-radius:0 0 2px 2px;transform:scaleX(0);transition:transform .2s}
.nav-btn.active::before{transform:scaleX(1)}
@media(max-width:480px){
  .ch-price{font-size:1.25rem}
  .ch-stats{gap:10px}
  .settings-panel{min-width:220px;max-width:calc(100vw - 24px)}
}
</style>
</head>
<body>
<div class="app">
  <!-- TOP BAR -->
  <div class="topbar">
    <div class="topbar-left">
      <button class="back-btn show" id="back-btn" onclick="goToWatchlist()">← Back</button>
      <span class="pair-label" id="pair-label" style="display:block"></span>
    </div>
    <div class="topbar-right">
      <span id="price-update-indicator" style="font-family:var(--mono);font-size:.6rem;color:var(--dim);display:none">↻ Live</span>
    </div>
  </div>

  <!-- MAIN -->
  <div class="main">

    <!-- PAGE 1: WATCHLIST -->
    <div class="page visible" id="page-watchlist">
      <div class="wl-header">
        <div class="wl-title">📡 Watchlist · 30 Pairs</div>
        <div class="wl-search">
          <span class="search-ico">🔍</span>
          <input type="text" id="search-input" placeholder="Search symbol..." oninput="filterWatchlist(this.value)"/>
        </div>
      </div>
      <div class="wl-list" id="wl-list"></div>
    </div>

    <!-- PAGE 2: CHART -->
    <div class="page hidden-right" id="page-chart">
      <div class="chart-header">
        <div class="ch-row1">
          <span class="ch-sym" id="ch-sym">–</span>
          <span class="ch-badge" id="ch-badge">PERPETUAL</span>
        </div>
        <div class="ch-price-row">
          <span class="ch-price" id="ch-price">–</span>
          <span class="ch-chg" id="ch-chg">–</span>
        </div>
        <div class="ch-countdown-row" id="ch-countdown-row" style="display:none;align-items:center;gap:6px;margin-bottom:4px">
          <span class="ch-stat-lbl">Closes in</span>
          <span id="candle-countdown" style="font-family:var(--mono);font-size:.78rem;font-weight:700;color:#ffc93c;letter-spacing:.05em">00:00</span>
        </div>
        <div class="ch-stats">
          <div class="ch-stat"><span class="ch-stat-lbl">High (1D)</span><span class="ch-stat-val" id="ch-high" style="color:var(--green)">–</span></div>
          <div class="ch-stat"><span class="ch-stat-lbl">Low (1D)</span><span class="ch-stat-val" id="ch-low" style="color:var(--red)">–</span></div>
          <div class="ch-stat"><span class="ch-stat-lbl">Signal</span><span class="ch-stat-val" id="ch-signal-mini">–</span></div>
        </div>
      </div>
      <!-- TIMEFRAME BAR -->
      <div class="tf-bar" id="tf-bar">
        <button class="tf-btn" data-tf="Min1"  onclick="switchTF(this,'Min1')">1m</button>
        <button class="tf-btn" data-tf="Min2"  onclick="switchTF(this,'Min2')">2m</button>
        <button class="tf-btn" data-tf="Min3"  onclick="switchTF(this,'Min3')">3m</button>
        <button class="tf-btn" data-tf="Min4"  onclick="switchTF(this,'Min4')">4m</button>
        <button class="tf-btn active" data-tf="Min5"  onclick="switchTF(this,'Min5')">5m</button>
        <button class="tf-btn" data-tf="Min15" onclick="switchTF(this,'Min15')">15m</button>
        <button class="tf-btn" data-tf="Min30" onclick="switchTF(this,'Min30')">30m</button>
        <button class="tf-btn" data-tf="Min45" onclick="switchTF(this,'Min45')">45m</button>
        <button class="tf-btn" data-tf="Min60" onclick="switchTF(this,'Min60')">1h</button>
        <button class="tf-btn" data-tf="Hour2" onclick="switchTF(this,'Hour2')">2h</button>
        <button class="tf-btn" data-tf="Hour3" onclick="switchTF(this,'Hour3')">3h</button>
        <button class="tf-btn" data-tf="Hour4" onclick="switchTF(this,'Hour4')">4h</button>
        <button class="tf-btn" data-tf="Day1"  onclick="switchTF(this,'Day1')">1D</button>
      </div>
      <!-- DRAWING TOOLBAR -->
      <div class="draw-toolbar" id="draw-toolbar">
        <button class="draw-btn" id="btn-cursor" onclick="setTool('cursor')" title="Cursor / Select">
          ↖ Cursor
        </button>
        <div class="draw-sep"></div>
        <button class="draw-btn" id="btn-rectangle" onclick="setTool('rectangle')" title="Rectangle Tool">
          ▭ Rectangle
        </button>
        <button class="draw-btn" id="btn-trendline" onclick="setTool('trendline')" title="Trendline Tool">
          ╱ Trendline
        </button>
        <div class="draw-sep"></div>
        <button class="draw-btn danger" id="btn-clear" onclick="clearAllDrawings()" title="Clear All Drawings">
          ✕ Clear
        </button>
        <span class="tool-hint" id="tool-hint">Select a tool to draw</span>
      </div>
      <!-- SIGNAL BAR -->
      <div class="signal-bar" id="signal-bar">
        <span class="signal-bar-ico">🎯</span>
        <div class="signal-bar-info">
          <div class="signal-bar-title" id="sb-title">–</div>
          <div class="signal-bar-sub" id="sb-sub">–</div>
        </div>
        <span class="signal-bar-rr" id="sb-rr">–</span>
      </div>
      <!-- CHART WRAP + CANVAS OVERLAY -->
      <div class="chart-wrap" id="chart-wrap">
        <div id="chart-container"></div>
        <canvas id="drawing-canvas"></canvas>
        <div class="chart-loading hidden" id="chart-loading">
          <div class="loading-ring"></div>
          <div class="loading-txt">Loading chart data...</div>
        </div>
        <div class="empty-chart" id="empty-chart" style="display:flex">
          <div class="empty-chart-ico">📈</div>
          <div class="empty-chart-t">Select a pair</div>
          <div class="empty-chart-s">Tap any pair in the Watchlist tab to view its live chart</div>
        </div>
      </div>
    </div>

  </div>

  <!-- BOTTOM NAV -->
  <nav class="bottom-nav">
    <button class="nav-btn active" id="nav-watchlist" onclick="goToWatchlist()">
      <span class="nav-ico">📋</span>
      <span class="nav-lbl">Watchlist</span>
    </button>
    <button class="nav-btn" id="nav-chart" onclick="goToLastChart()">
      <span class="nav-ico">📈</span>
      <span class="nav-lbl">Charts</span>
    </button>
  </nav>

  <!-- SETTINGS PANEL (floating popup) -->
  <div class="settings-panel" id="settings-panel">
    <div class="sp-title">
      <span id="sp-title-txt">Object Settings</span>
      <button class="sp-close" onclick="closeSettingsPanel()">✕</button>
    </div>
    <div id="sp-body"></div>
  </div>

</div>

<script>
(function(){
'use strict';

// ═══════════════════════════════════════════════════════════════════
// SECTION 1 — CONSTANTS & STATE
// ═══════════════════════════════════════════════════════════════════
const PAIRS=[
  "PENGU_USDT","GME_USDT","MEME_USDT","RIVER_USDT","DRIFT_USDT",
  "FARTCOIN_USDT","FLOKI_USDT","BONK_USDT","WIF_USDT","PEPE_USDT",
  "AVAX_USDT","POPCAT_USDT","ONDO_USDT","ARB_USDT","RENDER_USDT",
  "FET_USDT","OPG_USDT","APT_USDT","LINK_USDT","TAO_USDT",
  "INJ_USDT","SEI_USDT","HBAR_USDT","KAS_USDT","NEAR_USDT",
  "SUI_USDT","HYPE_USDT","MAT_USDT","XRP_USDT","BAN_USDT"
];
const TF_LABELS={Min1:"1m",Min2:"2m",Min3:"3m",Min4:"4m",Min5:"5m",Min10:"10m",Min15:"15m",Min30:"30m",Min45:"45m",Min60:"1h",Hour2:"2h",Hour3:"3h",Hour4:"4h",Hour8:"8h",Day1:"1D"};
const MEXC_BASE="https://contract.mexc.com/api/v1/contract";
const TF_SECS={Min1:60,Min2:120,Min3:180,Min4:240,Min5:300,Min10:600,Min15:900,Min30:1800,Min45:2700,Min60:3600,Hour2:7200,Hour3:10800,Hour4:14400,Hour8:28800,Day1:86400};

// Chart state
let prices={};
let signals_map={};
let currentPair=null;
let currentTF="Min5";
let chart=null;
let candleSeries=null;
let priceTimer=null;
let chartTimer=null;
let updateTimer=null;
let countdownInterval=null;

// ═══════════════════════════════════════════════════════════════════
// SECTION 2 — DRAWING ENGINE STATE
// ═══════════════════════════════════════════════════════════════════

// Tool mode: 'cursor' | 'rectangle' | 'trendline'
let activeTool='cursor';

// Storage for all drawn objects
// Rectangle: {id, type:'rect', x1,y1,x2,y2 (price/time coords), color, borderColor, thickness, opacity, locked}
// Trendline: {id, type:'trendline', t1,p1,t2,p2 (time/price), color, thickness, style, ray, extendLeft}
let drawObjects=[];
let nextId=1;

// Interaction state machine
let drawState={
  phase:'idle',         // idle | drawing_first | drawing | dragging | resizing
  objId:null,           // object being interacted with
  handleIdx:null,       // which handle is being dragged
  startX:0,startY:0,   // pointer down position (canvas px)
  lastX:0,lastY:0,
  // For rectangle in-progress:
  rectStart:null,       // {price, time} of first corner
  // For trendline in-progress:
  tlStart:null,         // {price, time} of first point
  shiftDupe:false,      // shift+drag to duplicate trendline
};

let selectedId=null;    // currently selected object id
let hoveredId=null;     // object under cursor
let hoveredHandle=null; // {objId, idx}

// Canvas reference
let canvas=null;
let ctx=null;

// Handle sizes (px, touch-friendly)
const CORNER_R=8;        // corner handle radius
const MIDPT_R=6;         // mid-point handle radius
const HIT_PAD=14;        // extra px for touch hit detection
const LINE_HIT=10;       // px tolerance for line hit

// ═══════════════════════════════════════════════════════════════════
// SECTION 3 — COORDINATE CONVERSION (screen ↔ chart price/time)
// ═══════════════════════════════════════════════════════════════════

/**
 * Convert canvas pixel position to chart coordinates {price, time (unix sec)}.
 * LightweightCharts v3 exposes chart.timeScale() and chart.priceScale() APIs.
 */
function canvasToChart(cx,cy){
  if(!chart)return null;
  try{
    const ts=chart.timeScale();
    const time=ts.coordinateToTime(cx);
    const price=candleSeries.coordinateToPrice(cy);
    return{time,price};
  }catch{return null;}
}

/**
 * Convert chart coordinates {price, time} to canvas pixel position {x, y}.
 */
function chartToCanvas(time,price){
  if(!chart||!candleSeries)return null;
  try{
    const ts=chart.timeScale();
    const x=ts.timeToCoordinate(time);
    const y=candleSeries.priceToCoordinate(price);
    if(x===null||x===undefined||y===null||y===undefined)return null;
    return{x,y};
  }catch{return null;}
}

/**
 * Get canvas pixel position from a pointer/touch event, relative to canvas.
 */
function evPos(e){
  const r=canvas.getBoundingClientRect();
  if(e.touches&&e.touches.length>0){
    return{x:e.touches[0].clientX-r.left,y:e.touches[0].clientY-r.top};
  }
  if(e.changedTouches&&e.changedTouches.length>0){
    return{x:e.changedTouches[0].clientX-r.left,y:e.changedTouches[0].clientY-r.top};
  }
  return{x:e.clientX-r.left,y:e.clientY-r.top};
}

// ═══════════════════════════════════════════════════════════════════
// SECTION 4 — DRAWING ENGINE INIT & RESIZE
// ═══════════════════════════════════════════════════════════════════

function initCanvas(){
  canvas=document.getElementById('drawing-canvas');
  ctx=canvas.getContext('2d');
  resizeCanvas();

  // Mouse events
  canvas.addEventListener('mousedown',onPointerDown,{passive:false});
  canvas.addEventListener('mousemove',onPointerMove,{passive:false});
  canvas.addEventListener('mouseup',onPointerUp,{passive:false});
  canvas.addEventListener('mouseleave',onPointerLeave);
  canvas.addEventListener('dblclick',onDblClick);
  // Touch events
  canvas.addEventListener('touchstart',onPointerDown,{passive:false});
  canvas.addEventListener('touchmove',onPointerMove,{passive:false});
  canvas.addEventListener('touchend',onPointerUp,{passive:false});
  canvas.addEventListener('touchcancel',onPointerLeave);

  // Resize observer — keeps canvas pixel-perfect
  const wrap=document.getElementById('chart-wrap');
  const ro=new ResizeObserver(resizeCanvas);
  ro.observe(wrap);
}

function resizeCanvas(){
  if(!canvas)return;
  const wrap=document.getElementById('chart-wrap');
  const w=wrap.clientWidth||1;
  const h=wrap.clientHeight||1;
  const dpr=window.devicePixelRatio||1;
  canvas.width=w*dpr;
  canvas.height=h*dpr;
  canvas.style.width=w+'px';
  canvas.style.height=h+'px';
  if(ctx){ctx.setTransform(dpr,0,0,dpr,0,0);}
  renderCanvas();
}

// ═══════════════════════════════════════════════════════════════════
// SECTION 5 — TOOL MANAGEMENT
// ═══════════════════════════════════════════════════════════════════

window.setTool=function(tool){
  activeTool=tool;
  // Update button states
  document.querySelectorAll('.draw-btn[id^="btn-"]').forEach(b=>b.classList.remove('active'));
  const btn=document.getElementById('btn-'+tool);
  if(btn)btn.classList.add('active');
  // Cursor: canvas passes events through to chart; drawing tools: canvas captures
  canvas.style.pointerEvents=(tool==='cursor')?'none':'auto';
  canvas.style.cursor=(tool==='cursor')?'default':(tool==='rectangle')?'crosshair':'crosshair';
  // Reset in-progress state
  drawState.phase='idle';
  drawState.rectStart=null;
  drawState.tlStart=null;
  selectedId=null;
  closeSettingsPanel();
  updateToolHint();
  renderCanvas();
};

function updateToolHint(){
  const el=document.getElementById('tool-hint');
  if(!el)return;
  const hints={
    cursor:'Click object to select · Drag to move',
    rectangle:'Tap 1st corner, then 2nd corner',
    trendline:'Tap 1st point, then 2nd point',
  };
  el.textContent=hints[activeTool]||'';
}

// ═══════════════════════════════════════════════════════════════════
// SECTION 6 — HIT DETECTION
// ═══════════════════════════════════════════════════════════════════

/**
 * Returns handles for a rectangle object as canvas px coords.
 * Handle indices:
 *  0=TL  1=TR  2=BR  3=BL  (corners)
 *  4=Top-mid  5=Right-mid  6=Bottom-mid  7=Left-mid  (edge mids)
 *  8=interior (for move)
 */
function getRectHandles(obj){
  const tl=chartToCanvas(obj.t1,obj.p1);
  const br=chartToCanvas(obj.t2,obj.p2);
  if(!tl||!br)return[];
  const L=Math.min(tl.x,br.x),R=Math.max(tl.x,br.x);
  const T=Math.min(tl.y,br.y),B=Math.max(tl.y,br.y);
  const Mx=(L+R)/2,My=(T+B)/2;
  return[
    {x:L,y:T,idx:0,type:'corner'},  // TL
    {x:R,y:T,idx:1,type:'corner'},  // TR
    {x:R,y:B,idx:2,type:'corner'},  // BR
    {x:L,y:B,idx:3,type:'corner'},  // BL
    {x:Mx,y:T,idx:4,type:'mid'},    // Top-mid
    {x:R,y:My,idx:5,type:'mid'},    // Right-mid
    {x:Mx,y:B,idx:6,type:'mid'},    // Bottom-mid
    {x:L,y:My,idx:7,type:'mid'},    // Left-mid
  ];
}

/**
 * Returns handles for a trendline object.
 * Handle indices: 0=p1, 1=p2, 2=body (move entire line)
 */
function getTLHandles(obj){
  const c1=chartToCanvas(obj.t1,obj.p1);
  const c2=chartToCanvas(obj.t2,obj.p2);
  if(!c1||!c2)return[];
  const mx=(c1.x+c2.x)/2,my=(c1.y+c2.y)/2;
  return[
    {x:c1.x,y:c1.y,idx:0,type:'endpoint'},
    {x:c2.x,y:c2.y,idx:1,type:'endpoint'},
    {x:mx,y:my,idx:2,type:'body'},
  ];
}

function distToSegment(px,py,ax,ay,bx,by){
  const dx=bx-ax,dy=by-ay;
  const lenSq=dx*dx+dy*dy;
  if(lenSq===0)return Math.hypot(px-ax,py-ay);
  let t=((px-ax)*dx+(py-ay)*dy)/lenSq;
  t=Math.max(0,Math.min(1,t));
  return Math.hypot(px-(ax+t*dx),py-(ay+t*dy));
}

function hitTestObject(obj,px,py){
  if(obj.type==='rect'){
    const tl=chartToCanvas(obj.t1,obj.p1);
    const br=chartToCanvas(obj.t2,obj.p2);
    if(!tl||!br)return false;
    const L=Math.min(tl.x,br.x)-HIT_PAD/2;
    const R=Math.max(tl.x,br.x)+HIT_PAD/2;
    const T=Math.min(tl.y,br.y)-HIT_PAD/2;
    const B=Math.max(tl.y,br.y)+HIT_PAD/2;
    return px>=L&&px<=R&&py>=T&&py<=B;
  }
  if(obj.type==='trendline'){
    const c1=chartToCanvas(obj.t1,obj.p1);
    const c2=chartToCanvas(obj.t2,obj.p2);
    if(!c1||!c2)return false;
    // For ray mode, extend to canvas edge
    let ex=c2.x,ey=c2.y;
    if(obj.ray){const ext=extendRay(c1,c2);ex=ext.x;ey=ext.y;}
    let el=c1.x,et=c1.y;
    if(obj.extendLeft){const extL=extendLeft(c1,c2);el=extL.x;et=extL.y;}
    return distToSegment(px,py,el,et,ex,ey)<=LINE_HIT+HIT_PAD/2;
  }
  return false;
}

function hitTestHandle(obj,px,py){
  const handles=(obj.type==='rect')?getRectHandles(obj):getTLHandles(obj);
  for(const h of handles){
    const r=(h.type==='corner'||h.type==='endpoint')?CORNER_R+HIT_PAD:MIDPT_R+HIT_PAD;
    if(Math.hypot(px-h.x,py-h.y)<=r)return h;
  }
  return null;
}

function findObjectAt(px,py){
  // Reverse order so top-most (last drawn) selected first
  for(let i=drawObjects.length-1;i>=0;i--){
    if(hitTestObject(drawObjects[i],px,py))return drawObjects[i];
  }
  return null;
}

// ═══════════════════════════════════════════════════════════════════
// SECTION 7 — EVENT HANDLERS
// ═══════════════════════════════════════════════════════════════════

function onPointerDown(e){
  if(activeTool==='cursor'){return;} // pass through
  // Prevent native scroll/chart drag only when a draw tool is active
  e.preventDefault();
  e.stopPropagation();
  const pos=evPos(e);
  const shiftKey=e.shiftKey||false;
  drawState.startX=pos.x;
  drawState.startY=pos.y;
  drawState.lastX=pos.x;
  drawState.lastY=pos.y;

  if(activeTool==='cursor'){
    // handled above
    return;
  }

  // ── CURSOR MODE behaviour is handled when tool='cursor' (canvas pointer-events:none)
  // ── RECTANGLE TOOL ──────────────────────────────────────────────
  if(activeTool==='rectangle'){
    if(drawState.phase==='idle'){
      // Check if clicking on existing object first
      const hit=findObjectAt(pos.x,pos.y);
      if(hit){
        selectedId=hit.id;
        const hh=hitTestHandle(hit,pos.x,pos.y);
        if(hh&&!hit.locked){
          drawState.phase='resizing';
          drawState.objId=hit.id;
          drawState.handleIdx=hh.idx;
        } else {
          drawState.phase='dragging';
          drawState.objId=hit.id;
          showSettingsPanel(hit.id);
        }
        renderCanvas();
        return;
      }
      // Start drawing new rectangle
      const cc=canvasToChart(pos.x,pos.y);
      if(!cc)return;
      drawState.phase='drawing_first';
      drawState.rectStart={t:cc.time,p:cc.price};
      selectedId=null;
      closeSettingsPanel();
    }
    return;
  }

  // ── TRENDLINE TOOL ───────────────────────────────────────────────
  if(activeTool==='trendline'){
    if(drawState.phase==='idle'){
      const hit=findObjectAt(pos.x,pos.y);
      if(hit){
        selectedId=hit.id;
        const hh=hitTestHandle(hit,pos.x,pos.y);
        if(hh&&!hit.locked){
          if(shiftKey&&hh.idx===2){
            // Shift+drag body = duplicate
            drawState.shiftDupe=true;
            const orig=drawObjects.find(o=>o.id===hit.id);
            const dup={...orig,id:nextId++,locked:false};
            drawObjects.push(dup);
            selectedId=dup.id;
            drawState.phase='dragging';
            drawState.objId=dup.id;
          } else {
            drawState.phase=(hh.idx===2)?'dragging':'resizing';
            drawState.objId=hit.id;
            drawState.handleIdx=hh.idx;
          }
        } else {
          drawState.phase='dragging';
          drawState.objId=hit.id;
          showSettingsPanel(hit.id);
        }
        renderCanvas();
        return;
      }
      // Start drawing new trendline
      const cc=canvasToChart(pos.x,pos.y);
      if(!cc)return;
      drawState.phase='drawing_first';
      drawState.tlStart={t:cc.time,p:cc.price};
      selectedId=null;
      closeSettingsPanel();
    }
    return;
  }
}

function onPointerMove(e){
  if(activeTool==='cursor')return;
  e.preventDefault();
  const pos=evPos(e);
  const dx=pos.x-drawState.lastX;
  const dy=pos.y-drawState.lastY;
  drawState.lastX=pos.x;
  drawState.lastY=pos.y;

  // ── Hover detection (cursor mode or tool idle) ───────────────────
  if(drawState.phase==='idle'){
    const hit=findObjectAt(pos.x,pos.y);
    hoveredId=hit?hit.id:null;
    if(hit){
      const hh=hitTestHandle(hit,pos.x,pos.y);
      hoveredHandle=hh?{objId:hit.id,idx:hh.idx}:null;
      canvas.style.cursor=hh?getCursorForHandle(hit,hh):'move';
    } else {
      hoveredHandle=null;
      canvas.style.cursor='crosshair';
    }
    renderCanvas();
    return;
  }

  // ── Drawing in progress ──────────────────────────────────────────
  if(drawState.phase==='drawing_first'||drawState.phase==='drawing'){
    drawState.phase='drawing';
    renderCanvas(); // live preview
    return;
  }

  // ── Dragging (moving whole object) ──────────────────────────────
  if(drawState.phase==='dragging'){
    const obj=drawObjects.find(o=>o.id===drawState.objId);
    if(!obj||obj.locked)return;
    // Convert dx,dy screen pixels to price/time delta
    const c1=chartToCanvas(obj.t1,obj.p1);
    const c2=chartToCanvas(obj.t2,obj.p2);
    if(!c1||!c2)return;
    const nc1=canvasToChart(c1.x+dx,c1.y+dy);
    const nc2=canvasToChart(c2.x+dx,c2.y+dy);
    if(!nc1||!nc2)return;
    obj.t1=nc1.time;obj.p1=nc1.price;
    obj.t2=nc2.time;obj.p2=nc2.price;
    renderCanvas();
    updateSettingsPanel(obj.id);
    return;
  }

  // ── Resizing via handle ──────────────────────────────────────────
  if(drawState.phase==='resizing'){
    const obj=drawObjects.find(o=>o.id===drawState.objId);
    if(!obj||obj.locked)return;
    const cc=canvasToChart(pos.x,pos.y);
    if(!cc)return;
    const idx=drawState.handleIdx;

    if(obj.type==='rect'){
      // Update the correct corner(s) based on handle index
      if(idx===0){obj.t1=cc.time;obj.p1=cc.price;}        // TL
      else if(idx===1){obj.t2=cc.time;obj.p1=cc.price;}   // TR: t2,p1
      else if(idx===2){obj.t2=cc.time;obj.p2=cc.price;}   // BR
      else if(idx===3){obj.t1=cc.time;obj.p2=cc.price;}   // BL: t1,p2
      else if(idx===4){obj.p1=cc.price;}                   // Top-mid: price1
      else if(idx===5){obj.t2=cc.time;}                    // Right-mid: time2
      else if(idx===6){obj.p2=cc.price;}                   // Bottom-mid: price2
      else if(idx===7){obj.t1=cc.time;}                    // Left-mid: time1
    }

    if(obj.type==='trendline'){
      if(idx===0){obj.t1=cc.time;obj.p1=cc.price;}        // endpoint 1
      else if(idx===1){obj.t2=cc.time;obj.p2=cc.price;}   // endpoint 2
    }

    renderCanvas();
    updateSettingsPanel(obj.id);
    return;
  }

  renderCanvas();
}

function onPointerUp(e){
  if(activeTool==='cursor')return;
  e.preventDefault();
  const pos=evPos(e);
  const phase=drawState.phase;
  const cc=canvasToChart(pos.x,pos.y);

  // ── Finish drawing rectangle ────────────────────────────────────
  if(activeTool==='rectangle'&&(phase==='drawing_first'||phase==='drawing')){
    if(!drawState.rectStart||!cc){drawState.phase='idle';renderCanvas();return;}
    const rs=drawState.rectStart;
    // Require minimum size (avoid accidental taps)
    const dp1=chartToCanvas(rs.t,rs.p);
    const dp2=chartToCanvas(cc.time,cc.price);
    const tooSmall=dp1&&dp2&&Math.abs(dp2.x-dp1.x)<5&&Math.abs(dp2.y-dp1.y)<5;
    if(!tooSmall){
      const obj={id:nextId++,type:'rect',t1:rs.t,p1:rs.p,t2:cc.time,p2:cc.price,
        color:'rgba(79,110,247,0.12)',borderColor:'#4f6ef7',thickness:1.5,opacity:0.12,locked:false};
      drawObjects.push(obj);
      selectedId=obj.id;
      showSettingsPanel(obj.id);
    }
    drawState.phase='idle';drawState.rectStart=null;
    saveDrawings();renderCanvas();return;
  }

  // ── Finish drawing trendline ────────────────────────────────────
  if(activeTool==='trendline'&&(phase==='drawing_first'||phase==='drawing')){
    if(!drawState.tlStart||!cc){drawState.phase='idle';renderCanvas();return;}
    const ts=drawState.tlStart;
    const dp1=chartToCanvas(ts.t,ts.p);
    const dp2=chartToCanvas(cc.time,cc.price);
    const tooSmall=dp1&&dp2&&Math.abs(dp2.x-dp1.x)<5&&Math.abs(dp2.y-dp1.y)<5;
    if(!tooSmall){
      const obj={id:nextId++,type:'trendline',t1:ts.t,p1:ts.p,t2:cc.time,p2:cc.price,
        color:'#4f6ef7',thickness:1.5,style:'solid',ray:false,extendLeft:false,locked:false};
      drawObjects.push(obj);
      selectedId=obj.id;
      showSettingsPanel(obj.id);
    }
    drawState.phase='idle';drawState.tlStart=null;
    saveDrawings();renderCanvas();return;
  }

  // ── End drag or resize ──────────────────────────────────────────
  if(phase==='dragging'||phase==='resizing'){
    drawState.phase='idle';
    drawState.objId=null;drawState.handleIdx=null;
    drawState.shiftDupe=false;
    saveDrawings();renderCanvas();return;
  }

  // ── Tap on empty area (idle) = deselect ─────────────────────────
  if(phase==='idle'){
    const hit=findObjectAt(pos.x,pos.y);
    if(hit){selectedId=hit.id;showSettingsPanel(hit.id);}
    else{selectedId=null;closeSettingsPanel();}
    renderCanvas();
  }
}

function onPointerLeave(){
  if(drawState.phase==='dragging'||drawState.phase==='resizing'){
    drawState.phase='idle';drawState.objId=null;
    saveDrawings();renderCanvas();
  }
  hoveredId=null;hoveredHandle=null;
  renderCanvas();
}

function onDblClick(e){
  // Double-click: deselect and close panel
  selectedId=null;closeSettingsPanel();renderCanvas();
}

function getCursorForHandle(obj,h){
  if(obj.type==='rect'){
    if(h.idx===0||h.idx===2)return'nwse-resize';
    if(h.idx===1||h.idx===3)return'nesw-resize';
    if(h.idx===4||h.idx===6)return'ns-resize';
    if(h.idx===5||h.idx===7)return'ew-resize';
  }
  if(obj.type==='trendline'){
    if(h.idx===2)return'move';
    return'crosshair';
  }
  return'move';
}

// ═══════════════════════════════════════════════════════════════════
// SECTION 8 — RENDERING ENGINE
// ═══════════════════════════════════════════════════════════════════

// Extend a ray from c1 through c2 to the canvas edge
function extendRay(c1,c2){
  const W=canvas.clientWidth,H=canvas.clientHeight;
  const dx=c2.x-c1.x,dy=c2.y-c1.y;
  if(dx===0&&dy===0)return c2;
  let t=Infinity;
  if(dx>0)t=Math.min(t,(W-c1.x)/dx);
  else if(dx<0)t=Math.min(t,-c1.x/dx);
  if(dy>0)t=Math.min(t,(H-c1.y)/dy);
  else if(dy<0)t=Math.min(t,-c1.y/dy);
  return{x:c1.x+dx*t,y:c1.y+dy*t};
}

function extendLeft(c1,c2){
  const dx=c2.x-c1.x,dy=c2.y-c1.y;
  if(dx===0&&dy===0)return c1;
  let t=Infinity;
  const W=canvas.clientWidth,H=canvas.clientHeight;
  if(dx<0)t=Math.min(t,(W-c1.x)/(-dx));
  else if(dx>0)t=Math.min(t,c1.x/dx);
  if(dy<0)t=Math.min(t,(H-c1.y)/(-dy));
  else if(dy>0)t=Math.min(t,c1.y/dy);
  return{x:c1.x-dx*t,y:c1.y-dy*t};
}

function applyLineStyle(ctx,style,thickness){
  ctx.lineWidth=thickness;
  if(style==='dashed'){ctx.setLineDash([8,5]);}
  else if(style==='dotted'){ctx.setLineDash([2,4]);}
  else{ctx.setLineDash([]);}
}

function drawRectObject(obj,isSelected,isHovered){
  const tl=chartToCanvas(obj.t1,obj.p1);
  const br=chartToCanvas(obj.t2,obj.p2);
  if(!tl||!br)return;
  const L=Math.min(tl.x,br.x),R=Math.max(tl.x,br.x);
  const T=Math.min(tl.y,br.y),B=Math.max(tl.y,br.y);
  const W=R-L,H=B-T;

  // Fill
  ctx.save();
  ctx.globalAlpha=obj.opacity;
  ctx.fillStyle=obj.borderColor||'#4f6ef7';
  ctx.fillRect(L,T,W,H);
  ctx.restore();

  // Border
  ctx.save();
  ctx.strokeStyle=isSelected?'#fff':(isHovered?lightenColor(obj.borderColor):obj.borderColor);
  ctx.lineWidth=(isSelected?obj.thickness+1:obj.thickness);
  ctx.setLineDash([]);
  ctx.strokeRect(L,T,W,H);
  ctx.restore();

  // Price labels on left edge
  const priceTop=Math.max(obj.p1,obj.p2);
  const priceBot=Math.min(obj.p1,obj.p2);
  ctx.save();
  ctx.font='bold 10px JetBrains Mono,monospace';
  ctx.fillStyle=obj.borderColor||'#4f6ef7';
  ctx.textAlign='right';
  ctx.fillText(formatPrice(priceTop),L-4,T+4);
  ctx.fillText(formatPrice(priceBot),L-4,B);
  ctx.restore();

  // Handles (only when selected)
  if(isSelected){
    const handles=getRectHandles(obj);
    handles.forEach(h=>{
      const isActive=hoveredHandle&&hoveredHandle.objId===obj.id&&hoveredHandle.idx===h.idx;
      const r=(h.type==='corner')?CORNER_R:MIDPT_R;
      ctx.beginPath();
      ctx.arc(h.x,h.y,r,0,Math.PI*2);
      ctx.fillStyle=isActive?'#fff':'#1a1f3a';
      ctx.fill();
      ctx.strokeStyle=obj.borderColor||'#4f6ef7';
      ctx.lineWidth=2;
      ctx.setLineDash([]);
      ctx.stroke();
    });
  }
}

function drawTrendlineObject(obj,isSelected,isHovered){
  const c1=chartToCanvas(obj.t1,obj.p1);
  const c2=chartToCanvas(obj.t2,obj.p2);
  if(!c1||!c2)return;

  let startPt=c1;
  let endPt=c2;
  if(obj.extendLeft)startPt=extendLeft(c1,c2);
  if(obj.ray)endPt=extendRay(c1,c2);

  // Main line
  ctx.save();
  ctx.strokeStyle=isSelected?lightenColor(obj.color):obj.color;
  applyLineStyle(ctx,obj.style||'solid',obj.thickness||(isSelected?obj.thickness+1:obj.thickness));
  ctx.lineWidth=(isSelected?obj.thickness+0.5:obj.thickness);
  ctx.beginPath();
  ctx.moveTo(startPt.x,startPt.y);
  ctx.lineTo(endPt.x,endPt.y);
  ctx.stroke();
  ctx.restore();

  // Angle label (when selected)
  if(isSelected){
    const dx=c2.x-c1.x,dy=c2.y-c1.y;
    const angleDeg=Math.atan2(-dy,dx)*180/Math.PI; // negative dy because canvas Y is flipped
    const mid={(c1.x+c2.x)/2,(c1.y+c2.y)/2};
    ctx.save();
    ctx.font='bold 10px JetBrains Mono,monospace';
    ctx.fillStyle=obj.color;
    ctx.textAlign='center';
    ctx.fillText(angleDeg.toFixed(1)+'°',(c1.x+c2.x)/2,(c1.y+c2.y)/2-10);
    ctx.restore();
  }

  // Price labels at endpoints
  ctx.save();
  ctx.font='10px JetBrains Mono,monospace';
  ctx.fillStyle=obj.color;
  ctx.textAlign='left';
  ctx.fillText(formatPrice(obj.p1),c1.x+5,c1.y-5);
  ctx.fillText(formatPrice(obj.p2),c2.x+5,c2.y-5);
  ctx.restore();

  // Handles (selected only)
  if(isSelected){
    const handles=getTLHandles(obj);
    handles.forEach(h=>{
      const isActive=hoveredHandle&&hoveredHandle.objId===obj.id&&hoveredHandle.idx===h.idx;
      const r=(h.idx===2)?MIDPT_R:CORNER_R;
      ctx.beginPath();
      ctx.arc(h.x,h.y,r,0,Math.PI*2);
      ctx.fillStyle=isActive?'#fff':'#1a1f3a';
      ctx.fill();
      ctx.strokeStyle=obj.color;
      ctx.lineWidth=2;
      ctx.setLineDash([]);
      ctx.stroke();
    });
  }
}

function drawPreview(pos){
  if(!pos)return;
  const cc=canvasToChart(pos.x,pos.y);
  if(!cc)return;
  ctx.save();
  ctx.setLineDash([5,4]);

  if(activeTool==='rectangle'&&drawState.rectStart){
    const rs=drawState.rectStart;
    const sp=chartToCanvas(rs.t,rs.p);
    if(!sp)return;
    ctx.strokeStyle='#4f6ef7';
    ctx.fillStyle='rgba(79,110,247,0.07)';
    ctx.lineWidth=1.5;
    const L=Math.min(sp.x,pos.x),R=Math.max(sp.x,pos.x);
    const T=Math.min(sp.y,pos.y),B=Math.max(sp.y,pos.y);
    ctx.fillRect(L,T,R-L,B-T);
    ctx.strokeRect(L,T,R-L,B-T);
    // Cross-hair at cursor
    ctx.strokeStyle='rgba(79,110,247,0.4)';
    ctx.lineWidth=0.5;
    ctx.beginPath();ctx.moveTo(pos.x,0);ctx.lineTo(pos.x,canvas.clientHeight);ctx.stroke();
    ctx.beginPath();ctx.moveTo(0,pos.y);ctx.lineTo(canvas.clientWidth,pos.y);ctx.stroke();
  }

  if(activeTool==='trendline'&&drawState.tlStart){
    const ts=drawState.tlStart;
    const sp=chartToCanvas(ts.t,ts.p);
    if(!sp)return;
    ctx.strokeStyle='#4f6ef7';
    ctx.lineWidth=1.5;
    ctx.beginPath();ctx.moveTo(sp.x,sp.y);ctx.lineTo(pos.x,pos.y);ctx.stroke();
    // Angle preview
    const dx=pos.x-sp.x,dy=sp.y-pos.y;
    const a=(Math.atan2(dy,dx)*180/Math.PI).toFixed(1);
    ctx.setLineDash([]);
    ctx.font='bold 10px JetBrains Mono,monospace';
    ctx.fillStyle='#4f6ef7';
    ctx.textAlign='left';
    ctx.fillText(a+'°',(sp.x+pos.x)/2+5,(sp.y+pos.y)/2-5);
  }
  ctx.restore();
}

/**
 * Main canvas render loop — called after any state change or pan/zoom.
 */
let _lastPos=null;
function renderCanvas(){
  if(!ctx||!canvas)return;
  const W=canvas.clientWidth,H=canvas.clientHeight;
  ctx.clearRect(0,0,W,H);

  // Draw all persisted objects
  drawObjects.forEach(obj=>{
    const isSel=obj.id===selectedId;
    const isHov=obj.id===hoveredId&&!isSel;
    if(obj.type==='rect')drawRectObject(obj,isSel,isHov);
    else if(obj.type==='trendline')drawTrendlineObject(obj,isSel,isHov);
  });

  // In-progress preview
  if(drawState.phase==='drawing'){
    drawPreview(_lastPos);
  }
}

// Capture mouse position for live preview during drawing phase
function onPointerMovePreview(e){
  _lastPos=evPos(e);
  if(drawState.phase==='drawing')renderCanvas();
}

// ═══════════════════════════════════════════════════════════════════
// SECTION 9 — SETTINGS PANEL
// ═══════════════════════════════════════════════════════════════════

function showSettingsPanel(objId){
  const obj=drawObjects.find(o=>o.id===objId);
  if(!obj)return;
  const panel=document.getElementById('settings-panel');
  const body=document.getElementById('sp-body');
  const title=document.getElementById('sp-title-txt');

  if(obj.type==='rect'){
    title.textContent='▭ Rectangle';
    body.innerHTML=buildRectPanel(obj);
  } else if(obj.type==='trendline'){
    title.textContent='╱ Trendline';
    body.innerHTML=buildTLPanel(obj);
  }

  panel.classList.add('show');
  // Position panel: top-left area, avoid overlapping toolbar
  const wrap=document.getElementById('chart-wrap');
  const wr=wrap.getBoundingClientRect();
  panel.style.left=(wr.left+10)+'px';
  panel.style.top=(wr.top+10)+'px';
}

function buildRectPanel(obj){
  const tl=chartToCanvas(obj.t1,obj.p1);
  const br=chartToCanvas(obj.t2,obj.p2);
  const wPx=tl&&br?Math.abs(br.x-tl.x).toFixed(0):'-';
  const hPx=tl&&br?Math.abs(br.y-tl.y).toFixed(0):'-';
  const pTop=Math.max(obj.p1,obj.p2);
  const pBot=Math.min(obj.p1,obj.p2);
  const pRange=(pTop-pBot);
  return`
  <div class="sp-row"><span class="sp-lbl">Fill Color</span>
    <input type="color" class="sp-color" id="sp-fillclr" value="${colorToHex(obj.borderColor)}"
      oninput="updateObjProp(${obj.id},'borderColor',this.value);updateObjProp(${obj.id},'color',this.value)"/>
  </div>
  <div class="sp-row"><span class="sp-lbl">Opacity</span>
    <input type="range" min="0" max="1" step="0.05" value="${obj.opacity}" style="width:90px;accent-color:var(--accent)"
      oninput="updateObjProp(${obj.id},'opacity',parseFloat(this.value));document.getElementById('sp-opval-${obj.id}').textContent=Math.round(this.value*100)+'%'"/>
    <span class="sp-val" id="sp-opval-${obj.id}">${Math.round(obj.opacity*100)}%</span>
  </div>
  <div class="sp-row"><span class="sp-lbl">Border Thickness</span>
    <input type="number" class="sp-inp" min="0.5" max="8" step="0.5" value="${obj.thickness}"
      oninput="updateObjProp(${obj.id},'thickness',parseFloat(this.value)||1)"/>
  </div>
  <hr class="sp-divider"/>
  <div class="sp-info-grid">
    <span>Top Price</span><span>${formatPrice(pTop)}</span>
    <span>Bottom Price</span><span>${formatPrice(pBot)}</span>
    <span>Price Range</span><span>${formatPrice(pRange)}</span>
    <span>Width (px)</span><span>${wPx}</span>
    <span>Height (px)</span><span>${hPx}</span>
  </div>
  <hr class="sp-divider"/>
  <div class="sp-row">
    <label class="sp-toggle"><input type="checkbox" ${obj.locked?'checked':''} onchange="updateObjProp(${obj.id},'locked',this.checked)"/> <span class="sp-lbl">Locked</span></label>
    <button class="sp-btn danger" onclick="deleteObject(${obj.id})">Delete</button>
  </div>`;
}

function buildTLPanel(obj){
  const dx=obj.t2-obj.t1;
  const dy=obj.p2-obj.p1;
  const c1=chartToCanvas(obj.t1,obj.p1);
  const c2=chartToCanvas(obj.t2,obj.p2);
  let angle=0;
  if(c1&&c2){angle=Math.atan2(-(c2.y-c1.y),c2.x-c1.x)*180/Math.PI;}
  return`
  <div class="sp-row"><span class="sp-lbl">Color</span>
    <input type="color" class="sp-color" value="${colorToHex(obj.color)}"
      oninput="updateObjProp(${obj.id},'color',this.value)"/>
  </div>
  <div class="sp-row"><span class="sp-lbl">Thickness</span>
    <input type="number" class="sp-inp" min="0.5" max="8" step="0.5" value="${obj.thickness}"
      oninput="updateObjProp(${obj.id},'thickness',parseFloat(this.value)||1)"/>
  </div>
  <div class="sp-row"><span class="sp-lbl">Style</span>
    <select class="sp-select" onchange="updateObjProp(${obj.id},'style',this.value)">
      <option value="solid" ${obj.style==='solid'?'selected':''}>Solid</option>
      <option value="dashed" ${obj.style==='dashed'?'selected':''}>Dashed</option>
      <option value="dotted" ${obj.style==='dotted'?'selected':''}>Dotted</option>
    </select>
  </div>
  <hr class="sp-divider"/>
  <div class="sp-row">
    <label class="sp-toggle"><input type="checkbox" ${obj.ray?'checked':''} onchange="updateObjProp(${obj.id},'ray',this.checked)"/> <span class="sp-lbl">Extend Right (Ray)</span></label>
  </div>
  <div class="sp-row">
    <label class="sp-toggle"><input type="checkbox" ${obj.extendLeft?'checked':''} onchange="updateObjProp(${obj.id},'extendLeft',this.checked)"/> <span class="sp-lbl">Extend Left</span></label>
  </div>
  <hr class="sp-divider"/>
  <div class="sp-info-grid">
    <span>Angle</span><span>${angle.toFixed(2)}°</span>
    <span>Price 1</span><span>${formatPrice(obj.p1)}</span>
    <span>Price 2</span><span>${formatPrice(obj.p2)}</span>
  </div>
  <hr class="sp-divider"/>
  <div class="sp-row">
    <label class="sp-toggle"><input type="checkbox" ${obj.locked?'checked':''} onchange="updateObjProp(${obj.id},'locked',this.checked)"/> <span class="sp-lbl">Locked</span></label>
    <button class="sp-btn danger" onclick="deleteObject(${obj.id})">Delete</button>
  </div>`;
}

function updateSettingsPanel(objId){
  const panel=document.getElementById('settings-panel');
  if(!panel.classList.contains('show'))return;
  if(objId!==selectedId)return;
  showSettingsPanel(objId);
}

window.updateObjProp=function(id,prop,val){
  const obj=drawObjects.find(o=>o.id===id);
  if(!obj)return;
  obj[prop]=val;
  renderCanvas();
  saveDrawings();
};

window.deleteObject=function(id){
  drawObjects=drawObjects.filter(o=>o.id!==id);
  selectedId=null;
  closeSettingsPanel();
  renderCanvas();
  saveDrawings();
};

window.closeSettingsPanel=function(){
  document.getElementById('settings-panel').classList.remove('show');
};

window.clearAllDrawings=function(){
  if(drawObjects.length===0)return;
  if(!confirm('Clear all drawings?'))return;
  drawObjects=[];selectedId=null;
  closeSettingsPanel();renderCanvas();saveDrawings();
};

// ═══════════════════════════════════════════════════════════════════
// SECTION 10 — PERSISTENCE (localStorage per pair+TF)
// ═══════════════════════════════════════════════════════════════════

function drawingsKey(){return'sc_draw_'+(currentPair||'')+'_'+currentTF;}

function saveDrawings(){
  try{localStorage.setItem(drawingsKey(),JSON.stringify(drawObjects));}catch{}
}

function loadDrawings(){
  try{
    const raw=localStorage.getItem(drawingsKey());
    if(!raw){drawObjects=[];return;}
    const arr=JSON.parse(raw);
    drawObjects=Array.isArray(arr)?arr:[];
    // Restore nextId to avoid collisions
    drawObjects.forEach(o=>{if(o.id>=nextId)nextId=o.id+1;});
  }catch{drawObjects=[];}
}

// ═══════════════════════════════════════════════════════════════════
// SECTION 11 — UTILITY HELPERS
// ═══════════════════════════════════════════════════════════════════

function formatPrice(v){
  const n=Number(v);
  if(!n&&n!==0)return'–';
  if(n>=10000)return n.toLocaleString('en',{maximumFractionDigits:2});
  if(n>=100)return n.toFixed(2);
  if(n>=1)return n.toFixed(4);
  if(n>=0.001)return n.toFixed(6);
  return n.toFixed(8);
}

function colorToHex(color){
  if(!color)return'#4f6ef7';
  if(color.startsWith('#'))return color.slice(0,7);
  // Try to parse rgb/rgba
  const m=color.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/);
  if(m){
    return '#'+[m[1],m[2],m[3]].map(x=>parseInt(x).toString(16).padStart(2,'0')).join('');
  }
  return'#4f6ef7';
}

function lightenColor(hex){
  const h=colorToHex(hex).replace('#','');
  const r=parseInt(h.slice(0,2),16);
  const g=parseInt(h.slice(2,4),16);
  const b=parseInt(h.slice(4,6),16);
  const blend=(v)=>Math.min(255,v+60).toString(16).padStart(2,'0');
  return'#'+blend(r)+blend(g)+blend(b);
}

// ═══════════════════════════════════════════════════════════════════
// SECTION 12 — CHART COORDINATE HOOKS (re-render on pan/zoom)
// ═══════════════════════════════════════════════════════════════════

/**
 * LightweightCharts fires no explicit pan/zoom event, but we can subscribe
 * to the visible logical range change to re-render the drawing canvas.
 */
function attachChartHooks(){
  if(!chart)return;
  chart.timeScale().subscribeVisibleLogicalRangeChange(()=>{
    renderCanvas();
  });
  // Also re-render on crosshair move (covers most user interaction)
  chart.subscribeCrosshairMove(()=>{
    if(drawState.phase==='drawing')return; // preview handled by pointer move
    renderCanvas();
  });
}

// ═══════════════════════════════════════════════════════════════════
// SECTION 13 — ORIGINAL CHART CODE (unchanged logic, augmented)
// ═══════════════════════════════════════════════════════════════════

function getPriceFormat(price){
  const p=Number(price)||0;
  if(p>=10000)return{type:'price',precision:2,minMove:0.01};
  if(p>=1000) return{type:'price',precision:2,minMove:0.01};
  if(p>=100)  return{type:'price',precision:2,minMove:0.01};
  if(p>=10)   return{type:'price',precision:4,minMove:0.0001};
  if(p>=1)    return{type:'price',precision:4,minMove:0.0001};
  if(p>=0.1)  return{type:'price',precision:5,minMove:0.00001};
  if(p>=0.01) return{type:'price',precision:6,minMove:0.000001};
  if(p>=0.001)return{type:'price',precision:7,minMove:0.0000001};
  return{type:'price',precision:8,minMove:0.00000001};
}

function updateCountdown(){
  const secs=TF_SECS[currentTF];
  const row=document.getElementById("ch-countdown-row");
  if(!secs||!currentPair){if(row)row.style.display="none";return;}
  const now=Math.floor(Date.now()/1000);
  const elapsed=now%secs;
  const remaining=secs-elapsed;
  const el=document.getElementById("candle-countdown");
  let txt;
  const isLongTF=secs>=7200;
  if(isLongTF&&remaining>=60){
    const totalMins=Math.floor(remaining/60);
    if(totalMins>=60){const h=Math.floor(totalMins/60);const m=totalMins%60;txt=h+'h '+String(m).padStart(2,'0')+'m';}
    else{const m=totalMins;const s=remaining%60;txt=String(m).padStart(2,'0')+':'+String(s).padStart(2,'0');}
  }else{const m=Math.floor(remaining/60);const s=remaining%60;txt=String(m).padStart(2,'0')+':'+String(s).padStart(2,'0');}
  if(el)el.textContent=txt;
  if(row)row.style.display="flex";
  if(el){el.style.color=remaining<=60?"var(--green)":"#ffc93c";}
}
function startCountdown(){stopCountdown();updateCountdown();countdownInterval=setInterval(updateCountdown,1000);}
function stopCountdown(){if(countdownInterval){clearInterval(countdownInterval);countdownInterval=null;}const row=document.getElementById("ch-countdown-row");if(row)row.style.display="none";}

function saveLastPair(sym,tf){try{localStorage.setItem("sc_last_pair",sym);localStorage.setItem("sc_last_tf",tf||"Min5");}catch{}}
function loadLastPair(){try{return{pair:localStorage.getItem("sc_last_pair"),tf:localStorage.getItem("sc_last_tf")||"Min5"};}catch{return{pair:null,tf:"Min5"};}}

function goToWatchlist(){
  document.getElementById("page-watchlist").className="page visible";
  document.getElementById("page-chart").className="page hidden-right";
  document.getElementById("nav-watchlist").classList.add("active");
  document.getElementById("nav-chart").classList.remove("active");
  document.getElementById("back-btn").classList.remove("show");
  document.getElementById("pair-label").style.display="none";
  stopChartUpdate();
  closeSettingsPanel();
}
window.goToWatchlist=goToWatchlist;

async function goToChart(sym,tf){
  document.getElementById("page-watchlist").className="page hidden-left";
  document.getElementById("page-chart").className="page visible";
  document.getElementById("pair-label").textContent=(sym||"").replace("_USDT","")+"/USDT";
  document.getElementById("pair-label").style.display="block";
  document.getElementById("back-btn").classList.add("show");
  document.getElementById("nav-chart").classList.add("active");
  document.getElementById("nav-watchlist").classList.remove("active");
  await loadChart(sym,tf||currentTF);
  saveLastPair(sym,tf||currentTF);
}

function goToLastChart(){
  const{pair,tf}=loadLastPair();
  if(pair){goToChart(pair,tf);}
  else{
    document.getElementById("page-watchlist").className="page hidden-left";
    document.getElementById("page-chart").className="page visible";
    document.getElementById("nav-chart").classList.add("active");
    document.getElementById("nav-watchlist").classList.remove("active");
    document.getElementById("back-btn").classList.add("show");
    document.getElementById("empty-chart").style.display="flex";
    document.getElementById("chart-loading").classList.add("hidden");
  }
}
window.goToLastChart=goToLastChart;

async function fetchPrices(){
  try{
    const r=await fetch("/api/prices");const data=await r.json();prices=data;
    renderWatchlist();updateChartHeader();
    const ind=document.getElementById("price-update-indicator");
    if(ind){ind.style.display="inline";ind.style.color="var(--green)";setTimeout(()=>{ind.style.color="var(--dim)";},800);}
  }catch{}
}
async function fetchSignals(){
  try{
    const r=await fetch("/api/signals?limit=200");const arr=await r.json();
    signals_map={};arr.forEach(s=>{if(!signals_map[s.symbol])signals_map[s.symbol]=s;});
    renderWatchlist();
  }catch{}
}

const PAIR_COLORS=[
  ["#4f6ef7","rgba(79,110,247,.15)"],["#7c3aed","rgba(124,58,237,.15)"],
  ["#00d4aa","rgba(0,212,170,.15)"],["#ff4d6a","rgba(255,77,106,.15)"],
  ["#ffc93c","rgba(255,201,60,.15)"],["#06b6d4","rgba(6,182,212,.15)"],
  ["#f97316","rgba(249,115,22,.15)"],["#a78bfa","rgba(167,139,250,.15)"],
  ["#10b981","rgba(16,185,129,.15)"],["#ec4899","rgba(236,72,153,.15)"],
];
function pairColor(sym){let h=0;for(let c of sym){h=(h*31+c.charCodeAt(0))&0xffff;}return PAIR_COLORS[h%PAIR_COLORS.length];}
function pairEmoji(sym){const base=sym.replace("_USDT","");const map={BTC:"₿",ETH:"Ξ",SOL:"◎",BNB:"⬡",AVAX:"🏔",LINK:"🔗",INJ:"💉",SUI:"💧",NEAR:"🔮",KAS:"💎",XRP:"✦",ARB:"🔵",HBAR:"ℏ",SEI:"🌊",APT:"⚡",TAO:"τ",RENDER:"🎨",FET:"🤖"};return map[base]||base.slice(0,2).toUpperCase();}
function fmtPrice(v){if(!v&&v!==0)return"–";const n=Number(v);if(n>=1000)return"$"+n.toLocaleString("en",{maximumFractionDigits:2});if(n>=100)return"$"+n.toFixed(2);if(n>=1)return"$"+n.toFixed(4);if(n>=0.01)return"$"+n.toFixed(5);if(n>=0.0001)return"$"+n.toFixed(6);return"$"+n.toFixed(8);}

let searchFilter="";
window.filterWatchlist=function(v){searchFilter=v.trim().toUpperCase();renderWatchlist();}

function renderWatchlist(){
  const list=document.getElementById("wl-list");
  const filtered=PAIRS.filter(sym=>!searchFilter||sym.includes(searchFilter)||sym.replace("_USDT","").includes(searchFilter));
  list.innerHTML=filtered.map(sym=>{
    const d=prices[sym]||{};const sig=signals_map[sym];
    const[clr,bg]=pairColor(sym);const base=sym.replace("_USDT","");
    const chg=Number(d.change||0);const up=chg>0,dn=chg<0;const chgCls=up?"up":dn?"dn":"flat";
    const chgTxt=(up?"+":"")+chg.toFixed(2)+"%";const hasPrice=d.price&&d.price>0;
    const sigDir=sig?sig.direction:"";const isBuy=sigDir==="BUY";
    const sigClasses=sig?" has-signal "+(isBuy?"buy-sig":"sell-sig"):"";
    const badgeColor=isBuy?"var(--green)":"var(--red)";
    const priceColor=hasPrice?(up?"var(--green)":dn?"var(--red)":"var(--text)"):"var(--dim)";
    return`<div class="wl-row${sigClasses}" onclick="openPair('${sym}')">
      <div class="wl-info">
        <div class="wl-sym">${base}<span>/USDT</span>${sig?`<span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:${badgeColor};margin-left:6px;vertical-align:middle;animation:blink 1.5s infinite"></span>`:""}</div>
        <div class="wl-base">MEXC · PERP${sig?` · <span style="color:${badgeColor};font-weight:700">${isBuy?"🟢 LONG":"🔴 SHORT"} ${sig.grade||""}</span>`:""}</div>
      </div>
      <div class="wl-right">
        <div class="wl-price" style="color:${priceColor}">${hasPrice?fmtPrice(d.price):"–"}</div>
        <span class="wl-chg ${chgCls}">${hasPrice?chgTxt:"–"}</span>
      </div>
    </div>`;
  }).join("");
}
window.openPair=function(sym){goToChart(sym,currentTF);}

window.switchTF=async function(btn,tf){
  document.querySelectorAll(".tf-btn").forEach(b=>b.classList.remove("active"));
  btn.classList.add("active");currentTF=tf;
  if(currentPair){await loadChart(currentPair,tf);saveLastPair(currentPair,tf);}
}

async function fetchCandles(sym,tf){
  try{
    const ctrl=new AbortController();const tid=setTimeout(()=>ctrl.abort(),5000);
    const r=await fetch(`/api/candles/${sym}?interval=${tf}&limit=300`,{signal:ctrl.signal});
    clearTimeout(tid);const data=await r.json();
    if(!Array.isArray(data)||!data.length)return[];
    return data.sort((a,b)=>a.time-b.time);
  }catch{return[];}
}

async function loadChart(sym,tf){
  stopChartUpdate();
  currentPair=sym;currentTF=tf||"Min5";

  document.querySelectorAll(".tf-btn").forEach(b=>{b.classList.toggle("active",b.dataset.tf===currentTF);});
  document.getElementById("ch-sym").textContent=sym.replace("_USDT","")+"/USDT";
  document.getElementById("empty-chart").style.display="none";
  document.getElementById("empty-chart").querySelector(".empty-chart-t").textContent="Select a pair";
  document.getElementById("empty-chart").querySelector(".empty-chart-s").textContent="Tap any pair in the Watchlist tab to view its live chart";
  document.getElementById("chart-loading").classList.remove("hidden");

  updateSignalBar(sym);
  await fetchTickerForChart(sym);
  const candles=await fetchCandles(sym,currentTF);

  if(chart){try{chart.remove();}catch{}chart=null;candleSeries=null;}
  // Reset draw state when switching chart
  drawState.phase='idle';drawState.rectStart=null;drawState.tlStart=null;
  selectedId=null;closeSettingsPanel();

  const container=document.getElementById("chart-container");
  container.innerHTML="";

  if(!candles.length){
    document.getElementById("chart-loading").classList.add("hidden");
    document.getElementById("empty-chart").querySelector(".empty-chart-t").textContent="No chart data";
    document.getElementById("empty-chart").querySelector(".empty-chart-s").textContent="Could not load candles for "+sym.replace("_USDT","")+"/USDT. Tap a timeframe button or try again.";
    document.getElementById("empty-chart").style.display="flex";
    return;
  }

  chart=LightweightCharts.createChart(container,{
    width:container.clientWidth,height:container.clientHeight,
    layout:{background:{type:"solid",color:"#070810"},textColor:"#6b7299"},
    grid:{vertLines:{color:"rgba(79,110,247,.08)"},horzLines:{color:"rgba(79,110,247,.08)"}},
    crosshair:{mode:LightweightCharts.CrosshairMode.Normal,vertLine:{color:"rgba(79,110,247,.4)"},horzLine:{color:"rgba(79,110,247,.4)"}},
    rightPriceScale:{borderColor:"rgba(79,110,247,.15)"},
    timeScale:{borderColor:"rgba(79,110,247,.15)",timeVisible:true,secondsVisible:false,fixLeftEdge:true},
    handleScroll:{mouseWheel:true,pressedMouseMove:true,horzTouchDrag:true},
    handleScale:{axisPressedMouseMove:true,mouseWheel:true,pinch:true},
  });

  const currentPrice=(prices[sym]||{}).price||0;
  candleSeries=chart.addCandlestickSeries({
    upColor:"#00d4aa",downColor:"#ff4d6a",
    borderUpColor:"#00d4aa",borderDownColor:"#ff4d6a",
    wickUpColor:"#00d4aa",wickDownColor:"#ff4d6a",
    priceFormat:getPriceFormat(currentPrice),
  });
  candleSeries.setData(candles);
  await drawSignalLines(sym);
  chart.timeScale().fitContent();
  document.getElementById("chart-loading").classList.add("hidden");
  updateChartHeader();

  // Chart resize observer
  const ro=new ResizeObserver(()=>{
    if(chart&&container.clientWidth&&container.clientHeight){
      chart.applyOptions({width:container.clientWidth,height:container.clientHeight});
    }
    resizeCanvas();
    renderCanvas();
  });
  ro.observe(container);

  // Attach pan/zoom hooks so drawing overlay stays aligned
  attachChartHooks();

  // Load saved drawings for this pair+TF
  loadDrawings();
  resizeCanvas();
  renderCanvas();

  // Add move listener for live preview (separate from pointer handler)
  canvas.removeEventListener('mousemove',onPointerMovePreview);
  canvas.removeEventListener('touchmove',onPointerMovePreview);
  canvas.addEventListener('mousemove',onPointerMovePreview,{passive:true});
  canvas.addEventListener('touchmove',onPointerMovePreview,{passive:true});

  startChartUpdate(sym,currentTF);
}

async function drawSignalLines(sym){
  if(!chart||!candleSeries)return;
  try{
    const r=await fetch(`/api/signal-detail/${sym}`);
    if(!r.ok)return;const d=await r.json();if(!d.found)return;
    const LS=LightweightCharts.LineStyle;
    function pline(price,color,title,style,width){
      try{const v=parseFloat(price);if(!v||isNaN(v)||v<=0)return;
        candleSeries.createPriceLine({price:v,color,lineWidth:width||1,lineStyle:style||LS.Dashed,axisLabelVisible:true,title});}catch{}
    }
    pline(d.crh,"rgba(255,201,60,.9)","CRH",LS.Dotted,1);
    pline(d.crl,"rgba(255,201,60,.9)","CRL",LS.Dotted,1);
    pline(d.ob_top,"rgba(167,139,250,.85)","OB↑",LS.Dotted,1);
    pline(d.ob_bot,"rgba(167,139,250,.85)","OB↓",LS.Dotted,1);
    pline(d.tbs_entry,"rgba(251,146,60,.85)","TBS",LS.Dashed,1);
    pline(d.entry,"rgba(255,201,60,1)","▶ Entry",LS.Dashed,2);
    pline(d.sl,"rgba(255,77,106,.95)","✕ SL",LS.Dotted,1);
    pline(d.tp,"rgba(0,212,170,.95)","✓ TP",LS.Dotted,1);
  }catch{}
}

function updateSignalBar(sym){
  const sig=signals_map[sym];const bar=document.getElementById("signal-bar");
  if(!sig){bar.className="signal-bar";return;}
  const dir=sig.direction||"BUY";
  bar.className="signal-bar show "+(dir==="BUY"?"buy":"sell");
  document.getElementById("sb-title").textContent=`${dir==="BUY"?"🟢 LONG":"🔴 SHORT"} · Model #${sig.model||"1"} · Score ${sig.score||0}/100 ${sig.grade||""}`;
  document.getElementById("sb-sub").textContent=`Entry ${fmtPrice(sig.entry)} · SL ${fmtPrice(sig.sl)} · TP ${fmtPrice(sig.tp)}`;
  document.getElementById("sb-rr").textContent=`${sig.rr||"–"}R`;
  const mini=document.getElementById("ch-signal-mini");
  if(mini)mini.textContent=`${dir==="BUY"?"🟢":"🔴"} ${sig.grade||"A"} ${sig.rr||"?"}R`;
}

function fmtHL(v){const n=Number(v);if(!n||n<=0)return"–";return fmtPrice(n);}

const _tickerFetching={};
async function fetchTickerForChart(sym){
  if(_tickerFetching[sym])return;_tickerFetching[sym]=true;
  try{
    const r=await fetch("/api/ticker/"+sym);if(!r.ok)return;const d=await r.json();
    if(d&&d.price>0){
      const prev=prices[sym]||{};
      prices[sym]={...prev,price:d.price,change:d.change,
        high:(d.high&&d.high>0)?d.high:prev.high,low:(d.low&&d.low>0)?d.low:prev.low};
      updateChartHeader();
    }
  }catch{}finally{_tickerFetching[sym]=false;}
}

function updateChartHeader(){
  if(!currentPair)return;
  const d=prices[currentPair]||{};const chg=Number(d.change||0);const up=chg>=0;
  const priceEl=document.getElementById("ch-price");const chgEl=document.getElementById("ch-chg");
  if(priceEl)priceEl.textContent=fmtPrice(d.price);
  if(priceEl)priceEl.style.color=up?"var(--green)":"var(--red)";
  if(chgEl){chgEl.textContent=(up?"+":"")+chg.toFixed(2)+"%";chgEl.className="ch-chg "+(up?"up":"dn");}
  const highEl=document.getElementById("ch-high");const lowEl=document.getElementById("ch-low");
  const highVal=fmtHL(d.high);const lowVal=fmtHL(d.low);
  if(highEl&&highVal!=="–"){if(highEl.textContent!==highVal){highEl.textContent=highVal;highEl.style.color="var(--green)";}}
  else if(highEl&&highEl.textContent===""){highEl.textContent="–";highEl.style.color="var(--dim)";}
  if(lowEl&&lowVal!=="–"){if(lowEl.textContent!==lowVal){lowEl.textContent=lowVal;lowEl.style.color="var(--red)";}}
  else if(lowEl&&lowEl.textContent===""){lowEl.textContent="–";lowEl.style.color="var(--dim)";}
  if(highVal==="–"||lowVal==="–"){fetchTickerForChart(currentPair);}
}

async function updateLatestCandle(sym,tf){
  if(!candleSeries||currentPair!==sym)return;
  try{
    const candles=await fetchCandles(sym,tf);
    if(candles.length>0){const last2=candles.slice(-2);last2.forEach(c=>candleSeries.update(c));}
  }catch{}
  await fetchTickerForChart(sym);
}

function startChartUpdate(sym,tf){
  stopChartUpdate();
  chartTimer=setInterval(()=>updateLatestCandle(sym,tf),3000);
  startCountdown();
}
function stopChartUpdate(){if(chartTimer){clearInterval(chartTimer);chartTimer=null;}stopCountdown();}

// ═══════════════════════════════════════════════════════════════════
// SECTION 14 — INIT
// ═══════════════════════════════════════════════════════════════════

function init(){
  initCanvas();
  setTool('cursor'); // default: cursor mode, canvas transparent
  renderWatchlist();
  fetchPrices();fetchSignals();
  priceTimer=setInterval(fetchPrices,1000);
  updateTimer=setInterval(fetchSignals,15000);
  updateToolHint();
}
init();

// Hash navigation
const hash=window.location.hash.replace("#","").trim().toUpperCase();
if(hash&&PAIRS.includes(hash)){setTimeout(()=>goToChart(hash),120);}

})();
</script>
</body>
</html>"""

# ════════ MARKET ROUTES ═══════════════════════════════════════════════

@app.route("/market")
def market():
    return make_response(MARKET_HTML, 200, {"Content-Type": "text/html"})


with app.app_context():
    _ensure_started()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)

