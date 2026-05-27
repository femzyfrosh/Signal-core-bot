import os, json, time, secrets, requests, threading, hmac, hashlib, urllib.parse
import random  # ADDED: needed for slippage simulation in paper trading
from datetime import datetime, timezone, timedelta
LOCAL_TZ = timezone(timedelta(hours=1))   # UTC+1 (user local time)
from flask import Flask, request, jsonify, make_response
from collections import deque

app = Flask(__name__)

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "PASTE_YOUR_TOKEN_HERE")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID",   "7411219487")
DASHBOARD_PASSWORD = os.environ.get("DASHBOARD_PASSWORD", "signal123")
MEXC_API_KEY    = os.environ.get("MEXC_API_KEY",    "PASTE_YOUR_API_KEY_HERE")
MEXC_API_SECRET = os.environ.get("MEXC_API_SECRET", "PASTE_YOUR_SECRET_KEY_HERE")

MAX_SIGNALS = 500
signals     = deque(maxlen=MAX_SIGNALS)
# Signed-cookie auth — survives restarts, no in-memory state needed
SESSION_SECRET = os.environ.get("SESSION_SECRET", DASHBOARD_PASSWORD + "_mmss_key")

# ── SETTINGS PERSISTENCE ──────────────────────────────────────────────
_DATA_DIR = "/data" if os.path.isdir("/data") else os.path.dirname(os.path.abspath(__file__))
SETTINGS_FILE = os.path.join(_DATA_DIR, "settings_saved.json")

def _load_saved():
    try:
        if os.path.exists(SETTINGS_FILE):
            with open(SETTINGS_FILE) as f:
                return json.load(f)
    except: pass
    return {}

def save_persisted_settings():
    try:
        os.makedirs(os.path.dirname(SETTINGS_FILE), exist_ok=True)
        with open(SETTINGS_FILE, "w") as f:
            json.dump({"scan": dict(scan_settings),
                       "trade": {k:v for k,v in trade_config.items()}}, f)
    except Exception as e:
        print(f"[settings] save error: {e}")

# ADDED: IP Detection & Notification ─────────────────────────────────────
# On every startup, the bot detects its outbound IP and sends it to Telegram.
# Use that IP to whitelist on MEXC API Management page, then restart the bot.
def notify_mexc_ip():
    """
    Fetch Railway outbound IP and send to Telegram.
    MEXC blocks requests from un-whitelisted IPs — this makes whitelisting easy.
    """
    try:
        ip = None
        for url in ["https://api.ipify.org", "https://ifconfig.me", "https://icanhazip.com"]:
            try:
                resp = requests.get(url, timeout=8)
                candidate = resp.text.strip()
                if candidate and len(candidate) < 50 and " " not in candidate:
                    ip = candidate
                    break
            except Exception:
                continue

        if not ip:
            print("⚠️ [IP] Could not detect outbound IP from any service")
            return

        print(f"\n{'='*55}")
        print(f"🚀  CURRENT OUTBOUND IP: {ip}")
        print(f"📋  Add to MEXC: API Management → IP Whitelist → {ip}")
        print(f"     OR set whitelist to 0.0.0.0/0 to skip whitelisting")
        print(f"{'='*55}\n")

        tok = os.environ.get("TELEGRAM_BOT_TOKEN", TELEGRAM_BOT_TOKEN)
        cid = os.environ.get("TELEGRAM_CHAT_ID",   TELEGRAM_CHAT_ID)
        if tok and "PASTE" not in tok and cid:
            msg = (
                "🚨 <b>Mad Man Scanner — Railway IP Notification</b>\n"
                "━━━━━━━━━━━━━━━━━━\n"
                f"<b>Outbound IP:</b> <code>{ip}</code>\n"
                "━━━━━━━━━━━━━━━━━━\n"
                "➡️ Go to MEXC → API Management\n"
                "➡️ Find your API Key → Edit → IP Whitelist\n"
                f"➡️ Add <code>{ip}</code>\n"
                "   (or set 0.0.0.0/0 to allow all IPs)\n"
                "━━━━━━━━━━━━━━━━━━\n"
                f"<i>Bot restarted: {datetime.now(LOCAL_TZ).strftime('%Y-%m-%d %H:%M UTC+1')}</i>"
            )
            r = requests.post(
                f"https://api.telegram.org/bot{tok}/sendMessage",
                json={"chat_id": cid, "text": msg, "parse_mode": "HTML"},
                timeout=10
            )
            if r.status_code == 200:
                print("✅ [IP] Notification sent to Telegram successfully")
            else:
                print(f"⚠️ [IP] Telegram send failed: {r.status_code} {r.text[:100]}")
    except Exception as e:
        print(f"⚠️ [IP] notify_mexc_ip error: {e}")


# ADDED: auto_whitelist_ip (keep for backward compat, now wraps notify_mexc_ip)
def auto_whitelist_ip():
    """
    Attempt MEXC auto-whitelist via API (may not be supported on all keys).
    Also calls notify_mexc_ip() so you always see the IP in Telegram.
    """
    # Always notify first — this always works
    notify_mexc_ip()

    key    = trade_config.get("api_key", "")
    secret = trade_config.get("api_secret", "")
    if not key or not secret or "PASTE" in key:
        return
    try:
        my_ip = requests.get("https://api.ipify.org", timeout=5).text.strip()
        import hmac as _hmac, hashlib as _hl, urllib.parse as _up
        ts     = str(int(time.time() * 1000))
        params = f"ipAddress={_up.quote(my_ip)}&timestamp={ts}"
        sig    = _hmac.new(secret.encode(), params.encode(), _hl.sha256).hexdigest()
        url    = f"https://api.mexc.com/api/v3/account/apiKey/ip?{params}&signature={sig}"
        r      = requests.post(url, headers={"X-MEXC-APIKEY": key}, timeout=10)
        if r.status_code == 200:
            log(f"✅ IP {my_ip} auto-whitelisted on MEXC successfully!")
        elif r.status_code == 400 and "already" in r.text.lower():
            log(f"✅ IP {my_ip} already whitelisted on MEXC.")
        else:
            log(f"⚠️ Auto-whitelist returned {r.status_code}: {r.text[:120]}")
    except Exception as e:
        log(f"⚠️ Auto-whitelist error: {e}")

_saved = _load_saved()
_ss = _saved.get("scan", {})
_st = _saved.get("trade", {})

# ── GLOBAL SETTINGS (editable from Settings page) ─────────────────────
scan_settings = {
    "price_interval":    _ss.get("price_interval", 1),
    "scan_interval":     _ss.get("scan_interval",  1),
    "cycle_rest":        _ss.get("cycle_rest",      5),
    "tg_signals":        _ss.get("tg_signals",      True),
    "tg_trades":         _ss.get("tg_trades",       True),
    "tg_bot_token":      _ss.get("tg_bot_token",    TELEGRAM_BOT_TOKEN),
    "tg_chat_id":        _ss.get("tg_chat_id",      TELEGRAM_CHAT_ID),
    "model1_enabled":    _ss.get("model1_enabled",  True),
    "model2_enabled":    _ss.get("model2_enabled",  True),
}
settings_lock = threading.Lock()

trade_config = {
    "enabled":       _st.get("enabled",   False),
    "api_key":       _st.get("api_key",    MEXC_API_KEY),
    "api_secret":    _st.get("api_secret", MEXC_API_SECRET),
    "risk_pct":      _st.get("risk_pct",   1.0),
    "max_trades":    _st.get("max_trades", 3),
    "leverage":      _st.get("leverage",   35),
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

TOP_PAIRS = ["BTC_USDT","ETH_USDT","SOL_USDT","BNB_USDT","XRP_USDT","DOGE_USDT"]
MEXC_BASE = "https://contract.mexc.com/api/v1/contract"
CRT_TFS   = ["Day1","Hour4","Hour3","Hour2","Min60"]
OB_TFS    = ["Hour4","Hour3","Hour2","Min60","Min45"]
TBS_TFS   = ["Min30","Min15","Min10","Min5"]
TBS_TF_MAP = {
    "Min60": ["Min1","Min2"],
    "Hour2": ["Min2","Min3"],
    "Hour3": ["Min3","Min4"],
    "Hour4": ["Min4","Min5"],
    "Day1":  ["Min45","Min60"],
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
    "enabled":    False,
    "auto_trade": False,
    "balance":    10000.0,
    "risk_pct":   1.0,
    "max_trades": 4,
}
paper_trades  = {}
paper_history = deque(maxlen=50)
paper_lock    = threading.Lock()
paper_stats   = {"total": 0, "wins": 0, "losses": 0, "total_pnl": 0.0}

# ════════ MEXC API ═══════════════════════════════════════════════════

# FIXED: get_all_pairs — added retry with exponential backoff, rate-limit handling
def get_all_pairs():
    """Fetch all active USDT perpetual pairs from MEXC with retry logic."""
    for attempt in range(4):
        try:
            r = requests.get(f"{MEXC_BASE}/detail", timeout=15)
            # FIXED: Handle MEXC rate-limit (429) with backoff
            if r.status_code == 429:
                wait = 2 ** attempt * 5
                log(f"⚠️ [Pairs] Rate limited (429) — waiting {wait}s before retry {attempt+1}/4")
                time.sleep(wait)
                continue
            data = r.json()
            if not data.get("success"):
                log(f"⚠️ [Pairs] API returned success=false: {data.get('message','')}")
                time.sleep(3)
                continue
            seen = set()
            pairs = []
            for item in data.get("data", []):
                sym = item.get("symbol","")
                if item.get("state") == 0 and sym.endswith("_USDT") and sym not in seen:
                    seen.add(sym)
                    pairs.append(sym)
            if pairs:
                return sorted(pairs)
            log("⚠️ [Pairs] Empty pair list returned — retrying")
        except requests.exceptions.Timeout:
            log(f"⚠️ [Pairs] Timeout on attempt {attempt+1}/4")
        except Exception as e:
            log(f"⚠️ [Pairs] Error attempt {attempt+1}/4: {e}")
        time.sleep(2 ** attempt * 2)   # exponential backoff: 2s, 4s, 8s
    log("❌ [Pairs] All retries exhausted — could not fetch pairs")
    return []

# FIXED: get_candles — retry with exponential backoff, handles rate limits & empty responses
def get_candles(symbol, interval, limit=150):
    """Fetch OHLCV candles from MEXC futures with retry and rate-limit handling."""
    for attempt in range(3):
        try:
            r = requests.get(
                f"{MEXC_BASE}/kline/{symbol}",
                params={"interval": interval, "limit": limit},
                timeout=10
            )
            if r.status_code == 429:
                wait = 2 ** attempt * 3
                log(f"⚠️ [Candles] Rate limited — waiting {wait}s ({symbol} {interval})")
                time.sleep(wait)
                continue
            if r.status_code != 200:
                log(f"⚠️ [Candles] HTTP {r.status_code} for {symbol}/{interval}")
                time.sleep(1)
                continue
            data = r.json()
            if not data.get("success") or not data.get("data"):
                # Not a hard error — pair may have no data on this TF
                return []
            raw = data["data"]
            out = []
            times  = raw.get("time",  [])
            opens  = raw.get("open",  [])
            highs  = raw.get("high",  [])
            lows   = raw.get("low",   [])
            closes = raw.get("close", [])
            for i in range(len(times)):
                try:
                    out.append({
                        "time":  int(times[i]),
                        "open":  float(opens[i]),
                        "high":  float(highs[i]),
                        "low":   float(lows[i]),
                        "close": float(closes[i])
                    })
                except Exception:
                    continue
            return out
        except requests.exceptions.Timeout:
            log(f"⚠️ [Candles] Timeout {symbol}/{interval} attempt {attempt+1}")
        except Exception as e:
            log(f"⚠️ [Candles] Error {symbol}/{interval}: {e}")
        time.sleep(1 + attempt)
    return []

def get_ticker(symbol):
    """
    Fetch live ticker from MEXC futures.
    MEXC returns priceChangePercent as a decimal fraction (0.012 = 1.2%)
    OR as a full percentage (1.2). We normalise to always show as percentage.
    Also tries risePriceFall and other field names for compatibility.
    """
    for attempt in range(3):
      try:
        r = requests.get(f"{MEXC_BASE}/ticker", params={"symbol": symbol}, timeout=6)
        if r.status_code == 429:
            time.sleep(2 ** attempt * 2)
            continue
        data = r.json()
        if data.get("success") and data.get("data"):
            d = data["data"]
            if isinstance(d, list): d = d[0]

            price  = float(d.get("lastPrice",  d.get("last", 0)))
            high   = float(d.get("high24h",    d.get("high", 0)))
            low    = float(d.get("low24h",     d.get("low",  0)))

            # Try every known field name for 24h change
            raw_chg = (
                d.get("priceChangePercent") or
                d.get("changeRate")         or
                d.get("riseFallRate")       or
                d.get("rate")               or
                d.get("change24h")          or
                0
            )
            change = float(raw_chg)

            # MEXC sometimes returns decimal (0.012) sometimes percent (1.2)
            # If absolute value < 1.5 it's almost certainly a decimal fraction → multiply by 100
            if change != 0 and abs(change) < 1.5:
                change = change * 100

            # Fallback: calculate from open24h or from high/low midpoint vs price
            if change == 0 and price > 0:
                open24 = float(d.get("open24h", d.get("openPrice", d.get("indexPrice", 0))))
                if open24 > 0:
                    change = round((price - open24) / open24 * 100, 2)

            return {
                "price":  round(price,  8),
                "change": round(change, 2),
                "high":   round(high,   8),
                "low":    round(low,    8),
            }
        break  # success or non-retryable error
      except requests.exceptions.Timeout:
        time.sleep(1 + attempt)
        continue
      except Exception:
        break
    return None

# ════════ MARKET STRUCTURE ═══════════════════════════════════════════

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
    bbs = []
    if len(candles) < 10: return bbs
    obs = find_obs(candles, "BULLISH" if direction=="BULLISH" else "BEARISH")
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
    if direction == "BULLISH":
        return all(ob["bot"] <= other["bot"] for other in all_obs if other is not ob)
    else:
        return all(ob["top"] >= other["top"] for other in all_obs if other is not ob)

def find_all_key_levels(candles, direction):
    """Only unmitigated FVGs and the single most-extreme OB are valid key levels."""
    zones = []
    obs_dir = "BULLISH" if direction == "BULLISH" else "BEARISH"
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
    recent = candles[-14:]
    if direction=="BULLISH":
        return any(c["low"]<=ob["top"] and c["high"]>=ob["bot"] for c in recent)
    else:
        return any(c["high"]>=ob["bot"] and c["low"]<=ob["top"] for c in recent)

# ════════ MAD MAN DETECTION ════════════════════════════════════════════

def detect_crt(candles, direction, ob=None):
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
    tfs_to_check = TBS_TF_MAP.get(crt_tf, TBS_TFS)
    for tf in tfs_to_check:
        candles = get_candles(symbol, tf, limit=120)
        if not candles or len(candles)<4: continue
        recent = candles[-80:]
        for i in range(len(recent)-1):
            c   = recent[i]
            nxt = recent[i+1]
            if direction=="BUY":
                if c["close"] < crl and nxt["close"] > crl:
                    return True, tf, round(c["open"],8), round(c["low"],8)
            else:
                if c["close"] > crh and nxt["close"] < crh:
                    return True, tf, round(c["open"],8), round(c["high"],8)
    return False, None, None, None

# ════════ CHOCH ═══════════════════════════════════════════════════════

def check_choch(symbol, tf, direction):
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
    candles = get_candles(symbol, tf, limit=80)
    if not candles or len(candles)<5: return False,None,None,None,None
    fresh=[]; ifvg=[]
    for i in range(len(candles)-3):
        c1=candles[i]; c3=candles[i+2]
        if direction=="BUY":
            if c3["low"]>c1["high"]:
                zbot=c1["high"]; ztop=c3["low"]
                mit=any(candles[j]["low"]<=ztop for j in range(i+3,len(candles)))
                if not mit:
                    fresh.append({"type":"FVG","entry":round(zbot,8),
                                  "zone_top":round(ztop,8),"zone_bot":round(zbot,8),"idx":i})
                else:
                    ifvg.append({"type":"IFVG","entry":round(ztop,8),
                                 "zone_top":round(ztop,8),"zone_bot":round(zbot,8),"idx":i})
        else:
            if c3["high"]<c1["low"]:
                ztop=c1["low"]; zbot=c3["high"]
                mit=any(candles[j]["high"]>=zbot for j in range(i+3,len(candles)))
                if not mit:
                    fresh.append({"type":"FVG","entry":round(ztop,8),
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
    for tf in tfs:
        found, level = check_choch(symbol, tf, direction)
        if found and level:
            return True, level
    return False, None

def find_fvg_multi(symbol, tfs, direction):
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
    elif score >= 35:
        grade = "B"
    elif score>=45:
        grade = "C"
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

# Model #2 monitor: watches for sweep → FVG → price tap → market order
m2_monitor = {}
m2_lock     = threading.Lock()
MAX_M2_MONITORED = 6

recent_trades = deque(maxlen=10)

diag = {
    "no_candles":0, "neutral":0, "not_continuous":0,
    "no_obs":0, "not_at_key":0, "not_in_zone":0,
    "no_liq":0, "not_tapping":0, "no_crts":0,
    "no_tbs":0, "rr_low":0, "passed":0,
    "1d_no_crts":0, "1d_no_tbs":0, "1d_rr_low":0,
}

def scan_pair(symbol):
    results = []

    ref_candles = get_candles(symbol, "Hour4", limit=200)
    if not ref_candles or len(ref_candles)<30:
        diag["no_candles"]+=1; return results

    trend, sh, sl = detect_trend(ref_candles)
    if trend=="NEUTRAL":
        diag["neutral"]+=1; return results

    continuous = is_continuous(sh, sl, trend, min_pts=2)
    if not continuous:
        diag["not_continuous"]+=1; return results

    direction = "BUY" if trend=="BULLISH" else "SELL"

    candles_1d = get_candles(symbol, "Day1", limit=120)
    if candles_1d and len(candles_1d)>=10:
        crts = detect_crt(candles_1d, direction, ob=None)
        if not crts:
            diag["1d_no_crts"]+=1
        for crt in crts:
            fake_ob = {"idx": len(candles_1d)-3, "top": crt["crh"], "bot": crt["crl"]}
            liq = liq_sweep_before_ob(candles_1d, fake_ob, direction)
            tbs_found, tbs_tf, tbs_entry, tbs_sl = check_tbs(symbol, direction, crt["crl"], crt["crh"])
            if not tbs_found:
                diag["1d_no_tbs"]+=1; continue
            if tbs_entry:
                entry      = tbs_entry
                entry_type = "Model #1 (TBS Open)"
                # SL = swing extreme of the HTF CRT C2 manipulation candle
                c2_candle  = crt.get("c2", {})
                sl_p       = round(c2_candle.get("low", tbs_sl), 8) if direction == "BUY" else round(c2_candle.get("high", tbs_sl), 8)
                # TP = opposite CRT level
                tp_p       = round(crt["crh"], 8) if direction == "BUY" else round(crt["crl"], 8)
            else:
                entry      = crt["entry"]
                entry_type = "C2 Close"
                sl_p       = crt["sl"]
                tp_p       = round(crt["crh"], 8) if direction == "BUY" else round(crt["crl"], 8)
            choch_found, choch_level = check_choch_multi(symbol, ["Min60","Min30"], direction)
            fvg_found, fvg_type, fvg_entry, fvg_top, fvg_bot = find_fvg_multi(symbol, ["Min60","Min30"], direction)
            risk=abs(entry-sl_p); reward=abs(tp_p-entry)
            rr=round(reward/risk,2) if risk>0 else 0
            if rr<2.0:
                diag["1d_rr_low"]+=1; continue
            crt_s=dict(crt); crt_s["entry"]=entry; crt_s["rr"]=rr
            score,grade,details=score_signal(
                crt_s,trend,liq,tbs_found,tbs_tf,
                fvg_found,fvg_type,choch_found,continuous,is_1d=True,
                sh=sh,sl=sl,direction=direction)
            results.append({
                "symbol":symbol,"tf":"Day1","ob_tf":"N/A","ob_zone":"–",
                "direction":direction,"trend":trend,
                "entry":round(entry,8),"entry_type":entry_type,
                "sl":round(sl_p,8),"tp":round(tp_p,8),"rr":rr,
                "crh":crt["crh"],"crl":crt["crl"],
                "ob_top":"–","ob_bot":"–",
                "score":score,"grade":grade,"details":details,
                "tbs_found":tbs_found,"tbs_tf":tbs_tf or "–","tbs_entry":tbs_entry or "–","tbs_sl":tbs_sl or "–",
                "fvg_found":fvg_found,"fvg_type":fvg_type or "–",
                "fvg_entry":fvg_entry or "–","fvg_top":fvg_top or "–","fvg_bot":fvg_bot or "–",
                "choch_found":choch_found,"choch_level":choch_level or "–",
                "liq_swept":liq,"ob_respected":False,"continuous":continuous,
                "timestamp":datetime.now(LOCAL_TZ).strftime("%Y-%m-%d %H:%M UTC+1"),
            })
            break

    if results: return results

    for crt_tf in ["Hour4","Hour3","Hour2","Min60"]:
        if results: break

        crt_candles = get_candles(symbol, crt_tf, limit=200)
        if not crt_candles or len(crt_candles)<20: continue

        for ob_tf in OB_TFS:
            if results: break
            ob_candles = get_candles(symbol, ob_tf, limit=150)
            if not ob_candles or len(ob_candles)<20: continue

            # ── Strict: only OB / BB / FVG in correct P/D zone ──────────────
            # Bullish: key level must be in DISCOUNT zone
            # Bearish: key level must be in PREMIUM zone
            raw_obs = find_obs(ob_candles, "BULLISH" if direction == "BULLISH" else "BEARISH")
            ob_resp = prev_obs_respected(raw_obs, ob_candles, direction, min_resp=1)

            # Build candidate list: OB + BB + FVG only (no RJB, no IFVG)
            valid_kls = []
            for ob in raw_obs[:6]:
                ob["kl_type"] = "OB"
                valid_kls.append(ob)
            for bb in find_breaker_block(ob_candles, direction)[:4]:
                valid_kls.append(bb)
            for i in range(max(0, len(ob_candles) - 50), len(ob_candles) - 3):
                c1x = ob_candles[i]; c3x = ob_candles[i + 2]
                if direction == "BULLISH" and c3x["low"] > c1x["high"]:
                    valid_kls.append({"top": c3x["low"], "bot": c1x["high"],
                                      "high": c3x["low"], "low": c1x["high"],
                                      "idx": i, "time": c1x["time"], "kl_type": "FVG"})
                elif direction == "BEARISH" and c3x["high"] < c1x["low"]:
                    valid_kls.append({"top": c1x["low"], "bot": c3x["high"],
                                      "high": c1x["low"], "low": c3x["high"],
                                      "idx": i, "time": c1x["time"], "kl_type": "FVG"})
            valid_kls.sort(key=lambda x: x.get("idx", 0), reverse=True)

            zone_found = False; zone_name = "–"; zone_top = None
            zone_bot = None; zone_type = "–"; matched_ob = None
            at_key = False; in_pd_zone = False

            for kl in valid_kls[:12]:
                zt = kl.get("top", kl.get("high", 0))
                zb = kl.get("bot", kl.get("low", 0))
                if zt <= zb: continue
                # P/D zone is MANDATORY — wrong zone = skip
                in_zone_pd, pd_name = ob_in_pd_zone(kl, ob_candles, direction)
                if not in_zone_pd: continue
                zone_found = True
                zone_top   = zt; zone_bot = zb
                zone_type  = kl.get("kl_type", "KL")
                zone_name  = pd_name + " · " + zone_type
                matched_ob = kl if zone_type in ("OB", "BB") else None
                at_key     = ob_at_key_level(kl, direction, sh, sl)
                in_pd_zone = True
                break

            # ── Fallback: check if price RECENTLY reacted from a zone ──
            # If no zone found currently, check if there was a recent reaction
            # and no opposing BOS since then → still valid setup
            if not zone_found:
                for kl in valid_kls[:12]:
                    zt = kl.get("top", kl.get("high", 0))
                    zb = kl.get("bot", kl.get("low", 0))
                    if zt <= zb: continue
                    in_zone_pd, pd_name = ob_in_pd_zone(kl, ob_candles, direction)
                    if not in_zone_pd: continue
                    # Check if price reacted from this zone recently
                    if price_reacted_from_zone(crt_candles, zt, zb, direction, lookback=30):
                        if not has_bos_since_reaction(crt_candles, direction, zb):
                            zone_found = True
                            zone_top   = zt; zone_bot = zb
                            zone_type  = kl.get("kl_type", "KL") + " (Recent React)"
                            zone_name  = pd_name + " · " + zone_type
                            matched_ob = kl if kl.get("kl_type") in ("OB", "BB") else None
                            at_key     = ob_at_key_level(kl, direction, sh, sl)
                            in_pd_zone = True
                            break

            if not zone_found:
                diag["not_in_zone"] += 1; continue
                diag["not_in_zone"]+=1; continue

            crts_raw = detect_crt(crt_candles, direction, ob=None)
            crts = [c for c in crts_raw if crt_inside_zone(c, zone_top, zone_bot)]

            if not crts:
                diag["no_crts"]+=1
                with manip_lock:
                    already_monitored = symbol in manip_monitor
                    monitor_full = len(manip_monitor) >= MAX_MONITORED
                if not already_monitored and not monitor_full:
                    # First try LIVE detection (current forming candle, 1–40 min window)
                    manip_pending = detect_manip_phase_live(crt_candles, direction, crt_tf)
                    # Fallback to historical detection if live didn't match
                    if not manip_pending:
                        manip_pending = detect_manip_phase(crt_candles, direction, crt_tf)
                    manip_in_zone = [m for m in manip_pending
                                     if crt_inside_zone(m, zone_top, zone_bot)]
                    if manip_in_zone:
                        mp = manip_in_zone[0]
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
                        log(f"👁 MONITORING: {symbol} {direction} {crt_tf} — manip phase | {mins_info} min to candle close | zone:{zone_type}")
                continue

            for crt in crts:
                tbs_found, tbs_tf, tbs_entry, tbs_sl = check_tbs(
                    symbol, direction, crt["crl"], crt["crh"], crt_tf)
                if not tbs_found:
                    diag["no_tbs"]+=1; continue

                if tbs_entry:
                    entry      = tbs_entry
                    entry_type = "Model #1 (TBS Open)"
                    # SL = swing extreme of the HTF CRT manipulation candle (C2)
                    c2_candle  = crt.get("c2", {})
                    sl_p       = round(c2_candle.get("low", tbs_sl), 8) if direction == "BUY" else round(c2_candle.get("high", tbs_sl), 8)
                    # TP = opposite CRT level (CRH for BUY, CRL for SELL)
                    tp_p       = round(crt["crh"], 8) if direction == "BUY" else round(crt["crl"], 8)
                else:
                    entry      = crt["entry"]
                    entry_type = "C2 Close"
                    sl_p       = crt["sl"]
                    tp_p       = round(crt["crh"], 8) if direction == "BUY" else round(crt["crl"], 8)

                ltf = get_ltf_for_crt(crt_tf)
                fallback_ltfs = {"Hour4":["Min15","Min10"],"Hour3":["Min15","Min10"],
                                  "Hour2":["Min10","Min5"],"Min60":["Min5"]}.get(crt_tf,[ltf])
                choch_found, choch_level = check_choch_multi(symbol, fallback_ltfs, direction)
                fvg_ltfs = {"Hour4":["Min15","Min10"],"Hour3":["Min15","Min10"],
                             "Hour2":["Min10","Min5"],"Min60":["Min5"]}.get(crt_tf,[ltf])
                fvg_found, fvg_type, fvg_entry, fvg_top, fvg_bot = find_fvg_multi(
                    symbol, fvg_ltfs, direction)

                risk   = abs(entry-sl_p)
                reward = abs(tp_p-entry)
                rr     = round(reward/risk,2) if risk>0 else 0
                if rr<2.0:
                    diag["rr_low"]+=1; continue

                crt_s = dict(crt); crt_s["entry"]=entry; crt_s["rr"]=rr
                score,grade,details = score_signal(
                    crt_s, trend, False, tbs_found, tbs_tf,
                    fvg_found, fvg_type, choch_found, continuous,
                    is_1d=False, ob=matched_ob, at_key=at_key,
                    ob_resp=ob_resp, ob_zone=zone_name,
                    sh=sh, sl=sl, direction=direction)

                diag["passed"]+=1
                results.append({
                    "symbol":    symbol,
                    "tf":        crt_tf,
                    "ob_tf":     ob_tf,
                    "ob_zone":   zone_name,
                    "zone_type": zone_type,
                    "direction": direction,
                    "trend":     trend,
                    "entry":     round(entry,8),
                    "entry_type":entry_type,
                    "sl":        round(sl_p,8),
                    "tp":        round(tp_p,8),
                    "rr":        rr,
                    "crh":       crt["crh"],
                    "crl":       crt["crl"],
                    "ob_top":    zone_top   or "–",
                    "ob_bot":    zone_bot   or "–",
                    "score":     score,
                    "grade":     grade,
                    "details":   details,
                    "tbs_found":    tbs_found,
                    "tbs_tf":       tbs_tf   or "–",
                    "tbs_entry":    tbs_entry or "–",
                    "tbs_sl":       tbs_sl   or "–",
                    "fvg_found":    fvg_found,
                    "fvg_type":     fvg_type  or "–",
                    "fvg_entry":    fvg_entry or "–",
                    "fvg_top":      fvg_top   or "–",
                    "fvg_bot":      fvg_bot   or "–",
                    "choch_found":  choch_found,
                    "choch_level":  choch_level or "–",
                    "liq_swept":    False,
                    "ob_respected": ob_resp,
                    "continuous":   continuous,
                    "timestamp":    datetime.now(LOCAL_TZ).strftime("%Y-%m-%d %H:%M UTC+1"),
                })
                break
            if results: break
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
    """
    fvgs = []
    end = min(to_idx, len(candles) - 2)
    for i in range(from_idx, end):
        c1 = candles[i]; c3 = candles[i + 2]
        if direction == "SELL" and c3["high"] < c1["low"]:
            fvg_top = c1["low"]; fvg_bot = c3["high"]
            # Check not mitigated by later candles
            mit = any(candles[j]["high"] >= fvg_bot for j in range(i + 3, len(candles)))
            if not mit:
                fvgs.append({"top": round(fvg_top,8), "bot": round(fvg_bot,8),
                             "tip": round(fvg_bot,8), "idx": i})
        elif direction == "BUY" and c3["low"] > c1["high"]:
            fvg_top = c3["low"]; fvg_bot = c1["high"]
            mit = any(candles[j]["low"] <= fvg_top for j in range(i + 3, len(candles)))
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


def scan_pair_model2(symbol):
    """
    Unified M2/M3 scanner.
    ANY pair tapping an HTF key level goes straight into the unified monitor.
    The monitor detects which model pattern completes and tags accordingly.
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
            in_pd, pd_name = ob_in_pd_zone(kl, htf_c, direction)
            if not in_pd: continue

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
                            "phase":       "AWAIT_PATTERN",   # single entry phase — monitor detects M2 or M3
                            "model":       None,               # assigned when pattern confirms
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
                    log(f"👁 MONITOR QUEUED: {symbol} {direction} | {htf} {pd_name} | ft={ft_extreme} | awaiting M2 or M3 pattern")
                break
            if symbol in m2_monitor: break
        if symbol in m2_monitor: break
    return results

def fmt_tg_m2(sig):
    e  = "🟢" if sig["direction"]=="BUY" else "🔴"
    b  = "█"*(sig["score"]//10)+"░"*(10-sig["score"]//10)
    hl = {"Day1":"1D","Hour4":"4H","Hour3":"3H","Hour2":"2H","Min60":"1H"}.get(sig.get("tf",""),"–")
    return (
        f"{e} <b>MAD MAN MODEL #2 — {sig['direction']}</b>\n"
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

# ════════ MEXC AUTO-TRADE ENGINE ═════════════════════════════════════

def live_trade_ready():
    """Returns (bool, reason_string). True only when live trading is properly configured."""
    if not trade_config.get("enabled"):
        return False, "Live trading disabled (enable it in Settings)"
    key = trade_config.get("api_key", "")
    secret = trade_config.get("api_secret", "")
    if not key or "PASTE" in key:
        return False, "MEXC API key not set — paste your key in Settings"
    if not secret or "PASTE" in secret:
        return False, "MEXC API secret not set — paste your secret in Settings"
    return True, "OK"


def test_api_connection():
    """
    Test MEXC API connectivity on startup.
    Hits /account/assets — a lightweight authenticated endpoint.
    Logs clear pass/fail so you know immediately if keys are working.
    """
    # ── AUTO-WHITELIST THIS SERVER'S IP ON MEXC ───────────────────────
    auto_whitelist_ip()

    key    = trade_config.get("api_key", "")
    secret = trade_config.get("api_secret", "")
    if not key or "PASTE" in key or not secret or "PASTE" in secret:
        log("⚠️ API test skipped — keys not configured yet")
        return
    data, err = mexc_request("GET", "/account/assets")
    if err:
        if "Access Denied" in err or "HTML" in err:
            log("❌ API TEST FAILED: Access Denied — your API key is blocked by MEXC. "
                "Go to MEXC → API Management → make sure: "
                "(1) Futures trading is enabled on the key, "
                "(2) Railway server IP is whitelisted (or set IP whitelist to 0.0.0.0/0), "
                "(3) Key has not expired")
        else:
            log(f"❌ API TEST FAILED: {err}")
    else:
        log("✅ MEXC API connection OK — futures account accessible")


def mexc_sign(api_key, timestamp_ms, query_string, secret):
    """
    MEXC Futures Contract API v1 signature.
    Format: HMAC-SHA256(apiKey + timestamp + requestParam, secretKey)
    Returns lowercase hex (standard HMAC output).
    """
    raw = str(api_key) + str(timestamp_ms) + str(query_string)
    return hmac.new(secret.encode("utf-8"), raw.encode("utf-8"), hashlib.sha256).hexdigest()


def mexc_request(method, path, params=None, signed=True):
    """
    MEXC Futures Contract API v1 — authenticated requests.

    Signature algorithm (per MEXC docs):
      sign_str = apiKey + timestamp_ms + requestParam
      signature = HMAC-SHA256(sign_str, secretKey).hexdigest()

    GET:  requestParam = URL query string (sorted, urlencode)
    POST: requestParam = raw JSON body string (compact, NO spaces)
          Body sent as raw bytes with Content-Type: application/json
    """
    api_key    = trade_config.get("api_key", "")
    api_secret = trade_config.get("api_secret", "")
    if not api_key or not api_secret:
        return None, "API keys not configured"

    params = params or {}
    ts     = str(int(time.time() * 1000))

    if method == "GET":
        # Sort params, build query string for signature
        sorted_params = sorted(params.items())
        query_str  = urllib.parse.urlencode(sorted_params) if sorted_params else ""
        body_bytes = None
    else:
        # POST: compact JSON, NO spaces — this is what gets signed AND sent
        body_str   = json.dumps(params, separators=(",", ":"), sort_keys=True)
        query_str  = body_str          # MEXC signs the raw body string for POST
        body_bytes = body_str.encode("utf-8")

    # Build signature: apiKey + timestamp + requestParam
    sign_str  = str(api_key) + str(ts) + str(query_str)
    signature = hmac.new(
        api_secret.encode("utf-8"),
        sign_str.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()

    headers = {
        "Content-Type": "application/json",
        "ApiKey":        api_key,
        "Request-Time":  ts,
        "Signature":     signature,
    }

    try:
        url = f"{MEXC_FUTURES}{path}"
        if method == "GET":
            r = requests.get(url, params=params, headers=headers, timeout=10)
        else:
            # Send raw bytes — NOT data= or json= which both alter the body
            r = requests.post(url, data=body_bytes, headers=headers, timeout=10)

        raw_text = r.text.strip()
        if not raw_text:
            return None, f"Empty response (HTTP {r.status_code}) on {path}"

        # MEXC sometimes returns HTML "Access Denied" — catch it cleanly
        if raw_text.startswith("<"):
            return None, f"MEXC returned HTML (Access Denied / blocked). Check API key IP whitelist and futures permissions. HTTP {r.status_code}"

        try:
            data = json.loads(raw_text)
        except Exception:
            return None, f"Non-JSON response (HTTP {r.status_code}): {raw_text[:200]}"

        if data.get("success") is True or str(data.get("code", "")) == "0":
            return data.get("data"), None

        err_msg = data.get("message", data.get("msg", f"code={data.get('code')}"))
        log(f"MEXC API error on {path}: {err_msg} | code={data.get('code')}")
        return None, err_msg

    except requests.exceptions.Timeout:
        return None, f"Timeout on {path} — MEXC not responding (try whitelisting IP)"
    except Exception as e:
        return None, f"Request exception on {path}: {e}"


# FIXED: mexc_request_with_retry — wraps mexc_request with exponential backoff
# Use this for all trading operations to handle transient MEXC failures
def mexc_request_with_retry(method, path, params=None, retries=3, base_wait=2):
    """
    Retry wrapper for mexc_request.
    Handles: timeouts, rate limits, transient errors.
    Does NOT retry on auth errors (wrong key / IP blocked).
    """
    last_err = "No attempts made"
    for attempt in range(retries):
        data, err = mexc_request(method, path, params)
        if data is not None:
            return data, None   # success
        last_err = err or "Unknown error"
        # Don't retry auth/IP errors — they won't self-heal
        if err and any(x in err for x in ["Access Denied", "blocked", "API keys", "HTML"]):
            log(f"❌ [MEXC] Non-retryable error on {path}: {err}")
            return None, err
        # Retry on timeout / rate-limit / transient
        wait = base_wait * (2 ** attempt)
        log(f"⚠️ [MEXC] Retry {attempt+1}/{retries} for {path} in {wait}s | {last_err}")
        time.sleep(wait)
    return None, f"All {retries} retries failed on {path}: {last_err}"

def get_account_balance():
    """Get available USDT balance from MEXC Futures account."""
    data, err = mexc_request("GET", "/account/assets")
    if err:
        return 0.0, err
    if not data:
        return 0.0, "No data returned"
    # MEXC may return a list of assets or a single asset dict
    assets = data if isinstance(data, list) else [data]
    for asset in assets:
        currency = asset.get("currency", asset.get("coin", ""))
        if currency.upper() == "USDT":
            bal = float(asset.get("availableBalance",
                        asset.get("available",
                        asset.get("walletBalance", 0))))
            return bal, None
    # If USDT not found in list, try treating data as direct balance
    if isinstance(data, dict):
        bal = float(data.get("availableBalance", data.get("available", 0)))
        if bal > 0:
            return bal, None
    return 0.0, "USDT balance not found — check account has USDT"


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
    if risk_amount < 0.1:
        return False, f"Risk amount ${risk_amount:.4f} is below minimum $0.10 — increase balance or risk %"
    sl_distance = abs(entry - sl)
    if sl_distance <= 0: return 0
    info = get_symbol_info(symbol)
    contracts = risk_amount / (sl_distance * info["contract_size"])
    min_vol = info["min_vol"]
    contracts = max(min_vol, round(contracts / min_vol) * min_vol)
    return int(contracts)

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
    risk_amount = balance * trade_config["risk_pct"] / 100
    if risk_amount < 0.10:
        return False, f"Risk ${risk_amount:.4f} below $0.10 minimum — raise balance or risk %"

    risk_amount_pre = balance * trade_config["risk_pct"] / 100
    total_used = len(open_trades) * risk_amount_pre
    if total_used >= balance * 0.90:
        return False, "90% balance cap reached across open trades"

    entry = float(sig["entry"])
    sl    = float(sig["sl"])
    tp    = float(sig["tp"])

    # ── STRICT RISK MANAGEMENT ──────────────────────────────────────
    # margin     = 20% of balance (cross margin per trade)
    # max_loss   = 100% of margin (hard cap — SL CANNOT exceed this)
    # leverage   = calculated so that: size * sl_dist * leverage <= max_loss
    # Formula:   leverage = max_loss / (margin * sl_pct)
    #            size (contracts) = margin * leverage / entry / contract_size

    margin    = risk_amount      # user-defined risk per trade
    max_loss  = margin             # SL can cost AT MOST 100% of margin
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

    # Cap leverage: 1x minimum, 500x maximum
    leverage = max(1, min(500, int(implied_lev)))

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

    side      = 1 if sig["direction"] == "BUY" else 2
    open_type = 2   # cross margin

    # Use manual leverage if user set one, otherwise use calculated
    manual_lev = trade_config.get("leverage", 0)
    if manual_lev and manual_lev > 0:
        leverage = max(1, min(500, int(manual_lev)))
        log(f"🔧 Using manual leverage: {leverage}x")

    # MEXC change_leverage is a POST request
    # FIXED: use retry wrapper for robustness
    lev_data, lev_err = mexc_request_with_retry("POST", "/position/change_leverage", {
        "symbol":       sig["symbol"],
        "leverage":     leverage,
        "openType":     open_type,
        "positionType": side,
    })
    if lev_err:
        log(f"⚠️ Leverage change warning: {lev_err} — proceeding anyway")

    # Market order (type=5) with SL/TP embedded — guarantees fill at current price
    order_params = {
        "symbol":         sig["symbol"],
        "side":           side,
        "openType":       open_type,
        "type":           5,          # 5 = market order (fills immediately)
        "vol":            size,
        "leverage":       leverage,
        "stopLossPrice":  round(sl, 8),
        "takeProfitPrice": round(tp, 8),
    }
    # FIXED: use retry wrapper so transient network failures don't silently drop orders
    order_data, err = mexc_request_with_retry("POST", "/order/submit", order_params)
    if err: return False, f"Order failed: {err}"

    order_id = order_data if isinstance(order_data, str) else (order_data.get("orderId","") if isinstance(order_data, dict) else "")

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

def close_trade(symbol, reason="Manual"):
    with trade_lock:
        if symbol not in open_trades:
            return False, "No open trade found"
        trade = open_trades[symbol]

    side = 2 if trade["direction"] == "BUY" else 1
    params = {
        "symbol":    symbol,
        "price":     0,
        "vol":       trade["size"],
        "side":      side,
        "type":      5,
        "openType":  1,
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


# ════════ FIXED: PAPER TRADING ENGINE ═══════════════════════════════
# Key fixes:
#   1. Realistic MEXC taker fee (0.01%) applied on open AND close
#   2. Realistic slippage simulation (0.01–0.05% random)
#   3. PnL uses actual futures formula: contracts * contract_size * price_diff
#   4. pnl_pct is relative to risk_amount (same as live P&L display)
#   5. Full logging for every event (open, update, SL hit, TP hit, manual)
#   6. Position sizing matches live trading exactly

# MEXC fee constants (taker fee = 0.01% per side)
PAPER_TAKER_FEE = 0.0001   # 0.01% of notional
PAPER_MAX_SLIP  = 0.0005   # 0.05% max slippage (one way)


def _simulate_fill_price(price, direction, is_entry=True):
    """
    Simulate realistic fill price with tiny random slippage.
    Entry: fills slightly worse than signal price (spread + impact).
    Exit:  fills slightly worse than trigger price.
    """
    slip_pct = random.uniform(0.00005, PAPER_MAX_SLIP)  # 0.005% – 0.05%
    if direction == "BUY":
        # BUY entry: fill above signal; BUY exit (TP/SL): also fills at or above SL/TP
        return round(price * (1 + slip_pct), 8)
    else:
        # SELL entry: fill below signal; SELL exit: at or below SL/TP
        return round(price * (1 - slip_pct), 8)


def place_paper_order(sig):
    """
    FIXED: Place a realistic simulated paper trade.
    - Uses live price from MEXC as entry (NOT signal price — signal may be stale)
    - Applies taker fee on open
    - Sizes positions identically to live trading
    """
    with paper_lock:
        if not paper_config["enabled"]:
            return False, "Paper trading disabled"
        if not paper_config["auto_trade"]:
            return False, "Paper auto-trade disabled"
        if len(paper_trades) >= paper_config["max_trades"]:
            return False, f"Max paper trades ({paper_config['max_trades']}) reached"
        if sig["symbol"] in paper_trades:
            return False, f"Already have paper trade on {sig['symbol']}"

        balance = paper_config["balance"]

    # FIXED: Get LIVE price for entry — never use potentially stale signal price
    ticker = get_ticker(sig["symbol"])
    if not ticker:
        return False, f"Could not fetch live price for {sig['symbol']}"
    live_price = ticker["price"]
    signal_entry = float(sig["entry"])

    # FIXED: Use live price as entry; warn if too far from signal
    price_diff_pct = abs(live_price - signal_entry) / signal_entry * 100 if signal_entry > 0 else 0
    if price_diff_pct > 1.0:
        log(f"⚠️ [PAPER] {sig['symbol']} signal entry {signal_entry} vs live {live_price} "
            f"({price_diff_pct:.2f}% apart) — using live price")

    direction = sig["direction"]
    # FIXED: simulate realistic fill (slippage)
    entry = _simulate_fill_price(live_price, direction, is_entry=True)
    sl    = float(sig["sl"])
    tp    = float(sig["tp"])

    # Recalculate sl/tp validity against actual fill price
    if direction == "BUY" and sl >= entry:
        return False, f"SL {sl} >= entry {entry} for BUY — invalid"
    if direction == "SELL" and sl <= entry:
        return False, f"SL {sl} <= entry {entry} for SELL — invalid"

    with paper_lock:
        risk_amount = max(balance * paper_config["risk_pct"] / 100, 0.10)
        sl_distance = abs(entry - sl)
        if sl_distance <= 0:
            return False, "SL distance is zero"

        sym_info      = get_symbol_info(sig["symbol"])
        contract_size = max(sym_info["contract_size"], 1e-8)
        min_vol       = max(sym_info["min_vol"], 1)
        leverage      = trade_config.get("leverage", 35)

        # Size contracts so: contracts * contract_size * sl_distance ≈ risk_amount
        size_raw  = risk_amount / (sl_distance * contract_size)
        size_raw  = max(min_vol, round(size_raw / min_vol) * min_vol)
        contracts = int(size_raw)

        # Safety: scale down if actual loss exceeds risk budget
        actual_max_loss = contracts * contract_size * sl_distance
        if actual_max_loss > risk_amount * 1.10:
            contracts = max(int(min_vol), int(contracts * (risk_amount / actual_max_loss)))

        position_value = contracts * contract_size * entry
        margin_used    = round(position_value / leverage, 4) if leverage > 0 else risk_amount

        # FIXED: Apply taker fee on entry (deducted from balance immediately)
        open_fee  = position_value * PAPER_TAKER_FEE
        paper_config["balance"] = round(paper_config["balance"] - open_fee, 2)

        paper_trades[sig["symbol"]] = {
            "symbol":         sig["symbol"],
            "direction":      direction,
            "entry":          round(entry, 8),
            "signal_entry":   signal_entry,          # for comparison logging
            "current_price":  round(entry, 8),
            "sl":             sl,
            "tp":             tp,
            "size":           contracts,
            "contract_size":  contract_size,
            "leverage":       leverage,
            "position_value": round(position_value, 4),
            "margin_used":    round(margin_used, 4),
            "risk_amount":    round(risk_amount, 2),
            "open_fee":       round(open_fee, 4),
            "rr":             sig["rr"],
            "score":          sig["score"],
            "grade":          sig["grade"],
            "model":          sig.get("model","1"),
            "tf":             sig.get("tf","–"),
            "ob_zone":        sig.get("ob_zone","–"),
            "pnl":            0.0,
            "pnl_pct":        0.0,
            "opened_at":      datetime.now(LOCAL_TZ).strftime("%Y-%m-%d %H:%M UTC+1"),
            "status":         "OPEN",
        }

    log(
        f"📝 [PAPER OPEN] {direction} {sig['symbol']} | "
        f"Entry:{round(entry,8)} (signal was {signal_entry}) | "
        f"SL:{sl} TP:{tp} RR:{sig['rr']}R | "
        f"Size:{contracts} contracts | "
        f"PosnValue:${round(position_value,2)} | "
        f"Margin:${round(margin_used,2)} | "
        f"OpenFee:${round(open_fee,4)} | "
        f"Risk:${round(risk_amount,2)} | "
        f"Balance:${round(paper_config['balance'],2)}"
    )
    return True, f"Paper trade placed on {sig['symbol']} @ {round(entry,8)}"


# FIXED: close_paper_trade — correct PnL formula, fees, slippage, detailed logging
def close_paper_trade(symbol, reason="Manual", close_price=None):
    """
    FIXED: Close a paper trade with realistic PnL calculation.
    - Futures PnL = contracts * contract_size * (exit - entry) [for BUY]
    - Taker fee applied on close (in addition to open fee)
    - Slippage applied to exit price
    - PnL capped at -(margin_used) — cannot lose more than margin
    - pnl_pct shown relative to risk_amount for meaningful display
    """
    with paper_lock:
        if symbol not in paper_trades:
            return False, "No paper trade found"
        trade = dict(paper_trades[symbol])

    # FIXED: For SL/TP hits use exact SL/TP price; for manual fetch live price
    if close_price is None:
        ticker = get_ticker(symbol)
        if ticker:
            raw_close = ticker["price"]
        else:
            raw_close = trade["entry"]   # fallback — rare
        # Apply slippage on manual/live closes
        close_price = _simulate_fill_price(raw_close, trade["direction"], is_entry=False)
    else:
        # SL/TP hit — already at exact level, apply tiny slippage
        close_price = _simulate_fill_price(close_price, trade["direction"], is_entry=False)

    entry         = trade["entry"]
    size          = trade["size"]
    direction     = trade["direction"]
    contract_size = max(trade.get("contract_size", 1.0), 1e-8)
    risk_amount   = max(trade.get("risk_amount", 1.0), 0.01)
    leverage      = max(trade.get("leverage", trade_config.get("leverage", 35)), 1)
    open_fee      = trade.get("open_fee", 0.0)

    # FIXED: Correct futures PnL formula
    if direction == "BUY":
        raw_pnl = size * contract_size * (close_price - entry)
    else:
        raw_pnl = size * contract_size * (entry - close_price)

    # FIXED: Close fee based on exit notional
    exit_notional = size * contract_size * close_price
    close_fee     = exit_notional * PAPER_TAKER_FEE

    # FIXED: Net PnL = raw_pnl minus both fees
    net_pnl = raw_pnl - close_fee

    # FIXED: Cap loss at margin_used (cannot lose more than posted margin)
    margin_used = trade.get("margin_used", risk_amount)
    net_pnl     = max(net_pnl, -margin_used)

    # FIXED: pnl_pct relative to risk_amount (shows realistic R-multiple)
    pnl_pct = round((net_pnl / risk_amount) * 100, 2) if risk_amount > 0 else 0.0

    # Guard against impossible PnL (sanity check)
    rr = trade.get("rr", 3.0)
    max_realistic_pnl = risk_amount * (rr + 0.5)   # allow slight overflow
    if net_pnl > max_realistic_pnl:
        log(f"⚠️ [PAPER] Capping unrealistic PnL: {net_pnl:.2f} → {max_realistic_pnl:.2f} on {symbol}")
        net_pnl = max_realistic_pnl
        pnl_pct = round((net_pnl / risk_amount) * 100, 2)

    with paper_lock:
        paper_config["balance"] = round(paper_config["balance"] + net_pnl, 2)
        completed = dict(paper_trades[symbol])
        completed.update({
            "status":      f"CLOSED ({reason})",
            "close_price": round(close_price, 8),
            "raw_pnl":     round(raw_pnl, 4),
            "close_fee":   round(close_fee, 4),
            "pnl":         round(net_pnl, 2),
            "pnl_pct":     pnl_pct,
            "closed_at":   datetime.now(LOCAL_TZ).strftime("%Y-%m-%d %H:%M UTC+1"),
        })
        paper_history.appendleft(completed)
        del paper_trades[symbol]

        paper_stats["total"]     += 1
        if net_pnl > 0: paper_stats["wins"]   += 1
        else:           paper_stats["losses"] += 1
        paper_stats["total_pnl"] = round(paper_stats["total_pnl"] + net_pnl, 2)

    sign = "+" if net_pnl >= 0 else ""
    result = "WIN 🟢" if net_pnl > 0 else "LOSS 🔴"
    log(
        f"📝 [PAPER CLOSE] {result} {direction} {symbol} | "
        f"Entry:{entry} Exit:{round(close_price,8)} | "
        f"Gross:{sign}{round(raw_pnl,2)} Fee:{round(close_fee,4)} "
        f"Net:{sign}{round(net_pnl,2)} USDT ({sign}{pnl_pct}%) | "
        f"Reason:{reason} | Balance:${paper_config['balance']:.2f}"
    )
    return True, f"Paper trade closed. PnL: {sign}{round(net_pnl,2)} USDT ({sign}{pnl_pct}%)"


# FIXED: paper_monitor_loop — correct SL/TP triggering, realistic PnL updates
def paper_monitor_loop():
    """
    FIXED: Background thread watching paper positions for SL/TP hits.
    - Uses bid/ask logic: BUY SL triggered when price trades AT or BELOW SL
    - Uses correct futures PnL formula
    - pnl_pct shown as % of risk_amount (not % of position — avoids inflated numbers)
    - Fees deducted during live PnL display so dashboard shows net P&L
    - Logs unrealised P&L every 5 minutes for monitoring
    """
    log("📝 [PAPER] Paper trading monitor started — checking every 10s")
    last_status_log = time.time()
    while True:
        try:
            with paper_lock:
                symbols = list(paper_trades.keys())

            for symbol in symbols:
                with paper_lock:
                    if symbol not in paper_trades: continue
                    trade = dict(paper_trades[symbol])

                ticker = get_ticker(symbol)
                if not ticker:
                    continue   # network blip — skip this tick, don't close
                price = ticker["price"]

                entry         = trade["entry"]
                size          = trade["size"]
                direction     = trade["direction"]
                sl            = trade["sl"]
                tp            = trade["tp"]
                contract_size = max(trade.get("contract_size", 1.0), 1e-8)
                risk_amount   = max(trade.get("risk_amount", 1.0), 0.01)
                leverage      = max(trade.get("leverage", trade_config.get("leverage", 35)), 1)

                # FIXED: Correct futures unrealised PnL
                if direction == "BUY":
                    raw_pnl = size * contract_size * (price - entry)
                else:
                    raw_pnl = size * contract_size * (entry - price)

                # FIXED: Deduct estimated close fee for realistic display
                exit_notional  = size * contract_size * price
                est_close_fee  = exit_notional * PAPER_TAKER_FEE
                net_pnl        = raw_pnl - est_close_fee

                # FIXED: Cap loss at margin (never show loss > margin)
                margin_used    = trade.get("margin_used", risk_amount)
                net_pnl        = max(net_pnl, -margin_used)

                # FIXED: pnl_pct as % of risk_amount for meaningful display
                pnl_pct = round((net_pnl / risk_amount) * 100, 2) if risk_amount > 0 else 0.0

                with paper_lock:
                    if symbol in paper_trades:
                        paper_trades[symbol]["current_price"] = round(price, 8)
                        paper_trades[symbol]["pnl"]           = round(net_pnl, 2)
                        paper_trades[symbol]["pnl_pct"]       = pnl_pct

                # FIXED: Proper SL/TP trigger logic
                # BUY: SL when price trades at or below SL; TP when at or above TP
                # SELL: SL when price trades at or above SL; TP when at or below TP
                if direction == "BUY":
                    if price <= sl:
                        log(f"🛑 [PAPER] SL HIT: {symbol} BUY | SL:{sl} Price:{price}")
                        close_paper_trade(symbol, "SL Hit", sl)
                    elif price >= tp:
                        log(f"🎯 [PAPER] TP HIT: {symbol} BUY | TP:{tp} Price:{price}")
                        close_paper_trade(symbol, "TP Hit", tp)
                else:
                    if price >= sl:
                        log(f"🛑 [PAPER] SL HIT: {symbol} SELL | SL:{sl} Price:{price}")
                        close_paper_trade(symbol, "SL Hit", sl)
                    elif price <= tp:
                        log(f"🎯 [PAPER] TP HIT: {symbol} SELL | TP:{tp} Price:{price}")
                        close_paper_trade(symbol, "TP Hit", tp)

            # Log unrealised positions every 5 minutes
            if time.time() - last_status_log > 300 and symbols:
                with paper_lock:
                    open_syms = list(paper_trades.keys())
                if open_syms:
                    for sym in open_syms:
                        with paper_lock:
                            t = paper_trades.get(sym, {})
                        if t:
                            sign = "+" if t.get("pnl",0) >= 0 else ""
                            log(f"📊 [PAPER STATUS] {t['direction']} {sym} | "
                                f"Entry:{t['entry']} Now:{t.get('current_price','?')} | "
                                f"PnL:{sign}{t.get('pnl',0):.2f} ({sign}{t.get('pnl_pct',0):.1f}%) | "
                                f"SL:{t['sl']} TP:{t['tp']}")
                last_status_log = time.time()

        except Exception as e:
            log(f"❌ [PAPER] Monitor error: {e}")

        time.sleep(10)   # FIXED: 10s poll (was 15s) for faster SL/TP reaction


# ════════ MANIPULATION PHASE MONITOR ════════════════════════════════

def detect_manip_phase(candles, direction, crt_tf="Hour4"):
    """
    Detect C2 (manipulation candle) that is CURRENTLY FORMING and:
    - Has already swept below CRL (bull) or above CRH (bear) with its body
    - Has NOT yet closed back inside the CRT range (still in manipulation)
    - Has between 1 and 40 minutes REMAINING before the candle closes

    Only looks at the LAST candle (index -1) as active C2.
    Uses candle open time + TF duration to compute time remaining.
    """
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
    if not (60 <= secs_left <= 2400):
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

def detect_manip_phase_live(candles, direction, tf_name, min_mins=1, max_mins=40):
    """
    Detect if the CURRENTLY FORMING candle is in manipulation phase
    with 1–40 minutes remaining before it closes.
    The last candle in the series is treated as the live, still-forming C2.
    """
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


def manip_monitor_loop():
    log("🔍 M1 Manipulation monitor started")
    while True:
        try:
            with manip_lock:
                symbols = list(manip_monitor.keys())

            for symbol in symbols:
                with manip_lock:
                    if symbol not in manip_monitor: continue
                    monitor = dict(manip_monitor[symbol])

                phase = monitor.get("phase", "MANIPULATION")

                # ── PHASE 2: TBS already confirmed, wait for price to tap TBS open ──
                if phase == "AWAIT_PRICE_TAP":
                    tbs_entry = monitor.get("tbs_entry")
                    direction = monitor.get("direction","BUY")
                    if not tbs_entry:
                        with manip_lock: manip_monitor.pop(symbol, None)
                        continue
                    ticker = get_ticker(symbol)
                    if not ticker:
                        continue
                    price = ticker["price"]
                    # Tolerance: 0.05% of tbs_entry — close enough to fire
                    tol = tbs_entry * 0.0005
                    tapped = (price <= tbs_entry + tol) if direction == "BUY" else (price >= tbs_entry - tol)
                    if not tapped:
                        continue   # price not there yet, keep watching

                    log(f"🎯 M1 PRICE TAPPED TBS OPEN: {symbol} {direction} price={price} entry={tbs_entry}")
                    sl_final  = monitor.get("sl_final", tbs_entry)
                    tp        = monitor.get("tp", tbs_entry)
                    rr        = monitor.get("rr", 0)
                    grade     = monitor.get("grade", "A")
                    zone_name = monitor.get("zone_name","–")
                    trend     = monitor.get("trend","NEUTRAL")
                    kl_type   = monitor.get("kl_type","KL")
                    crt_tf    = monitor.get("crt_tf","Hour4")
                    tbs_tf    = monitor.get("tbs_tf","–")

                    sig = {
                        "symbol":      symbol,
                        "tf":          crt_tf,
                        "ob_tf":       monitor.get("ob_tf","–"),
                        "ob_zone":     zone_name,
                        "zone_type":   kl_type,
                        "direction":   direction,
                        "trend":       trend,
                        "entry":       round(price, 8),   # market — use live price
                        "entry_type":  "Model #1 (TBS Tap → Market)",
                        "sl":          sl_final,
                        "tp":          tp,
                        "rr":          rr,
                        "crh":         monitor.get("crh", tp),
                        "crl":         monitor.get("crl", sl_final),
                        "ob_top":      monitor.get("zone_top","–"),
                        "ob_bot":      monitor.get("zone_bot","–"),
                        "score":       85 if grade=="A+" else 75 if grade=="A" else 60,
                        "grade":       grade,
                        "details":     [f"✅ M1 Manip+TBS confirmed","✅ Price tapped TBS open",f"TBS:{tbs_tf} | RR:{rr}R"],
                        "tbs_found":   True, "tbs_tf": tbs_tf,
                        "tbs_entry":   tbs_entry, "tbs_sl": sl_final,
                        "fvg_found":   False, "fvg_type":"–","fvg_entry":"–","fvg_top":"–","fvg_bot":"–",
                        "choch_found": False, "choch_level":"–",
                        "liq_swept":   False, "ob_respected":False, "continuous":True,
                        "from_monitor":True, "market_order": True,
                        "model":       "1",
                        "timestamp":   datetime.now(LOCAL_TZ).strftime("%Y-%m-%d %H:%M UTC+1"),
                    }
                    signals.appendleft(sig)
                    send_telegram(fmt_tg(sig))
                    log(f"🚀 M1 MARKET ORDER: {direction} {symbol} | {grade} | {rr}R")

                    ready, reason = live_trade_ready()
                    if ready:
                        ok, msg = place_order(sig)
                        log(f"{'✅' if ok else '❌'} M1 market order: {msg}")
                        if not ok:
                            log(f"[REJECTED] {symbol} {direction}: {msg}")
                            send_telegram(f"Trade REJECTED: {symbol} {direction}\nReason: {msg}", kind="trade")
                    else:
                        log(f"⚠️ Live trade skipped ({symbol}): {reason}")

                    if paper_config["enabled"] and paper_config["auto_trade"]:
                        ok2, msg2 = place_paper_order(sig)
                        if ok2: log(f"📝 Paper auto: {msg2}")

                    with manip_lock:
                        manip_monitor.pop(symbol, None)
                    continue

                # ── PHASE 1: MANIPULATION — wait for C2 to close back inside range ──
                tf = monitor.get("crt_tf","Hour4")
                candles = get_candles(symbol, tf, limit=30)
                if candles:
                    crh = monitor["crh"]; crl = monitor["crl"]
                    direction = monitor["direction"]
                    c2 = monitor.get("c2",{})
                    if direction == "BUY":
                        body_low = min(c2.get("open",0), c2.get("close",0))
                        if body_low < crl and c2.get("close",crl) < crl:
                            with manip_lock:
                                manip_monitor.pop(symbol, None)
                            log(f"❌ INVALIDATED: {symbol} — C2 body closed below CRL")
                            continue
                    else:
                        body_high = max(c2.get("open",0), c2.get("close",0))
                        if body_high > crh and c2.get("close",crh) > crh:
                            with manip_lock:
                                manip_monitor.pop(symbol, None)
                            log(f"❌ INVALIDATED: {symbol} — C2 body closed above CRH")
                            continue

                completed, fresh_candles = check_manip_completed(symbol, monitor)
                if not completed:
                    continue

                log(f"✅ MANIP COMPLETE: {symbol} {monitor.get('direction')} — searching TBS...")

                crt_tf = monitor.get("crt_tf","Hour4")
                crh = monitor["crh"]; crl = monitor["crl"]
                direction = monitor.get("direction","BUY")
                tbs_found, tbs_tf, tbs_entry, tbs_sl = check_tbs(
                    symbol, direction, crl, crh, crt_tf)

                if not tbs_found:
                    log(f"⏳ {symbol} — TBS not yet confirmed, still watching")
                    continue

                log(f"🐢 TBS CONFIRMED: {symbol} on {tbs_tf} | Entry:{tbs_entry} — now watching for price to tap TBS open")

                # TP = opposite CRT level (CRH for BUY, CRL for SELL)
                tp = round(crh, 8) if direction=="BUY" else round(crl, 8)
                # SL = swing extreme of the HTF CRT C2 manipulation candle
                c2_mon = monitor.get("c2", {})
                if direction == "BUY":
                    sl_final = round(c2_mon.get("low", tbs_sl), 8)
                else:
                    sl_final = round(c2_mon.get("high", tbs_sl), 8)
                risk   = abs(tbs_entry - sl_final)
                reward = abs(tp - tbs_entry)
                rr     = round(reward/risk, 2) if risk > 0 else 0
                log(f"   SL:{sl_final} | TP:{tp} | RR:{rr}R")

                if rr < 2.0:
                    log(f"⚠️ {symbol} RR too low ({rr}R) after TBS — skipping")
                    with manip_lock: manip_monitor.pop(symbol, None)
                    continue

                zone_name = monitor.get("zone_name","–")
                trend     = monitor.get("trend","NEUTRAL")
                kl_type   = monitor.get("kl_type","KL")
                has_pd    = "DISCOUNT" in zone_name or "PREMIUM" in zone_name
                grade     = "A+" if has_pd and rr >= 3.0 else "A" if rr >= 3.0 else "B"

                # Store TBS data back into monitor — wait for price to tap tbs_entry
                with manip_lock:
                    manip_monitor[symbol].update({
                        "phase":      "AWAIT_PRICE_TAP",
                        "tbs_entry":  round(tbs_entry, 8),
                        "tbs_tf":     tbs_tf,
                        "sl_final":   sl_final,
                        "tp":         round(tp, 8),
                        "rr":         rr,
                        "grade":      grade,
                        "zone_name":  zone_name,
                        "trend":      trend,
                        "kl_type":    kl_type,
                    })
                log(f"👁 M1 AWAIT TAP: {symbol} watching for price to reach {tbs_entry}")
                continue   # keep in monitor — price tap check below handles firing

        except Exception as e:
            log(f"❌ Manip monitor error: {e}")

        time.sleep(10)


# FIXED: scanner_loop — one failed pair does not stop the cycle;
#         rate-limit aware sleep between pairs; robust error handling
def scanner_loop():
    with scan_lock: scan_state["running"] = True
    log("🚀 Mad Man Strategy Scanner started — scanning USDT perpetual pairs")
    consecutive_empty = 0   # FIXED: track repeated empty fetches
    while True:
        try:
            with scan_lock:
                if not scan_state["enabled"]:
                    scan_state["running"] = False
            if not scan_state["enabled"]:
                time.sleep(5); continue
            with scan_lock: scan_state["running"] = True

            pairs = get_all_pairs()
            if not pairs:
                consecutive_empty += 1
                wait = min(30 * consecutive_empty, 300)   # back off up to 5min
                log(f"⚠️ No pairs fetched (attempt {consecutive_empty}) — retrying in {wait}s")
                time.sleep(wait)
                continue
            consecutive_empty = 0   # reset on success

            with scan_lock:
                scan_state["total_pairs"]=len(pairs)
                scan_state["pairs_done"]=0
                scan_state["scan_count"]+=1

            log(f"🔄 Scan #{scan_state['scan_count']} — {len(pairs)} USDT pairs")

            scanned_this_cycle = set()  # Each pair scanned at most once per cycle

            for i,symbol in enumerate(pairs):
                if not scan_state["enabled"]: break

                # Skip pairs already scanned in this cycle
                if symbol in scanned_this_cycle:
                    continue
                scanned_this_cycle.add(symbol)

                # Skip pairs already queued in the manipulation monitor — they are
                # being watched; re-scanning them would generate duplicate signals
                with manip_lock:
                    already_in_monitor = symbol in manip_monitor
                if already_in_monitor:
                    log(f"⏩ {symbol} — already in monitor queue, skipping scan")
                    time.sleep(1)
                    continue

                with scan_lock:
                    scan_state["current_pair"]=symbol
                    scan_state["pairs_done"]=i+1
                try:
                    m1 = scan_pair(symbol)        if scan_settings.get("model1_enabled", True) else []
                    m2 = scan_pair_model2(symbol) if scan_settings.get("model2_enabled", True) else []
                    all_res = m1 + m2
                    for sig in all_res:
                        m = sig.get("model","1")
                        recent_sigs = list(signals)[:50]
                        duplicate = any(
                            s.get("symbol")==sig["symbol"] and
                            s.get("direction")==sig["direction"] and
                            s.get("tf")==sig["tf"] and
                            s.get("model","1")==m
                            for s in recent_sigs
                        )
                        if duplicate:
                            log(f"⏭ SKIP dup M#{m}: {sig['direction']} {symbol} {sig['tf']}")
                            continue
                        diag["passed"]+=1
                        signals.appendleft(sig)
                        with scan_lock: scan_state["signals_found"]+=1
                        tf_lbl={"Day1":"1D","Hour4":"4H","Hour3":"3H","Hour2":"2H","Min60":"1H"}.get(sig["tf"],"–")
                        if m=="2":
                            log(f"🔥 M#2 {sig['direction']} {symbol} | HTF:{tf_lbl} | LTF:{sig['ob_tf']} | Score:{sig['score']} {sig['grade']} | RR:{sig['rr']}R")
                            send_telegram(fmt_tg_m2(sig), kind="signal")
                        else:
                            log(f"🎯 M#1 {sig['direction']} {symbol} | {tf_lbl} | OB:{sig['ob_tf']} | Score:{sig['score']} {sig['grade']} | RR:{sig['rr']}R | TBS:{sig['tbs_tf']}")
                            send_telegram(fmt_tg(sig), kind="signal")
                        ready, reason = live_trade_ready()
                        if ready:
                            res_ok, res_msg = place_order(sig)
                            if not res_ok:
                                log(f"[REJECTED] {sig['symbol']} {sig['direction']}: {res_msg}")
                                send_telegram(f"Trade REJECTED: {sig['symbol']} {sig['direction']}\nReason: {res_msg}", kind="trade")
                            log(f"{'✅' if res_ok else '❌'} Auto-trade: {res_msg}")
                        else:
                            log(f"⚠️ Live trade skipped ({sig['symbol']}): {reason}")
                        if paper_config["enabled"] and paper_config["auto_trade"]:
                            res_ok2, res_msg2 = place_paper_order(sig)
                            if res_ok2: log(f"📝 Paper auto: {res_msg2}")
                except Exception as e:
                    # FIXED: One pair error never crashes the cycle
                    log(f"⚠️ [Scan] Error on {symbol}: {type(e).__name__}: {e}")
                # FIXED: Rate-limit aware sleep between pairs
                sleep_s = max(0.3, scan_settings.get("scan_interval", 1))
                time.sleep(sleep_s)
                if (i+1) % 50 == 0:
                    log(f"📊 Progress: {i+1}/{scan_state['total_pairs']} pairs scanned")

            with scan_lock: scan_state["last_scan"]=datetime.now(LOCAL_TZ).strftime("%H:%M UTC+1")
            log(f"✅ Scan #{scan_state['scan_count']} complete — {len(pairs)} pairs | "
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
            # FIXED: Outer catch — log fully and keep running
            import traceback
            log(f"❌ [Scanner] Outer error: {type(e).__name__}: {e}")
            log(f"❌ [Scanner] Traceback: {traceback.format_exc()[-300:]}")
            time.sleep(15)

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
body{font-family:'Nunito',sans-serif;background:var(--bg);color:var(--text);min-height:100vh;padding-bottom:80px}
body::before{content:'';position:fixed;inset:0;background:repeating-linear-gradient(0deg,transparent,transparent 3px,rgba(0,0,0,.025) 3px,rgba(0,0,0,.025) 4px);pointer-events:none;z-index:998}
.bg-glow{position:fixed;inset:0;pointer-events:none;z-index:0}
.bg-glow::before{content:'';position:absolute;width:600px;height:600px;border-radius:50%;background:radial-gradient(circle,rgba(124,58,237,.12),transparent 70%);top:-200px;left:-200px}
.bg-glow::after{content:'';position:absolute;width:500px;height:500px;border-radius:50%;background:radial-gradient(circle,rgba(219,39,119,.1),transparent 70%);bottom:-150px;right:-150px}
.hdr{background:rgba(12,11,24,.95);border-bottom:2px solid var(--border);position:sticky;top:0;z-index:200;backdrop-filter:blur(20px)}
.hdr-glow{position:absolute;bottom:-1px;left:0;right:0;height:1px;background:linear-gradient(90deg,transparent,var(--purple),var(--pink),transparent);opacity:.5}
.hdr-in{max-width:1360px;margin:0 auto;padding:0 20px;height:60px;display:flex;align-items:center;justify-content:space-between;gap:14px}
.brand{display:flex;align-items:center;gap:11px}
.brand-icon{font-size:1.7rem;animation:rock 3s ease-in-out infinite;filter:drop-shadow(0 0 8px rgba(124,58,237,.6))}
@keyframes rock{0%,100%{transform:rotate(-8deg)}50%{transform:rotate(8deg)}}
.brand-name{font-family:'Fredoka One',sans-serif;font-size:1.18rem;letter-spacing:.04em;color:#c4b5fd;line-height:1.2}
.brand-sub{font-family:'JetBrains Mono',monospace;font-size:.52rem;color:var(--dim);letter-spacing:.08em}
.scan-pill{display:flex;align-items:center;gap:7px;background:rgba(16,185,129,.08);border:1.5px solid rgba(16,185,129,.22);border-radius:20px;padding:6px 14px}
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
.prog{background:rgba(12,11,24,.9);border-bottom:1px solid var(--border);padding:8px 20px;position:relative;z-index:10}
.prog-in{max-width:1360px;margin:0 auto;display:flex;align-items:center;gap:14px}
.prog-lbl{font-family:'JetBrains Mono',monospace;font-size:.62rem;color:var(--dim);white-space:nowrap;min-width:200px;overflow:hidden;text-overflow:ellipsis}
.prog-track{flex:1;height:6px;background:var(--s3);border-radius:3px;overflow:hidden}
.prog-fill{height:100%;background:linear-gradient(90deg,var(--purple),var(--pink),var(--blue));border-radius:3px;transition:width .5s ease}
.prog-cnt{font-family:'JetBrains Mono',monospace;font-size:.62rem;color:var(--dim);white-space:nowrap}
.sec{max-width:1360px;margin:20px auto 0;padding:0 20px;position:relative;z-index:1}
.sec-hdr{display:flex;align-items:center;gap:10px;margin-bottom:11px}
.sec-ttl{font-family:'Fredoka One',sans-serif;font-size:1rem;letter-spacing:.06em;color:rgba(167,139,250,.8)}
.sec-line{flex:1;height:2px;background:linear-gradient(90deg,rgba(124,58,237,.3),transparent);border-radius:1px}
.sec-note{font-family:'JetBrains Mono',monospace;font-size:.56rem;color:var(--dim)}
.prices-grid{display:grid;grid-template-columns:repeat(6,1fr);gap:10px}
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
.tab-wrap{max-width:1360px;margin:20px auto 0;padding:0 20px;position:relative;z-index:1}
.tabs{display:flex;gap:5px;background:var(--s1);border:2px solid var(--border);border-radius:16px;padding:5px;margin-bottom:18px;overflow-x:auto}
.tab{flex:1;min-width:75px;padding:9px 8px;border:none;border-radius:12px;font-family:'Nunito',sans-serif;font-size:.76rem;font-weight:800;cursor:pointer;transition:all .2s;color:var(--dim);background:transparent;white-space:nowrap;text-align:center}
.tab:hover{color:var(--text)}.tab.active{background:linear-gradient(135deg,var(--purple),var(--pink));color:#fff;box-shadow:0 4px 16px rgba(124,58,237,.4)}
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
.toast{position:fixed;bottom:26px;left:50%;transform:translateX(-50%) translateY(100px);background:var(--s2);border:2px solid var(--border2);border-radius:16px;padding:12px 22px;font-family:'Nunito',sans-serif;font-size:.85rem;font-weight:800;box-shadow:0 18px 50px rgba(0,0,0,.6);opacity:0;transition:all .4s cubic-bezier(.34,1.56,.64,1);pointer-events:none;z-index:9999;white-space:nowrap}
.toast.show{transform:translateX(-50%) translateY(0);opacity:1}
.toast.bt{border-color:rgba(16,185,129,.4);color:var(--green)}.toast.st{border-color:rgba(239,68,68,.4);color:var(--red)}.toast.tt{border-color:rgba(249,115,22,.4);color:var(--orange)}.toast.pt{border-color:rgba(167,139,250,.4);color:#a78bfa}
@media(max-width:820px){.stats-grid{grid-template-columns:1fr 1fr 1fr}.prices-grid{grid-template-columns:repeat(3,1fr)}.hdr-in,.sec,.tab-wrap{padding:0 13px}.prog{padding:7px 13px}.snum{display:none}.lvl-grid{grid-template-columns:1fr 1fr}.trade-form{grid-template-columns:1fr}}
@media(max-width:480px){.stats-grid{grid-template-columns:1fr 1fr}.prices-grid{grid-template-columns:repeat(2,1fr)}}
</style>
</head>
<body>
<div class="bg-glow"></div>
<div class="pb" id="pb">⏸ SCANNER PAUSED — HIT RESUME! 🚀</div>
<header class="hdr">
  <div class="hdr-glow"></div>
  <div class="hdr-in">
    <div class="brand"><span class="brand-icon">📡</span><div><div class="brand-name">Mad Man Strategy Scanner</div><div class="brand-sub">MEXC USDT PERP · CROSS MARGIN · UP TO 500X</div></div></div>
    <div class="scan-pill"><div class="sdot" id="sdot"></div><span class="stxt" id="stxt">SCANNING...</span></div>
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
  <div class="prices-grid" id="pgrid"><div class="pc" style="min-height:70px"></div><div class="pc" style="min-height:70px"></div><div class="pc" style="min-height:70px"></div><div class="pc" style="min-height:70px"></div><div class="pc" style="min-height:70px"></div><div class="pc" style="min-height:70px"></div></div>
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
<div class="tab-wrap">
  <div class="tabs">
    <button class="tab active" onclick="sw('signals',this)">📊 Signals</button>
    <button class="tab" onclick="sw('trades',this)">💹 Live Trades</button>
    <button class="tab" onclick="sw('monitor',this)">👁 Monitor</button>
    <button class="tab" onclick="sw('history',this)">📜 History</button>
    <button class="tab" onclick="sw('trade-cfg',this)">🤖 Auto-Trade</button>
    <button class="tab" onclick="sw('paper',this)">📝 Paper Trade</button>
    <button class="tab" onclick="sw('log',this)">🖥️ Log</button>
    <button class="tab" onclick="sw('settings',this)">⚙️ Settings</button>
  </div>
  <!-- SIGNALS -->
  <div id="tab-signals">
    <div class="frow">
      <div class="ftitle">🎯 Mad Man Model #1</div>
      <div class="fgrp">
        <select class="fsel" id="fd" onchange="renderSigs()"><option value="">All</option><option value="BUY">🟢 BUY</option><option value="SELL">🔴 SELL</option></select>
        <select class="fsel" id="fg" onchange="renderSigs()"><option value="">All Grades</option><option value="A+">⭐ A+</option><option value="A">A</option><option value="B">B+</option></select>
        <select class="fsel" id="ftf" onchange="renderSigs()"><option value="">All TFs</option><option value="Day1">1D</option><option value="Hour4">4H</option><option value="Hour3">3H</option><option value="Hour2">2H</option><option value="Min60">1H</option></select>
      </div>
    </div>
    <div class="sig-list" id="slist"><div class="empty"><div class="empty-ico">🔭</div><div class="empty-t">Scanning the galaxy...</div><div class="empty-s">Hunting Mad Man Model #1 &amp; #2 setups. TBS (M#1) · FVG sweep (M#2). Min 2R. 🎯</div></div></div>
  </div>
  <!-- LIVE TRADES -->
  <div id="tab-trades" style="display:none">
    <div class="panel">
      <div class="panel-ttl">💹 Running Trades <span id="trades-count" style="font-family:'JetBrains Mono',monospace;font-size:.72rem;color:var(--dim)">(0)</span><button class="action-btn tb-chk" style="margin-left:auto;border:none;padding:6px 14px" onclick="fetchPnl()">🔄 Refresh</button></div>
      <div id="live-trades-wrap"><div class="empty" style="padding:40px"><div class="empty-ico">😴</div><div class="empty-t">No open trades</div></div></div>
    </div>
  </div>
  <!-- MONITOR -->
  <div id="tab-monitor" style="display:none">
    <div class="panel">
      <div class="panel-ttl">👁 Manipulation Monitor <span id="mon-count" style="font-family:'JetBrains Mono',monospace;font-size:.72rem;color:var(--dim)">(0/4)</span></div>
      <div id="monitor-wrap"><div class="empty" style="padding:40px"><div class="empty-ico">🌙</div><div class="empty-t">Nothing monitored yet</div><div class="empty-s">Pairs in manipulation phase appear here automatically</div></div></div>
    </div>
  </div>
  <!-- HISTORY -->
  <div id="tab-history" style="display:none">
    <div class="panel">
      <div class="panel-ttl">📜 Recent Trades (Last 10)</div>
      <div id="history-wrap"><div class="empty" style="padding:40px"><div class="empty-ico">📭</div><div class="empty-t">No completed trades yet</div></div></div>
    </div>
  </div>
  <!-- AUTO-TRADE -->
  <div id="tab-trade-cfg" style="display:none">
    <div class="panel">
      <div class="panel-ttl">🤖 Auto-Trade Settings <span id="trade-badge" style="font-size:.7rem;padding:3px 10px;border-radius:8px;background:rgba(239,68,68,.1);border:1.5px solid rgba(239,68,68,.3);color:var(--red);font-family:'JetBrains Mono',monospace;font-weight:700">DISABLED</span></div>
      <div class="trade-form">
        <div class="tf-group"><div class="tf-lbl">MEXC API Key</div><input class="tf-inp" type="text" id="t-apikey" placeholder="Your API key"/></div>
        <div class="tf-group"><div class="tf-lbl">MEXC Secret Key</div><input class="tf-inp" type="password" id="t-secret" placeholder="Your secret key"/></div>
        <div class="tf-group"><div class="tf-lbl">Risk per Trade (%)</div><input class="tf-inp" type="number" id="t-risk" value="1" min="0.1" max="100" step="0.1" oninput="checkRiskWarning()"/></div><div class="tf-group"><div class="tf-lbl">Leverage (10–500x) <span style="font-size:.7rem;color:var(--dim)">0 = auto-calculate</span></div><input class="tf-inp" type="number" id="t-leverage" value="0" min="0" max="500" step="1" placeholder="0 = auto"/><div style="font-size:.68rem;color:var(--dim);margin-top:4px;font-family:'JetBrains Mono',monospace">Auto: bot calculates leverage to risk exactly your % per trade · Manual: overrides auto</div></div>
        <div class="tf-group"><div class="tf-lbl">Max Simultaneous Trades</div><input class="tf-inp" type="number" id="t-max" value="3" min="1" max="10" step="1"/></div>
        <div class="tf-group"><div class="tf-lbl">Account Balance</div><div class="bal-chip">💰 $<span id="bal-val">–</span> USDT</div></div>
      </div>
      <div class="info-box info-blue">ℹ️ <b>Risk model:</b> Cross margin · Auto-leverage 10x–500x · SL capped at 100% of margin per trade</div>
      <div class="trade-actions">
        <button class="trade-btn tb-save" onclick="saveTradeConfig()">💾 Save</button>
        <button class="trade-btn tb-on" id="t-enable-btn" onclick="enableTrade(true)">▶ Enable</button>
        <button class="trade-btn tb-off" id="t-disable-btn" onclick="enableTrade(false)" style="display:none">⏹ Disable</button>
        <button class="trade-btn tb-chk" onclick="fetchBalance()">🔄 Balance</button>
      </div>
      <div class="t-status" id="trade-msg"></div>
      <div class="t-status" id="risk-warn" style="margin-top:6px"></div>
      <div class="info-box info-red">⚠️ Real money risk. The bot uses cross margin with auto-calculated leverage (up to 500x). SL never exceeds 100% of your 20% margin. Start with 0.5–1% risk setting and monitor closely.</div>
    </div>
    <!-- Live open positions (mirrored from trades tab) -->
    <div class="panel">
      <div class="panel-ttl">💹 Open Positions <span id="live-pos-count" style="font-family:'JetBrains Mono',monospace;font-size:.72rem;color:var(--dim)">(0)</span><button class="action-btn tb-chk" style="margin-left:auto;border:none;padding:6px 14px" onclick="fetchTradeCfgData()">🔄 Refresh</button></div>
      <div id="live-trades-wrap2"><div class="empty" style="padding:40px"><div class="empty-ico">😴</div><div class="empty-t">No open trades</div></div></div>
    </div>
    <!-- Live trade history (mirrored from history tab) -->
    <div class="panel">
      <div class="panel-ttl">📜 Trade History</div>
      <div id="history-wrap2"><div class="empty" style="padding:40px"><div class="empty-ico">📭</div><div class="empty-t">No completed trades yet</div></div></div>
    </div>
  </div>
  <!-- PAPER TRADING -->
  <div id="tab-paper" style="display:none">
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
    <!-- Paper stats -->
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
    <!-- Open paper positions -->
    <div class="panel">
      <div class="panel-ttl">📂 Open Paper Positions <span id="paper-trades-count" style="font-family:'JetBrains Mono',monospace;font-size:.72rem;color:var(--dim)">(0)</span></div>
      <div id="paper-trades-wrap"><div class="empty" style="padding:40px"><div class="empty-ico">📝</div><div class="empty-t">No open paper trades</div><div class="empty-s">Enable paper trading and turn on auto-trade to place trades from signals automatically</div></div></div>
    </div>
    <!-- Paper trade history -->
    <div class="panel">
      <div class="panel-ttl">📜 Paper Trade History</div>
      <div id="paper-history-wrap"><div class="empty" style="padding:30px"><div class="empty-ico">📭</div><div class="empty-t">No paper trades yet</div></div></div>
    </div>
  </div>
  <!-- LOG -->
  <div id="tab-log" style="display:none">
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
  <!-- SETTINGS -->
  <div id="tab-settings" style="display:none">
    <!-- Account -->
    <div class="panel">
      <div class="panel-ttl">🔑 MEXC API Keys</div>
      <div class="trade-form">
        <div class="tf-group">
          <div class="tf-lbl">API Key</div>
          <input class="tf-inp" type="text" id="s-apikey" placeholder="Your MEXC API key"/>
        </div>
        <div class="tf-group">
          <div class="tf-lbl">Secret Key</div>
          <div style="display:flex;gap:8px;align-items:center">
            <input class="tf-inp" type="password" id="s-secret" placeholder="Enter new secret to update" style="flex:1"/>
            <button class="trade-btn tb-chk" style="padding:9px 14px;white-space:nowrap" onclick="toggleSecretVis()">👁</button>
          </div>
          <div style="margin-top:6px;font-size:.72rem;color:var(--dim);font-family:'JetBrains Mono',monospace" id="s-secret-display"></div>
        </div>
        <div class="tf-group">
          <div class="tf-lbl">Risk per Trade (%)</div>
          <input class="tf-inp" type="number" id="s-risk" min="0.1" max="100" step="0.1" value="1"/>
        </div>
        <div class="tf-group">
          <div class="tf-lbl">Default Leverage (0 = auto-calculate)</div>
          <input class="tf-inp" type="number" id="s-leverage" min="0" max="500" step="1" value="0" placeholder="0 = auto"/>
          <div style="font-size:.68rem;color:var(--dim);margin-top:4px">Set to 0 for auto-leverage · Manual overrides bot calculation · Max 500x</div>
        </div>
        <div class="tf-group">
          <div class="tf-lbl">Account Balance</div>
          <div style="display:flex;gap:8px;align-items:center">
            <div class="bal-chip" style="flex:1">💰 $<span id="s-bal-val">–</span> USDT</div>
            <button class="trade-btn tb-chk" style="padding:9px 14px;white-space:nowrap" onclick="loadSettingsBalance()">🔄 Fetch</button>
          </div>
        </div>
      </div>
    </div>
    <!-- Telegram -->
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
          <div style="display:flex;gap:12px;margin-top:6px">
            <label style="display:flex;align-items:center;gap:8px;color:var(--text);font-size:.85rem;cursor:pointer">
              <input type="checkbox" id="s-tg-signals" style="width:18px;height:18px;accent-color:#a78bfa"/> Signal alerts
            </label>
            <label style="display:flex;align-items:center;gap:8px;color:var(--text);font-size:.85rem;cursor:pointer">
              <input type="checkbox" id="s-tg-trades" style="width:18px;height:18px;accent-color:#a78bfa"/> Trade alerts
            </label>
          </div>
        </div>
      </div>
    </div>
    <!-- Update Intervals -->
    <div class="panel">
      <div class="panel-ttl">⚡ Update Intervals</div>
      <div class="trade-form">
        <div class="tf-group">
          <div class="tf-lbl">Live Price Refresh (seconds)</div>
          <input class="tf-inp" type="number" id="s-price-int" min="1" max="60" step="1" value="1"/>
        </div>
        <div class="tf-group">
          <div class="tf-lbl">Scan Interval — delay between pairs (seconds)</div>
          <input class="tf-inp" type="number" id="s-scan-int" min="1" max="30" step="1" value="1"/>
        </div>
        <div class="tf-group">
          <div class="tf-lbl">Cycle Rest — pause after full scan (seconds)</div>
          <input class="tf-inp" type="number" id="s-cycle-rest" min="1" max="3600" step="1" value="5"/>
        </div>
      </div>
    </div>
    <!-- Save -->
    <div style="padding:0 4px 20px">
      <button class="trade-btn tb-save" style="width:100%;padding:14px;font-size:1rem" onclick="saveAllSettings()">💾 Save All Settings</button>
      <div class="t-status" id="settings-msg" style="margin-top:10px"></div>
    </div>
  </div>
</div>
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
        <div id="tc-exit-row" style="display:none;background:rgba(239,68,68,.07);border:1px solid rgba(239,68,68,.2);border-radius:10px;padding:7px 12px;margin-bottom:10px">
          <div style="font-family:'Nunito',sans-serif;font-size:.55rem;font-weight:700;color:rgba(239,68,68,.7);text-transform:uppercase;letter-spacing:.08em;margin-bottom:3px">EXIT PRICE</div>
          <div style="font-family:'JetBrains Mono',monospace;font-size:.85rem;font-weight:700;color:#ef4444" id="tc-exit-val">–</div>
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
let allSigs=[],toastT,activeTab='signals',tick=0,lastCount=0,paperAutoOn=false;
const $=id=>document.getElementById(id);
function toast(m,t,d=3500){const el=$("toast");el.textContent=m;el.className="toast show"+(t==="buy"?" bt":t==="sell"?" st":t==="trade"?" tt":t==="paper"?" pt":"");clearTimeout(toastT);toastT=setTimeout(()=>el.classList.remove("show"),d);}
function scoreColor(s){return s>=88?"#fbbf24":s>=75?"#a78bfa":s>=60?"var(--blue)":s>=45?"var(--orange)":"var(--dim)";}
function fmt(v){if(v===null||v===undefined||v==="–"||v===false||v==="false")return"–";const n=Number(v);if(isNaN(n))return String(v);if(n>=10000)return n.toLocaleString(undefined,{maximumFractionDigits:2});if(n>=1)return n.toFixed(4);return n.toFixed(6);}
function fmtP(v){const n=Number(v);if(!n)return"–";if(n>=10000)return"$"+n.toLocaleString(undefined,{maximumFractionDigits:2});if(n>=1)return"$"+n.toFixed(4);return"$"+n.toFixed(6);}
const TFM={"Day1":"1D","Hour4":"4H","Hour3":"3H","Hour2":"2H","Min60":"1H","Min45":"45m","Min30":"30m","Min15":"15m","Min10":"10m","Min5":"5m","Min4":"4m","Min3":"3m","Min2":"2m","Min1":"1m"};
const TOP=["BTC_USDT","ETH_USDT","SOL_USDT","BNB_USDT","XRP_USDT","DOGE_USDT"];
async function fetchPrices(){try{const r=await fetch("/api/prices");const data=await r.json();$("pupd").textContent="Updated "+new Date().toLocaleTimeString();$("pgrid").innerHTML=TOP.map(sym=>{const d=data[sym],name=sym.replace("_USDT","");if(!d)return`<div class="pc"><div class="pc-sym">${name}</div><div class="pc-price" style="color:var(--dim)">–</div></div>`;const up=d.change>=0;return`<div class="pc ${up?"up":"dn"}"><div class="pc-sym">${name}/USDT</div><div class="pc-price ${up?"up":"dn"}">${fmtP(d.price)}</div><span class="pc-chg ${up?"up":"dn"}">${up?"▲":"▼"} ${Math.abs(d.change).toFixed(2)}%</span></div>`;}).join("");}catch{}}
function buildCard(s,idx){const dir=(s.direction||"BUY").toUpperCase();const sc=s.score||0,gr=s.grade||"–";const gc={"A+":"gAp","A":"gA","B":"gB","C":"gC","D":"gD"}[gr]||"gD";const crtTF=TFM[s.tf]||s.tf||"–";const obTF=TFM[s.ob_tf]||s.ob_tf||"–";const isND=s.tf==="Day1";const zt=s.zone_type||s.ob_zone||"–";const isAplus=gr==="A+";const isM2=s.model==="2";const details=(s.details||[]).join("\n");const cf=(ok,l)=>`<span class="cf ${ok?"cf-ok":"cf-no"}">${ok?"✓":"✗"} ${l}</span>`;const cfw=(ok,l)=>`<span class="cf ${ok?"cf-ok":"cf-w"}">${ok?"✓":"⚠"} ${l}</span>`;const cfg=(ok,l)=>`<span class="cf ${ok?"cf-g":"cf-no"}">${ok?"💎":"◇"} ${l}</span>`;
const barFill=Math.round(sc/100*100);const barColor=sc>=88?"var(--yellow)":sc>=75?"#a78bfa":sc>=60?"var(--blue)":"var(--orange)";
return`<div class="scard ${dir.toLowerCase()}"><div class="card-hdr"><span class="dtag ${dir}">${dir}</span><span class="csym">${s.symbol||"–"}</span><div class="chips"><span class="chip chip-tf">${isM2?"🔥 M#2":"🎯 M#1"} ${crtTF}</span>${!isND&&s.ob_tf&&s.ob_tf!=="N/A"?`<span class="chip chip-ob">${zt} ${obTF}</span>`:""}<span class="chip chip-tr ${s.trend}">${s.trend}</span>${isAplus?'<span class="chip chip-aplus">⭐ A+</span>':""} ${s.from_monitor?'<span class="chip" style="color:#fbbf24;border-color:rgba(251,191,36,.3);background:rgba(251,191,36,.07)">👁 Monitored</span>':""}</div><span class="gtag ${gc}">${gr}</span><span class="cts">${s.timestamp||""}</span></div>
${isM2?`<div class="lvl-grid"><div class="lv lv-e"><div class="lv-lbl">🎯 Entry (FVG Tip)</div><div class="lv-val">${fmt(s.entry)}</div></div><div class="lv lv-e" style="border-color:rgba(249,168,212,.2)"><div class="lv-lbl">Sweep Extreme</div><div class="lv-val" style="color:#f9a8d4">${fmt(s.sweep_extreme)}</div></div><div class="lv lv-s"><div class="lv-lbl">🛑 Stop Loss</div><div class="lv-val">${fmt(s.sl)}</div></div><div class="lv lv-t" style="border-color:rgba(251,191,36,.3)"><div class="lv-lbl">🎯 TP1 (50%)</div><div class="lv-val" style="color:#fbbf24">${fmt(s.tp1)}</div></div><div class="lv lv-t"><div class="lv-lbl">🏆 TP2 (Liq)</div><div class="lv-val">${fmt(s.tp2)}</div></div><div class="lv lv-r"><div class="lv-lbl">📊 RR</div><div class="lv-val">${s.rr}R</div></div><div class="lv" style="border-color:rgba(16,185,129,.25)"><div class="lv-lbl">🔔 Trail SL</div><div class="lv-val" style="color:var(--green);font-size:.7rem">→TP1 @ 70%</div></div></div>`:`<div class="lvl-grid"><div class="lv lv-e"><div class="lv-lbl">🎯 Entry (TBS Open)</div><div class="lv-val">${fmt(s.entry)}</div></div><div class="lv lv-e" style="border-color:rgba(167,139,250,.2)"><div class="lv-lbl">TBS TF</div><div class="lv-val" style="color:#a78bfa">${TFM[s.tbs_tf]||s.tbs_tf||"–"}</div></div><div class="lv lv-s"><div class="lv-lbl">🛑 Stop Loss</div><div class="lv-val">${fmt(s.sl)}</div></div><div class="lv lv-t"><div class="lv-lbl">🎯 Take Profit</div><div class="lv-val">${fmt(s.tp)}</div></div><div class="lv lv-r"><div class="lv-lbl">📊 RR</div><div class="lv-val">${s.rr}R</div></div><div class="lv"><div class="lv-lbl">CRH</div><div class="lv-val" style="color:#f9a8d4">${fmt(s.crh)}</div></div><div class="lv"><div class="lv-lbl">CRL</div><div class="lv-val" style="color:#6ee7b7">${fmt(s.crl)}</div></div></div>`}
${isM2?'<div class="cfms">'+cf(true,"HTF KL")+cf(true,"Sweep")+cf(true,"FVG")+cfw(true,"Liq")+'<span class="cf cf-ok" style="color:#fbbf24">🔔 TP1@50%/TP2@Liq</span>'+cfg(isAplus,"A+")+'</div>':'<div class="cfms">'+cf(s.tbs_found,"TBS "+(TFM[s.tbs_tf]||s.tbs_tf||"?"))+cfw(s.fvg_found,s.fvg_type||"FVG")+cfw(s.choch_found,"CHOCH")+cfw(s.liq_swept,"Liq Sweep")+cfw(s.ob_respected,"OB Resp")+cfg(isAplus,"A+")+'</div>'}
<div class="srow"><span class="slbl">Score</span><div class="strack"><div class="sfill" style="width:${barFill}%;background:${barColor}"></div></div><span class="snum2" style="color:${barColor}">${sc}/100</span></div>
<button class="dettog" onclick="toggleDet(${idx})">▶ Score Breakdown</button><div class="detbox" id="det-${idx}">${details}</div></div>`;}
window.toggleDet=function(i){const b=$("det-"+i);if(!b)return;b.classList.toggle("open");const t=b.previousElementSibling;if(t)t.textContent=b.classList.contains("open")?"▼ Score Breakdown":"▶ Score Breakdown";};
window.renderSigs=function(){const dF=$("fd").value,gF=$("fg").value,tfF=$("ftf").value;let f=allSigs.filter(s=>{if(dF&&s.direction!==dF)return false;if(tfF&&s.tf!==tfF)return false;if(gF){if(gF==="A+"&&s.grade!=="A+")return false;if(gF==="A"&&s.grade!=="A")return false;if(gF==="B"&&!["A+","A","B"].includes(s.grade))return false;}return true;});const list=$("slist");if(!f.length){list.innerHTML='<div class="empty"><div class="empty-ico">🔭</div><div class="empty-t">Scanning the galaxy...</div><div class="empty-s">Hunting Mad Man Model #1 &amp; #2 setups. TBS (M#1) · FVG sweep (M#2). Min 2R.</div></div>';return;}list.innerHTML=f.slice(0,100).map((s,i)=>buildCard(s,i)).join("");};
async function fetchSigs(){try{const r=await fetch("/api/signals?limit=200");const data=await r.json();allSigs=data;if(data.length>lastCount&&lastCount>0){const n=data[0];toast(`🎯 ${n.direction} ${n.symbol} · ${n.score}/100 ${n.grade} · ${n.rr}R`,n.direction==="BUY"?"buy":"sell");}lastCount=data.length;renderSigs();}catch{}}
async function fetchStats(){try{const r=await fetch("/api/stats");const d=await r.json();$("st").textContent=d.total||0;$("sb").textContent=d.buys||0;$("ss").textContent=d.sells||0;}catch{}}
async function fetchState(){try{const r=await fetch("/api/scan-state");const d=await r.json();const pct=d.total_pairs>0?Math.round(d.pairs_done/d.total_pairs*100):0;$("pfill").style.width=pct+"%";$("pcnt").textContent=`${d.pairs_done}/${d.total_pairs}`;$("cpair").textContent=d.current_pair?`🔍 ${d.current_pair}`:"⏳ Waiting...";$("sc2").textContent=d.scan_count||0;$("sl2").textContent=d.last_scan?`Last: ${d.last_scan}`:"–";$("snum").textContent=`Scan #${d.scan_count||0}`;const en=d.enabled!==false;$("tbtn").textContent=en?"⏹ Stop":"▶ Resume";$("tbtn").className="tbtn "+(en?"on":"off");$("sdot").className="sdot"+(en?"":" off");$("stxt").textContent=en?"SCANNING...":"PAUSED";$("stxt").className="stxt"+(en?"":" off");$("pb").className="pb"+(en?"":" show");}catch{}}
async function fetchMonitor(){if(activeTab!=="monitor")return;try{const r=await fetch("/api/monitor");const data=await r.json();$("smon").textContent=data.length;$("mon-count").textContent=`(${data.length}/4)`;const wrap=$("monitor-wrap");if(!wrap)return;if(!data.length){wrap.innerHTML='<div class="empty" style="padding:40px"><div class="empty-ico">🌙</div><div class="empty-t">Nothing monitored</div><div class="empty-s">Pairs in manipulation phase appear here automatically</div></div>';return;}wrap.innerHTML=`<div class="monitor-grid">${data.map(m=>{const dir=(m.direction||"BUY").toUpperCase();const tf=TFM[m.crt_tf]||m.crt_tf||"–";return`<div class="mon-card ${dir.toLowerCase()}"><div class="mon-sym">${dir==="BUY"?"🟢":"🔴"} ${m.symbol||"–"}</div><div class="mon-row"><span>Mad Man TF</span><span>${tf}</span></div><div class="mon-row"><span>Key Level</span><span>${m.kl_type||"–"}</span></div><div class="mon-row"><span>Trend</span><span>${m.trend||"–"}</span></div><div class="mon-row"><span>CRH</span><span>${fmt(m.crh)}</span></div><div class="mon-row"><span>CRL</span><span>${fmt(m.crl)}</span></div><div class="mon-row"><span>Zone</span><span>${m.zone_name||"–"}</span></div><div class="mon-row"><span>Added</span><span>${m.added_at||"–"}</span></div><div class="mon-status">⏳ AWAITING C2 CLOSE</div></div>`;}).join("")}</div>`;}catch{}}
window.fetchPnl=async function(){if(activeTab!=="trades")return;try{const[tr,pnl]=await Promise.all([fetch("/api/trades").then(r=>r.json()),fetch("/api/pnl").then(r=>r.json())]);$("trades-count").textContent=`(${tr.length})`;const wrap=$("live-trades-wrap");if(!wrap)return;const pnlMap={};(pnl.positions||[]).forEach(p=>pnlMap[p.symbol]=p);if(!tr.length){wrap.innerHTML='<div class="empty" style="padding:40px"><div class="empty-ico">😴</div><div class="empty-t">No open trades</div></div>';return;}window._liveTradesData=tr;window._livePnlMap=pnlMap;wrap.innerHTML=`<div style="overflow-x:auto"><table class="tbl"><thead><tr><th>Pair</th><th>Dir</th><th>Entry</th><th>Current</th><th>SL</th><th>TP</th><th>RR</th><th>Lev</th><th>Margin</th><th>Live PnL</th><th>ROI%</th><th>Grade</th><th>Card</th><th>Action</th></tr></thead><tbody>${tr.map((t,i)=>{const live=pnlMap[t.symbol]||{};const pv=live.pnl||0;const roi=live.roi_pct||0;const cur=live.current||0;const lev=live.leverage||t.leverage||"–";const margin=(live.margin||0).toFixed(2);return`<tr><td style="font-weight:800;color:var(--text)">${t.symbol}</td><td class="${t.direction==="BUY"?"buy":"sell"}">${t.direction}</td><td>${fmt(t.entry)}</td><td style="color:var(--yellow)">${cur?fmt(cur):"–"}</td><td style="color:var(--red)">${fmt(t.sl)}</td><td style="color:var(--green)">${fmt(t.tp)}</td><td style="color:var(--yellow)">${t.rr}R</td><td style="color:#a78bfa">${lev}x</td><td>$${margin}</td><td class="pos-pnl ${pv>=0?"pos":"neg"}">${pv>=0?"+":""}${pv.toFixed(2)}</td><td class="pos-pnl ${roi>=0?"pos":"neg"}">${roi>=0?"+":""}${roi.toFixed(2)}%</td><td style="color:${scoreColor(t.score||0)};font-family:'Fredoka One',sans-serif">${t.grade||"–"}</td><td><button class="action-btn share-btn" onclick="showTradeCard({symbol:'${t.symbol}',direction:'${t.direction}',entry:${t.entry},sl:${t.sl},tp:${t.tp},rr:'${t.rr}',grade:'${t.grade||'–'}',score:${t.score||0},pnl:${pv.toFixed(2)},pnl_pct:${roi.toFixed(2)},market_price:${cur},status:'RUNNING'},true,'LIVE')">📸</button></td><td><button class="action-btn close-btn" onclick="closeTrade('${t.symbol}')">✕</button></td></tr>`;}).join("")}</tbody></table></div>`;}catch{}};
async function fetchHistory(){if(activeTab!=="history")return;try{const r=await fetch("/api/recent-trades");const data=await r.json();const wrap=$("history-wrap");if(!wrap)return;if(!data.length){wrap.innerHTML='<div class="empty" style="padding:40px"><div class="empty-ico">📭</div><div class="empty-t">No completed trades yet</div></div>';return;}window._histData=data;wrap.innerHTML=`<div style="overflow-x:auto"><table class="tbl"><thead><tr><th>Pair</th><th>Dir</th><th>Entry</th><th>Exit</th><th>SL</th><th>TP</th><th>RR</th><th>Grade</th><th>Status</th><th>Opened</th><th>Card</th></tr></thead><tbody>${data.map((t,i)=>{const isW=(t.status||"").toLowerCase().includes("tp");const pnlSign=isW?"+":"";return`<tr><td style="font-weight:800;color:var(--text)">${t.symbol}</td><td class="${t.direction==="BUY"?"buy":"sell"}">${t.direction}</td><td>${fmt(t.entry)}</td><td style="color:var(--yellow)">${fmt(t.close_price||t.exit_price||"–")}</td><td style="color:var(--red)">${fmt(t.sl)}</td><td style="color:var(--green)">${fmt(t.tp)}</td><td style="color:var(--yellow)">${t.rr}R</td><td style="color:${scoreColor(t.score||0)};font-family:'Fredoka One',sans-serif">${t.grade||"–"}</td><td style="color:var(--dim);font-size:.62rem">${t.status||"–"}</td><td style="color:var(--dim)">${(t.opened_at||"").replace(" UTC","")}</td><td><button class="action-btn share-btn" onclick="showTradeCard(window._histData[${i}],false,'LIVE')">📸</button></td></tr>`;}).join("")}</tbody></table></div>`;}catch{}}
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

  // Price boxes — use ENTRY PRICE and CURRENT PRICE labels
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

  // Exit row — only show for closed trades
  const exitRow=$("tc-exit-row");
  if(!isOpen && curVal){
    exitRow.style.display="block";
    $("tc-exit-val").textContent=fmt(curVal);
  } else {
    exitRow.style.display="none";
  }

  // PnL
  const pnlRow=$("tc-pnl-row");
  const pnlV=parseFloat(t.pnl||t.live_pnl||0);
  const pnlP=parseFloat(t.pnl_pct||t.roi_pct||0);
  const pnlCls=pnlV>0?"pos":pnlV<0?"neg":"neutral";
  pnlRow.className="tc-pnl-row "+pnlCls;
  $("tc-pnl-val").textContent=(pnlV>=0?"+":"")+pnlV.toFixed(2)+" USDT";
  $("tc-pnl-pct").textContent=(pnlP>=0?"+":"")+pnlP.toFixed(2)+"%";

  // Footer labels — model number: 1, 2 or 3
  const modelNum=t.model||"1";
  $("tc-type-lbl").textContent=(tradingType==="PAPER"?"📝 PAPER TRADE":"🤖 LIVE TRADE")+" · MAD MAN MODEL #"+modelNum;
  $("tc-rr-lbl").textContent=(t.rr||"–")+"R";

  $("tc-modal").classList.add("show");
};
async function fetchLog(){if(activeTab!=="log")return;try{const r=await fetch("/api/log");const d=await r.json();const body=$("lbody");if(!d.log||!d.log.length){body.innerHTML='<div style="color:rgba(56,189,248,.5);font-style:italic">Waiting for log entries... The scanner logs appear here automatically.</div>';return;}body.innerHTML=d.log.map(l=>{const cls=l.includes("🎯")||l.includes("SIGNAL")?"ll-s":l.includes("📝")||l.includes("PAPER")?"ll-p":l.includes("🤖")||l.includes("TRADE")?"ll-t":l.includes("❌")||l.includes("Error")||l.includes("error")?"ll-e":l.includes("👁")||l.includes("MONITOR")||l.includes("MANIP")?"ll-m":"ll-i";return`<div class="${cls}">${l}</div>`;}).join("");}catch(e){const body=$("lbody");if(body)body.innerHTML=`<div class="ll-e">Log fetch error: ${e}</div>`;}}
async function loadTradeConfig(){try{const r=await fetch("/api/trade-config");const d=await r.json();if(d.api_key)$("t-apikey").value=d.api_key;$("t-risk").value=d.risk_pct||1;if($("t-leverage"))$("t-leverage").value=d.leverage||0;if($("t-max"))$("t-max").value=d.max_trades||3;updateTradeBadge(d.enabled);checkRiskWarning();}catch{}}
window.fetchTradeCfgData=async function(){try{const[tr,pnl,hist]=await Promise.all([fetch("/api/trades").then(r=>r.json()),fetch("/api/pnl").then(r=>r.json()),fetch("/api/recent-trades").then(r=>r.json())]);const posEl=$("live-pos-count");if(posEl)posEl.textContent=`(${tr.length})`;const wrap2=$("live-trades-wrap2");if(wrap2){const pnlMap={};(pnl.positions||[]).forEach(p=>pnlMap[p.symbol]=p);if(!tr.length){wrap2.innerHTML='<div class="empty" style="padding:40px"><div class="empty-ico">😴</div><div class="empty-t">No open trades</div></div>';}else{window._liveTradesData2=tr;wrap2.innerHTML=`<div style="overflow-x:auto"><table class="tbl"><thead><tr><th>Pair</th><th>Dir</th><th>Entry Price</th><th>Current Price</th><th>SL</th><th>TP</th><th>RR</th><th>Lev</th><th>Margin</th><th>Live PnL</th><th>ROI%</th><th>Grade</th><th>Card</th><th>Action</th></tr></thead><tbody>${tr.map((t,i)=>{const live=pnlMap[t.symbol]||{};const pv=live.pnl||0;const roi=live.roi_pct||0;const cur=live.current||0;const lev=live.leverage||t.leverage||"–";const margin=(live.margin||0).toFixed(2);return`<tr><td style="font-weight:800;color:var(--text)">${t.symbol}</td><td class="${t.direction==="BUY"?"buy":"sell"}">${t.direction}</td><td>${fmt(t.entry)}</td><td style="color:var(--yellow)">${cur?fmt(cur):"–"}</td><td style="color:var(--red)">${fmt(t.sl)}</td><td style="color:var(--green)">${fmt(t.tp)}</td><td style="color:var(--yellow)">${t.rr}R</td><td style="color:#a78bfa">${lev}x</td><td>$${margin}</td><td class="pos-pnl ${pv>=0?"pos":"neg"}">${pv>=0?"+":""}${pv.toFixed(2)}</td><td class="pos-pnl ${roi>=0?"pos":"neg"}">${roi>=0?"+":""}${roi.toFixed(2)}%</td><td style="color:${scoreColor(t.score||0)};font-family:'Fredoka One',sans-serif">${t.grade||"–"}</td><td><button class="action-btn share-btn" onclick="showTradeCard({symbol:'${t.symbol}',direction:'${t.direction}',entry:${t.entry},sl:${t.sl},tp:${t.tp},rr:'${t.rr}',grade:'${t.grade||'–'}',score:${t.score||0},pnl:${pv.toFixed(2)},pnl_pct:${roi.toFixed(2)},market_price:${cur},status:'RUNNING'},true,'LIVE')">📸</button></td><td><button class="action-btn close-btn" onclick="closeTrade('${t.symbol}')">✕</button></td></tr>`;}).join("")}</tbody></table></div>`;}}const hw2=$("history-wrap2");if(hw2){if(!hist.length){hw2.innerHTML='<div class="empty" style="padding:40px"><div class="empty-ico">📭</div><div class="empty-t">No completed trades yet</div></div>';}else{window._histData2=hist;hw2.innerHTML=`<div style="overflow-x:auto"><table class="tbl"><thead><tr><th>Pair</th><th>Dir</th><th>Entry Price</th><th>Exit Price</th><th>SL</th><th>TP</th><th>RR</th><th>PnL $</th><th>PnL %</th><th>Grade</th><th>Status</th><th>Opened</th><th>Card</th></tr></thead><tbody>${hist.map((t,i)=>{const pv=t.pnl||0;const pp=t.pnl_pct||0;const isW=(t.status||"").toLowerCase().includes("tp");return`<tr><td style="font-weight:800;color:var(--text)">${t.symbol}</td><td class="${t.direction==="BUY"?"buy":"sell"}">${t.direction}</td><td>${fmt(t.entry)}</td><td style="color:var(--yellow)">${fmt(t.close_price||t.exit_price||"–")}</td><td style="color:var(--red)">${fmt(t.sl)}</td><td style="color:var(--green)">${fmt(t.tp)}</td><td style="color:var(--yellow)">${t.rr}R</td><td class="pos-pnl ${pv>=0?"pos":"neg"}">${pv>=0?"+":""}${pv.toFixed(2)}</td><td class="pos-pnl ${pp>=0?"pos":"neg"}">${pp>=0?"+":""}${pp.toFixed(2)}%</td><td style="color:${scoreColor(t.score||0)};font-family:'Fredoka One',sans-serif">${t.grade||"–"}</td><td style="color:${isW?"var(--green)":"var(--red)"};font-size:.62rem">${t.status||"–"}</td><td style="color:var(--dim)">${(t.opened_at||"").replace(" UTC","")}</td><td><button class="action-btn share-btn" onclick="showTradeCard(window._histData2[${i}],false,'LIVE')">📸</button></td></tr>`;}).join("")}</tbody></table></div>`;}}catch{}}
function updateTradeBadge(en){const b=$("trade-badge"),eb=$("t-enable-btn"),db=$("t-disable-btn");if(en){b.textContent="ENABLED";b.style.cssText="font-size:.7rem;padding:3px 10px;border-radius:8px;background:rgba(16,185,129,.12);border:1.5px solid rgba(16,185,129,.35);color:var(--green);font-family:'JetBrains Mono',monospace;font-weight:700";eb.style.display="none";db.style.display="";}else{b.textContent="DISABLED";b.style.cssText="font-size:.7rem;padding:3px 10px;border-radius:8px;background:rgba(239,68,68,.1);border:1.5px solid rgba(239,68,68,.3);color:var(--red);font-family:'JetBrains Mono',monospace;font-weight:700";eb.style.display="";db.style.display="none";}}
function showTradeMsg(msg,ok){const el=$("trade-msg");el.textContent=msg;el.className="t-status "+(ok?"ok":"err");setTimeout(()=>el.className="t-status",4000);}
function checkRiskWarning(){
  const riskEl=$("t-risk");
  const warnEl=$("risk-warn");
  if(!riskEl||!warnEl)return;
  const riskPct=parseFloat(riskEl.value)||1;
  // Get balance from the live balance display (s-bal-val in settings, or from last stats fetch)
  const bal=parseFloat(window._lastBalance||0);
  const riskAmt=(bal*riskPct/100);
  if(bal>0&&riskAmt<0.1){
    warnEl.textContent="⚠️ Risk $"+riskAmt.toFixed(4)+" is below the $0.10 minimum — raise balance or increase risk %";
    warnEl.className="t-status err";
  }else if(bal>0&&riskAmt>=0.1){
    warnEl.textContent="✅ Risk per trade: $"+riskAmt.toFixed(2)+" USDT";
    warnEl.className="t-status ok";
  }else{
    warnEl.textContent="ℹ️ Fetch balance in Settings to validate risk amount";
    warnEl.className="t-status ok";
  }
}
function showPaperMsg(msg,ok){const el=$("paper-msg");el.textContent=msg;el.className="t-status "+(ok?"ok":"err");setTimeout(()=>el.className="t-status",4000);}
window.saveTradeConfig=async function(){const cfg={api_key:$("t-apikey").value.trim(),api_secret:$("t-secret").value.trim(),risk_pct:parseFloat($("t-risk").value)||1,leverage:parseInt(($("t-leverage")||{}).value||0)||0,max_trades:parseInt(($("t-max")||{}).value||3)||3};if(!cfg.api_key){showTradeMsg("❌ API key required",false);return;}try{const r=await fetch("/api/trade-config",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(cfg)});const d=await r.json();if(d.ok){showTradeMsg("✅ Saved!",true);toast("💾 Saved!","trade");}else showTradeMsg("❌ Save failed",false);}catch{showTradeMsg("❌ Error",false);}};
window.enableTrade=async function(en){try{const r=await fetch("/api/trade-config",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({enabled:en})});const d=await r.json();if(d.ok){updateTradeBadge(en);showTradeMsg(en?"✅ Auto-trade ENABLED!":"✅ Disabled",true);toast(en?"🤖 Auto-trade ON!":"⏹ Off","trade");}}catch{showTradeMsg("❌ Error",false);}};
window.fetchBalance=async function(){try{const r=await fetch("/api/balance");const d=await r.json();if(d.error)showTradeMsg("❌ "+d.error,false);else{$("bal-val").textContent=Number(d.balance).toFixed(2);showTradeMsg("✅ Balance loaded",true);}}catch{showTradeMsg("❌ Check API keys",false);}};
window.closeTrade=async function(sym){if(!confirm("Close "+sym+"?"))return;try{const r=await fetch("/api/trade-close",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({symbol:sym})});const d=await r.json();toast(d.ok?"✅ "+sym+" closed":"❌ "+d.message,"trade");await window.fetchPnl();}catch{toast("❌ Close failed","trade");}};
window.toggleScanner=async function(){try{const r=await fetch("/api/toggle-scanner",{method:"POST"});const d=await r.json();toast(d.enabled?"▶ Scanner on! 🚀":"⏹ Paused",d.enabled?"buy":"sell");await fetchState();}catch{}};

/* ─── PAPER TRADING ─────────────────────────── */
async function loadPaperConfig(){try{const r=await fetch("/api/paper-config");const d=await r.json();$("p-balance").value=d.balance||10000;$("p-risk").value=d.risk_pct||1;$("p-max").value=d.max_trades||4;$("p-bal-val").textContent=Number(d.balance||10000).toLocaleString(undefined,{minimumFractionDigits:2,maximumFractionDigits:2});paperAutoOn=d.auto_trade||false;updatePaperBadge(d.enabled,d.auto_trade);}catch{}}
function updatePaperBadge(en,auto){const b=$("paper-badge"),ab=$("paper-auto-badge"),eb=$("p-enable-btn"),db=$("p-disable-btn"),autobtn=$("p-auto-btn");if(en){b.textContent="ENABLED";b.style.cssText="font-size:.7rem;padding:3px 10px;border-radius:8px;background:rgba(16,185,129,.12);border:1.5px solid rgba(16,185,129,.35);color:var(--green);font-family:'JetBrains Mono',monospace;font-weight:700";eb.style.display="none";db.style.display="";}else{b.textContent="DISABLED";b.style.cssText="font-size:.7rem;padding:3px 10px;border-radius:8px;background:rgba(239,68,68,.1);border:1.5px solid rgba(239,68,68,.3);color:var(--red);font-family:'JetBrains Mono',monospace;font-weight:700";eb.style.display="";db.style.display="none";}if(ab){ab.style.display=auto?"":"none";}if(autobtn){autobtn.textContent=`🤖 Auto-Trade: ${auto?"ON":"OFF"}`;autobtn.style.background=auto?"rgba(167,139,250,.2)":"rgba(167,139,250,.1)";autobtn.style.borderColor=auto?"rgba(167,139,250,.6)":"rgba(167,139,250,.3)";}}
window.savePaperConfig=async function(){const cfg={risk_pct:parseFloat($("p-risk").value)||1,max_trades:parseInt($("p-max").value)||4};try{const r=await fetch("/api/paper-config",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(cfg)});const d=await r.json();if(d.ok){showPaperMsg("✅ Settings saved!",true);toast("💾 Paper settings saved","paper");}else showPaperMsg("❌ Save failed",false);}catch{showPaperMsg("❌ Error",false);}};
window.setPaperBalance=async function(){const bal=parseFloat($("p-balance").value);if(!bal||bal<100){showPaperMsg("❌ Minimum balance $100",false);return;}try{const r=await fetch("/api/paper-config",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({balance:bal})});const d=await r.json();if(d.ok){$("p-bal-val").textContent=bal.toLocaleString(undefined,{minimumFractionDigits:2,maximumFractionDigits:2});showPaperMsg(`✅ Balance set to $${bal.toLocaleString()}`,true);toast(`💰 Paper balance: $${bal.toLocaleString()}`,"paper");}else showPaperMsg("❌ Failed",false);}catch{showPaperMsg("❌ Error",false);}};
window.enablePaper=async function(en){try{const r=await fetch("/api/paper-config",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({enabled:en})});const d=await r.json();if(d.ok){updatePaperBadge(en,paperAutoOn);showPaperMsg(en?"✅ Paper trading ENABLED!":"✅ Paper trading disabled",true);toast(en?"📝 Paper ON!":"⏹ Paper off","paper");}else showPaperMsg("❌ Error",false);}catch{showPaperMsg("❌ Error",false);}};
window.togglePaperAuto=async function(){paperAutoOn=!paperAutoOn;try{const r=await fetch("/api/paper-config",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({auto_trade:paperAutoOn})});const d=await r.json();if(d.ok){updatePaperBadge(d.config.enabled,paperAutoOn);showPaperMsg(paperAutoOn?"✅ Auto-trade ON — signals will auto paper-trade!":"✅ Auto-trade OFF",true);toast(paperAutoOn?"🤖 Paper auto-trade ON!":"⏹ Auto off","paper");}else showPaperMsg("❌ Error",false);}catch{showPaperMsg("❌ Error",false);}};
window.closePaperTrade=async function(sym){if(!confirm("Close paper trade on "+sym+"?"))return;try{const r=await fetch("/api/paper-close",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({symbol:sym})});const d=await r.json();if(d.ok){toast(`📝 Paper closed: ${sym} · ${d.message}`,"paper");await fetchPaperData();}else toast("❌ "+d.message,"sell");}catch{toast("❌ Error","sell");}};
window.resetPaperStats=async function(){if(!confirm("Reset all paper trading stats and history?"))return;try{const r=await fetch("/api/paper-reset",{method:"POST"});const d=await r.json();if(d.ok){toast("📝 Paper stats reset","paper");await fetchPaperData();}else toast("❌ Reset failed","sell");}catch{toast("❌ Error","sell");}};
async function fetchPaperData(){if(activeTab!=="paper")return;try{const[cfg,trades,hist,stats]=await Promise.all([fetch("/api/paper-config").then(r=>r.json()),fetch("/api/paper-trades").then(r=>r.json()),fetch("/api/paper-history").then(r=>r.json()),fetch("/api/paper-stats").then(r=>r.json())]);
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

async function fetchDiag(){try{const r=await fetch("/api/diag");const d=await r.json();const labels={neutral:"😴 Neutral",not_continuous:"📉 Structure",no_obs:"📦 No OBs",not_at_key:"🎯 Not Key",not_in_zone:"📍 Zone",not_tapping:"👆 Tapping",no_crts:"🕯 No Setup",no_tbs:"🐢 No TBS",rr_low:"📊 Low RR","1d_no_crts":"1D No Setup","1d_no_tbs":"1D NoTBS","1d_rr_low":"1D LowRR",passed:"✅ PASSED"};const colors={neutral:"var(--dim)",not_continuous:"var(--dim)",no_obs:"var(--orange)",not_at_key:"var(--orange)",not_in_zone:"var(--orange)",not_tapping:"var(--red)",no_crts:"var(--red)",no_tbs:"var(--red)",rr_low:"var(--orange)","1d_no_crts":"var(--dim)","1d_no_tbs":"var(--dim)","1d_rr_low":"var(--dim)",passed:"var(--green)"};const grid=$("diag-grid");if(grid)grid.innerHTML=Object.entries(d).map(([k,v])=>`<div class="dg"><div class="dg-lbl">${labels[k]||k}</div><div class="dg-val" style="color:${colors[k]||"var(--text)"}">${v}</div></div>`).join("");}catch{}}

window.sw=function(tab,btn){activeTab=tab;document.querySelectorAll(".tab").forEach(b=>b.classList.remove("active"));btn.classList.add("active");["signals","trades","monitor","history","trade-cfg","paper","log","settings"].forEach(t=>{const el=$("tab-"+t);if(el)el.style.display=t===tab?"block":"none";});if(tab==="log"){fetchLog();fetchDiag();}if(tab==="trades")window.fetchPnl();if(tab==="monitor")fetchMonitor();if(tab==="history")fetchHistory();if(tab==="trade-cfg"){loadTradeConfig();window.fetchTradeCfgData();}if(tab==="paper"){loadPaperConfig();fetchPaperData();}if(tab==="settings")loadSettings();};

// ── SETTINGS PAGE JS ────────────────────────────────────────────────
let secretVisible=false;
async function loadSettings(){
  try{
    const r=await fetch("/api/settings");const d=await r.json();
    const si=id=>document.getElementById(id);
    if(si("s-apikey"))   si("s-apikey").value   = d.api_key||"";
    if(si("s-risk"))     si("s-risk").value      = d.risk_pct||1;if(si("s-leverage"))si("s-leverage").value=d.leverage||0;
    if(si("s-tg-token")) si("s-tg-token").value  = d.tg_bot_token||"";
    if(si("s-tg-chat"))  si("s-tg-chat").value   = d.tg_chat_id||"";
    if(si("s-tg-signals")) si("s-tg-signals").checked = !!d.tg_signals;
    if(si("s-tg-trades"))  si("s-tg-trades").checked  = !!d.tg_trades;
    if(si("s-price-int"))  si("s-price-int").value  = d.price_interval||1;
    if(si("s-scan-int"))   si("s-scan-int").value   = d.scan_interval||1;
    if(si("s-cycle-rest")) si("s-cycle-rest").value  = d.cycle_rest||5;
    const disp=document.getElementById("s-secret-display");
    if(disp) disp.textContent = d.api_secret_set ? ("Current: "+d.api_secret_masked) : "No secret key saved";
  }catch(e){console.error("loadSettings",e);}
}
function toggleSecretVis(){
  secretVisible=!secretVisible;
  const el=document.getElementById("s-secret");
  if(el) el.type=secretVisible?"text":"password";
}
async function loadSettingsBalance(){
  const el=document.getElementById("s-bal-val");
  if(el) el.textContent="…";
  try{const r=await fetch("/api/balance");const d=await r.json();
    if(d.balance!=null&&d.balance>0){
      window._lastBalance=Number(d.balance);
      if(el) el.textContent=Number(d.balance).toFixed(2);
    } else {
      if(el) el.textContent=d.error||"–";
    }
    checkRiskWarning();
  }catch{if(el)el.textContent="err";}
}
function _sMsg(msg,text,type){
  if(!msg)return;
  msg.textContent=text;
  msg.className="t-status "+(type==="ok"?"ok":type==="warn"?"ok":"err");
  if(type==="warn"){msg.style.background="rgba(245,158,11,.1)";msg.style.borderColor="rgba(245,158,11,.3)";msg.style.color="var(--yellow)";}
  else{msg.style.background="";msg.style.borderColor="";msg.style.color="";}
}
async function saveAllSettings(){
  const si=id=>document.getElementById(id);
  const msg=document.getElementById("settings-msg");
  _sMsg(msg,"💾 Saving…","warn");
  const payload={
    api_key:       (si("s-apikey")||{}).value||"",
    risk_pct:      parseFloat((si("s-risk")||{}).value)||1,
    leverage:      parseInt((si("s-leverage")||{}).value||0)||0,
    tg_bot_token:  (si("s-tg-token")||{}).value||"",
    tg_chat_id:    (si("s-tg-chat")||{}).value||"",
    tg_signals:    (si("s-tg-signals")||{}).checked,
    tg_trades:     (si("s-tg-trades")||{}).checked,
    price_interval:parseInt((si("s-price-int")||{}).value)||1,
    scan_interval: parseInt((si("s-scan-int")||{}).value)||1,
    cycle_rest:    parseInt((si("s-cycle-rest")||{}).value)||5,
  };
  const sec=(si("s-secret")||{}).value||"";
  if(sec.trim()) payload.api_secret=sec.trim();
  try{
    const r=await fetch("/api/settings",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(payload)});
    const d=await r.json();
    if(d.ok){
      _sMsg(msg,"✅ All settings saved!","ok");
      if(si("s-secret")) si("s-secret").value="";
      loadSettings();
    }else{_sMsg(msg,"❌ Save failed — check API keys","err");}
  }catch(e){_sMsg(msg,"❌ Error: "+e.message,"err");}
  setTimeout(()=>{if(msg){msg.className="t-status";msg.style.background="";msg.style.borderColor="";msg.style.color="";}},5000);
}
window.saveAllSettings     = saveAllSettings;
window.loadSettings        = loadSettings;
window.loadSettingsBalance = loadSettingsBalance;
window.toggleSecretVis     = toggleSecretVis;
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

async function poll(){tick++;const ps=[fetchSigs(),fetchStats(),fetchState()];if(tick%2===0)ps.push(fetchPrices());if(activeTab==="log")ps.push(fetchLog());if(activeTab==="log"&&tick%3===0)ps.push(fetchDiag());if(activeTab==="monitor"&&tick%2===0)ps.push(fetchMonitor());if(activeTab==="trades"&&tick%3===0)ps.push(window.fetchPnl());if(activeTab==="history"&&tick%5===0)ps.push(fetchHistory());if(activeTab==="trade-cfg"&&tick%3===0)ps.push(window.fetchTradeCfgData());if(activeTab==="paper"&&tick%2===0)ps.push(fetchPaperData());await Promise.all(ps);setTimeout(poll,1000);}
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
    token = request.cookies.get("session")
    if token:
        try:
            payload, sig = token.rsplit(".", 1)
            expected = hmac.new(SESSION_SECRET.encode(), payload.encode(), hashlib.sha256).hexdigest()
            if hmac.compare_digest(sig, expected):
                return make_response(DASHBOARD_HTML, 200, {"Content-Type": "text/html"})
        except Exception:
            pass
    return make_response(LOGIN_HTML, 200, {"Content-Type": "text/html"})

@app.route("/dashboard")
def dashboard():
    return make_response(DASHBOARD_HTML,200,{"Content-Type":"text/html"})

@app.route("/api/login",methods=["POST"])
def api_login():
    data=request.get_json(silent=True) or {}
    if data.get("password") == DASHBOARD_PASSWORD:
        payload = "auth." + secrets.token_hex(16)
        sig     = hmac.new(SESSION_SECRET.encode(), payload.encode(), hashlib.sha256).hexdigest()
        token   = payload + "." + sig
        resp=make_response(jsonify({"ok":True,"token":token}))
        resp.set_cookie("session",token,max_age=86400*7,httponly=True,samesite="Lax")
        return resp
    return jsonify({"ok":False}),401

@app.route("/api/logout",methods=["POST"])
def api_logout():
    resp = make_response(jsonify({"ok": True}))
    resp.delete_cookie("session")
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

@app.route("/api/prices")
def api_prices():
    out={}
    for sym in TOP_PAIRS:
        t=get_ticker(sym)
        if t: out[sym]=t
    return jsonify(out)

@app.route("/api/trade-config", methods=["GET","POST"])
def api_trade_config():
    if request.method == "POST":
        data = request.get_json(silent=True) or {}
        with trade_lock:
            if "api_key"     in data: trade_config["api_key"]    = data["api_key"]
            if "api_secret"  in data: trade_config["api_secret"] = data["api_secret"]
            if "risk_pct"    in data: trade_config["risk_pct"]   = float(data["risk_pct"])
            if "leverage"    in data: trade_config["leverage"]   = int(data.get("leverage",0))
            if "max_trades"  in data: trade_config["max_trades"] = int(data["max_trades"])
            if "leverage"    in data: trade_config["leverage"]   = int(data["leverage"])
            if "enabled"     in data: trade_config["enabled"]    = bool(data["enabled"])
        log(f"⚙️ Trade config updated. Auto-trade: {'ON' if trade_config['enabled'] else 'OFF'}")
        save_persisted_settings()
        return jsonify({"ok": True, "config": {k:v for k,v in trade_config.items() if k!="api_secret"}})
    cfg = {k: ("***" if k=="api_secret" and v else v) for k,v in trade_config.items()}
    return jsonify(cfg)

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
            for k in ["price_interval","scan_interval","cycle_rest"]:
                if k in data:
                    try: scan_settings[k] = max(1, int(data[k]))
                    except: pass
            for k in ["tg_signals","tg_trades"]:
                if k in data: scan_settings[k] = bool(data[k])
            if "tg_bot_token" in data and data["tg_bot_token"].strip():
                scan_settings["tg_bot_token"] = data["tg_bot_token"].strip()
            if "tg_chat_id" in data and data["tg_chat_id"].strip():
                scan_settings["tg_chat_id"] = data["tg_chat_id"].strip()
            if "api_key" in data and data["api_key"].strip():
                trade_config["api_key"] = data["api_key"].strip()
            if "api_secret" in data and data["api_secret"].strip():
                trade_config["api_secret"] = data["api_secret"].strip()
            if "risk_pct" in data:
                try: trade_config["risk_pct"] = max(0.1, min(100.0, float(data["risk_pct"])))
                except: pass
            if "leverage" in data:
                try: trade_config["leverage"] = max(1, min(500, int(data["leverage"])))
                except: pass
            for k in ["model1_enabled", "model2_enabled"]:
                if k in data: scan_settings[k] = bool(data[k])
        log("⚙️ Settings updated from Settings page")
        save_persisted_settings()
        return jsonify({"ok": True})
    with settings_lock:
        out = dict(scan_settings)
        out["api_key"]      = trade_config.get("api_key","")
        out["risk_pct"]     = trade_config.get("risk_pct", 1.0)
        out["leverage"]     = trade_config.get("leverage", 35)
        out["model1_enabled"] = scan_settings.get("model1_enabled", True)
        out["model2_enabled"] = scan_settings.get("model2_enabled", True)
        # Mask secret: show first 4 + asterisks
        sec = trade_config.get("api_secret","")
        out["api_secret_masked"] = (sec[:4] + "●●●●●●●●●●●●●●●●") if len(sec) > 4 else ("●" * len(sec))
        out["api_secret_set"]    = bool(sec)
    return jsonify(out)


@app.route("/api/health")
def api_health():
    return jsonify({"status":"healthy","signals":len(signals),"scanning":scan_state["running"]}),200

@app.route("/api/diag")
def api_diag():
    return jsonify(dict(diag))

@app.route("/api/monitor")
def api_monitor():
    with manip_lock:
        return jsonify(list(manip_monitor.values()))

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


# ════════ MODEL #2 MONITOR LOOP ══════════════════════════════════════
# Flow: HTF key level spotted → sharp sweep detected → wait for FVG to form
#       → watch until price taps FVG → instant market order

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
    log("🔍 Unified M2/M3 monitor started")
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
                        confirmed_model  = "3"
                        sweep_idx_used   = m3_sweep_idx
                        sweep_c_used     = m3_sweep_c
                        choch_idx_used   = m3_choch_idx
                        choch_lvl_used   = m3_choch_lvl
                    elif m2_sweep_idx is not None:
                        confirmed_model  = "2"
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
                    fvg_source     = None   # "pre_choch" or "post_sweep"
                    sl_final       = sl_sweep

                    if model_tag == "3" and choch_idx is not None:
                        # M3: prefer FVG that formed BEFORE CHoCH (higher RR)
                        pre_fvgs = _find_fvg_in_range(ltf_c, sweep_idx + 1, choch_idx, direction)
                        if pre_fvgs:
                            fvg_found_data = pre_fvgs[0]   # newest unmitigated pre-choch FVG
                            fvg_source     = "pre_choch"
                            sl_final       = sl_sweep       # SL above sweep candle
                        else:
                            # Pre-CHoCH FVGs all mitigated — find post-CHoCH FVG
                            post_fvgs = _find_fvg_in_range(ltf_c, choch_idx, len(ltf_c), direction)
                            if post_fvgs:
                                fvg_found_data = post_fvgs[0]
                                fvg_source     = "post_choch"
                                # SL above OB above the FVG
                                ob_extreme = _find_ob_above_fvg(
                                    ltf_c, fvg_found_data["top"], fvg_found_data["bot"], direction)
                                sl_final = (round(ob_extreme * 1.001, 8) if ob_extreme
                                            else sl_sweep)
                    else:
                        # M2: FVG forms after sweep
                        post_fvgs = _find_fvg_in_range(ltf_c, sweep_idx + 1, len(ltf_c), direction)
                        if post_fvgs:
                            fvg_found_data = post_fvgs[0]
                            fvg_source     = "post_sweep"
                            sl_final       = sl_sweep   # SL above sweep candle

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
                    if direction == "SELL" and price > fvg_top * 1.003:
                        with m2_lock: m2_monitor.pop(symbol, None)
                        log(f"❌ M{model_tag} INVALID: {symbol} blew through FVG top")
                        continue
                    if direction == "BUY"  and price < fvg_bot * 0.997:
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
                        entry_type = "Model #2 (Sweep→FVG Retest)"
                        details = [
                            "✅ HTF Key Level (P/D zone)",
                            f"✅ Swing point + single candle sweep of first touch",
                            f"✅ Sweep candle closes with wick only",
                            f"✅ Displacement FVG → retest",
                            f"✅ RR:{rr}R | SL: above sweep candle",
                        ]
                    else:
                        entry_type = "Model #3 (Sweep→CHoCH→FVG Retest)"
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
                        "choch_found":   model_tag == "3",
                        "choch_level":   choch_lvl if model_tag=="3" else "–",
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
                            send_telegram(f"Trade REJECTED: {symbol} {direction}\nReason: {msg}", kind="trade")
                    else:
                        log(f"⚠️ M{model_tag} live trade skipped ({symbol}): {reason}")

                    if paper_config["enabled"] and paper_config["auto_trade"]:
                        ok2, msg2 = place_paper_order(sig)
                        if ok2: log(f"📝 M{model_tag} paper: {msg2}")

                    with m2_lock:
                        m2_monitor.pop(symbol, None)

        except Exception as e:
            import traceback
            log(f"❌ [M2Mon] Error: {type(e).__name__}: {e}")
            log(f"   {traceback.format_exc()[-200:]}")
        time.sleep(5)   # FIXED: always sleeps even after exceptions


# FIXED: start_scanner — notify IP at boot, then launch all threads
def start_scanner():
    # ADDED: Send IP to Telegram immediately on every deploy/restart
    # This lets you whitelist it on MEXC before the first trade attempt
    log("🌐 [Startup] Detecting outbound IP for MEXC whitelisting...")
    try:
        notify_mexc_ip()   # ADDED: Always notify on startup
    except Exception as e:
        log(f"⚠️ [Startup] IP notification failed: {e}")

    test_api_connection()   # verify MEXC keys on every startup

    t  = threading.Thread(target=scanner_loop,       daemon=True, name="scanner"); t.start()
    m  = threading.Thread(target=manip_monitor_loop, daemon=True, name="manip");   m.start()
    m2 = threading.Thread(target=m2_monitor_loop,    daemon=True, name="m2mon");   m2.start()
    p  = threading.Thread(target=paper_monitor_loop, daemon=True, name="paper");   p.start()
    log("🚀 Scanner + M1 monitor + M2 monitor + paper monitor threads launched.")

def _delayed_start():
    time.sleep(2)
    start_scanner()

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

with app.app_context():
    _ensure_started()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
