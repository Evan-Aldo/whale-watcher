#!/usr/bin/env python3
"""
Prediction-Market Whale Watcher  ->  Discord
Watches BOTH Polymarket and Kalshi for large ("whale") trades and posts
platform-tagged embeds to a Discord channel via a webhook.

No account or API key is needed for either exchange -- both trade feeds are public.

TWO WAYS TO RUN
  * Always-on (default): runs forever, polling every POLL_SECONDS. Use on a host
    that keeps a process alive (Railway, a VPS, etc.). Near-real-time (~30s).
  * Single-shot (set RUN_ONCE=true): does ONE poll and exits. This is what the
    included GitHub Actions workflow uses to run free on a schedule (~every 5 min).

HOW EACH PLATFORM WORKS (different on purpose)
  Polymarket : trades carry a real trader identity (name / wallet); the API
               filters by cash value server-side. You get the "who" + the size.
  Kalshi     : trades are ANONYMOUS and there's no server-side size filter, so the
               bot pulls recent trades and filters locally. Kalshi flags large
               "block trades", which the bot can alert on regardless of size.

Everything in CONFIG can be set with environment variables (best for deployment)
or by editing the defaults directly.
"""

import json
import os
import sys
import time
from datetime import datetime, timezone

import requests

# ----------------------------------------------------------------------------
# CONFIG
# ----------------------------------------------------------------------------
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "PASTE_YOUR_WEBHOOK_URL_HERE")

ENABLE_POLYMARKET = os.getenv("ENABLE_POLYMARKET", "true").lower() == "true"
ENABLE_KALSHI     = os.getenv("ENABLE_KALSHI", "true").lower() == "true"

# Per-platform thresholds. Kalshi fills run thinner than Polymarket's biggest
# markets, so its bar is lower by default.
POLY_WHALE_USD   = float(os.getenv("POLY_WHALE_USD", "10000"))
POLY_BIG_USD     = float(os.getenv("POLY_BIG_USD", "50000"))   # "mega" -> gold + optional ping
KALSHI_WHALE_USD = float(os.getenv("KALSHI_WHALE_USD", "5000"))
KALSHI_BIG_USD   = float(os.getenv("KALSHI_BIG_USD", "25000"))

# Alert on every Kalshi block trade even if it's under KALSHI_WHALE_USD.
KALSHI_BLOCK_ALWAYS = os.getenv("KALSHI_BLOCK_ALWAYS", "true").lower() == "true"

# Run mode
RUN_ONCE     = os.getenv("RUN_ONCE", "false").lower() == "true"   # one poll then exit (for cron)
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "30"))               # always-on cadence
LOOKBACK_MIN = int(os.getenv("LOOKBACK_MIN", "30"))               # RUN_ONCE: window to scan each run
POST_BACKLOG = os.getenv("POST_BACKLOG", "false").lower() == "true"

# Optional: prepend to MEGA alerts, e.g. "<@&ROLE_ID>" (role ping) or "@here".
MEGA_MENTION = os.getenv("MEGA_MENTION", "").strip()

# Optional: restrict Polymarket to specific markets (comma-separated 0x conditionIds).
POLY_MARKET_CONDITION_IDS = os.getenv("POLY_MARKET_CONDITION_IDS", "").strip()
POLY_MAX_PAGES = int(os.getenv("POLY_MAX_PAGES", "5"))            # 100 whale trades/page

# Kalshi production host. Official is external-api.kalshi.com; api.elections.kalshi.com
# is the long-standing alias. Both serve ALL markets. Override if one is blocked.
KALSHI_BASE = os.getenv("KALSHI_BASE", "https://api.elections.kalshi.com/trade-api/v2")
KALSHI_MAX_PAGES = int(os.getenv("KALSHI_MAX_PAGES", "3"))        # 1000 trades/page

STATE_FILE = os.getenv("STATE_FILE", "whale_state.json")

# ----------------------------------------------------------------------------
POLY_TRADES_URL = "https://data-api.polymarket.com/trades"
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "prediction-whale-bot/2.1"})

COLOR_POLY, COLOR_KALSHI, COLOR_MEGA = 0x1652F0, 0x00B894, 0xF1C40F  # blue / teal / gold
MAX_SEEN = 6000
_kalshi_title_cache = {}  # ticker -> (title, event_ticker)


def log(msg):
    print(f"[{datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S}Z] {msg}", flush=True)


# ---- state ------------------------------------------------------------------
def load_state():
    try:
        with open(STATE_FILE) as f:
            d = json.load(f)
            return set(d.get("seen", [])), bool(d.get("seeded", False))
    except (FileNotFoundError, json.JSONDecodeError):
        return set(), False


def save_state(seen, seeded):
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump({"seen": list(seen)[-MAX_SEEN:], "seeded": seeded}, f)
    os.replace(tmp, STATE_FILE)


def parse_ts(v):
    if isinstance(v, (int, float)):
        return int(v)
    try:
        return int(datetime.fromisoformat(str(v).replace("Z", "+00:00")).timestamp())
    except Exception:
        return int(time.time())


# ---- Polymarket -------------------------------------------------------------
def fetch_polymarket_whales(since_ts):
    """Recent whale trades, paging back with offset until we pass since_ts."""
    out, offset = [], 0
    for _ in range(POLY_MAX_PAGES):
        params = {
            "limit": 100, "offset": offset, "takerOnly": "true",
            "filterType": "CASH", "filterAmount": POLY_WHALE_USD,
        }
        if POLY_MARKET_CONDITION_IDS:
            params["market"] = POLY_MARKET_CONDITION_IDS
        r = SESSION.get(POLY_TRADES_URL, params=params, timeout=20)
        r.raise_for_status()
        batch = r.json() or []
        if not batch:
            break
        oldest = min(parse_ts(t.get("timestamp")) for t in batch)
        for t in batch:
            size = float(t.get("size", 0) or 0)
            price = float(t.get("price", 0) or 0)
            usd = size * price
            if usd < POLY_WHALE_USD:
                continue
            event_slug = t.get("eventSlug") or t.get("slug") or ""
            wallet = t.get("proxyWallet", "")
            tx = t.get("transactionHash", "")
            out.append({
                "platform": "Polymarket",
                "key": "PM|" + "|".join(str(t.get(k, "")) for k in
                        ("transactionHash", "asset", "proxyWallet", "size", "price", "timestamp")),
                "usd": usd,
                "side": (t.get("side") or "").upper(),
                "outcome": t.get("outcome") or "?",
                "title": t.get("title") or t.get("slug") or "Unknown market",
                "market_url": f"https://polymarket.com/event/{event_slug}" if event_slug else "https://polymarket.com",
                "trader": t.get("name") or t.get("pseudonym") or (wallet[:10] if wallet else None),
                "trader_url": f"https://polymarket.com/profile/{wallet}" if wallet else None,
                "tx_url": f"https://polygonscan.com/tx/{tx}" if tx else None,
                "price_cents": price * 100,
                "shares": size,
                "timestamp": parse_ts(t.get("timestamp")),
                "icon": t.get("icon"),
                "is_block": False,
                "mega": usd >= POLY_BIG_USD,
            })
        offset += 100
        if oldest < since_ts or len(batch) < 100:
            break
    return out


# ---- Kalshi -----------------------------------------------------------------
def kalshi_title(ticker):
    """ticker -> (human title, event_ticker), cached. Falls back to the ticker."""
    if ticker in _kalshi_title_cache:
        return _kalshi_title_cache[ticker]
    title, event_ticker = ticker, None
    try:
        r = SESSION.get(f"{KALSHI_BASE}/markets/{ticker}", timeout=15)
        if r.ok:
            m = r.json().get("market", {}) or {}
            title = m.get("title") or m.get("subtitle") or ticker
            event_ticker = m.get("event_ticker")
    except requests.RequestException:
        pass
    _kalshi_title_cache[ticker] = (title, event_ticker)
    return title, event_ticker


def _kalshi_count(t):
    if t.get("count_fp") is not None:
        try:
            return float(t["count_fp"])
        except (TypeError, ValueError):
            pass
    return float(t.get("count", 0) or 0)


def _kalshi_prices(t):
    """Return (yes_price_dollars, no_price_dollars)."""
    yd, nd = t.get("yes_price_dollars"), t.get("no_price_dollars")
    if yd is not None:
        yes = float(yd)
        no = float(nd) if nd is not None else round(1 - yes, 4)
        return yes, no
    yc = t.get("yes_price")
    if yc is not None:
        yes = float(yc) / 100.0
        no = (float(t["no_price"]) / 100.0) if t.get("no_price") is not None else round(1 - yes, 4)
        return yes, no
    return 0.0, 0.0


def fetch_kalshi_whales(since_ts):
    raw, cursor = [], None
    for _ in range(KALSHI_MAX_PAGES):
        params = {"limit": 1000}
        if cursor:
            params["cursor"] = cursor
        r = SESSION.get(f"{KALSHI_BASE}/markets/trades", params=params, timeout=20)
        r.raise_for_status()
        page = r.json() or {}
        batch = page.get("trades", []) or []
        if not batch:
            break
        raw.extend(batch)
        cursor = page.get("cursor")
        if not cursor or min(parse_ts(t.get("created_time")) for t in batch) <= since_ts:
            break

    out = []
    for t in raw:
        count = _kalshi_count(t)
        yes, no = _kalshi_prices(t)
        taker = (t.get("taker_side") or "").lower()
        if taker == "yes":
            side, price_paid = "YES", yes
        elif taker == "no":
            side, price_paid = "NO", no
        else:
            side, price_paid = None, yes
        usd = count * price_paid
        is_block = bool(t.get("is_block_trade"))
        if usd < KALSHI_WHALE_USD and not (KALSHI_BLOCK_ALWAYS and is_block):
            continue

        ticker = t.get("ticker", "")
        title, event_ticker = kalshi_title(ticker)
        url = f"https://kalshi.com/markets/{event_ticker}" if event_ticker else \
              (f"https://kalshi.com/markets/{ticker}" if ticker else "https://kalshi.com")
        out.append({
            "platform": "Kalshi",
            "key": "KAL|" + (t.get("trade_id") or f"{ticker}-{t.get('created_time')}-{count}-{price_paid}"),
            "usd": usd,
            "side": side,
            "outcome": (side or "").title() or "\u2014",
            "title": title,
            "market_url": url,
            "trader": None,
            "trader_url": None,
            "tx_url": None,
            "price_cents": price_paid * 100,
            "shares": count,
            "timestamp": parse_ts(t.get("created_time")),
            "icon": None,
            "is_block": is_block,
            "mega": usd >= KALSHI_BIG_USD,
        })
    return out


# ---- Discord ----------------------------------------------------------------
def build_embed(n):
    side = n["side"]
    up = side in ("BUY", "YES")
    dot = "\U0001F7E2" if up else ("\U0001F534" if side else "\u26AA")
    whale = "\U0001F40B\U0001F525" if n["mega"] else "\U0001F40B"
    tag = "\U0001F9F1 BLOCK " if n["is_block"] else ""

    color = COLOR_MEGA if n["mega"] else (COLOR_POLY if n["platform"] == "Polymarket" else COLOR_KALSHI)
    side_txt = f"{dot} {side}" if side else f"{dot} \u2014"

    links = f"[Market]({n['market_url']})"
    if n.get("trader_url"):
        links += f" \u00B7 [Trader]({n['trader_url']})"
    if n.get("tx_url"):
        links += f" \u00B7 [Tx]({n['tx_url']})"

    price = n["price_cents"]
    embed = {
        "author": {"name": f"{n['platform']} whale alert"},
        "title": f"{whale} {tag}${n['usd']:,.0f} {side or ''} \u2014 {n['outcome']}".replace("  ", " ").strip(),
        "url": n["market_url"],
        "description": f"**{n['title']}**\n{links}",
        "color": color,
        "fields": [
            {"name": "Side",   "value": side_txt, "inline": True},
            {"name": "Size",   "value": f"{n['shares']:,.0f}", "inline": True},
            {"name": "Price",  "value": f"{price:.1f}\u00A2 ({price:.0f}% implied)", "inline": True},
            {"name": "Value",  "value": f"${n['usd']:,.0f}", "inline": True},
            {"name": "Trader", "value": n["trader"] if n["trader"] else "Anonymous", "inline": True},
        ],
        "footer": {"text": f"{n['platform']} whale watcher"},
        "timestamp": datetime.fromtimestamp(n["timestamp"], tz=timezone.utc).isoformat(),
    }
    if n.get("icon"):
        embed["thumbnail"] = {"url": n["icon"]}
    return embed


def post_to_discord(n):
    payload = {"embeds": [build_embed(n)]}
    if n["mega"] and MEGA_MENTION:
        payload["content"] = MEGA_MENTION
        payload["allowed_mentions"] = {"parse": ["roles", "everyone"]}
    for _ in range(5):
        resp = SESSION.post(DISCORD_WEBHOOK_URL, json=payload, timeout=20)
        if resp.status_code == 429:
            wait = 1.0
            try:
                wait = float(resp.json().get("retry_after", 1.0))
            except Exception:
                pass
            log(f"Discord rate limited, waiting {wait:.1f}s")
            time.sleep(wait + 0.25)
            continue
        resp.raise_for_status()
        return
    log("Gave up posting one embed after repeated rate limits.")


# ---- orchestration ----------------------------------------------------------
def gather(since_ts):
    trades = []
    if ENABLE_POLYMARKET:
        try:
            trades += fetch_polymarket_whales(since_ts)
        except requests.RequestException as e:
            log(f"Polymarket error: {e}")
    if ENABLE_KALSHI:
        try:
            trades += fetch_kalshi_whales(since_ts)
        except requests.RequestException as e:
            log(f"Kalshi error: {e}")
    trades.sort(key=lambda x: x["timestamp"])
    return trades


def post_new(trades, seen):
    new = [t for t in trades if t["key"] not in seen]
    for t in new:
        post_to_discord(t)
        seen.add(t["key"])
        time.sleep(0.4)  # stay under Discord's webhook rate limit
    if new:
        counts = {}
        for t in new:
            counts[t["platform"]] = counts.get(t["platform"], 0) + 1
        log("Posted " + ", ".join(f"{v} {k}" for k, v in counts.items()) + " whale trade(s).")
    return new


def main():
    if not DISCORD_WEBHOOK_URL or "PASTE_YOUR_WEBHOOK_URL" in DISCORD_WEBHOOK_URL:
        sys.exit("ERROR: set DISCORD_WEBHOOK_URL (env var or in the CONFIG block).")

    on = [p for p, e in (("Polymarket", ENABLE_POLYMARKET), ("Kalshi", ENABLE_KALSHI)) if e]
    mode = "single-shot" if RUN_ONCE else "always-on"
    log(f"Starting ({mode}). Watching {', '.join(on) or 'nothing!'}. "
        f"Poly>=${POLY_WHALE_USD:,.0f}, Kalshi>=${KALSHI_WHALE_USD:,.0f}"
        f"{' + all block trades' if KALSHI_BLOCK_ALWAYS else ''}.")

    seen, seeded = load_state()
    now = int(time.time())

    # ---- single-shot (cron / GitHub Actions) ----
    if RUN_ONCE:
        since = now - LOOKBACK_MIN * 60
        trades = gather(since)
        if not seeded and not POST_BACKLOG:
            for t in trades:
                seen.add(t["key"])
            save_state(seen, True)
            log(f"Seeded {len(seen)} existing trade(s); won't re-post. Next run posts new ones.")
            return
        post_new(trades, seen)
        save_state(seen, True)
        return

    # ---- always-on loop ----
    if not seeded and not POST_BACKLOG:
        for t in gather(now - 300):
            seen.add(t["key"])
        seeded = True
        save_state(seen, seeded)
        log(f"Seeded {len(seen)} existing trade(s); won't re-post. Watching for new ones...")

    window = max(POLL_SECONDS * 4, 120)  # scan a few polls wide; dedup handles overlap
    while True:
        try:
            new = post_new(gather(int(time.time()) - window), seen)
            if new:
                save_state(seen, True)
        except Exception as e:
            log(f"Loop error: {e}")
        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log("Stopped.")
