#!/usr/bin/env python3
"""Listings & Unlocks Telegram bot v2. Termux-friendly: requests + stdlib only.

Modes:
  python bot.py --check      test sources, price feeds, start-time parsing, bot token
  python bot.py --dry-run    print posts instead of sending
  python bot.py --chat-id    show chat ids that messaged the bot (for ADMIN_CHAT_ID)
  python bot.py --preview    DM sample posts to ADMIN_CHAT_ID only (never the channel)
  python bot.py --loop       run forever, polling every 5 min (servers like Railway)
  python bot.py              poll once and post new items (run from cron)

Env: BOT_TOKEN, CHANNEL (@name), ADMIN_CHAT_ID (optional: heartbeat + failure DMs),
     DISABLE_SOURCES (optional, comma list of source names to skip),
     DB_PATH (optional, e.g. /data/state.db on a persistent volume)
     SKIP_POSTS (optional, comma list of scheduled posts to skip: digest,recap,heartbeat)
"""
import argparse
import csv
import hashlib
import html
import os
import re
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone

import requests

TZ = timezone(timedelta(hours=3))
UTC = timezone.utc
HERE = os.path.dirname(os.path.abspath(__file__))
DB = os.environ.get("DB_PATH") or os.path.join(HERE, "state.db")
TAKE = os.path.join(HERE, "take.txt")
UNLOCKS = os.path.join(HERE, "unlocks.csv")
UA = {"User-Agent": "Mozilla/5.0 (Linux; Android 13) AppleWebKit/537.36 Chrome/120 Mobile Safari/537.36"}
MAX_POSTS_PER_RUN = 10
SEND_GAP = 3.1          # Telegram: max 20 msgs/min to one channel
FOLLOW_DELAY = 1800     # price check 30 min after the alert
FOLLOW_MIN_MOVE = 3.0   # post follow-up only if |move| >= 3%
STALE = 7200            # drop follow-ups missed by more than 2h
DIGEST_HOUR, HEARTBEAT_HOUR, RECAP_HOUR = 8, 9, 21  # UTC+3

KEEP = ("will list", "to list", "new listing", "will launch", "delist", "monitoring tag",
        "seed tag", "will add", "will remove", "마켓 추가", "신규 거래지원", "거래지원 종료", "유의")
SKIP = ("trading bot", "collateral", "cross margin", "isolated margin", "binance margin",
        "copy trading", "promotion", "maintenance", "convert", "simple earn", "binance earn")
NOT_TICKERS = {"UTC", "KST", "USD", "USDT", "KRW", "BTC", "ETH", "BNB"}


def wanted(title):
    t = title.lower()
    return any(k in t for k in KEEP) and not any(k in t for k in SKIP)


def classify(title):
    t = title.lower()
    if "delist" in t or "will remove" in t or "거래지원 종료" in t:
        return "DELISTING"
    if "monitoring" in t or "유의" in t:
        return "MONITORING"
    return "LISTING"


def market(title):
    t = title.lower()
    if "perpetual" in t or "futures" in t:
        return "perp"
    if "spot" in t or "will list" in t or "to list" in t or "krw" in t:
        return "spot"
    return ""


DELIST_RX = re.compile(r"(?i:delist|remove)\s+((?:[A-Z0-9]{2,12}(?:\s*,\s*|\s+and\s+|\s*&\s*))*[A-Z0-9]{2,12})\b")


def tickers(title):
    found = re.findall(r"\(([A-Z0-9]{2,12})\)", title)
    found += re.findall(r"\b([A-Z0-9]{2,12})/USDT\b", title)
    found += re.findall(r"\b([A-Z0-9]{2,12})USDT\b", title)
    if not found:
        m = DELIST_RX.search(title)  # "Will Delist ABC, DEF on ..." / "to delist A, B and C ..."
        if m:
            found = re.findall(r"[A-Z0-9]{2,12}", m.group(1))
    return [t for t in dict.fromkeys(found) if t not in NOT_TICKERS][:6]


def impact(i):
    """High-impact: Upbit KRW listings and Binance spot listings."""
    t = i["title"].lower()
    if classify(i["title"]) != "LISTING":
        return False
    if i["ex"] == "upbit":
        return "krw" in t
    if i["ex"] == "binance":
        return market(i["title"]) == "spot" and "alpha" not in t
    return False


# ---------- sources ----------

def http_get(url, params=None):
    last = None
    for n in range(3):
        try:
            r = requests.get(url, params=params, headers=UA, timeout=15)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last = e
            if n < 2:
                time.sleep(2 ** (n + 1))
    raise last


def item(ex, raw_id, title, url, ts, ref=None, text=""):
    return {"id": f"{ex}:{raw_id}", "ex": ex, "title": title.strip(), "url": url,
            "ts": ts, "ref": ref, "text": text or ""}


def binance(cat):
    d = http_get("https://www.binance.com/bapi/composite/v1/public/cms/article/list/query",
                 {"type": 1, "catalogId": cat, "pageNo": 1, "pageSize": 15})
    return [item("binance", a["id"], a["title"],
                 f"https://www.binance.com/en/support/announcement/{a['code']}",
                 a["releaseDate"] / 1000, ref=a["code"])
            for c in d["data"]["catalogs"] for a in c["articles"]]


def okx(ann):
    d = http_get("https://www.okx.com/api/v5/support/announcements", {"annType": ann, "page": 1})
    return [item("okx", x["url"], x["title"], x["url"], int(x["pTime"]) / 1000, ref=x["url"])
            for blk in d["data"] for x in blk["details"]]


def bybit(kind):
    d = http_get("https://api.bybit.com/v5/announcements/index",
                 {"locale": "en-US", "type": kind, "limit": 20})
    return [item("bybit", x["url"], x["title"], x["url"], x["dateTimestamp"] / 1000,
                 ref=x["url"], text=x.get("description", ""))
            for x in d["result"]["list"]]


def upbit():
    d = http_get("https://api-manager.upbit.com/api/v1/announcements",
                 {"os": "web", "page": 1, "per_page": 20, "category": "trade"})
    return [item("upbit", n["id"], n["title"],
                 f"https://upbit.com/service_center/notice?id={n['id']}",
                 datetime.fromisoformat(n["listed_at"]).timestamp(), ref=n["id"])
            for n in d["data"]["notices"]]


SOURCES = [
    ("binance-new", lambda: binance(48)),
    ("binance-delist", lambda: binance(161)),
    ("okx-new", lambda: okx("announcements-new-listings")),
    ("okx-delist", lambda: okx("announcements-delistings")),
    ("bybit-new", lambda: bybit("new_crypto")),
    ("bybit-delist", lambda: bybit("delistings")),
    ("upbit", upbit),
]


def active_sources():
    off = {s.strip() for s in os.environ.get("DISABLE_SOURCES", "").split(",") if s.strip()}
    return [(n, f) for n, f in SOURCES if n not in off]


# ---------- start time from announcement body ----------

MONTHS = {m: n for n, m in enumerate(
    ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"), 1)}
RANGE = r"(?:\s*[-\u2013]\s*\d{1,2}:\d{2})?"  # optional '- 10:00' range end
AMPM = r"(?:\s?(?P<ap>[AaPp][Mm]))?"
TZ_RX = r"\s*\(?\s*(?P<z>UTC|KST)"
TIME_RX = [
    # 2026-09-01 11:30 (UTC)   |   2026.09.01 18:00 (KST)
    re.compile(r"(?P<y>\d{4})[-.]\s?(?P<mo>\d{1,2})[-.]\s?(?P<d>\d{1,2})\.?[ T]*"
               r"(?P<h>\d{1,2}):(?P<mi>\d{2})(?::\d{2})?" + AMPM + RANGE + TZ_RX),
    # Jul 10, 2026 12:00 UTC  |  Oct 12, 2026, 11:00AM UTC  |  October 3, 2026, 08:00 - 10:00 UTC
    re.compile(r"\b(?P<mon>[A-Z][a-z]{2})[a-z]* (?P<d>\d{1,2}), (?P<y>\d{4}),? (?:at )?"
               r"(?P<h>\d{1,2}):(?P<mi>\d{2})(?::\d{2})?" + AMPM + RANGE + TZ_RX),
    # 08:00 UTC on September 30, 2026
    re.compile(r"(?P<h>\d{1,2}):(?P<mi>\d{2})(?::\d{2})?" + AMPM + r" ?\(?(?P<z>UTC|KST)\)? on "
               r"(?P<mon>[A-Z][a-z]{2})[a-z]* (?P<d>\d{1,2}), (?P<y>\d{4})"),
]


def strip_tags(s):
    s = re.sub(r"(?is)<(script|style).*?</\1>", " ", s)
    s = re.sub(r"<[^>]+>", " ", s)
    return html.unescape(re.sub(r"\s+", " ", s))


def fetch_text(i):
    """Best-effort announcement body as plain text ('' on any failure)."""
    ex, ref = i["ex"], i.get("ref")
    try:
        if ex == "binance":
            url = ("https://www.binance.com/bapi/composite/v1/public/cms/article/detail/query"
                   f"?articleCode={ref}")
        elif ex == "upbit":
            url = f"https://api-manager.upbit.com/api/v1/announcements/{ref}"
        else:
            url = ref
        r = requests.get(url, headers=UA, timeout=15)
        r.raise_for_status()
        return strip_tags(r.text.replace("\\n", " ").replace('\\"', '"'))
    except Exception:
        return ""


def find_start(text, ts=None, earliest=False):
    """Trading-start (or, with earliest=True, earliest effective) datetime in text, else None.
    ts = announcement time; dates outside [ts-1 day, ts+45 days] are ignored."""
    cands = []
    for rx in TIME_RX:
        for m in rx.finditer(text):
            g = m.groupdict()
            try:
                y, d = int(g["y"]), int(g["d"])
                mo = MONTHS[g["mon"]] if g.get("mon") else int(g["mo"])
                hh, mm = int(g["h"]), int(g["mi"])
                if g.get("ap"):
                    hh = hh % 12 + (12 if g["ap"].lower() == "pm" else 0)
                dt = datetime(y, mo, d, hh, mm,
                              tzinfo=timezone(timedelta(hours=9)) if g["z"] == "KST" else UTC)
            except (ValueError, KeyError):
                continue
            if ts and not (ts - 86400 <= dt.timestamp() <= ts + 45 * 86400):
                continue
            clause = re.split(r"[.:;|\n]\s", text[max(0, m.start() - 150):m.start()])[-1].lower()
            if "deposit" in clause or "withdraw" in clause:
                continue  # deposit/withdrawal times are not trading times
            ctx = text[max(0, m.start() - 120):m.start()].lower()
            good = any(k in ctx for k in ("trading", "list", "launch", "open", "start",
                                          "effective", "delist", "remove", "cease", "suspend"))
            cands.append((not good, m.start(), dt))
    if not cands:
        return None
    return min(c[2] for c in cands) if earliest else min(cands)[2]


# ---------- prices and perps ----------

def exname(ex):
    return {"okx": "OKX"}.get(ex, ex.capitalize())


def fp(p):
    return f"{p:,.2f}" if p >= 100 else f"{p:.6g}"


class Market:
    """Lazy per-run price/funding snapshots. A failed feed becomes {}."""

    def __init__(self):
        self._c = {}

    def _load(self, key, fn):
        if key not in self._c:
            try:
                self._c[key] = fn()
            except Exception:
                self._c[key] = {}
        return self._c[key]

    def bn_spot(self):
        return self._load("bs", lambda: {
            x["symbol"]: float(x["price"])
            for x in http_get("https://api.binance.com/api/v3/ticker/price")})

    def bn_perp(self):
        return self._load("bp", lambda: {
            x["symbol"]: (float(x["markPrice"]), float(x["lastFundingRate"] or 0) * 100)
            for x in http_get("https://fapi.binance.com/fapi/v1/premiumIndex")})

    def by_perp(self):
        return self._load("yp", lambda: {
            x["symbol"]: (float(x["lastPrice"]), float(x["fundingRate"] or 0) * 100)
            for x in http_get("https://api.bybit.com/v5/market/tickers",
                              {"category": "linear"})["result"]["list"] if x.get("lastPrice")})

    def price(self, token):
        s = token + "USDT"
        if s in self.bn_spot():
            return self.bn_spot()[s]
        if s in self.bn_perp():
            return self.bn_perp()[s][0]
        if s in self.by_perp():
            return self.by_perp()[s][0]
        return None

    def perps_line(self, toks):
        """'Perps (funding): FOO: Binance +0.010%, Bybit -0.020% | BAR: none' or ''."""
        if not (self.bn_perp() or self.by_perp()):
            return ""  # feeds down: say nothing rather than claim 'none'
        parts = []
        for t in toks[:2]:
            s = t + "USDT"
            out = [f"{n} {d[s][1]:+.3f}%" for n, d in
                   (("Binance", self.bn_perp()), ("Bybit", self.by_perp())) if s in d]
            parts.append(f"{t}: " + (", ".join(out) if out else "none"))
        return "Perps (funding): " + " | ".join(parts)


# ---------- state ----------

def db():
    c = sqlite3.connect(DB)
    c.execute("CREATE TABLE IF NOT EXISTS seen(id TEXT PRIMARY KEY)")
    c.execute("CREATE TABLE IF NOT EXISTS kv(k TEXT PRIMARY KEY, v TEXT)")
    c.execute("CREATE TABLE IF NOT EXISTS posts(ts REAL, kind TEXT, ex TEXT, toks TEXT, hi INTEGER)")
    c.execute("CREATE TABLE IF NOT EXISTS track(id TEXT PRIMARY KEY, token TEXT, ex TEXT, "
              "p0 REAL, due REAL, msg_id INTEGER, done INTEGER DEFAULT 0, chg REAL)")
    return c


def kv_get(c, k):
    row = c.execute("SELECT v FROM kv WHERE k=?", (k,)).fetchone()
    return row[0] if row else None


def kv_set(c, k, v):
    c.execute("INSERT OR REPLACE INTO kv VALUES(?,?)", (k, str(v)))
    c.commit()


# ---------- telegram ----------

def send(chat, text, reply_to=None, button=None):
    token = os.environ["BOT_TOKEN"]
    body = {"chat_id": chat, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
    if button:
        body["reply_markup"] = {"inline_keyboard": [[{"text": "\U0001F517 Announcement", "url": button}]]}
    if reply_to:
        body["reply_parameters"] = {"message_id": reply_to, "allow_sending_without_reply": True}
    for _ in range(3):
        r = requests.post(f"https://api.telegram.org/bot{token}/sendMessage", json=body, timeout=15)
        if r.status_code == 429:
            time.sleep(r.json().get("parameters", {}).get("retry_after", 10) + 1)
            continue
        r.raise_for_status()
        return r.json()["result"]["message_id"]
    raise RuntimeError("Telegram rate limit not clearing")


def fmt_time(dt):
    u, l = dt.astimezone(UTC), dt.astimezone(TZ)
    loc = l.strftime("%H:%M") if l.date() == u.date() else l.strftime("%d %b %H:%M")
    return u.strftime("%d %b") + " \u00b7 " + u.strftime("%H:%M") + " UTC (" + loc + " UTC+3)"


def clean_title(t):
    t = re.sub(r"^New listing:\s*", "", t.strip(), flags=re.I)
    t = re.sub(r"^(Binance|OKX|Bybit|Upbit)\s+", "", t)
    t = re.sub(r"\s*[-\u2013]?\s*\(?\d{4}-\d{2}-\d{2}\)?\s*$", "", t)
    t = re.sub(r"\s+(on|at)$", "", t)
    return t if len(t) <= 110 else t[:107].rstrip() + "..."


def meta_line(title):
    t = title.lower()
    bits = []
    m = market(title)
    if m:
        bits.append({"perp": "Perpetual", "spot": "Spot"}[m])
    if "seed tag" in t:
        bits.append("Seed Tag")
    if "krw" in t:
        bits.append("KRW market")
    return " \u00b7 ".join(bits)


UP, DOWN = "\U0001F680", "\U0001F53B"
KIND_STYLE = {"LISTING": ("\U0001F7E2", "NEW LISTING"), "DELISTING": ("\U0001F534", "DELISTING"),
              "MONITORING": ("\U0001F7E1", "MONITORING TAG")}


def fmt(i, start=None, perps="", take=""):
    kind = classify(i["title"])
    icon, label = KIND_STYLE[kind]
    tk = tickers(i["title"])
    lines = [f"{icon} <b>{label}</b> \u00b7 {exname(i['ex'])}"]
    if impact(i):
        lines.append("\U0001F525 <i>High impact</i>")
    if tk:
        lines.append(("\U0001F48E" if kind == "LISTING" else "\U0001FA99") + " <b>"
                     + " \u00b7 ".join("$" + t for t in tk) + "</b>")
    else:
        lines.append("\U0001F4CC " + html.escape(clean_title(i["title"])))
    meta = meta_line(i["title"])
    if meta:
        lines.append("\U0001F4CA " + meta)
    if start:
        word = "Starts" if kind == "LISTING" else "Effective"
        lines.append(f"\u23F0 {word} <b>{html.escape(fmt_time(start))}</b>")
    if perps:
        lines.append("\U0001F4C8 " + html.escape(perps))
    if take:
        lines += ["", f"\U0001F4AC <b>Take:</b> {html.escape(take)}"]
    tags = ["#" + exname(i["ex"]).replace(" ", "")] + ["#" + t for t in tk]
    lines += ["", " ".join(tags)]
    return "\n".join(lines)


def enrich(i, mk):
    """(start datetime or None, perps line) for one announcement."""
    start = find_start(i["title"] + " | " + i.get("text", "") + " " + fetch_text(i), ts=i["ts"],
                       earliest=classify(i["title"]) != "LISTING")
    perps = ""
    tk = tickers(i["title"])
    if classify(i["title"]) == "LISTING" and market(i["title"]) != "perp" and tk:
        perps = mk.perps_line(tk)
    return start, perps


def pop_take(dry, c):
    """Return take.txt text once. Locally the file is cleared after use. On GitHub Actions the
    file lives in the repo and cannot be cleared, so a hash in state marks it as used."""
    try:
        with open(TAKE, encoding="utf-8") as f:
            t = f.read().strip()
    except FileNotFoundError:
        return ""
    if not t:
        return ""
    if os.environ.get("GITHUB_ACTIONS"):
        h = hashlib.sha1(t.encode("utf-8")).hexdigest()
        if kv_get(c, "take_hash") == h:
            return ""
        if not dry:
            kv_set(c, "take_hash", h)
    elif not dry:
        open(TAKE, "w").close()
    return t


# ---------- price reaction follow-ups ----------

def start_tracking(c, i, mid, mk):
    if classify(i["title"]) != "LISTING":
        return
    for t in tickers(i["title"])[:2]:
        p0 = mk.price(t)
        if p0:
            c.execute("INSERT OR IGNORE INTO track VALUES(?,?,?,?,?,?,0,NULL)",
                      (f"{i['id']}:{t}", t, i["ex"], p0, time.time() + FOLLOW_DELAY, mid))


def followups(c, mk, channel, dry, failed):
    now = time.time()
    rows = c.execute("SELECT id, token, ex, p0, due, msg_id FROM track "
                     "WHERE done=0 AND due<=?", (now,)).fetchall()
    for id_, tok, ex, p0, due, mid in rows:
        if now - due > STALE:
            c.execute("UPDATE track SET done=1 WHERE id=?", (id_,))
            continue
        p1 = mk.price(tok)
        if not p1:
            continue
        chg = (p1 / p0 - 1) * 100
        if abs(chg) >= FOLLOW_MIN_MOVE:
            icon = "\U0001F680" if chg >= 0 else "\U0001F53B"
            text = (f"{icon} <b>{tok} {chg:+.1f}%</b> in 30 min after alert ({exname(ex)})\n"
                    f"{fp(p0)} \u2192 {fp(p1)}")
            if dry:
                print(text, "\n---")
                continue
            try:
                send(channel, text, reply_to=mid)
            except Exception as e:
                failed.append(f"followup: {type(e).__name__}")
                continue
            time.sleep(SEND_GAP)
        if not dry:
            c.execute("UPDATE track SET done=1, chg=? WHERE id=?", (chg, id_))
    c.commit()


# ---------- scheduled posts: unlock digest, recap, heartbeat ----------

def digest_text():
    today = datetime.now(TZ).date()
    rows = []
    try:
        f = open(UNLOCKS, newline="", encoding="utf-8")
    except FileNotFoundError:
        return ""
    with f:
        for r in csv.DictReader(f):
            try:
                d = datetime.strptime(r["date"].strip(), "%Y-%m-%d").date()
                pct = float(r.get("pct_supply") or 0)
                usd = float(r.get("usd_millions") or 0)
                tok = r["token"].strip().upper()
            except (ValueError, KeyError):
                continue
            if 0 <= (d - today).days <= 7 and (usd >= 5 or pct >= 1):
                rows.append((d, tok, pct, usd))
    if not rows:
        return ""
    rows.sort()
    out = ["\U0001F4C5 <b>UNLOCKS</b> \u00b7 next 7 days", "<pre>Date    Token     %Sup   USD"]
    out += [f"{d:%d %b}  {t:<8}{p:>5.1f}%  ${u:,.0f}M" for d, t, p, u in rows]
    out.append("</pre>")
    return "\n".join(out)


def recap_text(c):
    now = datetime.now(TZ)
    mid = now.replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
    cnt = dict(c.execute("SELECT kind, COUNT(*) FROM posts WHERE ts>=? GROUP BY kind", (mid,)))
    if not cnt:
        return ""
    lines = [f"\U0001F4CA <b>RECAP</b> \u00b7 {now:%d %b}",
             f"\U0001F7E2 Listings {cnt.get('LISTING', 0)} \u00b7 \U0001F534 Delistings {cnt.get('DELISTING', 0)} "
             f"\u00b7 \U0001F7E1 Monitoring {cnt.get('MONITORING', 0)}"]
    moves = c.execute("SELECT token, ex, chg FROM track WHERE chg IS NOT NULL AND due>=? "
                      "ORDER BY ABS(chg) DESC LIMIT 3", (mid,)).fetchall()
    if moves:
        lines += ["", "\U0001F525 <b>Biggest 30 min moves</b>"]
        lines += [f"{UP if chg >= 0 else DOWN} {t} {chg:+.1f}% ({exname(ex)})"
                  for t, ex, chg in moves]
    return "\n".join(lines)


def runner():
    return "github" if os.environ.get("GITHUB_ACTIONS") else os.environ.get("RUNNER_NAME", "local")


def heartbeat_text(c):
    n = c.execute("SELECT COUNT(*) FROM posts WHERE ts>=?", (time.time() - 86400,)).fetchone()[0]
    return f"Bot alive ({runner()}). Alerts in 24h: {n}. Failing sources: {kv_get(c, 'last_failed') or 'none'}"


# ---------- modes ----------

def check():
    print("Source check:")
    got = {}
    for name, fn in active_sources():
        try:
            items = fn()
            got[name] = items
            first = items[0]["title"][:60] if items else "-"
            print(f"  OK    {name:<15} {len(items):>2} items | {first}")
        except Exception as e:
            print(f"  FAIL  {name:<15} {type(e).__name__}: {str(e)[:70]}")
    print("Price feeds:")
    mk = Market()
    for label, d in (("binance-spot", mk.bn_spot()), ("binance-perp", mk.bn_perp()),
                     ("bybit-perp", mk.by_perp())):
        print(f"  {'OK  ' if d else 'FAIL'}  {label:<13} {len(d)} symbols")
    print("Start-time parsing (latest matching item per source):")
    for name, items in got.items():
        cand = [i for i in items if wanted(i["title"])][:1]
        if not cand:
            print(f"  --    {name:<15} no matching item")
            continue
        i = cand[0]
        start = find_start(i["title"] + " | " + i.get("text", "") + " " + fetch_text(i), ts=i["ts"],
                           earliest=classify(i["title"]) != "LISTING")
        shown = f"{start.astimezone(TZ):%d %b %H:%M} UTC+3" if start else "not found"
        print(f"  {'OK  ' if start else 'none'}  {name:<15} {shown} | {i['title'][:40]}")
    token = os.environ.get("BOT_TOKEN")
    if token:
        try:
            r = requests.get(f"https://api.telegram.org/bot{token}/getMe", timeout=15).json()
            print("  Bot:", "OK @" + r["result"]["username"] if r.get("ok") else "FAIL " + str(r))
        except Exception as e:
            print("  Bot: FAIL", e)
    else:
        print("  Bot: BOT_TOKEN not set")


def chat_ids():
    r = requests.get(f"https://api.telegram.org/bot{os.environ['BOT_TOKEN']}/getUpdates",
                     timeout=15).json()
    seen = {}
    for u in r.get("result", []):
        ch = (u.get("message") or u.get("channel_post") or {}).get("chat", {})
        if ch.get("id"):
            seen[ch["id"]] = ch.get("username") or ch.get("first_name") or ch.get("title")
    print(seen or "No messages found. Send /start to the bot, then rerun.")


def preview():
    admin = os.environ.get("ADMIN_CHAT_ID")
    if not (os.environ.get("BOT_TOKEN") and admin):
        sys.exit("Set BOT_TOKEN and ADMIN_CHAT_ID. Previews go to your DM only, never the channel.")
    now = time.time()
    t0 = datetime(2026, 10, 12, 10, 0, tzinfo=UTC)
    samples = [
        (item("binance", "p1", "Binance Will List Foo (FOO) with Seed Tag Applied",
              "https://www.binance.com/en/support/announcement/list/48", now), t0,
         "Perps (funding): FOO: Binance +0.010%, Bybit -0.020%", "Seed tag = high volatility."),
        (item("bybit", "p2", "New listing: Listing of PBRUSDT on Bybit TradFi Perpetual on Oct 12, 2026, 11:00AM UTC",
              "https://announcements.bybit.com/en/", now), t0.replace(hour=11), "", ""),
        (item("binance", "p3", "Binance Futures Will Launch Multiple TradFi USD\u24c8-Margined Perpetual Contracts (2026-10-12)",
              "https://www.binance.com/en/support/announcement/list/48", now), t0.replace(hour=9), "", ""),
        (item("upbit", "p4", "\uce74\uc774\uc544(KAIA) \uc2e0\uaddc \uac70\ub798\uc9c0\uc6d0 \uc548\ub0b4 (KRW, BTC, USDT \ub9c8\ucf13)",
              "https://upbit.com/service_center/notice", now), t0.replace(hour=3), "", ""),
        (item("okx", "p5", "OKX to delist DORA, ICX, STORJ, ZEUS and ELF spot trading pairs",
              "https://www.okx.com/help/section/announcements-delistings", now), t0 + timedelta(days=18), "", ""),
    ]
    send(admin, "\U0001F440 <b>Preview</b>: sample posts below. Nothing was sent to the channel.")
    for it, st, perps, take in samples:
        send(admin, fmt(it, st, perps, take), button=it["url"])
        time.sleep(1)
    send(admin, "\U0001F680 <b>FOO +25.0%</b> in 30 min after alert (Binance)\n1 \u2192 1.25")
    send(admin, "\U0001F4C5 <b>UNLOCKS</b> \u00b7 next 7 days\n<pre>Date    Token     %Sup   USD\n"
                "14 Oct  ARB        2.1%  $45M</pre>")


def run(dry):
    if not dry and not (os.environ.get("BOT_TOKEN") and os.environ.get("CHANNEL")):
        sys.exit("Set BOT_TOKEN and CHANNEL (or use --dry-run).")
    channel = os.environ.get("CHANNEL", "")
    admin = os.environ.get("ADMIN_CHAT_ID")
    c = db()
    mk = Market()
    new, failed = [], []

    for name, fn in active_sources():
        try:
            items = fn()
        except Exception as e:
            failed.append(f"{name}: {type(e).__name__}")
            continue
        if kv_get(c, f"seeded:{name}") is None:  # first run per source: seed, don't post
            c.executemany("INSERT OR IGNORE INTO seen VALUES(?)", [(i["id"],) for i in items])
            kv_set(c, f"seeded:{name}", 1)
            print(f"seeded {name} ({len(items)})")
            continue
        for i in items:
            if wanted(i["title"]) and not c.execute(
                    "SELECT 1 FROM seen WHERE id=?", (i["id"],)).fetchone():
                new.append(i)

    new.sort(key=lambda i: i["ts"])
    take = pop_take(dry, c) if new else ""
    for n, i in enumerate(new[:MAX_POSTS_PER_RUN]):
        start, perps = enrich(i, mk)
        text = fmt(i, start, perps, take if n == 0 else "")
        if dry:
            print(text, "\n[button] " + i["url"], "\n---")
            continue
        try:
            mid = send(channel, text, button=i["url"])
        except Exception as e:
            failed.append(f"telegram: {type(e).__name__}")
            break
        c.execute("INSERT OR IGNORE INTO seen VALUES(?)", (i["id"],))
        c.execute("INSERT INTO posts VALUES(?,?,?,?,?)",
                  (time.time(), classify(i["title"]), i["ex"],
                   ",".join(tickers(i["title"])), int(impact(i))))
        start_tracking(c, i, mid, mk)
        c.commit()
        time.sleep(SEND_GAP)

    followups(c, mk, channel, dry, failed)

    now = datetime.now(TZ)
    skip = {x.strip() for x in os.environ.get("SKIP_POSTS", "").split(",") if x.strip()}
    for key, hour, build, target in (
            ("digest", DIGEST_HOUR, digest_text, channel),
            ("recap", RECAP_HOUR, lambda: recap_text(c), channel),
            ("heartbeat", HEARTBEAT_HOUR, lambda: heartbeat_text(c), admin)):
        if key in skip or now.hour < hour or kv_get(c, key) == str(now.date()) or (not target and not dry):
            continue
        text = build()
        if text:
            if dry:
                print(text, "\n---")
            else:
                try:
                    send(target, text)
                except Exception as e:
                    failed.append(f"{key}: {type(e).__name__}")
                    continue
        if not dry:
            kv_set(c, key, now.date())

    if not dry:
        kv_set(c, "last_failed", "; ".join(failed))
    if failed:
        print("failed:", "; ".join(failed))
        last = float(kv_get(c, "last_alert") or 0)
        if admin and not dry and time.time() - last > 3600:
            try:
                send(admin, f"Bot sources failing ({runner()}): " + "; ".join(failed))
                kv_set(c, "last_alert", time.time())
            except Exception:
                pass


def loop():
    while True:
        try:
            run(False)
        except Exception as e:  # keep the server process alive; cron-like retry in 5 min
            print("run error:", repr(e), flush=True)
        time.sleep(300)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--check", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--chat-id", action="store_true")
    ap.add_argument("--loop", action="store_true")
    ap.add_argument("--preview", action="store_true")
    a = ap.parse_args()
    if a.loop:
        loop()
    elif a.check:
        check()
    elif a.chat_id:
        chat_ids()
    elif a.preview:
        preview()
    else:
        run(a.dry_run)
