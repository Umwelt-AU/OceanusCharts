#!/usr/bin/env python3
"""
GitHub Actions AIS fetcher.
Connects to aisstream.io for 90 seconds, collects vessel positions,
writes data/vessels.json for GitHub Pages to serve.
Also fetches NGA MSI alerts into data/alerts.json.

Env:  AIS_API_KEY  (set as GitHub Actions secret)
"""

import asyncio
import json
import os
import re
import time
import urllib.request
from datetime import datetime, timezone, timedelta
from pathlib import Path

try:
    import websockets
except ImportError:
    import subprocess, sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "websockets", "-q"])
    import websockets

AIS_KEY      = os.environ.get("AIS_API_KEY", "5bfce7eabb88f7717bef025177beda52d618d2dc")
AIS_URL      = "wss://stream.aisstream.io/v0/stream"
COLLECT_SECS = 90          # how long to listen before writing
DATA_DIR     = Path("data")
OUT_VESSELS  = DATA_DIR / "vessels.json"
OUT_ALERTS   = DATA_DIR / "alerts.json"

vessels: dict = {}


# ── AIS COLLECTION ───────────────────────────────────────────────────────────
async def collect():
    deadline = time.time() + COLLECT_SECS
    print(f"[AIS] Connecting ({COLLECT_SECS}s window)…")

    async with websockets.connect(AIS_URL, open_timeout=10, ping_interval=20) as ws:
        await ws.send(json.dumps({
            "APIKey":             AIS_KEY,
            "BoundingBoxes":      [[[-90, -180], [90, 180]]],
            "FilterMessageTypes": ["PositionReport", "ShipStaticData"],
        }))
        print("[AIS] Subscribed — collecting…")

        async for raw in ws:
            if time.time() > deadline:
                break
            try:
                msg  = json.loads(raw)
                kind = msg.get("MessageType", "")

                if kind == "PositionReport":
                    pr   = msg["Message"]["PositionReport"]
                    meta = msg.get("MetaData", {})
                    mmsi = str(meta.get("MMSI") or pr.get("UserID", ""))
                    if not mmsi:
                        continue
                    lat = pr.get("Latitude")
                    lon = pr.get("Longitude")
                    if lat is None or lon is None:
                        continue
                    if abs(lat) < 0.001 and abs(lon) < 0.001:
                        continue
                    hdg = pr.get("TrueHeading", 511)
                    if hdg == 511:
                        hdg = pr.get("Cog", 0)
                    prev = vessels.get(mmsi, {})
                    vessels[mmsi] = {
                        **prev,
                        "mmsi":    mmsi,
                        "lat":     round(lat, 5),
                        "lon":     round(lon, 5),
                        "speed":   round(pr.get("Sog", 0), 1),
                        "heading": round(hdg),
                        "course":  round(pr.get("Cog", 0), 1),
                        "navstat": pr.get("NavigationalStatus", 0),
                        "ts":      round(time.time()),
                    }

                elif kind == "ShipStaticData":
                    sd   = msg["Message"]["ShipStaticData"]
                    meta = msg.get("MetaData", {})
                    mmsi = str(meta.get("MMSI") or sd.get("UserID", ""))
                    if not mmsi:
                        continue
                    dim  = sd.get("Dimension") or {}
                    prev = vessels.get(mmsi, {})
                    vessels[mmsi] = {
                        **prev,
                        "mmsi":        mmsi,
                        "name":        (sd.get("Name") or "").strip() or prev.get("name"),
                        "type":        sd.get("Type") or prev.get("type", 0),
                        "callsign":    (sd.get("CallSign") or "").strip(),
                        "destination": (sd.get("Destination") or "").strip(),
                        "imo":         sd.get("ImoNumber", 0),
                        "draught":     sd.get("MaximumStaticDraught", 0),
                        "dim_a":       dim.get("A", 0),
                        "dim_b":       dim.get("B", 0),
                        "ts":          prev.get("ts", round(time.time())),
                    }

            except Exception:
                pass

    print(f"[AIS] Collected {len(vessels)} vessels")


# ── NGA MSI ALERTS ────────────────────────────────────────────────────────────
_MONTHS = dict(JAN=1, FEB=2, MAR=3, APR=4, MAY=5, JUN=6,
               JUL=7, AUG=8, SEP=9, OCT=10, NOV=11, DEC=12)

def _dms(s):
    h = s[-1]; parts = s[:-1].split("-")
    v = float(parts[0]) + (float(parts[1]) / 60 if len(parts) > 1 else 0)
    return -v if h in ("S", "W") else v

def _coords(text):
    m = re.search(r"(\d{1,3}-\d{1,2}(?:\.\d+)?[NS])\s+(\d{1,3}-\d{1,2}(?:\.\d+)?[EW])", text)
    if not m:
        return None, None
    try:
        return _dms(m.group(1)), _dms(m.group(2))
    except Exception:
        return None, None

def _navtex_dt(text):
    m = re.match(r"(\d{2})(\d{2})(\d{2})Z\s+(\w{3})\s+(\d{2,4})", text.strip())
    if not m:
        return None
    try:
        yr = int(m.group(5)); yr = yr + 2000 if yr < 100 else yr
        return datetime(yr, _MONTHS.get(m.group(4).upper(), 1),
                        int(m.group(1)), int(m.group(2)), int(m.group(3)),
                        tzinfo=timezone.utc)
    except Exception:
        return None

def _severity(text):
    t = text.upper()
    if any(k in t for k in ["PIRACY", "ARMED ROBBERY", "ATTACK", "HIJACK", "MINE ", "MISSILE", "HOSTILE"]):
        return "HIGH", "#ff4560", "🔴"
    if any(k in t for k in ["DISTRESS", "RESCUE", "MAYDAY", "TSUNAMI", "HURRICANE", "TYPHOON"]):
        return "HIGH", "#ff4560", "🔴"
    if any(k in t for k in ["FIRING", "EXERCISE", "MILITARY", "TORPEDO", "GUNNERY", "WEAPONS"]):
        return "MED", "#f0a500", "🟡"
    if any(k in t for k in ["CABLE", "SURVEY", "DREDGE", "BUOY", "UNLIT", "UNRELIABLE", "WRECK"]):
        return "LOW", "#00d4ff", "🔵"
    return "INFO", "#00e5a0", "⚓"

def _time_ago(dt):
    if not dt:
        return "recent"
    m = int((datetime.now(timezone.utc) - dt).total_seconds() / 60)
    return f"{m}m ago" if m < 60 else f"{m//60}h {m%60:02d}m ago"

def fetch_alerts():
    cutoff = datetime.now(timezone.utc) - timedelta(hours=12)
    results = []
    urls = [
        "https://msi.nga.mil/api/publications/broadcast-warn?status=active&output=json",
        *[f"https://msi.nga.mil/api/publications/broadcast-warn?navArea={r}&status=active&output=json"
          for r in ["IV", "XII", "I", "II", "III", "V", "VI", "VII", "VIII", "IX", "X", "XI"]]
    ]
    fetched_global = False
    for url in urls:
        if fetched_global:
            break
        try:
            req  = urllib.request.Request(url, headers={
                "User-Agent": "OCEANUS/1.0",
                "Accept":     "application/json",
            })
            resp = urllib.request.urlopen(req, timeout=15)
            raw  = json.loads(resp.read())
            warnings = raw if isinstance(raw, list) else (
                raw.get("navwarnings") or raw.get("broadcastWarn") or
                raw.get("broadcast-warn") or raw.get("warnings") or []
            )
            if not warnings:
                continue
            if "navArea=" not in url:
                fetched_global = True
            for w in warnings:
                text = (w.get("text") or w.get("msgText") or "").strip()
                if not text:
                    continue
                entry_str = w.get("entryDate") or w.get("lastModDate") or ""
                try:
                    issue_dt = datetime.fromisoformat(entry_str.replace("Z", "+00:00")) if entry_str else None
                except Exception:
                    issue_dt = None
                issue_dt = issue_dt or _navtex_dt(text)
                if issue_dt and issue_dt < cutoff:
                    continue
                lat, lon = _coords(text)
                sev, col, icon = _severity(text)
                lines = [l.strip() for l in text.split("\n") if l.strip()]
                title = re.sub(r"^\d{6}Z\s+\w{3}\s+\d{2,4}\s+", "", lines[0] if lines else "")[:120]
                body  = " ".join(lines[1:4])[:350] if len(lines) > 1 else text[:250]
                area_num = str(w.get("navArea") or "")
                roman_map = {str(i): r for i, r in enumerate(
                    ["I","II","III","IV","V","VI","VII","VIII","IX","X","XI","XII","XIII","XIV","XV","XVI"], 1
                )}
                navarea = f"NAVAREA {roman_map.get(area_num, area_num)}" if area_num else ""
                results.append({
                    "sev": sev, "col": col, "icon": icon,
                    "title": title, "body": body,
                    "time": _time_ago(issue_dt),
                    "lat": lat, "lon": lon,
                    "navarea": navarea,
                    "msgNum": f"{w.get('msgYear','')}/{w.get('msgNumber','')}",
                    "source": "NGA MSI",
                })
        except Exception as e:
            print(f"[ALERTS] {url[:60]}… {e}")
            continue

    seen, unique = set(), []
    for r in results:
        k = r["title"][:60]
        if k not in seen:
            seen.add(k); unique.append(r)
    unique.sort(key=lambda x: {"HIGH":0,"MED":1,"LOW":2,"INFO":3}.get(x["sev"], 9))
    return unique


# ── MAIN ──────────────────────────────────────────────────────────────────────
async def main():
    DATA_DIR.mkdir(exist_ok=True)

    # Run AIS collection
    try:
        await collect()
    except Exception as e:
        print(f"[AIS] Error: {e}")
        # Load existing data so we don't overwrite with empty
        if OUT_VESSELS.exists():
            existing = json.loads(OUT_VESSELS.read_text())
            for v in existing.get("vessels", []):
                vessels[v["mmsi"]] = v

    # Write vessels
    out = {
        "vessels":  list(vessels.values()),
        "count":    len(vessels),
        "updated":  datetime.now(timezone.utc).isoformat(),
    }
    OUT_VESSELS.write_text(json.dumps(out, separators=(",", ":")))
    print(f"[AIS] Wrote {len(vessels)} vessels → {OUT_VESSELS}")

    # Fetch and write alerts
    print("[ALERTS] Fetching NGA MSI…")
    try:
        alerts = fetch_alerts()
    except Exception as e:
        print(f"[ALERTS] Error: {e}")
        alerts = []
    alert_out = {
        "alerts":  alerts,
        "count":   len(alerts),
        "updated": datetime.now(timezone.utc).isoformat(),
        "source":  "NGA MSI Broadcast Warnings",
    }
    OUT_ALERTS.write_text(json.dumps(alert_out, separators=(",", ":")))
    print(f"[ALERTS] Wrote {len(alerts)} alerts → {OUT_ALERTS}")


if __name__ == "__main__":
    asyncio.run(main())
