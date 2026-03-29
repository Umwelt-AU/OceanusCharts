#!/usr/bin/env python3
"""
OCEANUS — GitHub Actions AIS fetcher + vessel metadata enrichment.

What this does each run:
  1. Streams aisstream.io for 90s → data/vessels.json
  2. Fetches NGA MSI alerts       → data/alerts.json
  3. Enriches new IMOs with:
       a. Datalastic API (free tier, 100 calls/month)
       b. VesselFinder public vessel page (fallback, no key needed)
     → data/vessels_meta.json  (persistent cache, grows over time)
  4. Merges meta into vessels.json for the frontend

Meta fields added per vessel:
  dwt          — deadweight tonnage (cargo capacity proxy)
  gt           — gross tonnage
  year_built   — build year
  flag         — flag state (ISO 2-letter)
  flag_name    — full country name
  vessel_class — human description (VLCC, Panamax, etc.)
  commodity    — inferred commodity from type + dwt
  owner        — registered owner (when available)
  length, beam — dimensions in metres

Env vars (all optional — script works without them):
  AIS_API_KEY      — aisstream.io key
  DATALASTIC_KEY   — datalastic.com key (free: 100 calls/month)
"""

import asyncio, json, os, re, time, urllib.request, urllib.parse
from datetime import datetime, timezone, timedelta
from pathlib import Path

try:
    import websockets
except ImportError:
    import subprocess, sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "websockets", "-q"])
    import websockets

# ── Config ────────────────────────────────────────────────────────────────────
AIS_KEY          = os.environ.get("AIS_API_KEY",    "5bfce7eabb88f7717bef025177beda52d618d2dc")
DATALASTIC_KEY   = os.environ.get("DATALASTIC_KEY", "")   # optional
AIS_URL          = "wss://stream.aisstream.io/v0/stream"
COLLECT_SECS     = 90
ENRICH_MAX       = 40    # max new IMOs to enrich per run (stay in free-tier budget)
DATA_DIR         = Path("data")
OUT_VESSELS      = DATA_DIR / "vessels.json"
OUT_ALERTS       = DATA_DIR / "alerts.json"
OUT_META         = DATA_DIR / "vessels_meta.json"   # persistent, cumulative

vessels: dict = {}

# ── AIS type → commodity mapping ─────────────────────────────────────────────
# AIS type codes: https://api.vesselfinder.com/docs/ref-aistypes.html
def infer_commodity(ais_type: int, dwt: int) -> str:
    t = ais_type
    if 80 <= t <= 84:   # tanker subtypes
        if dwt >= 200_000: return "Crude Oil (VLCC)"
        if dwt >= 80_000:  return "Crude Oil (Suezmax)"
        if dwt >= 40_000:  return "Crude Oil (Aframax)"
        if dwt > 0:        return "Oil Products / Chemicals"
        return "Tanker (unspecified)"
    if t == 85:            return "LNG"
    if t == 86:            return "LPG"
    if t in (87, 88, 89): return "Chemical Tanker"
    if 70 <= t <= 79:
        if dwt >= 180_000: return "Bulk — Iron Ore / Coal (Capesize)"
        if dwt >= 60_000:  return "Bulk — Grain / Coal (Panamax)"
        if dwt >= 25_000:  return "Bulk — Grain / Fertiliser (Handymax)"
        if dwt > 0:        return "Bulk / General Cargo"
        return "Cargo (unspecified)"
    if 30 <= t <= 39:      return "Fish / Seafood"
    if t == 36:            return "Sailing / Leisure"
    if 40 <= t <= 49:      return "High Speed / Passenger Ferry"
    if 60 <= t <= 69:      return "Passengers"
    if t == 52:            return "Tug / Port Service"
    if t == 51:            return "Search & Rescue"
    if t == 55:            return "Law Enforcement"
    return "General / Unknown"

def infer_vessel_class(ais_type: int, dwt: int, length: int) -> str:
    t = ais_type
    if 80 <= t <= 89:   # tankers
        if dwt >= 200_000: return "VLCC"
        if dwt >= 120_000: return "Suezmax"
        if dwt >= 80_000:  return "Aframax"
        if dwt >= 40_000:  return "Panamax Tanker"
        if dwt >= 10_000:  return "MR Tanker"
        if dwt > 0:        return "Small Tanker"
        if t == 85:        return "LNG Carrier"
        if t == 86:        return "LPG Carrier"
        return "Tanker"
    if 70 <= t <= 79:   # cargo
        if dwt >= 180_000: return "Capesize Bulk"
        if dwt >= 65_000:  return "Panamax Bulk"
        if dwt >= 40_000:  return "Supramax"
        if dwt >= 10_000:  return "Handysize"
        if length >= 300:  return "Large Container"
        if length >= 200:  return "Container"
        if dwt > 0:        return "General Cargo"
        return "Cargo"
    if 60 <= t <= 69:   return "Passenger / Cruise"
    if 40 <= t <= 49:   return "High Speed Craft"
    if 30 <= t <= 39:   return "Fishing"
    return "Other"

# ── Flag code → country name ──────────────────────────────────────────────────
FLAG_NAMES = {
    "PA":"Panama","LR":"Liberia","MH":"Marshall Islands","HK":"Hong Kong",
    "SG":"Singapore","MT":"Malta","BS":"Bahamas","CY":"Cyprus","CN":"China",
    "GB":"United Kingdom","NO":"Norway","GR":"Greece","JP":"Japan","KR":"South Korea",
    "US":"United States","DE":"Germany","NL":"Netherlands","IT":"Italy","FR":"France",
    "ES":"Spain","DK":"Denmark","SE":"Sweden","FI":"Finland","BE":"Belgium",
    "PT":"Portugal","IN":"India","BR":"Brazil","AU":"Australia","NZ":"New Zealand",
    "RU":"Russia","TR":"Turkey","AE":"UAE","SA":"Saudi Arabia","KW":"Kuwait",
    "OM":"Oman","QA":"Qatar","BH":"Bahrain","IR":"Iran","PK":"Pakistan",
    "BD":"Bangladesh","MM":"Myanmar","TH":"Thailand","MY":"Malaysia","ID":"Indonesia",
    "PH":"Philippines","VN":"Vietnam","TW":"Taiwan","MX":"Mexico","CL":"Chile",
    "AR":"Argentina","CO":"Colombia","PE":"Peru","EG":"Egypt","ZA":"South Africa",
    "NG":"Nigeria","KE":"Kenya","TZ":"Tanzania","MA":"Morocco","AG":"Antigua & Barbuda",
    "VU":"Vanuatu","PW":"Palau","KI":"Kiribati","TV":"Tuvalu","CK":"Cook Islands",
    "TO":"Tonga","WF":"Wallis & Futuna","CV":"Cape Verde","MG":"Madagascar",
    "MU":"Mauritius","SC":"Seychelles","KM":"Comoros","DJ":"Djibouti",
    "SO":"Somalia","ER":"Eritrea","YE":"Yemen","JO":"Jordan","IL":"Israel",
    "LB":"Lebanon","SY":"Syria","IQ":"Iraq","AF":"Afghanistan",
}


# ── Destination → country parsing ─────────────────────────────────────────────
# UN/LOCODE first 2 chars are ISO country, e.g. NLRTM → NL (Netherlands)
# Also handles common free-text destinations
DEST_TEXT_MAP = {
    "ROTTERDAM":"NL","AMSTERDAM":"NL","ANTWERP":"BE","HAMBURG":"DE",
    "SINGAPORE":"SG","HONG KONG":"HK","SHANGHAI":"CN","NINGBO":"CN",
    "BUSAN":"KR","TOKYO":"JP","YOKOHAMA":"JP","OSAKA":"JP",
    "LOS ANGELES":"US","LONG BEACH":"US","NEW YORK":"US","HOUSTON":"US",
    "MIAMI":"US","SAVANNAH":"US","NORFOLK":"US","SEATTLE":"US",
    "VANCOUVER":"CA","MONTREAL":"CA","HALIFAX":"CA",
    "FELIXSTOWE":"GB","LONDON":"GB","SOUTHAMPTON":"GB","LIVERPOOL":"GB",
    "LE HAVRE":"FR","MARSEILLE":"FR","DUNKERQUE":"FR",
    "GENOA":"IT","TRIESTE":"IT","NAPLES":"IT","LA SPEZIA":"IT",
    "BARCELONA":"ES","VALENCIA":"ES","ALGECIRAS":"ES","BILBAO":"ES",
    "PIRAEUS":"GR","THESSALONIKI":"GR",
    "ISTANBUL":"TR","MERSIN":"TR","IZMIR":"TR",
    "JEBEL ALI":"AE","DUBAI":"AE","ABU DHABI":"AE","SHARJAH":"AE",
    "DAMMAM":"SA","JUBAIL":"SA","JEDDAH":"SA","YANBU":"SA",
    "MUMBAI":"IN","NHAVA SHEVA":"IN","CHENNAI":"IN","KOLKATA":"IN",
    "KARACHI":"PK","COLOMBO":"LK","CHITTAGONG":"BD",
    "PORT KLANG":"MY","TANJUNG PELEPAS":"MY","PENANG":"MY",
    "LAEM CHABANG":"TH","BANGKOK":"TH",
    "JAKARTA":"ID","SURABAYA":"ID","BELAWAN":"ID",
    "SYDNEY":"AU","MELBOURNE":"AU","BRISBANE":"AU","FREMANTLE":"AU",
    "AUCKLAND":"NZ","TAURANGA":"NZ",
    "SANTOS":"BR","RIO DE JANEIRO":"BR","ITAGUAI":"BR",
    "BUENOS AIRES":"AR","MONTEVIDEO":"UY",
    "CALLAO":"PE","VALPARAISO":"CL",
    "DURBAN":"ZA","CAPE TOWN":"ZA","PORT ELIZABETH":"ZA",
    "MOMBASA":"KE","DAR ES SALAAM":"TZ","LAGOS":"NG","DAKAR":"SN",
    "CASABLANCA":"MA","ALEXANDRIA":"EG","PORT SAID":"EG","SUEZ":"EG",
    "ADEN":"YE","MUSCAT":"OM","SALALAH":"OM","SOHAR":"OM",
    "BANDAR ABBAS":"IR","ASSALUYEH":"IR",
}

def parse_destination_country(dest: str) -> tuple:
    """Return (iso2, country_name) from a raw AIS destination string."""
    if not dest or dest in ("", "---", "N/A", "NONE", "AT ANCHOR", "AT SEA"):
        return None, None
    d = dest.strip().upper()
    # Try UN/LOCODE: 5-char alphanumeric, first 2 = ISO country
    if re.match(r'^[A-Z]{2}[A-Z0-9]{3}$', d):
        iso = d[:2]
        if iso in FLAG_NAMES:
            return iso, FLAG_NAMES[iso]
    # Try text map (longest match first)
    for key in sorted(DEST_TEXT_MAP, key=len, reverse=True):
        if key in d:
            iso = DEST_TEXT_MAP[key]
            return iso, FLAG_NAMES.get(iso, iso)
    return None, None


# ── Vessel enrichment — Datalastic + VesselFinder fallback ───────────────────
def _http_get(url: str, headers: dict = None, timeout: int = 10) -> bytes:
    req = urllib.request.Request(url, headers={
        "User-Agent": "OCEANUS/1.0 maritime-dashboard",
        "Accept": "application/json,text/html",
        **(headers or {}),
    })
    return urllib.request.urlopen(req, timeout=timeout).read()


def enrich_via_datalastic(imo: str) -> dict:
    """Fetch vessel details from Datalastic free API."""
    if not DATALASTIC_KEY:
        return {}
    try:
        url  = f"https://api.datalastic.com/api/v0/vessel?api-key={DATALASTIC_KEY}&imo={imo}"
        data = json.loads(_http_get(url))
        v    = data.get("data", {})
        if not v:
            return {}
        dwt    = int(v.get("deadweight",   0) or 0)
        gt     = int(v.get("gross_tonnage",0) or 0)
        length = int(v.get("length",       0) or 0)
        beam   = int(v.get("breadth",      0) or 0)
        year   = int(v.get("year_built",   0) or 0)
        flag   = (v.get("flag_code") or "").strip().upper()[:2]
        atype  = int(v.get("vessel_type_code", 0) or 0)
        return {
            "dwt":          dwt,
            "gt":           gt,
            "length":       length,
            "beam":         beam,
            "year_built":   year,
            "flag":         flag,
            "flag_name":    FLAG_NAMES.get(flag, flag),
            "owner":        (v.get("manager") or v.get("owner") or "").strip(),
            "vessel_class": infer_vessel_class(atype, dwt, length),
            "commodity":    infer_commodity(atype, dwt),
            "_src":         "datalastic",
        }
    except Exception as e:
        print(f"  [META] Datalastic IMO {imo}: {e}")
        return {}


def enrich_via_vesselfinder(imo: str) -> dict:
    """
    Scrape basic stats from VesselFinder public vessel page.
    No API key required — just HTML scraping of public data.
    Polite: only called when Datalastic fails/unavailable.
    """
    try:
        url  = f"https://www.vesselfinder.com/vessels/details/{imo}"
        html = _http_get(url, timeout=12).decode("utf-8", errors="ignore")
        def _extract(pattern):
            m = re.search(pattern, html, re.IGNORECASE)
            return m.group(1).strip() if m else ""
        dwt_s    = _extract(r'Deadweight[^<]*</[^>]+>\s*<[^>]+>([0-9,]+)')
        gt_s     = _extract(r'Gross Tonnage[^<]*</[^>]+>\s*<[^>]+>([0-9,]+)')
        len_s    = _extract(r'Length[^<]*</[^>]+>\s*<[^>]+>([0-9]+)')
        beam_s   = _extract(r'Beam[^<]*</[^>]+>\s*<[^>]+>([0-9]+)')
        year_s   = _extract(r'Year Built[^<]*</[^>]+>\s*<[^>]+>([0-9]{4})')
        flag_s   = _extract(r'Flag[^<]*</[^>]+>\s*<[^>]+>([A-Za-z ]+)')
        owner_s  = _extract(r'(?:Manager|Owner)[^<]*</[^>]+>\s*<[^>]+>([^<]{3,60})')
        def _int(s): return int(s.replace(",","")) if re.search(r'\d', s) else 0
        dwt    = _int(dwt_s)
        gt     = _int(gt_s)
        length = _int(len_s)
        beam   = _int(beam_s)
        year   = _int(year_s)
        flag   = ""
        flag_name = flag_s.strip()
        # Try to resolve flag text → ISO
        for iso, name in FLAG_NAMES.items():
            if name.lower() in flag_name.lower():
                flag = iso; break
        if not any([dwt, gt, length, year]):
            return {}
        return {
            "dwt":          dwt,
            "gt":           gt,
            "length":       length,
            "beam":         beam,
            "year_built":   year,
            "flag":         flag,
            "flag_name":    flag_name or FLAG_NAMES.get(flag, ""),
            "owner":        owner_s.strip(),
            "vessel_class": infer_vessel_class(0, dwt, length),
            "commodity":    infer_commodity(0, dwt),
            "_src":         "vesselfinder_scrape",
        }
    except Exception as e:
        print(f"  [META] VesselFinder IMO {imo}: {e}")
        return {}


def enrich_vessels(vessels: dict) -> dict:
    """
    Load persistent meta cache, enrich new IMOs, return updated cache.
    Merges AIS type codes into commodity/class inference when Datalastic
    doesn't return a type (vessel_class inference uses AIS type instead).
    """
    # Load existing cache
    meta_cache = {}
    if OUT_META.exists():
        try:
            meta_cache = json.loads(OUT_META.read_text())
            print(f"[META] Loaded {len(meta_cache)} cached records")
        except Exception:
            pass

    # Find IMOs we haven't enriched yet
    new_imos = []
    for v in vessels.values():
        imo = str(v.get("imo", 0))
        if imo and imo != "0" and imo not in meta_cache:
            new_imos.append((imo, v.get("type", 0)))

    # Deduplicate and cap
    seen_imos = set()
    unique_new = []
    for imo, atype in new_imos:
        if imo not in seen_imos:
            seen_imos.add(imo)
            unique_new.append((imo, atype))

    to_enrich = unique_new[:ENRICH_MAX]
    print(f"[META] {len(unique_new)} new IMOs, enriching {len(to_enrich)}")

    enriched_count = 0
    for imo, ais_type in to_enrich:
        meta = enrich_via_datalastic(imo)
        if not meta:
            time.sleep(0.3)   # polite delay
            meta = enrich_via_vesselfinder(imo)
        if meta:
            # If Datalastic/VF didn't give us a type, use AIS type for commodity
            if ais_type and not meta.get("_ais_type"):
                dwt = meta.get("dwt", 0)
                length = meta.get("length", 0)
                meta["commodity"]    = infer_commodity(ais_type, dwt)
                meta["vessel_class"] = infer_vessel_class(ais_type, dwt, length)
                meta["_ais_type"]    = ais_type
            meta["_enriched_at"] = datetime.now(timezone.utc).isoformat()
            meta_cache[imo] = meta
            enriched_count += 1
            print(f"  [META] IMO {imo}: {meta.get('vessel_class','')} "
                  f"DWT={meta.get('dwt',0):,} flag={meta.get('flag','')} "
                  f"src={meta.get('_src','')}")
        else:
            # Cache negative result so we don't retry every run
            meta_cache[imo] = {"_no_data": True, "_enriched_at": datetime.now(timezone.utc).isoformat()}
        time.sleep(0.2)

    print(f"[META] Enriched {enriched_count} vessels. Cache now {len(meta_cache)} records.")

    # Save updated cache
    OUT_META.write_text(json.dumps(meta_cache, separators=(",", ":")))

    return meta_cache


def merge_meta_into_vessels(vessels: dict, meta_cache: dict) -> list:
    """
    Merge enrichment meta + destination parsing into vessel records.
    Returns list ready to write to vessels.json.
    """
    out = []
    for v in vessels.values():
        imo   = str(v.get("imo", 0))
        meta  = meta_cache.get(imo, {})
        dest  = v.get("destination", "")
        dest_iso, dest_country = parse_destination_country(dest)

        # Use AIS type for commodity if meta enrichment is absent/minimal
        ais_type = v.get("type", 0)
        dwt      = meta.get("dwt", 0)
        length   = meta.get("length", 0)

        merged = {
            **v,
            # From enrichment
            "dwt":          meta.get("dwt",          0),
            "gt":           meta.get("gt",            0),
            "length_m":     meta.get("length",        0),
            "beam_m":       meta.get("beam",          0),
            "year_built":   meta.get("year_built",    0),
            "flag":         meta.get("flag",          ""),
            "flag_name":    meta.get("flag_name",     ""),
            "owner":        meta.get("owner",         ""),
            "vessel_class": meta.get("vessel_class",  infer_vessel_class(ais_type, dwt, length)),
            "commodity":    meta.get("commodity",     infer_commodity(ais_type, dwt)),
            # Destination parsing
            "dest_iso":     dest_iso   or "",
            "dest_country": dest_country or "",
        }
        out.append(merged)
    return out


# ── AIS COLLECTION ────────────────────────────────────────────────────────────
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
                    meta_m = msg.get("MetaData", {})
                    mmsi = str(meta_m.get("MMSI") or sd.get("UserID", ""))
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
_MONTHS = dict(JAN=1,FEB=2,MAR=3,APR=4,MAY=5,JUN=6,
               JUL=7,AUG=8,SEP=9,OCT=10,NOV=11,DEC=12)

def _dms(s):
    h=s[-1]; parts=s[:-1].split("-")
    v=float(parts[0])+(float(parts[1])/60 if len(parts)>1 else 0)
    return -v if h in ("S","W") else v

def _coords(text):
    m=re.search(r"(\d{1,3}-\d{1,2}(?:\.\d+)?[NS])\s+(\d{1,3}-\d{1,2}(?:\.\d+)?[EW])",text)
    if not m: return None,None
    try: return _dms(m.group(1)),_dms(m.group(2))
    except: return None,None

def _navtex_dt(text):
    m=re.match(r"(\d{2})(\d{2})(\d{2})Z\s+(\w{3})\s+(\d{2,4})",text.strip())
    if not m: return None
    try:
        yr=int(m.group(5)); yr=yr+2000 if yr<100 else yr
        return datetime(yr,_MONTHS.get(m.group(4).upper(),1),
                        int(m.group(1)),int(m.group(2)),int(m.group(3)),tzinfo=timezone.utc)
    except: return None

def _severity(text):
    t=text.upper()
    if any(k in t for k in ["PIRACY","ARMED ROBBERY","ATTACK","HIJACK","MINE ","MISSILE","HOSTILE"]):
        return "HIGH","#ff4560","🔴"
    if any(k in t for k in ["DISTRESS","RESCUE","MAYDAY","TSUNAMI","HURRICANE","TYPHOON"]):
        return "HIGH","#ff4560","🔴"
    if any(k in t for k in ["FIRING","EXERCISE","MILITARY","TORPEDO","GUNNERY","WEAPONS"]):
        return "MED","#f0a500","🟡"
    if any(k in t for k in ["CABLE","SURVEY","DREDGE","BUOY","UNLIT","UNRELIABLE","WRECK"]):
        return "LOW","#00d4ff","🔵"
    return "INFO","#00e5a0","⚓"

def _time_ago(dt):
    if not dt: return "recent"
    m=int((datetime.now(timezone.utc)-dt).total_seconds()/60)
    return f"{m}m ago" if m<60 else f"{m//60}h {m%60:02d}m ago"

def fetch_alerts():
    cutoff=datetime.now(timezone.utc)-timedelta(hours=48)
    results=[]
    urls=[
        "https://msi.nga.mil/api/publications/broadcast-warn?status=active&output=json",
        *[f"https://msi.nga.mil/api/publications/broadcast-warn?navArea={r}&status=active&output=json"
          for r in ["IV","XII","I","II","III","V","VI","VII","VIII","IX","X","XI"]]
    ]
    fetched_global=False
    for url in urls:
        if fetched_global: break
        try:
            req=urllib.request.Request(url,headers={"User-Agent":"OCEANUS/1.0","Accept":"application/json"})
            raw=json.loads(urllib.request.urlopen(req,timeout=15).read())
            warnings=raw if isinstance(raw,list) else (
                raw.get("navwarnings") or raw.get("broadcastWarn") or raw.get("warnings") or [])
            if not warnings: continue
            if "navArea=" not in url: fetched_global=True
            roman_map={str(i):r for i,r in enumerate(
                ["I","II","III","IV","V","VI","VII","VIII","IX","X","XI","XII","XIII","XIV","XV","XVI"],1)}
            for w in warnings:
                text=(w.get("text") or "").strip()
                if not text: continue
                entry_str=w.get("entryDate") or w.get("lastModDate") or ""
                try: issue_dt=datetime.fromisoformat(entry_str.replace("Z","+00:00")) if entry_str else None
                except: issue_dt=None
                issue_dt=issue_dt or _navtex_dt(text)
                if issue_dt and issue_dt<cutoff: continue
                lat,lon=_coords(text)
                sev,col,icon=_severity(text)
                lines=[l.strip() for l in text.split("\n") if l.strip()]
                title=re.sub(r"^\d{6}Z\s+\w{3}\s+\d{2,4}\s+","",lines[0] if lines else "")[:120]
                body=" ".join(lines[1:4])[:350] if len(lines)>1 else text[:250]
                area_num=str(w.get("navArea") or "")
                navarea=f"NAVAREA {roman_map.get(area_num,area_num)}" if area_num else ""
                results.append({"sev":sev,"col":col,"icon":icon,"title":title,"body":body,
                    "time":_time_ago(issue_dt),"lat":lat,"lon":lon,"navarea":navarea,
                    "msgNum":f"{w.get('msgYear','')}/{w.get('msgNumber','')}","source":"NGA MSI"})
        except Exception as e:
            print(f"[ALERTS] {url[:60]}… {e}"); continue
    seen,unique=set(),[]
    for r in results:
        k=r["title"][:60]
        if k not in seen: seen.add(k); unique.append(r)
    unique.sort(key=lambda x:{"HIGH":0,"MED":1,"LOW":2,"INFO":3}.get(x["sev"],9))
    return unique


# ── MAIN ──────────────────────────────────────────────────────────────────────
async def main():
    DATA_DIR.mkdir(exist_ok=True)

    # 1. Collect AIS
    try:
        await collect()
    except Exception as e:
        print(f"[AIS] Error: {e}")
        if OUT_VESSELS.exists():
            existing = json.loads(OUT_VESSELS.read_text())
            for v in existing.get("vessels", []):
                vessels[v["mmsi"]] = v

    # 2. Enrich with metadata
    print("[META] Starting enrichment…")
    meta_cache = enrich_vessels(vessels)

    # 3. Merge meta + destination parsing into vessel records
    merged_vessels = merge_meta_into_vessels(vessels, meta_cache)

    # 4. Write vessels.json
    out = {
        "vessels": merged_vessels,
        "count":   len(merged_vessels),
        "updated": datetime.now(timezone.utc).isoformat(),
    }
    OUT_VESSELS.write_text(json.dumps(out, separators=(",", ":")))
    print(f"[AIS] Wrote {len(merged_vessels)} vessels → {OUT_VESSELS}")

    # 5. Fetch and write alerts
    print("[ALERTS] Fetching NGA MSI…")
    try:
        alerts = fetch_alerts()
    except Exception as e:
        print(f"[ALERTS] Error: {e}"); alerts = []
    OUT_ALERTS.write_text(json.dumps({
        "alerts":  alerts,
        "count":   len(alerts),
        "updated": datetime.now(timezone.utc).isoformat(),
        "source":  "NGA MSI Broadcast Warnings",
    }, separators=(",", ":")))
    print(f"[ALERTS] Wrote {len(alerts)} alerts → {OUT_ALERTS}")


if __name__ == "__main__":
    asyncio.run(main())
