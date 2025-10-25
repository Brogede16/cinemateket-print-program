# app.py
import os
import re
import sys
import time
import json
from datetime import datetime, date
from urllib.parse import urlparse, urljoin

import requests
from bs4 import BeautifulSoup
from flask import Flask, jsonify, request, Response, send_from_directory

# ---------------- Konfiguration ----------------
BASE = "https://www.dfi.dk"
CALENDAR_PRIMARY = f"{BASE}/cinemateket/biograf/alle-film"   # "Alle film" fungerer som primær kilde
SERIES_INDEX_URL = f"{BASE}/cinemateket/biograf/filmserier"
EVENTS_INDEX_URL = f"{BASE}/cinemateket/biograf/events"

ALLOWED_HOSTS = {"www.dfi.dk", "dfi.dk"}
TIMEOUT = float(os.environ.get("UPSTREAM_TIMEOUT", "25"))
SLEEP_BETWEEN = float(os.environ.get("SCRAPE_SLEEP", "0.12"))
UA = "Mozilla/5.0 (compatible; CinemateketPrint/3.1; +https://www.dfi.dk/)"

app = Flask(__name__, static_folder=".", static_url_path="")

session = requests.Session()
session.headers.update({
    "User-Agent": UA,
    "Accept-Language": "da-DK,da;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Connection": "keep-alive",
})

# ---------------- Utilities ----------------
MONTHS = {
    "januar": 1, "februar": 2, "marts": 3, "april": 4, "maj": 5, "juni": 6,
    "juli": 7, "august": 8, "september": 9, "oktober": 10, "november": 11, "december": 12
}
MONTHS_DA = {
    "jan":1,"januar":1,"feb":2,"februar":2,"mar":3,"marts":3,"apr":4,"april":4,"maj":5,
    "jun":6,"juni":6,"jul":7,"juli":7,"aug":8,"august":8,"sep":9,"september":9,"okt":10,"oktober":10,
    "nov":11,"november":11,"dec":12,"december":12
}
DAY_RE = re.compile(r"^(Mandag|Tirsdag|Onsdag|Torsdag|Fredag|Lørdag|Søndag)\s+(\d{1,2})\.\s*(\w+)", re.I)

def log(*args):
    print("[APP]", *args, file=sys.stdout, flush=True)

def abs_url(href: str) -> str:
    try:
        return urljoin(BASE, href)
    except Exception:
        return href

def allowed(url: str) -> bool:
    try:
        p = urlparse(url)
        return p.scheme in ("http", "https") and p.netloc in ALLOWED_HOSTS and p.path.startswith("/cinemateket/")
    except Exception:
        return False

def _bs(html_text: str) -> BeautifulSoup:
    return BeautifulSoup(html_text or "", "html.parser")

def get_soup(url: str) -> BeautifulSoup:
    last_text = ""
    for i in range(3):
        try:
            r = session.get(url, timeout=TIMEOUT)
            last_text = r.text
            if r.status_code in (429, 500, 502, 503, 504):
                log(f"Retry {i+1}/3 on {url} status={r.status_code}")
                time.sleep(0.4 * (i + 1))
                continue
            if r.status_code != 200:
                log(f"Non-200 on {url}: {r.status_code}")
            return _bs(last_text)
        except requests.RequestException as e:
            log(f"Request error on {url}: {e}")
            time.sleep(0.4 * (i + 1))
    return _bs(last_text)

def today_iso() -> str:
    return date.today().isoformat()

def iso_from_label(label: str, year: int) -> str | None:
    m = DAY_RE.search(label.strip())
    if not m:
        return None
    day = int(m.group(2))
    mon = MONTHS.get(m.group(3).lower())
    if not mon:
        return None
    try:
        return date(year, mon, day).isoformat()
    except ValueError:
        return None

def clean_synopsis(txt: str) -> str:
    if not txt:
        return ""
    blacklist_exact = [
        "Gør dit lærred lidt bredere", "Filmtaget", "Se alle", "Læs mere",
        "Køb billetter", "Relaterede programmer", "Cinemateket", "Dansk film under åben himmel",
    ]
    lines = [ln.strip() for ln in re.split(r"\n+", txt)]
    lines = [
        ln for ln in lines
        if ln
        and not any(b.lower() == ln.lower() for b in blacklist_exact)
        and not re.match(r"^(Medvirkende|Instruktør|Original titel|Sprog|Aldersgrænse|Længde)\s*:", ln, re.I)
    ]
    t = "\n\n".join(lines).strip()
    words = t.split()
    if len(words) > 160:
        t = " ".join(words[:160]) + "…"
    return t

def extract_title(doc: BeautifulSoup, url: str) -> str:
    try:
        og = doc.select_one('meta[property="og:title"]')
        if og and og.get("content") and og["content"].strip().lower() != "cinemateket":
            return og["content"].strip()
    except Exception:
        pass
    try:
        tw = doc.select_one('meta[name="twitter:title"]')
        if tw and tw.get("content") and tw["content"].strip().lower() != "cinemateket":
            return tw["content"].strip()
    except Exception:
        pass
    for s in doc.select('script[type="application/ld+json"]'):
        try:
            obj = json.loads(s.text or "")
            if isinstance(obj, list):
                for it in obj:
                    n = str(it.get("name", "")).strip()
                    if n and n.lower() != "cinemateket":
                        return n
            else:
                n = str(obj.get("name", "")).strip()
                if n and n.lower() != "cinemateket":
                    return n
        except Exception:
            continue
    try:
        h = doc.find(["h1", "h2"])
        if h:
            hv = h.get_text(strip=True)
            if hv and hv.lower() != "cinemateket":
                return hv
    except Exception:
        pass
    try:
        t = doc.title.get_text(strip=True) if doc.title else ""
        if t and t.lower() != "cinemateket":
            return t
    except Exception:
        pass
    try:
        seg = urlparse(url).path.strip("/").split("/")[-1]
        slug = re.sub(r"[-_]+", " ", seg).strip()
        slug = re.sub(r"\d{1,2}-\d{1,2}(-\d{2,4})?", "", slug).strip()
        slug = " ".join(w.capitalize() for w in slug.split())
        return slug or "Titel"
    except Exception:
        return "Titel"

def extract_body_block(doc: BeautifulSoup):
    for sel in [".field--name-field-body", ".field--name-body", "article", "main"]:
        node = doc.select_one(sel)
        if node:
            return node
    return doc

def extract_image(doc: BeautifulSoup) -> str | None:
    try:
        wrap = extract_body_block(doc)
        img = wrap.select_one("img") if wrap else None
        if not img:
            img = doc.select_one("article img, main img, img")
        if img and img.get("src"):
            return abs_url(img["src"])
    except Exception:
        pass
    return None

# ---------- De-dup & normalisering ----------
TITLE_TRIM = re.compile(r"\s+")
PAREN_TRIM = re.compile(r"\s*\([^)]*\)\s*$")

def canonical_title(raw: str) -> str:
    if not raw:
        return ""
    t = raw.strip()
    t = PAREN_TRIM.sub("", t)        # fjern trailing parenteser som "(Q&A)"
    t = TITLE_TRIM.sub(" ", t)
    return t.lower()

def merge_dates(existing: list[str], incoming: list[str]) -> list[str]:
    s = set(existing or [])
    for dt in incoming or []:
        if dt:
            s.add(dt)
    out = sorted(s)
    return out

def weekday_label_from_iso(iso_date: str) -> str:
    WEEKDAYS = ["Mandag","Tirsdag","Onsdag","Torsdag","Fredag","Lørdag","Søndag"]
    MONTHS_FULL = ['januar','februar','marts','april','maj','juni','juli','august','september','oktober','november','december']
    y, m, d = map(int, iso_date.split("-"))
    wd = WEEKDAYS[date(y, m, d).weekday()]
    return f"{wd} {d}. {MONTHS_FULL[m-1]}"

# ---------------- Hjælp: list-opsamling ----------------
def collect_list_items(start_url: str, within_path_prefix: str) -> set[str]:
    """
    Generisk opsamler fra lister med pagination.
    Finder alle film-/event-links under et path-prefix, inkl. ?page=...
    """
    found: set[str] = set()
    visited: set[str] = set()
    queue: list[str] = [start_url]
    while queue:
        url = queue.pop(0)
        if url in visited:
            continue
        visited.add(url)
        doc = get_soup(url)
        for a in doc.select('a[href]'):
            href = abs_url(a.get("href", ""))
            if not allowed(href):
                continue
            if "/cinemateket/biograf/alle-film/film/" in href or "/cinemateket/biograf/events/event/" in href:
                found.add(href)
        for p in doc.select('a[href*="?page="]'):
            ph = abs_url(p.get("href", ""))
            if ph.startswith(within_path_prefix) and ph not in visited:
                queue.append(ph)
        time.sleep(SLEEP_BETWEEN)
    return found

# ---------------- Serier ----------------
def build_series_registry() -> tuple[dict, dict]:
    """
    Returnerer:
      - by_href: {item_href -> serienavn}
      - meta:    {serienavn -> {"intro": ..., "banner": ...}}
    Lag:
      1) Serie-indeks: alle item-links pr. serie, inkl. foldede sektioner og ?page=
      2) Fallback A: brede lister “Alle film” og “Events”
      3) Fallback B: Breadcrumb på item-sider
    """
    by_href: dict[str, str] = {}
    meta: dict[str, dict] = {}

    def collect_series_items(series_url: str) -> set[str]:
        found: set[str] = set()
        visited: set[str] = set()
        queue: list[str] = [series_url]
        root = series_url.split("?")[0]
        while queue:
            url = queue.pop(0)
            if url in visited:
                continue
            visited.add(url)
            sdoc = get_soup(url)
            for it in sdoc.select('a[href*="/cinemateket/biograf/alle-film/film/"], a[href*="/cinemateket/biograf/events/event/"]'):
                ih = abs_url(it.get("href", ""))
                if allowed(ih):
                    found.add(ih)
            for p in sdoc.select('a[href*="?page="]'):
                ph = abs_url(p.get("href", ""))
                if ph.startswith(root) and ph not in visited:
                    queue.append(ph)
            time.sleep(SLEEP_BETWEEN)
        return found

    # Serieindeks
    idx = get_soup(SERIES_INDEX_URL)
    anchors = idx.select('a[href*="/cinemateket/biograf/filmserier/serie/"]') or []
    seen_series_pages = set()
    for a in anchors:
        s_url = abs_url(a.get("href", ""))
        if not s_url or s_url in seen_series_pages:
            continue
        seen_series_pages.add(s_url)
        try:
            sdoc = get_soup(s_url)
            sname = extract_title(sdoc, s_url).strip() or "Serie"
            wrap = extract_body_block(sdoc)
            ps = [p.get_text(" ", strip=True) for p in (wrap.select("p") if wrap else [])]
            intro = clean_synopsis("\n\n".join(ps[:4])) if ps else ""
            banner = extract_image(sdoc)
            meta[sname] = {"intro": intro, "banner": banner}
            for ih in collect_series_items(s_url):
                by_href[ih] = sname
        except Exception as ex:
            log("series harvest error:", s_url, ex)
        time.sleep(SLEEP_BETWEEN)

    # Fallback-lister: Alle film + Events
    all_items = set()
    try:
        all_items |= collect_list_items(CALENDAR_PRIMARY, CALENDAR_PRIMARY.split("?")[0])
    except Exception:
        pass
    try:
        all_items |= collect_list_items(EVENTS_INDEX_URL, EVENTS_INDEX_URL.split("?")[0])
    except Exception:
        pass

    # Breadcrumb fallback
    seen_items = set()
    for ih in sorted(all_items):
        if not allowed(ih) or ih in seen_items:
            continue
        seen_items.add(ih)
        if ih in by_href:
            continue
        try:
            d = get_soup(ih)
            s_anchor = d.select_one('a[href*="/cinemateket/biograf/filmserier/serie/"]')
            if not s_anchor:
                continue
            s_url = abs_url(s_anchor.get("href", ""))
            s_doc = get_soup(s_url)
            sname = extract_title(s_doc, s_url).strip() or "Serie"
            if sname not in meta:
                wrap = extract_body_block(s_doc)
                ps = [p.get_text(" ", strip=True) for p in (wrap.select("p") if wrap else [])]
                intro = clean_synopsis("\n\n".join(ps[:4])) if ps else ""
                banner = extract_image(s_doc)
                meta[sname] = {"intro": intro, "banner": banner}
            by_href[ih] = sname
        except Exception:
            pass
        time.sleep(SLEEP_BETWEEN)

    log(f"Series registry total: {len(by_href)} items, {len(meta)} series")
    return by_href, meta

# ---------------- Kalender & detaljer ----------------
def parse_calendar() -> list[dict]:
    """
    Pseudo-dage fra “Alle film”. Finder dato-chunks nær linket.
    """
    doc = get_soup(CALENDAR_PRIMARY)
    cards = doc.select(
        'a[href*="/cinemateket/biograf/alle-film/film/"], a[href*="/cinemateket/biograf/events/event/"]'
    ) or []
    day_map: dict[str, list] = {}
    current_year = datetime.now().year

    def parse_dates_chunk(text):
        out = []
        parts = [p.strip() for p in re.split(r"[,\u2013\-]+", text) if p.strip()]
        for p in parts:
            m = re.search(r"(\d{1,2})\.\s*([A-Za-zæøåÆØÅ]+)", p)
            if not m:
                continue
            day = int(m.group(1))
            mon = MONTHS_DA.get(m.group(2).lower())
            if not mon:
                continue
            try:
                out.append(date(current_year, mon, day).isoformat())
            except ValueError:
                continue
        return out

    for a in cards:
        href = abs_url(a.get("href", ""))
        if not allowed(href):
            continue
        title = a.get_text(strip=True) or ""
        date_text = ""
        el = a.parent
        hops = 0
        while el and hops < 5 and not date_text:
            txt = el.get_text(" ", strip=True)
            if re.search(r"\d{1,2}\.\s*[A-Za-zæøåÆØÅ]+", txt):
                date_text = txt
                break
            el = el.parent
            hops += 1

        iso_list = parse_dates_chunk(date_text)
        for iso in iso_list:
            entry = {"time": "00:00", "title": title, "href": href}
            day_map.setdefault(iso, []).append(entry)

    out = []
    for iso, entries in sorted(day_map.items()):
        label = weekday_label_from_iso(iso)  # servervalideret ugedag
        out.append({"label": label, "entries": entries})
    return out

def fetch_item_details(url: str) -> dict:
    """
    Returnerer {title, synopsis, image, times, datetimes}
    - times: rene klokkeslæt fundet på siden, fx ["19:15", ...]
    - datetimes: ISO "YYYY-MM-DD HH:MM" parset fra tekst som "25. nov kl. 19:15"
    """
    doc = get_soup(url)
    title = extract_title(doc, url)
    wrap = extract_body_block(doc)
    try:
        ps = [p.get_text(" ", strip=True) for p in (wrap.select("p") if wrap else [])]
    except Exception:
        ps = []
    raw = "\n\n".join(ps[:6]) if ps else ""
    synopsis = clean_synopsis(raw)
    image = extract_image(doc)

    text_all = doc.get_text(" ", strip=True)

    # 1) klokkeslæt
    times = sorted(set(re.findall(r"\b(\d{1,2}:\d{2})\b", text_all)))

    # 2) dato+tid
    datetimes = []
    current_year = datetime.now().year
    dt_pattern = re.compile(
        r"(\d{1,2})\.\s*([A-Za-zæøåÆØÅ]+)(?:\s*(?:kl\.?|KL\.?)\s*)?(\d{1,2}:\d{2})",
        re.I
    )
    for g in dt_pattern.finditer(text_all):
        day = int(g.group(1))
        mon_name = g.group(2).lower()
        tm = g.group(3)
        mon = MONTHS_DA.get(mon_name)
        if not mon:
            continue
        try:
            iso = date(current_year, mon, day).isoformat()
            datetimes.append(f"{iso} {tm}")
        except ValueError:
            continue

    datetimes = sorted(set(datetimes))
    return {"title": title, "synopsis": synopsis, "image": image, "times": times, "datetimes": datetimes}

# ---------------- Routes ----------------
@app.after_request
def add_headers(resp: Response):
    resp.headers.setdefault("Access-Control-Allow-Origin", "*")
    resp.headers.setdefault("X-Content-Type-Options", "nosniff")
    resp.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
    return resp

@app.get("/health")
def health():
    return "ok", 200

@app.get("/")
def index():
    return send_from_directory(".", "index.html")

@app.get("/program")
def program():
    """
    JSON:
      {
        generated_at, scope,
        series: [
          {name,intro,banner,items:[
            {url,title,image,synopsis,dates:[ISO "YYYY-MM-DD HH:MM"], canon}
          ]}
        ]
      }
    """
    try:
        mode = request.args.get("mode", "all")
        d_from = request.args.get("from", today_iso())
        d_to = request.args.get("to")

        by_href, meta = build_series_registry()
        days = parse_calendar()
        current_year = datetime.now().year

        series_map: dict[str, dict] = {}

        for d in days:
            iso = iso_from_label(d.get("label", ""), current_year)
            if not iso:
                continue

            if mode == "all":
                if iso < today_iso():
                    continue
            else:
                if not d_from or not d_to:
                    return jsonify({"error": "range mode requires 'from' and 'to'"}), 400
                if not (d_from <= iso <= d_to):
                    continue

            for e in d.get("entries", []):
                href = e.get("href")
                if not href or not allowed(href):
                    continue

                sname = by_href.get(href, "Uden for serie")
                if sname not in series_map:
                    series_map[sname] = {
                        "intro": meta.get(sname, {}).get("intro", ""),
                        "banner": meta.get(sname, {}).get("banner", None),
                        "items": {}
                    }

                # Hent detaljer og de-dup på titel inden for serien
                try:
                    det = fetch_item_details(href)
                except Exception as ex:
                    log("fetch_item_details failed:", href, ex)
                    det = {"title": e.get("title") or "Titel", "synopsis": "", "image": None, "times": [], "datetimes": []}

                title_eff = det.get("title") or (e.get("title") or "Titel")
                canon = canonical_title(title_eff)

                # eksisterende item med samme kanoniske titel?
                existing_key = None
                for k, v in series_map[sname]["items"].items():
                    if v.get("canon") == canon:
                        existing_key = k
                        break

                # konstruer nye datoer for denne dag
                new_dates = []
                if e.get("time") == "00:00":
                    if det.get("times"):
                        for tm in det["times"]:
                            new_dates.append(f"{iso} {tm}")
                    else:
                        for dt_full in det.get("datetimes", []):
                            if dt_full.startswith(iso):
                                new_dates.append(dt_full)
                else:
                    new_dates.append(f"{iso} {e.get('time')}")

                if existing_key:
                    item = series_map[sname]["items"][existing_key]
                    if not item.get("image") and det.get("image"):
                        item["image"] = det["image"]
                    if not item.get("synopsis") and det.get("synopsis"):
                        item["synopsis"] = det["synopsis"]
                    item["dates"] = merge_dates(item["dates"], new_dates)
                else:
                    series_map[sname]["items"][href] = {
                        "canon": canon,
                        "url": href,
                        "title": title_eff,
                        "image": det.get("image"),
                        "synopsis": det.get("synopsis", ""),
                        "times": det.get("times", []),
                        "dates": sorted(set(new_dates))
                    }

                time.sleep(SLEEP_BETWEEN)

        # Fallback: gennemgå alle by_href for at få titler der ikke var i "Alle film"-dagene
        for href, sname in by_href.items():
            # findes der allerede et item med samme canonical-title?
            try:
                det = fetch_item_details(href)
            except Exception as ex:
                log("fallback fetch_item_details failed:", href, ex)
                continue

            title_eff = det.get("title") or "Titel"
            canon = canonical_title(title_eff)

            existed = False
            if sname in series_map:
                for v in series_map[sname]["items"].values():
                    if v.get("canon") == canon:
                        existed = True
                        # filtrér filmsidens datotider ind i scope
                        valid_dt = []
                        for dt_full in det.get("datetimes", []):
                            iso_dt = dt_full[:10]
                            if mode == "all":
                                if iso_dt >= today_iso():
                                    valid_dt.append(dt_full)
                            else:
                                if d_from <= iso_dt <= d_to:
                                    valid_dt.append(dt_full)
                        v["dates"] = merge_dates(v["dates"], valid_dt)
                        if not v.get("image") and det.get("image"):
                            v["image"] = det["image"]
                        if not v.get("synopsis") and det.get("synopsis"):
                            v["synopsis"] = det["synopsis"]
                        break

            if existed:
                continue

            # hvis serien slet ikke er oprettet endnu
            if sname not in series_map:
                series_map[sname] = {
                    "intro": meta.get(sname, {}).get("intro", ""),
                    "banner": meta.get(sname, {}).get("banner", None),
                    "items": {}
                }

            valid_dt = []
            for dt_full in det.get("datetimes", []):
                iso_dt = dt_full[:10]
                if mode == "all":
                    if iso_dt >= today_iso():
                        valid_dt.append(dt_full)
                else:
                    if d_from <= iso_dt <= d_to:
                        valid_dt.append(dt_full)

            if not valid_dt:
                continue

            series_map[sname]["items"][href] = {
                "canon": canon,
                "url": href,
                "title": title_eff,
                "image": det.get("image"),
                "synopsis": det.get("synopsis", ""),
                "times": det.get("times", []),
                "dates": sorted(set(valid_dt))
            }

        # Byg output
        out_series = []
        for name, data in series_map.items():
            items = list(data["items"].values())
            for it in items:
                it["dates"].sort()
            if not items:
                continue
            items.sort(key=lambda x: x["dates"][0] if x["dates"] else "9999-99-99 99:99")
            out_series.append({
                "name": name,
                "intro": data["intro"],
                "banner": data["banner"],
                "items": items
            })

        def first_dt(s):
            if not s["items"]:
                return "9999-99-99 99:99"
            return s["items"][0]["dates"][0]

        out_series.sort(key=lambda s: (first_dt(s), s["name"]))

        payload = {
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "scope": {"mode": mode, "from": d_from, "to": d_to},
            "series": out_series
        }
        return jsonify(payload), 200

    except Exception as e:
        log("PROGRAM ROUTE ERROR:", repr(e))
        return jsonify({"error": "internal", "detail": str(e)}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
