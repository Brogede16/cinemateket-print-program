import os
import re
import sys
import time
from datetime import datetime, date
from urllib.parse import urlparse, urljoin

import requests
from bs4 import BeautifulSoup
from flask import Flask, jsonify, request, Response, send_from_directory

# ---------------- Konfiguration ----------------
BASE = "https://www.dfi.dk"
# Brug Alle film i stedet for kalender, da kalender returnerer 404
CALENDAR_PRIMARY = f"{BASE}/cinemateket/biograf/alle-film"
CALENDAR_FALLBACK = CALENDAR_PRIMARY
SERIES_INDEX_URL = f"{BASE}/cinemateket/biograf/filmserier"

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
    "jan":1,"januar":1,
    "feb":2,"februar":2,
    "mar":3,"marts":3,
    "apr":4,"april":4,
    "maj":5,
    "jun":6,"juni":6,
    "jul":7,"juli":7,
    "aug":8,"august":8,
    "sep":9,"september":9,
    "okt":10,"oktober":10,
    "nov":11,"november":11,
    "dec":12,"december":12
}
DAY_RE = re.compile(r"^(Mandag|Tirsdag|Onsdag|Torsdag|Fredag|Lørdag|Søndag)\s+(\d{1,2})\.\s*(\w+)", re.I)
TIME_RE = re.compile(r"^\d{1,2}:\d{2}$")

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
        "Gør dit lærred lidt bredere",
        "Filmtaget",
        "Se alle",
        "Læs mere",
        "Køb billetter",
        "Relaterede programmer",
        "Cinemateket",
        "Dansk film under åben himmel",
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
            import json
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

# ---------------- Kerneskridt ----------------

def build_series_registry() -> tuple[dict, dict]:
    """
    Returnerer:
      - by_href: dict {item_href -> serienavn}
      - meta: dict {serienavn -> {"intro": ..., "banner": ...}}
    Strategi:
      1) Høst serier fra serie-indekssiden.
      2) Fallback: gå via Alle film og læs serien via breadcrumb på hver itemside.
    """
    by_href: dict[str, str] = {}
    meta: dict[str, dict] = {}

    # Forsøg 1: serie-indeks
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
            item_anchors = sdoc.select(
                'a[href*="/cinemateket/biograf/alle-film/film/"], a[href*="/cinemateket/biograf/events/event/"]'
            ) or []
            for it in item_anchors:
                ih = abs_url(it.get("href", ""))
                if allowed(ih):
                    by_href[ih] = sname
        except Exception:
            pass
        time.sleep(SLEEP_BETWEEN)

    if by_href:
        log(f"Series registry (index): {len(by_href)} items")
        return by_href, meta

    # Forsøg 2: via Alle film
    al = get_soup(CALENDAR_PRIMARY)
    item_links = al.select(
        'a[href*="/cinemateket/biograf/alle-film/film/"], a[href*="/cinemateket/biograf/events/event/"]'
    ) or []
    seen_items = set()

    for a in item_links:
        ih = abs_url(a.get("href", ""))
        if not allowed(ih) or ih in seen_items:
            continue
        seen_items.add(ih)
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

    log(f"Series registry (fallback): {len(by_href)} items")
    return by_href, meta

def parse_calendar() -> list[dict]:
    """
    Bygger pseudo-dage ud fra Alle film-listen.
    Vi læser dato-strenge fra kortene, fx "25. okt, 28. okt".
    Returnerer [{label, entries:[{time,title,href}]}] hvor time er "00:00" som placeholder.
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

    WEEKDAYS = ["Mandag","Tirsdag","Onsdag","Torsdag","Fredag","Lørdag","Søndag"]
    out = []
    for iso, entries in sorted(day_map.items()):
        y, m, d = map(int, iso.split("-"))
        wd = WEEKDAYS[date(y, m, d).weekday()]
        label = f"{wd} {d}. {['januar','februar','marts','april','maj','juni','juli','august','september','oktober','november','december'][m-1]}"
        out.append({"label": label, "entries": entries})
    return out

def fetch_item_details(url: str) -> dict:
    """
    Returnerer {title, synopsis, image, times}
    """
    doc = get_soup(url)
    title = extract_title(doc, url)
    wrap = extract_body_block(doc)
    try:
        ps = [p.get_text(" ", strip=True) for p in (wrap.select("p") if wrap else [])]
    except Exception:
        ps = []
    raw = "\n\n".join(ps[:4]) if ps else ""
    if not raw:
        try:
            ps_all = [p.get_text(" ", strip=True) for p in doc.select("p")]
            raw = "\n\n".join(ps_all[:4])
        except Exception:
            raw = ""
    synopsis = clean_synopsis(raw)
    image = extract_image(doc)

    times = []
    try:
        for tnode in doc.find_all(string=re.compile(r"\b\d{1,2}:\d{2}\b")):
            st = tnode.strip()
            if re.match(r"^\d{1,2}:\d{2}$", st):
                times.append(st)
    except Exception:
        pass

    return {"title": title, "synopsis": synopsis, "image": image, "times": sorted(set(times))}

# ---------------- HTTP routes ----------------

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
    JSON-output:
      generated_at, scope, series:[{name,intro,banner,items:[{url,title,image,synopsis,dates[]}]}]
    """
    try:
        mode = request.args.get("mode", "all")
        d_from = request.args.get("from", today_iso())
        d_to = request.args.get("to", None)

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

                bucket = series_map[sname]["items"]
                if href not in bucket:
                    try:
                        det = fetch_item_details(href)
                    except Exception as ex:
                        log("fetch_item_details failed:", href, ex)
                        det = {"title": e.get("title") or "Titel", "synopsis": "", "image": None, "times": []}
                    bucket[href] = {
                        "url": href,
                        "title": det.get("title") or (e.get("title") or "Titel"),
                        "image": det.get("image"),
                        "synopsis": det.get("synopsis", ""),
                        "times": det.get("times", []),
                        "dates": []
                    }
                    time.sleep(SLEEP_BETWEEN)

                # erstat placeholder 00:00 med faktiske tider hvis de findes
                if bucket[href]["times"] and e.get("time") == "00:00":
                    for tm in bucket[href]["times"]:
                        dt_full = f"{iso} {tm}"
                        if dt_full not in bucket[href]["dates"]:
                            bucket[href]["dates"].append(dt_full)
                else:
                    dt = f"{iso} {e.get('time')}"
                    if dt not in bucket[href]["dates"]:
                        bucket[href]["dates"].append(dt)

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

        return jsonify({
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "scope": {"mode": mode, "from": d_from, "to": d_to},
            "series": out_series
        }), 200

    except Exception as e:
        log("PROGRAM ROUTE ERROR:", repr(e))
        return jsonify({"error": "internal", "detail": str(e)}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
