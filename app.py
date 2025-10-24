import os
import re
import time
from datetime import datetime, date
from urllib.parse import urlparse, urljoin

import requests
from bs4 import BeautifulSoup
from flask import Flask, jsonify, request, Response, send_from_directory

# ---------------- Konfiguration ----------------
BASE = "https://www.dfi.dk"
CALENDAR_PRIMARY = f"{BASE}/cinemateket/biograf/kalender"
CALENDAR_FALLBACK = CALENDAR_PRIMARY  # node/41948 findes ikke længere
SERIES_INDEX_URL = f"{BASE}/cinemateket/biograf/filmserier"

ALLOWED_HOST = "www.dfi.dk"
TIMEOUT = float(os.environ.get("UPSTREAM_TIMEOUT", "25"))
SLEEP_BETWEEN = float(os.environ.get("SCRAPE_SLEEP", "0.12"))  # sekunder mellem sidekald
UA = "Mozilla/5.0 (compatible; CinemateketPrint/2.0; +https://www.dfi.dk/)"

# Flask som static file server for index.html i repo-roden
app = Flask(__name__, static_folder=".", static_url_path="")

# HTTP session med simple retries
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
DAY_RE = re.compile(r"^(Mandag|Tirsdag|Onsdag|Torsdag|Fredag|Lørdag|Søndag)\s+(\d{1,2})\.\s*(\w+)", re.I)
TIME_RE = re.compile(r"^\d{1,2}:\d{2}$")

def abs_url(href: str) -> str:
    try:
        return urljoin(BASE, href)
    except Exception:
        return href

def allowed(url: str) -> bool:
    try:
        p = urlparse(url)
        return p.scheme in ("http", "https") and p.netloc == ALLOWED_HOST and p.path.startswith("/cinemateket/")
    except Exception:
        return False

def get_soup(url: str) -> BeautifulSoup:
    # 2 enkle retries på 429 og 5xx
    last = None
    for i in range(3):
        r = session.get(url, timeout=TIMEOUT)
        if r.status_code in (429, 500, 502, 503, 504) and i < 2:
            time.sleep(0.4 * (i + 1))
            last = r
            continue
        r.raise_for_status()
        return BeautifulSoup(r.text, "lxml")
    # sidste svar
    if last is not None:
        last.raise_for_status()
    # fallback tom
    return BeautifulSoup("<html></html>", "lxml")

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
    # Linjevist filtrering
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
    og = doc.select_one('meta[property="og:title"]')
    if og and og.get("content") and og["content"].strip().lower() != "cinemateket":
        return og["content"].strip()
    tw = doc.select_one('meta[name="twitter:title"]')
    if tw and tw.get("content") and tw["content"].strip().lower() != "cinemateket":
        return tw["content"].strip()
    # JSON-LD
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
            pass
    # H1 eller H2
    h = doc.find(["h1", "h2"])
    if h and h.get_text(strip=True) and h.get_text(strip=True).lower() != "cinemateket":
        return h.get_text(strip=True)
    # <title>
    t = doc.title.get_text(strip=True) if doc.title else ""
    if t and t.lower() != "cinemateket":
        return t
    # slug fallback
    seg = urlparse(url).path.strip("/").split("/")[-1]
    slug = re.sub(r"[-_]+", " ", seg).strip()
    slug = re.sub(r"\d{1,2}-\d{1,2}(-\d{2,4})?", "", slug).strip()
    slug = " ".join(w.capitalize() for w in slug.split())
    return slug or "Titel"

def extract_body_block(doc: BeautifulSoup) -> BeautifulSoup | None:
    for sel in [".field--name-field-body", ".field--name-body", "article", "main"]:
        node = doc.select_one(sel)
        if node:
            return node
    return doc

def extract_image(doc: BeautifulSoup) -> str | None:
    wrap = extract_body_block(doc)
    img = wrap.select_one("img") if wrap else None
    if not img:
        img = doc.select_one("article img, main img, img")
    if img and img.get("src"):
        return abs_url(img["src"])
    return None

# ---------------- Kerneskridt ----------------

def build_series_registry() -> tuple[dict, dict]:
    """
    Returnerer:
      - by_href: dict {item_href -> serienavn}
      - meta: dict {serienavn -> {"intro": ..., "banner": ...}}
    """
    by_href: dict[str, str] = {}
    meta: dict[str, dict] = {}

    idx = get_soup(SERIES_INDEX_URL)
    series_anchors = idx.select('a[href*="/cinemateket/biograf/filmserier/serie/"]')
    seen_series = set()

    for a in series_anchors:
        name = a.get_text(strip=True)
        href = abs_url(a.get("href", ""))
        if not name or not href or href in seen_series:
            continue
        seen_series.add(href)

        try:
            sdoc = get_soup(href)
            wrap = extract_body_block(sdoc)
            # intro
            ps = [p.get_text(" ", strip=True) for p in wrap.select("p")] if wrap else []
            intro = clean_synopsis("\n\n".join(ps[:4]))
            # banner
            banner = extract_image(sdoc)
            meta[name] = {"intro": intro, "banner": banner}

            # registrer alle item-links i serien
            for it in sdoc.select('a[href*="/cinemateket/biograf/alle-film/film/"], a[href*="/cinemateket/biograf/events/event/"]'):
                ih = abs_url(it.get("href", ""))
                if allowed(ih):
                    by_href[ih] = name
        except Exception:
            # fejl i én serie må ikke stoppe alt
            pass

        time.sleep(SLEEP_BETWEEN)

    return by_href, meta

def parse_calendar() -> list[dict]:
    """
    Returnerer en liste af dage: [{label, entries:[{time,title,href}]}]
    """
    # prøv primær, fald tilbage
    try:
        doc = get_soup(CALENDAR_PRIMARY)
    except Exception:
        doc = get_soup(CALENDAR_FALLBACK)

    # tekstnodes
    body_text_nodes = []
    for el in doc.find_all(text=True):
        t = (el or "").strip()
        if t:
            body_text_nodes.append(t)

    # identifikér dag-overskrifter
    day_idx = [i for i, t in enumerate(body_text_nodes) if DAY_RE.search(t)]
    days = []
    for i, start in enumerate(day_idx):
        end = day_idx[i+1] if i+1 < len(day_idx) else len(body_text_nodes)
        label = body_text_nodes[start]
        chunk = body_text_nodes[start:end]
        entries = []
        j = 0
        while j < len(chunk):
            st = chunk[j]
            if TIME_RE.match(st):
                tm = st
                # find første ikke-tomme tekst efter tiden som titel
                k = j + 1
                title = ""
                while k < len(chunk) and not title:
                    cand = chunk[k].strip()
                    if cand:
                        title = cand
                        break
                    k += 1
                # find link i hele dokumentet der matcher titlen, ellers første film/event-link
                link = None
                for a in doc.select("a"):
                    if a.get_text(strip=True) == title:
                        link = a
                        break
                if not link:
                    link = doc.select_one('a[href*="/cinemateket/biograf/alle-film/film/"], a[href*="/cinemateket/biograf/events/event/"]')

                href = abs_url(link.get("href", "")) if link else None
                entries.append({"time": tm, "title": title, "href": href})
                j = k
            else:
                j += 1

        days.append({"label": label, "entries": entries})

    return days

def fetch_item_details(url: str) -> dict:
    """
    Returnerer {title, synopsis, image}
    """
    doc = get_soup(url)
    title = extract_title(doc, url)
    wrap = extract_body_block(doc)
    ps = [p.get_text(" ", strip=True) for p in wrap.select("p")] if wrap else []
    raw = "\n\n".join(ps[:4]) if ps else ""
    if not raw:
        ps_all = [p.get_text(" ", strip=True) for p in doc.select("p")]
        raw = "\n\n".join(ps_all[:4])
    synopsis = clean_synopsis(raw)
    image = extract_image(doc)
    return {"title": title, "synopsis": synopsis, "image": image}

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
    Returnerer JSON:
    {
      "generated_at": "...",
      "scope": {"mode": "all"|"range", "from": "...", "to": "..."},
      "series": [
        {
          "name": "Serienavn",
          "intro": "...",
          "banner": "https://...",
          "items": [
            {
              "url": "https://...",
              "title": "Titel",
              "image": "https://...",
              "synopsis": "...",
              "dates": ["YYYY-MM-DD HH:MM", ...]
            }
          ]
        }
      ]
    }
    """
    mode = request.args.get("mode", "all")  # "all" eller "range"
    d_from = request.args.get("from", today_iso())
    d_to = request.args.get("to", None)

    # 1) Byg serie-register
    by_href, meta = build_series_registry()

    # 2) Læs kalender
    days = parse_calendar()
    current_year = datetime.now().year

    # 3) Byg data
    series_map: dict[str, dict] = {}  # name -> {intro, banner, items: {url -> item}}
    for d in days:
        iso = iso_from_label(d["label"], current_year)
        if not iso:
            continue

        # scope-filter
        if mode == "all":
            if iso < today_iso():
                continue
        else:
            if not d_from or not d_to:
                return jsonify({"error": "range mode requires 'from' and 'to'"}), 400
            if not (d_from <= iso <= d_to):
                continue

        for e in d["entries"]:
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
                except Exception:
                    det = {"title": e.get("title") or "Titel", "synopsis": "", "image": None}
                bucket[href] = {
                    "url": href,
                    "title": det["title"] or (e.get("title") or "Titel"),
                    "image": det.get("image"),
                    "synopsis": det.get("synopsis", ""),
                    "dates": []
                }
                time.sleep(SLEEP_BETWEEN)

            dt = f"{iso} {e.get('time')}"
            if dt not in bucket[href]["dates"]:
                bucket[href]["dates"].append(dt)

    # 4) Ryd tomme serier, sorter
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

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
