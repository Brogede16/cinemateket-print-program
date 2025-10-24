import os
import time
from urllib.parse import urlparse, urlunparse
from flask import Flask, request, Response, jsonify, abort
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# -----------------------------------------------------------
# Konfiguration, kan styres via environment variables i Render
# -----------------------------------------------------------
ALLOWED_HOST = os.environ.get("ALLOWED_HOST", "www.dfi.dk")
ALLOWED_PATH_PREFIXES = os.environ.get(
    "ALLOWED_PATH_PREFIXES",
    "/cinemateket/,/node/41948"
).split(",")

# TTL for cache i sekunder
CACHE_TTL = int(os.environ.get("CACHE_TTL", "300"))
# Maks antal cachede nøgler
CACHE_MAX_SIZE = int(os.environ.get("CACHE_MAX_SIZE", "512"))
# Timeout mod upstream, sekunder
UPSTREAM_TIMEOUT = float(os.environ.get("UPSTREAM_TIMEOUT", "20"))

# Simpel rate-limit: antal requests pr. minut pr. IP
RATE_LIMIT_RPM = int(os.environ.get("RATE_LIMIT_RPM", "120"))

# Debug endpoints
ENABLE_DEBUG_ENDPOINTS = os.environ.get("ENABLE_DEBUG_ENDPOINTS", "false").lower() == "true"

# -----------------------------------------------------------
# Flask-app
# -----------------------------------------------------------
app = Flask(__name__, static_folder=".", static_url_path="")

# -----------------------------------------------------------
# Sikker requests-session med retry
# -----------------------------------------------------------
session = requests.Session()
retries = Retry(
    total=3,
    backoff_factor=0.3,
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=frozenset(["GET", "HEAD"])
)
adapter = HTTPAdapter(max_retries=retries, pool_connections=10, pool_maxsize=20)
session.mount("http://", adapter)
session.mount("https://", adapter)

DEFAULT_HEADERS = {
    "User-Agent": "CinemateketPrint/1.0 (+render-proxy)",
    # Accept-Language hjælper ofte med dansk tekst
    "Accept-Language": "da-DK,da;q=0.9,en;q=0.8"
}

# -----------------------------------------------------------
# Simpel TTL-cache i RAM
# -----------------------------------------------------------
_cache = {}  # key -> (expires_epoch, bytes, content_type)
_cache_order = []  # nøgleordning for simpel LRU-adfærd

def cache_get(key):
    now = time.time()
    rec = _cache.get(key)
    if not rec:
        return None
    exp, data, ct = rec
    if exp < now:
        # udløbet
        _cache.pop(key, None)
        try:
            _cache_order.remove(key)
        except ValueError:
            pass
        return None
    # flyt nøgle til slutningen som "nylig brugt"
    try:
        _cache_order.remove(key)
    except ValueError:
        pass
    _cache_order.append(key)
    return data, ct

def cache_set(key, data, ct):
    now = time.time()
    # trim størrelse
    while len(_cache) >= CACHE_MAX_SIZE:
        oldest = _cache_order.pop(0)
        _cache.pop(oldest, None)
    _cache[key] = (now + CACHE_TTL, data, ct)
    # opdater ordre
    try:
        _cache_order.remove(key)
    except ValueError:
        pass
    _cache_order.append(key)

# -----------------------------------------------------------
# Meget simpel rate-limit pr. IP
# -----------------------------------------------------------
_rl_bucket = {}  # ip -> (window_start_epoch, count)

def check_rate_limit(ip):
    window = 60
    now = int(time.time())
    start = now - (now % window)
    win, cnt = _rl_bucket.get(ip, (start, 0))
    if win != start:
        win, cnt = start, 0
    cnt += 1
    _rl_bucket[ip] = (win, cnt)
    return cnt <= RATE_LIMIT_RPM

# -----------------------------------------------------------
# Hjælpefunktioner
# -----------------------------------------------------------
def is_allowed_url(url: str) -> bool:
    try:
        p = urlparse(url)
    except Exception:
        return False
    if p.scheme not in ("http", "https"):
        return False
    if p.netloc != ALLOWED_HOST:
        return False
    # Kun tilladte stier
    return any(p.path.startswith(prefix) for prefix in ALLOWED_PATH_PREFIXES)

def follow_redirects_safely(url: str):
    """
    Følg op til 3 redirects manuelt og afvis, hvis vi ender uden for whitelist.
    Returnér endelig URL, content bytes og content-type.
    """
    current = url
    for _ in range(3):
        r = session.get(current, headers=DEFAULT_HEADERS, timeout=UPSTREAM_TIMEOUT, allow_redirects=False)
        if 300 <= r.status_code < 400 and "Location" in r.headers:
            nxt = r.headers["Location"]
            # gør redirect absolut
            nxt_abs = urlparse(nxt)._replace()
            if not urlparse(nxt).netloc:
                # relativ redirect
                base = urlparse(current)
                nxt = urlunparse((base.scheme, base.netloc, nxt, "", "", ""))
            # check whitelist på næste hop
            if not is_allowed_url(nxt):
                return None, None, None, 403
            current = nxt
            continue
        # ikke redirect
        ct = r.headers.get("content-type", "text/html; charset=utf-8")
        return current, r.content, ct, r.status_code
    # for mange redirects
    return None, None, None, 508

# -----------------------------------------------------------
# Security headers på alle svar
# -----------------------------------------------------------
@app.after_request
def add_security_headers(resp):
    resp.headers.setdefault("X-Content-Type-Options", "nosniff")
    resp.headers.setdefault("X-Frame-Options", "SAMEORIGIN")
    resp.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
    # CORS så frontend kan kalde /fetch
    resp.headers.setdefault("Access-Control-Allow-Origin", "*")
    return resp

# -----------------------------------------------------------
# Health og index
# -----------------------------------------------------------
@app.get("/health")
def health():
    return "ok", 200

@app.get("/")
def index():
    # server statisk index.html fra repo-roden
    return app.send_static_file("index.html")

# -----------------------------------------------------------
# Proxy endpoint
# -----------------------------------------------------------
@app.get("/fetch")
def fetch():
    ip = request.headers.get("X-Forwarded-For", request.remote_addr or "unknown").split(",")[0].strip()
    if not check_rate_limit(ip):
        return Response("Rate limit exceeded", status=429)

    url = request.args.get("url", "")
    if not url:
        return Response("Missing ?url=", status=400)

    if not is_allowed_url(url):
        return Response("Forbidden host or path", status=403)

    # Cache-hit
    cached = cache_get(url)
    if cached:
        data, ct = cached
        return Response(data, status=200, content_type=ct)

    try:
        final_url, data, ct, code = follow_redirects_safely(url)
        if code == 403:
            return Response("Forbidden after redirect", status=403)
        if code == 508:
            return Response("Too many redirects", status=508)
        if final_url is None:
            return Response("Upstream error", status=502)
        # gem i cache kun ved 200 OK
        if code == 200 and data:
            cache_set(url, data, ct)
        return Response(data, status=code, content_type=ct)
    except requests.RequestException as e:
        return Response(f"Upstream error: {e}", status=502)

# -----------------------------------------------------------
# Debug / introspektion, slå til med ENABLE_DEBUG_ENDPOINTS=true
# -----------------------------------------------------------
if ENABLE_DEBUG_ENDPOINTS:
    @app.get("/__config")
    def cfg():
        return jsonify({
            "allowed_host": ALLOWED_HOST,
            "allowed_path_prefixes": ALLOWED_PATH_PREFIXES,
            "cache_ttl": CACHE_TTL,
            "cache_size": len(_cache),
            "rate_limit_rpm": RATE_LIMIT_RPM
        }), 200

# -----------------------------------------------------------
# Lokal start. På Render anbefales Gunicorn:
# gunicorn -w 2 -k gthread -b 0.0.0.0:$PORT app:app
# -----------------------------------------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
