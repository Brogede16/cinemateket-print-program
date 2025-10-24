import os
import time
from urllib.parse import urlparse, urlunparse
from flask import Flask, request, Response, jsonify
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- Konfiguration ---
ALLOWED_HOST = "www.dfi.dk"
ALLOWED_PATH_PREFIXES = ["/cinemateket/", "/node/41948", "/cinemateket/biograf/kalender"]

CACHE_TTL = 300
CACHE_MAX_SIZE = 256
UPSTREAM_TIMEOUT = 20
RATE_LIMIT_RPM = 120

app = Flask(__name__, static_folder=".", static_url_path="")

# --- Requests session med retry ---
session = requests.Session()
retry = Retry(total=3, backoff_factor=0.3, status_forcelist=(500,502,503,504))
adapter = HTTPAdapter(max_retries=retry)
session.mount("http://", adapter)
session.mount("https://", adapter)
DEFAULT_HEADERS = {"User-Agent": "CinemateketPrint/1.0", "Accept-Language": "da-DK,da;q=0.9"}

_cache = {}
_order = []

def cache_get(k):
    now = time.time()
    rec = _cache.get(k)
    if not rec: return None
    exp, data, ct = rec
    if exp < now:
        _cache.pop(k, None)
        return None
    return data, ct

def cache_set(k,data,ct):
    while len(_cache) >= CACHE_MAX_SIZE: _cache.pop(_order.pop(0), None)
    _cache[k] = (time.time()+CACHE_TTL, data, ct)
    _order.append(k)

def allowed(url):
    try: p = urlparse(url)
    except: return False
    if p.scheme not in ("http","https") or p.netloc != ALLOWED_HOST: return False
    return any(p.path.startswith(pre) for pre in ALLOWED_PATH_PREFIXES)

@app.after_request
def headers(resp):
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["X-Content-Type-Options"] = "nosniff"
    return resp

@app.get("/health")
def health(): return "ok",200

@app.get("/")
def index(): return app.send_static_file("index.html")

@app.get("/fetch")
def fetch():
    url=request.args.get("url")
    if not url: return Response("Missing url",400)
    if not allowed(url): return Response("Forbidden",403)
    cached=cache_get(url)
    if cached: data,ct=cached; return Response(data,200,content_type=ct)
    try:
        r=session.get(url,timeout=UPSTREAM_TIMEOUT,headers=DEFAULT_HEADERS)
        ct=r.headers.get("content-type","text/html; charset=utf-8")
        if r.status_code==200: cache_set(url,r.content,ct)
        return Response(r.content,r.status_code,content_type=ct)
    except Exception as e:
        return Response(f"Upstream error: {e}",502)

if __name__=="__main__":
    port=int(os.environ.get("PORT",10000))
    app.run(host="0.0.0.0",port=port)
