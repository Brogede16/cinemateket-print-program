from flask import Flask, request, Response
import requests
from urllib.parse import urlparse

app = Flask(__name__, static_folder=".", static_url_path="")

# Vi tillader kun læsning fra dfi.dk/cinemateket og kalender-node
ALLOWED_HOST = "www.dfi.dk"
ALLOWED_PATH_PREFIXES = ["/cinemateket/", "/node/41948"]

@app.get("/health")
def health():
    return "ok", 200

@app.get("/")
def index():
    # Serverer index.html fra repo-roden
    return app.send_static_file("index.html")

@app.get("/fetch")
def fetch():
    url = request.args.get("url", "")
    if not url:
        return Response("Missing ?url=", status=400)

    p = urlparse(url)
    if p.scheme not in ("http", "https") or p.netloc != ALLOWED_HOST:
        return Response("Forbidden host", status=403)
    if not any(p.path.startswith(prefix) for prefix in ALLOWED_PATH_PREFIXES):
        return Response("Forbidden path", status=403)

    try:
        r = requests.get(url, timeout=25, headers={"User-Agent": "CinemateketPrint/1.0"})
        ct = r.headers.get("content-type", "text/html; charset=utf-8")
        resp = Response(r.content, status=r.status_code, content_type=ct)
        resp.headers["Access-Control-Allow-Origin"] = "*"
        return resp
    except requests.RequestException as e:
        return Response(f"Upstream error: {e}", status=502)

if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
