import os
from flask import Flask, send_from_directory, jsonify

app = Flask(__name__, static_folder=".", static_url_path="")

@app.after_request
def add_headers(resp):
    resp.headers.setdefault("Access-Control-Allow-Origin", "*")
    resp.headers.setdefault("X-Content-Type-Options", "nosniff")
    resp.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
    return resp

@app.get("/health")
def health():
    return "ok", 200

@app.get("/")
def index():
    # Server index.html fra repo-roden
    return send_from_directory(".", "index.html")

# Hvis du bruger server-side scraping senere, ligger dine API-endpoints her
# fx: @app.get("/program") ...

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
