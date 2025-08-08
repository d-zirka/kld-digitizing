import os
import base64
import logging
from typing import Optional, List
from urllib.parse import urljoin, urlparse
from itertools import product

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import dropbox
from dropbox.files import WriteMode

from flask import Flask, request, jsonify, url_for, redirect, send_from_directory
from bs4 import BeautifulSoup
from werkzeug.exceptions import HTTPException

# -----------------------------------------------------------------------------
# Flask app & logging
# -----------------------------------------------------------------------------
app = Flask(__name__)

# Більш інформативні логи
logging.basicConfig(level=logging.INFO)
app.logger.setLevel(logging.INFO)

# -----------------------------------------------------------------------------
# HTTP session з таймаутами та ретраями
# -----------------------------------------------------------------------------
DEFAULT_TIMEOUT = 30

def _requests_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": "AR-server/1.1"})
    retries = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "POST", "HEAD", "OPTIONS"])
    )
    s.mount("http://", HTTPAdapter(max_retries=retries))
    s.mount("https://", HTTPAdapter(max_retries=retries))
    return s

session = _requests_session()

# -----------------------------------------------------------------------------
# Маршрути службові
# -----------------------------------------------------------------------------
@app.route("/healthz")
def healthz():
    return "ok", 200

@app.route("/favicon.ico")
def favicon():
    # Якщо є статичний файл — віддамо його
    static_path = os.path.join(app.root_path, "static")
    fav = os.path.join(static_path, "favicon.png")
    if os.path.exists(fav):
        return send_from_directory(static_path, "favicon.png", mimetype="image/png")
    # Інакше — не шумимо 404 у логах
    return "", 204

# -----------------------------------------------------------------------------
# Головна сторінка
# -----------------------------------------------------------------------------
@app.route("/")
def index():
    icon = url_for('static', filename='favicon.png')
    return f"""
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Canadian AR Server</title>
  <link rel="icon" href="{icon}" type="image/png">
  <style>
    body {{ font-family: sans-serif; padding: 2rem; line-height: 1.4; }}
    h1 {{ font-size: 2.2em; margin-bottom: .5em; }}
    pre {{ font-size: 1.05em; white-space: pre-wrap; }}
    .muted {{ color:#666; }}
  </style>
</head>
<body>
  <h1>Canadian AR Server is running! 🚀</h1>
  <pre>
Functionality:
• Download AR PDFs for Quebec (GM#) and Ontario
• Create report folders and files for Quebec, Ontario, New Brunswick:
    – Copy & rename Instructions.xlsx
    – Copy & rename Geochemistry.gdb
    – Copy & rename DDH.gdb

API:
POST /download_gm
Body (JSON): {{
  "ar_number": "GM123456" | "20000000",
  "province": "Quebec" | "Ontario" | "New Brunswick",
  "project": "MyProjectName"
}}
  </pre>
  <p class="muted">Health: <code>/healthz</code></p>
</body>
</html>
"""

# -----------------------------------------------------------------------------
# Dropbox helpers
# -----------------------------------------------------------------------------
def get_dropbox_access_token() -> str:
    cid = os.getenv("DROPBOX_CLIENT_ID")
    csec = os.getenv("DROPBOX_CLIENT_SECRET")
    rtok = os.getenv("DROPBOX_REFRESH_TOKEN")
    if not all([cid, csec, rtok]):
        raise RuntimeError("Missing Dropbox credentials")
    auth = base64.b64encode(f"{cid}:{csec}".encode()).decode()
    resp = session.post(
        "https://api.dropbox.com/oauth2/token",
        data={"grant_type": "refresh_token", "refresh_token": rtok},
        headers={"Authorization": f"Basic {auth}"},
        timeout=DEFAULT_TIMEOUT,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]

def ensure_folder(dbx: dropbox.Dropbox, path: str) -> None:
    try:
        dbx.files_get_metadata(path)
    except dropbox.exceptions.ApiError:
        dbx.files_create_folder_v2(path)

# -----------------------------------------------------------------------------
# Scrape & download helpers
# -----------------------------------------------------------------------------
def _extract_pdf_links(html: str, base: str) -> List[str]:
    """Шукає PDF-посилання, коректно обробляє відносні/абсолютні шляхи."""
    soup = BeautifulSoup(html, "html.parser")
    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        # якщо абсолютний URL і це pdf — беремо як є
        if href.lower().endswith(".pdf") and urlparse(href).scheme in ("http", "https"):
            links.append(href)
            continue
        # якщо відносний шлях і закінчується на .pdf — нормалізуємо
        if href.lower().endswith(".pdf"):
            links.append(urljoin(base, href))
    return list(dict.fromkeys(links))  # унікальні, зберігаючи порядок

def _case_variants(ext: str) -> List[str]:
    """Генерує всі комбінації регістру для розширення без крапки, напр. 'pdf' -> ['pdf','pdF',...]."""
    if not ext:
        return []
    return [''.join(p) for p in product(*[(c.lower(), c.upper()) for c in ext])]

def _try_get(url: str) -> Optional[bytes]:
    try:
        r = session.get(url, timeout=DEFAULT_TIMEOUT)
        r.raise_for_status()
        return r.content
    except requests.HTTPError:
        return None

def download_ar_generic(
    ar_number: str,
    province: str,
    project: str,
    list_page_url: str | None = None,
    base_url: str | None = None
) -> int:
    """
    1) Створює структуру папок і копіює шаблони:
       - Instructions.xlsx
       - Geochemistry.gdb
       - DDH.gdb
    2) Якщо list_page_url задано — скрапить PDF і завантажує їх у Dropbox.
    Повертає кількість завантажених PDF.
    """
    token = get_dropbox_access_token()
    dbx = dropbox.Dropbox(token)

    base = f"/KENORLAND_DIGITIZING/ASSESSMENT_REPORTS/1 - NEW REPORTS/{province}/{project}/{ar_number}"
    instr = f"{base}/Instructions"
    srcdata = f"{base}/Source Data"

    # Папки
    for p in (base, instr, srcdata):
        ensure_folder(dbx, p)

    # Копіювання шаблонів (якщо вже існують — просто лог, помилку не піднімаємо)
    try:
        dbx.files_copy_v2(
            "/KENORLAND_DIGITIZING/ASSESSMENT_REPORTS/_Documents/Instructions/01_Instructions.xlsx",
            f"{instr}/{ar_number}_Instructions.xlsx",
            autorename=False
        )
    except dropbox.exceptions.ApiError as e:
        app.logger.warning(f"Instructions copy failed: {e}")

    try:
        dbx.files_copy_v2(
            "/KENORLAND_DIGITIZING/ASSESSMENT_REPORTS/_Documents/Instructions/ReportID_Geochemistry.gdb",
            f"{base}/{ar_number}_Geochemistry.gdb",
            autorename=False
        )
    except dropbox.exceptions.ApiError as e:
        app.logger.warning(f"Geochemistry copy failed: {e}")

    try:
        dbx.files_copy_v2(
            "/KENORLAND_DIGITIZING/ASSESSMENT_REPORTS/_Documents/Instructions/ReportID_DDH.gdb",
            f"{base}/{ar_number}_DDH.gdb",
            autorename=False
        )
    except dropbox.exceptions.ApiError as e:
        app.logger.warning(f"DDH copy failed: {e}")

    # Якщо не задано сторінку — тільки структура/шаблони
    if not list_page_url:
        return 0

    # Скрап сторінки зі списком
    resp = session.get(list_page_url, timeout=DEFAULT_TIMEOUT)
    resp.raise_for_status()

    # 1) Спершу беремо всі явні .pdf-посилання на сторінці
    pdf_links = _extract_pdf_links(resp.text, list_page_url)

    # 2) Якщо Ontario-варіант (base_url заданий) — спробуємо також конструювати посилання
    #    за патерном <base_url>/<ar_number>/<root>.<extVariants>
    more_links: List[str] = []
    if base_url:
        soup = BeautifulSoup(resp.text, "html.parser")
        hrefs = [a["href"].strip() for a in soup.find_all("a", href=True)]
        # беремо тільки ті href, що вказують на pdf (навіть якщо регістр ext інший)
        candidates = []
        for h in hrefs:
            name = os.path.basename(h)
            root, ext = os.path.splitext(name)
            if ext:
                ext_clean = ext[1:]
                if ext_clean.lower() == "pdf":
                    candidates.append(root)

        # Якщо на сторінці не було явних .pdf, але були посилання з іменами — використаємо їх
        if not candidates:
            # fallback: побудуємо з будь-яких посилань, де є ім'я файлу
            for h in hrefs:
                name = os.path.basename(h)
                root, ext = os.path.splitext(name)
                if root:
                    candidates.append(root)

        candidates = list(dict.fromkeys(candidates))
        for root in candidates:
            for v in _case_variants("pdf"):
                more_links.append(f"{base_url}/{ar_number}/{root}.{v}")

    # Об'єднуємо та унікалізуємо
    all_links = list(dict.fromkeys(pdf_links + more_links))

    # Завантаження в Dropbox
    count = 0
    for url in all_links:
        try:
            content = _try_get(url)
            if not content:
                continue
            filename = os.path.basename(urlparse(url).path) or "file.pdf"
            dst = f"{srcdata}/{filename}"
            dbx.files_upload(content, dst, mode=WriteMode.overwrite)
            count += 1
        except Exception as e:
            app.logger.error(f"PDF upload error [{url}]: {e}")

    return count

# -----------------------------------------------------------------------------
# API: завантаження звітів
# -----------------------------------------------------------------------------
@app.route("/download_gm", methods=["POST"])
def download_gm():
    data = request.get_json(force=True, silent=True) or {}
    num  = str(data.get("ar_number", "")).strip()
    prov = str(data.get("province", "")).strip()
    proj = str(data.get("project", "")).strip()

    if not all([num, prov, proj]):
        return jsonify(error="Missing parameters"), 400

    try:
        if prov == "Quebec" and num.upper().startswith("GM"):
            url = f"https://gq.mines.gouv.qc.ca/documents/EXAMINE/{num}/"
            cnt = download_ar_generic(num, prov, proj, url)
        elif prov == "Ontario":
            url = f"https://www.geologyontario.mndm.gov.on.ca/mndmfiles/afri/data/records/{num}.html"
            blob = "https://prd-0420-geoontario-0000-blob-cge0eud7azhvfsf7.z01.azurefd.net/lrc-geology-documents/assessment"
            cnt = download_ar_generic(num, prov, proj, url, blob)
        elif prov == "New Brunswick":
            cnt = download_ar_generic(num, prov, proj)
        else:
            return jsonify(error="Invalid province or AR#"), 400

        msg = f"Downloaded {cnt} PDFs" if cnt > 0 else "Folders created. No PDFs downloaded."
        return jsonify(message=msg), 200

    except requests.HTTPError as he:
        app.logger.error(f"HTTP error: {he}", exc_info=True)
        return jsonify(error=str(he)), 502
    except Exception as e:
        app.logger.error(f"Unexpected error: {e}", exc_info=True)
        return jsonify(error=str(e)), 500

# -----------------------------------------------------------------------------
# Глобальний хендлер помилок:
# - HTTPException (включно з 404) повертаємо як є
# - решту — 500 JSON
# -----------------------------------------------------------------------------
@app.errorhandler(Exception)
def all_errors(e):
    if isinstance(e, HTTPException):
        return e
    app.logger.error(f"Unhandled: {e}", exc_info=True)
    return jsonify(error="Internal server error"), 500

# -----------------------------------------------------------------------------
# Локальний запуск (на Render стартує gunicorn)
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port)
