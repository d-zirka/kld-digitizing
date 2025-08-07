import os
import base64
import requests
import dropbox
from dropbox.files import WriteMode
from flask import Flask, request, jsonify, url_for
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from itertools import product

app = Flask(__name__)
session = requests.Session()
executor = ThreadPoolExecutor(max_workers=5)

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
    h1 {{ font-size: 2.5em; margin-bottom: .5em; }}
    pre {{ font-size: 1.2em; }}
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
    – Create Plan Maps & Sections
  </pre>
</body>
</html>
"""

def get_dropbox_access_token() -> str:
    cid = os.getenv("DROPBOX_CLIENT_ID")
    csec = os.getenv("DROPBOX_CLIENT_SECRET")
    rtok = os.getenv("DROPBOX_REFRESH_TOKEN")
    if not all([cid, csec, rtok]):
        raise RuntimeError("Missing Dropbox credentials")
    auth = base64.b64encode(f"{cid}:{csec}".encode()).decode()
    resp = session.post(
        "https://api.dropbox.com/oauth2/token",
        data={"grant_type":"refresh_token","refresh_token":rtok},
        headers={"Authorization":f"Basic {auth}"}
    )
    resp.raise_for_status()
    return resp.json()["access_token"]

def ensure_folder(dbx: dropbox.Dropbox, path: str) -> None:
    try:
        dbx.files_get_metadata(path)
    except dropbox.exceptions.ApiError:
        dbx.files_create_folder_v2(path)

def download_ar_generic(
    ar_number: str,
    province: str,
    project: str,
    list_page_url: str = None,
    base_url: str = None
) -> int:
    """
    1) Створює структуру папок і копіює шаблони:
       - Instructions.xlsx
       - Geochemistry.gdb
       - DDH.gdb
       - Plan Maps/, Sections/
    2) Якщо list_page_url задано — скрапить PDF і завантажує їх.
    Повертає кількість завантажених PDF.
    """
    # --- Dropbox setup ---
    token = get_dropbox_access_token()
    dbx = dropbox.Dropbox(token)

    base = f"/KENORLAND_DIGITIZING/ASSESSMENT_REPORTS/1 - NEW REPORTS/{province}/{project}/{ar_number}"
    instr = f"{base}/Instructions"
    srcdata = f"{base}/Source Data"

    # Створюємо потрібні папки
    for p in (base, instr, srcdata, f"{base}/Plan Maps", f"{base}/Sections"):
        ensure_folder(dbx, p)

    # Копіюємо шаблони
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

    # Якщо не задано сторінку — припиняємо тут
    if not list_page_url:
        return 0

    # Інакше — скрапимо PDF
    resp = session.get(list_page_url)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    pdfs = [a["href"] for a in soup.find_all("a", href=True)
            if a["href"].lower().endswith(".pdf")]

    count = 0
    for href in pdfs:
        name = os.path.basename(href)
        root, ext = os.path.splitext(name)
        variants = [''.join(p) for p in product(*[(c.lower(),c.upper()) for c in ext[1:]])]
        for v in variants:
            url = f"{base_url}/{ar_number}/{root}.{v}" if base_url else list_page_url + href
            try:
                r = session.get(url)
                r.raise_for_status()
                dbx.files_upload(
                    r.content,
                    f"{srcdata}/{os.path.basename(url)}",
                    mode=WriteMode.overwrite
                )
                count += 1
                break
            except requests.HTTPError:
                continue
            except Exception as e:
                app.logger.error(f"PDF upload error [{url}]: {e}")
                break

    return count

@app.route("/download_gm", methods=["POST"])
def download_gm():
    data      = request.get_json(force=True)
    num       = str(data.get("ar_number","")).strip()
    prov      = str(data.get("province","")).strip()
    proj      = str(data.get("project","")).strip()
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
            # NB: лише копіюємо шаблони + створюємо папки
            cnt = download_ar_generic(num, prov, proj)
        else:
            return jsonify(error="Invalid province or AR#"), 400

        msg = f"Downloaded {cnt} PDFs" if cnt>0 else "Folders created. No PDFs downloaded."
        return jsonify(message=msg), 200

    except requests.HTTPError as he:
        app.logger.error(f"HTTP error: {he}")
        return jsonify(error=str(he)), 502
    except Exception as e:
        app.logger.error(f"Unexpected error: {e}", exc_info=True)
        return jsonify(error=str(e)), 500

@app.errorhandler(Exception)
def all_errors(e):
    app.logger.error(f"Unhandled: {e}", exc_info=True)
    return jsonify(error="Internal server error"), 500

if __name__ == "__main__":
    port = int(os.getenv("PORT", 81))
    app.run(host="0.0.0.0", port=port)
