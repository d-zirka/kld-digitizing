import os
import base64
import requests
import dropbox
from dropbox.files import WriteMode
from flask import Flask, request, jsonify
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from itertools import product

app = Flask(__name__)
session = requests.Session()
executor = ThreadPoolExecutor(max_workers=5)

# === Здоров’я-схема ===
@app.route("/")
def index():
    return "Canadian AR Server is running! 🚀"

def get_dropbox_access_token() -> str:
    client_id     = os.getenv("DROPBOX_CLIENT_ID")
    client_secret = os.getenv("DROPBOX_CLIENT_SECRET")
    refresh_token = os.getenv("DROPBOX_REFRESH_TOKEN")
    if not all([client_id, client_secret, refresh_token]):
        raise RuntimeError("Missing Dropbox credentials")
    auth_str = f"{client_id}:{client_secret}"
    b64_auth = base64.b64encode(auth_str.encode()).decode()
    resp = session.post(
        "https://api.dropbox.com/oauth2/token",
        data={"grant_type": "refresh_token", "refresh_token": refresh_token},
        headers={"Authorization": f"Basic {b64_auth}"}
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
    list_page_url: str,
    base_url: str = None
) -> int:
    # 1) Парсимо сторінку, збираємо всі .pdf
    resp = session.get(list_page_url)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    pdf_links = [a["href"] for a in soup.find_all("a", href=True)
                 if a["href"].lower().endswith(".pdf")]
    if not pdf_links:
        return 0

    # 2) Ініціалізуємо Dropbox
    access_token = get_dropbox_access_token()
    dbx = dropbox.Dropbox(access_token)

    # 3) Створюємо структуру папок
    base_folder = f"/KENORLAND_DIGITIZING/ASSESSMENT_REPORTS/1 - NEW REPORTS/{province}/{project}/{ar_number}"
    ensure_folder(dbx, base_folder)
    ensure_folder(dbx, f"{base_folder}/Instructions")
    ensure_folder(dbx, f"{base_folder}/Source Data")

    # 4) Копіюємо шаблон інструкцій і перейменовуємо
    TEMPLATE_INSTR_PATH = (
        "/KENORLAND_DIGITIZING/ASSESSMENT_REPORTS/_Documents/Instructions/"
        "01_Instructions.xlsx"
    )
    dest_instr = f"{base_folder}/Instructions/{ar_number}_Instructions.xlsx"
    try:
        dbx.files_copy_v2(TEMPLATE_INSTR_PATH, dest_instr)
        app.logger.info(f"Copied instructions => {dest_instr}")
    except dropbox.exceptions.ApiError as e:
        app.logger.error(f"Failed to copy instructions: {e}")

    # 5) Завантажуємо PDF-ки
    count = 0
    for href in pdf_links:
        filename = os.path.basename(href)
        name_root, ext = os.path.splitext(filename)
        variants = [''.join(p) for p in product(*[
            (c.lower(), c.upper()) for c in ext[1:]
        ])]

        for variant in variants:
            # абсолютний URL?
            if href.lower().startswith(("http://", "https://")):
                pdf_url = href
            # якщо передано base_url (Ontario)
            elif base_url:
                pdf_url = f"{base_url}/{ar_number}/{name_root}.{variant}"
            # інакше — відносний шлях від list_page_url
            else:
                pdf_url = list_page_url.rstrip("/") + "/" + href.lstrip("/")

            try:
                r = session.get(pdf_url)
                r.raise_for_status()
                dest = f"{base_folder}/Source Data/{os.path.basename(pdf_url)}"
                dbx.files_upload(r.content, dest, mode=WriteMode.overwrite)
                count += 1
                break
            except requests.HTTPError:
                continue
            except Exception as e:
                app.logger.error(f"Error fetching/uploading {pdf_url}: {e}")
                break

    return count

@app.route("/download_gm", methods=["POST"])
def download_gm() -> tuple:
    data      = request.get_json(force=True)
    ar_number = str(data.get("ar_number", "")).strip()
    province  = str(data.get("province",  "")).strip()
    project   = str(data.get("project",   "")).strip()

    if not all([ar_number, province, project]):
        return jsonify(error="Missing required parameters"), 400

    try:
        if province == "Quebec" and ar_number.upper().startswith("GM"):
            url = f"https://gq.mines.gouv.qc.ca/documents/EXAMINE/{ar_number}/"
            downloaded = download_ar_generic(ar_number, province, project, url)
        elif province == "Ontario":
            list_page = (
                f"https://www.geologyontario.mndm.gov.on.ca/"
                f"mndmfiles/afri/data/records/{ar_number}.html"
            )
            blob_base = (
                "https://prd-0420-geoontario-0000-blob-cge0eud7azhvfsf7."
                "z01.azurefd.net/lrc-geology-documents/assessment"
            )
            downloaded = download_ar_generic(
                ar_number, province, project, list_page, blob_base
            )
        else:
            return jsonify(error="Invalid province or AR number format"), 400

        msg = (f"Downloaded {downloaded} PDFs"
               if downloaded > 0 else "No PDFs found")
        return jsonify(message=msg), 200

    except requests.HTTPError as http_err:
        app.logger.error(f"HTTP error: {http_err}")
        return jsonify(error=str(http_err)), 502
    except Exception as e:
        app.logger.error(f"Unexpected error: {e}", exc_info=True)
        return jsonify(error=str(e)), 500

@app.errorhandler(Exception)
def handle_all_errors(e):
    app.logger.error(f"Unhandled exception: {e}", exc_info=True)
    return jsonify(error="Internal server error"), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 81)))
