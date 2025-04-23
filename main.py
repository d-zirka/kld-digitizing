import os
import base64
import requests
import dropbox
from dropbox.files import WriteMode
from flask import Flask, request, jsonify
from bs4 import BeautifulSoup
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import product

app = Flask(__name__)
# Reuse HTTP session for connection pooling
session = requests.Session()
# ThreadPool for parallel PDF downloads
executor = ThreadPoolExecutor(max_workers=5)

@app.route("/")
def index():
    return "Canadian AR Server is running! 🚀"


def get_dropbox_access_token() -> str:
    """
    Obtain a short-lived Dropbox access token using refresh token.
    """
    client_id = os.getenv("DROPBOX_CLIENT_ID")
    client_secret = os.getenv("DROPBOX_CLIENT_SECRET")
    refresh_token = os.getenv("DROPBOX_REFRESH_TOKEN")

    if not all([client_id, client_secret, refresh_token]):
        raise RuntimeError("Missing Dropbox credentials")

    auth_str = f"{client_id}:{client_secret}"
    b64_auth = base64.b64encode(auth_str.encode()).decode()
    token_url = "https://api.dropbox.com/oauth2/token"
    resp = session.post(
        token_url,
        data={"grant_type": "refresh_token", "refresh_token": refresh_token},
        headers={"Authorization": f"Basic {b64_auth}"}
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def ensure_folder(dbx: dropbox.Dropbox, path: str) -> None:
    """
    Create folder at path if it does not exist.
    """
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
    """
    Generic AR report downloader: scrape page, find PDF links, upload in parallel.
    If base_url is provided, constructs URLs: base_url/ar_number/filename
    Else uses list_page_url + href.
    Returns number of PDFs downloaded.
    """
    resp = session.get(list_page_url)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    # collect hrefs ending with any case variant of .pdf
    pdf_links = [a["href"] for a in soup.find_all("a", href=True)
                 if a["href"].lower().endswith(".pdf")]
    if not pdf_links:
        return 0

    access_token = get_dropbox_access_token()
    dbx = dropbox.Dropbox(access_token)
    base_folder = f"/KENORLAND_DIGITIZING/ASSESSMENT_REPORTS/1 - NEW REPORTS/{province}/{project}/{ar_number}"
    ensure_folder(dbx, base_folder)
    ensure_folder(dbx, base_folder + "/Instructions")
    ensure_folder(dbx, base_folder + "/Source Data")

    count = 0
    for href in pdf_links:
        filename = os.path.basename(href)
        name_root, ext = os.path.splitext(filename)
        # generate all case combinations for extension letters
        ext_chars = ext[1:]  # strip dot
        variants = [''.join(p) for p in product(*[(c.lower(), c.upper()) for c in ext_chars])]

        # attempt each variant
        for variant in variants:
            if base_url:
                pdf_url = f"{base_url}/{ar_number}/{name_root}.{variant}"
            else:
                pdf_url = list_page_url + href
            try:
                r = session.get(pdf_url)
                r.raise_for_status()
                dest = base_folder + "/Source Data/" + os.path.basename(pdf_url)
                dbx.files_upload(r.content, dest, mode=WriteMode.overwrite)
                count += 1
                break
            except requests.HTTPError:
                continue
            except Exception as e:
                app.logger.error(f"Failed to download or upload {pdf_url}: {e}")
                break
    return count

@app.route("/download_gm", methods=["POST"])
def download_gm() -> tuple:
    """
    Download AR reports for Quebec or Ontario.
    """
    data = request.get_json(force=True)
    ar_number = str(data.get("ar_number", "")).strip()
    province  = str(data.get("province",  "")).strip()
    project   = str(data.get("project",   "")).strip()


    if not all([ar_number, province, project]):
        return jsonify(error="Missing required parameters"), 400

    try:
        if province == "Quebec" and ar_number.upper().startswith("GM"):
            list_page = f"https://gq.mines.gouv.qc.ca/documents/EXAMINE/{ar_number}/"
            downloaded = download_ar_generic(ar_number, province, project, list_page)
        elif province == "Ontario":
            list_page = f"https://www.geologyontario.mndm.gov.on.ca/mndmfiles/afri/data/records/{ar_number}.html"
            blob_base = "https://prd-0420-geoontario-0000-blob-cge0eud7azhvfsf7.z01.azurefd.net/lrc-geology-documents/assessment"
            downloaded = download_ar_generic(ar_number, province, project, list_page, blob_base)
        else:
            return jsonify(error="Invalid province or AR number format"), 400

        if downloaded > 0:
            return jsonify(message=f"Downloaded {downloaded} PDFs"), 200
        else:
            return jsonify(message="No PDFs found"), 200

    except requests.HTTPError as http_err:
        app.logger.error(f"HTTP error: {http_err}")
        return jsonify(error=str(http_err)), 502
    except Exception as e:
        app.logger.error(f"Unexpected error: {e}", exc_info=True)
        return jsonify(error=str(e)), 500

@app.errorhandler(Exception)
def handle_all_errors(e):
    """
    Global error handler.
    """
    app.logger.error(f"Unhandled exception: {e}", exc_info=True)
    return jsonify(error="Internal server error"), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 81)))
