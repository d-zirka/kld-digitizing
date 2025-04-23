import os
import base64
import requests
import dropbox
from dropbox.files import WriteMode
from flask import Flask, request, jsonify
from bs4 import BeautifulSoup
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

app = Flask(__name__)
# Reuse HTTP session for connection pooling
session = requests.Session()
# ThreadPool for parallel PDF downloads
executor = ThreadPoolExecutor(max_workers=5)


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


def download_gm_generic(
    gm_number: str,
    province: str,
    project: str,
    list_page_url: str,
    url_transform,
) -> int:
    """
    Generic GM report downloader: scrape page, find PDF links, upload in parallel.
    - list_page_url: full URL to fetch HTML listing PDFs
    - url_transform: function(pdf_href) -> absolute PDF URL
    Returns number of PDFs downloaded.
    """
    resp = session.get(list_page_url)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    pdf_links = [a["href"] for a in soup.find_all("a", href=True)
                 if a["href"].lower().endswith(".pdf")]
    if not pdf_links:
        return 0

    access_token = get_dropbox_access_token()
    dbx = dropbox.Dropbox(access_token)
    base_folder = f"/KENORLAND_DIGITIZING/ASSESSMENT_REPORTS/1 - NEW REPORTS/{province}/{project}/{gm_number}"
    ensure_folder(dbx, base_folder)
    ensure_folder(dbx, base_folder + "/Instructions")
    ensure_folder(dbx, base_folder + "/Source Data")

    futures = []
    for href in pdf_links:
        pdf_url = url_transform(href)
        filename = os.path.basename(href)
        dest = base_folder + "/Source Data/" + filename
        futures.append(executor.submit(_upload_pdf, dbx, pdf_url, dest))

    count = 0
    for future in as_completed(futures):
        if future.result():
            count += 1
    return count


def _upload_pdf(dbx: dropbox.Dropbox, url: str, dest_path: str) -> bool:
    try:
        r = session.get(url)
        r.raise_for_status()
        dbx.files_upload(r.content, dest_path, mode=WriteMode.overwrite)
        return True
    except Exception as e:
        app.logger.error(f"Failed to download or upload {url}: {e}")
        return False


@app.route("/download_gm", methods=["POST"])
def download_gm() -> tuple:
    """
    Download GM reports for Quebec or Ontario.
    """
    data = request.get_json(force=True)
    gm_number = data.get("gm_number", "").strip()
    province = data.get("province", "").strip()
    project = data.get("project", "").strip()

    if not all([gm_number, province, project]):
        return jsonify(error="Missing required parameters"), 400

    try:
        if province == "Quebec" and gm_number.upper().startswith("GM"):
            url = f"https://gq.mines.gouv.qc.ca/documents/EXAMINE/{gm_number}/"
            downloaded = download_gm_generic(
                gm_number, province, project,
                url,
                lambda href: url + href
            )
        elif province == "Ontario":
            page_url = f"https://www.geologyontario.mndm.gov.on.ca/mndmfiles/afri/data/records/{gm_number}.html"
            base_url = "https://prd-0420-geoontario-0000-blob-cge0eud7azhvfsf7.z01.azurefd.net/lrc-geology-documents/assessment"
            downloaded = download_gm_generic(
                gm_number, province, project,
                page_url,
                lambda href: base_url + href
            )
        else:
            return jsonify(error="Invalid province or GM number format"), 400

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
