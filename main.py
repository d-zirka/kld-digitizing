import os
import base64
import requests
import dropbox
from flask import Flask, request, jsonify
from bs4 import BeautifulSoup
import re
import threading
import time

app = Flask(__name__)


# Отримання short-lived токену для Dropbox
def get_dropbox_access_token():
    client_id = os.environ.get("DROPBOX_CLIENT_ID")
    client_secret = os.environ.get("DROPBOX_CLIENT_SECRET")
    refresh_token = os.environ.get("DROPBOX_REFRESH_TOKEN")

    if not client_id or not client_secret or not refresh_token:
        raise Exception("Missing Dropbox credentials")

    token_url = "https://api.dropbox.com/oauth2/token"
    auth_str = f"{client_id}:{client_secret}"
    base64_auth = base64.b64encode(auth_str.encode()).decode()

    payload = {"grant_type": "refresh_token", "refresh_token": refresh_token}
    headers = {"Authorization": f"Basic {base64_auth}"}

    resp = requests.post(token_url, data=payload, headers=headers)
    if resp.status_code != 200:
        raise Exception(f"Error refreshing token: {resp.text}")
    return resp.json()["access_token"]


@app.route("/")
def index():
    return "GM Reports Server is running."


@app.route("/download_gm", methods=["POST"])
def download_gm():
    """
    Завантаження звітів для Quebec або Ontario.
    """
    data = request.json
    if not data:
        return jsonify({"error": "No JSON provided"}), 400

    gm_number = data.get("gm_number")
    province = data.get("province")
    project = data.get("project")

    if not gm_number or not province or not project:
        return jsonify({"error": "Missing required parameters"}), 400

    # Вибір логіки для провінції
    if province == "Quebec" and gm_number.startswith("GM"):
        return download_gm_quebec(gm_number, province, project)
    elif province == "Ontario":
        return download_gm_ontario(gm_number, province, project)
    else:
        return jsonify(
            {"error":
             "Invalid combination of province and report number"}), 400


def download_gm_quebec(gm_number, province, project):
    """
    Завантаження звітів для Quebec.
    """
    base_url = f"https://gq.mines.gouv.qc.ca/documents/EXAMINE/{gm_number}/"
    try:
        response = requests.get(base_url)
        if response.status_code != 200:
            return jsonify({"error": f"Could not access {base_url}"}), 500

        soup = BeautifulSoup(response.text, "html.parser")
        pdf_links = [
            a["href"] for a in soup.find_all("a", href=True)
            if a["href"].lower().endswith(".pdf")
        ]

        if not pdf_links:
            return jsonify({"message": "No PDFs found"}), 200

        access_token = get_dropbox_access_token()
        dbx = dropbox.Dropbox(access_token)

        base_folder = f"/KENORLAND_DIGITIZING/ASSESSMENT_REPORTS/1 - NEW REPORTS/{province}/{project}/{gm_number}"
        create_folder_if_missing(dbx, base_folder + "/Instructions")
        create_folder_if_missing(dbx, base_folder + "/Source Data")

        for pdf in pdf_links:
            pdf_url = base_url + pdf
            pdf_path = base_folder + "/Source Data/" + pdf
            r = requests.get(pdf_url)
            if r.status_code == 200:
                dbx.files_upload(r.content,
                                 pdf_path,
                                 mode=dropbox.files.WriteMode.overwrite)

        return jsonify({"message": "Quebec PDFs downloaded"}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


def download_gm_ontario(gm_number, province, project):
    """
    Завантаження звітів для Ontario.
    """
    page_url = f"https://www.geologyontario.mndm.gov.on.ca/mndmfiles/afri/data/records/{gm_number}.html"
    base_url = "https://prd-0420-geoontario-0000-blob-cge0eud7azhvfsf7.z01.azurefd.net/lrc-geology-documents/assessment"

    try:
        response = requests.get(page_url)
        if response.status_code != 200:
            return jsonify({"error": f"Could not access {page_url}"}), 500

        soup = BeautifulSoup(response.content, "html.parser")
        # Шукаємо всі посилання на PDF на першій сторінці
        pdf_links = [
            a["href"] for a in soup.find_all("a", href=True)
            if re.search(r'\.pdf$', a["href"], re.IGNORECASE)
        ]

        if not pdf_links:
            return jsonify({"message": "No PDFs found"}), 200

        access_token = get_dropbox_access_token()
        dbx = dropbox.Dropbox(access_token)

        base_folder = f"/KENORLAND_DIGITIZING/ASSESSMENT_REPORTS/1 - NEW REPORTS/{province}/{project}/{gm_number}"
        create_folder_if_missing(dbx, base_folder + "/Instructions")
        create_folder_if_missing(dbx, base_folder + "/Source Data")

        for pdf_link in pdf_links:
            path_parts = pdf_link.strip("/").split("/")
            if len(path_parts) >= 2:
                pdf_folder = path_parts[-2]
                pdf_filename = path_parts[-1]
                pdf_url = f"{base_url}/{pdf_folder}/{pdf_filename}"
                r = requests.get(pdf_url)
                if r.status_code == 200:
                    dropbox_path = f"{base_folder}/Source Data/{pdf_filename}"
                    dbx.files_upload(r.content,
                                     dropbox_path,
                                     mode=dropbox.files.WriteMode.overwrite)

        return jsonify({"message": "Ontario PDFs downloaded"}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


def create_folder_if_missing(dbx, path):
    """
    Перевіряємо, чи існує папка 'path'. Якщо ні - створюємо.
    """
    try:
        dbx.files_get_metadata(path)
    except dropbox.exceptions.ApiError:
        dbx.files_create_folder_v2(path)


# Додано функцію Keep-Alive
def keep_alive():
    """
    Періодично пінгує сервер, щоб він не засинав.
    """
    while True:
        try:
            requests.get("http://127.0.0.1:81")  # Локальний пінг сервера
            print("Keep-alive ping sent.")
        except Exception as e:
            print(f"Keep-alive failed: {e}")
        time.sleep(300)  # Пінг кожні 5 хвилин


if __name__ == "__main__":
    # Запускаємо фоновий потік для Keep-Alive
    threading.Thread(target=keep_alive, daemon=True).start()

    # Запускаємо основний Flask-сервер
    app.run(host="0.0.0.0", port=81)
