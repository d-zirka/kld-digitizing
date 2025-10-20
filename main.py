import os
import base64
import logging
from typing import Optional, List
from urllib.parse import urljoin, urlparse
from itertools import product
from flask_cors import CORS
CORS(app, resources={r"/api/*": {"origins": "*"}})


import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import dropbox
from dropbox.files import WriteMode

import io
import pikepdf

from flask import Flask, request, jsonify, url_for, send_from_directory, render_template_string
from bs4 import BeautifulSoup
from werkzeug.exceptions import HTTPException

# -----------------------------------------------------------------------------
# Flask app & logging
# -----------------------------------------------------------------------------
app = Flask(__name__)

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
# Службові маршрути
# -----------------------------------------------------------------------------
@app.route("/healthz")
def healthz():
    return "ok", 200

@app.route("/favicon.ico")
def favicon():
    static_path = os.path.join(app.root_path, "static")
    fav = os.path.join(static_path, "favicon.png")
    if os.path.exists(fav):
        return send_from_directory(static_path, "favicon.png", mimetype="image/png")
    return "", 204

# -----------------------------------------------------------------------------
# Головна сторінка
# -----------------------------------------------------------------------------

@app.route("/")
def index():
    return render_template_string("""
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>Kenorland Digitizing</title>
  <link rel="icon" href="/static/favicon.png">
  <style>
    :root{
      --bg:#f5f7fa; --fg:#111827; --muted:#6b7280; --card:#ffffff; --sub:#fafafa; --border:#e5e7eb; --accent:#2563eb; --ok:#10b981; --danger:#ef4444;
      --shadow:0 8px 28px rgba(0,0,0,.08);
    }
    *{box-sizing:border-box}
    html,body{height:100%}
    body{
      margin:0;background:var(--bg);color:var(--fg);
      font:15px/1.6 ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,"Helvetica Neue",Arial;
      display:flex;align-items:center;justify-content:center;padding:20px;overflow-x:hidden;
    }
    .wrap{
      width:100%;max-width:1100px;background:var(--card);border:1px solid var(--border);border-radius:16px;box-shadow:var(--shadow);padding:24px;
    }
    header{display:flex;gap:12px;align-items:center;margin-bottom:8px}
    .logo{width:36px;height:36px;border-radius:10px;background:linear-gradient(135deg,#2563eb,#10b981);display:grid;place-items:center;color:#fff;font-weight:700}
    h1{margin:0;font-size:clamp(22px,3vw,30px)}
    .tag{color:var(--ok);font-weight:600;margin-left:auto}
    h2{margin:10px 0 12px;font-size:14px;letter-spacing:.12em;color:var(--muted)}

    /* дві колонки */
    .cols{display:grid;grid-template-columns:1fr;gap:16px}
    @media (min-width:980px){ .cols{grid-template-columns:1.3fr 1fr} } /* робимо праву вужчою */
    section{background:var(--sub);border:1px solid var(--border);border-radius:12px;padding:16px 18px}
    section h3{margin:0 0 6px}
    ul{margin:8px 0 0 18px;padding:0}

    /* чипи й кнопки знизу */
    .actions{display:flex;flex-wrap:wrap;gap:8px;justify-content:center;margin-top:16px}
    .chip{
      display:inline-block;background:#f3f4f6;border:1px solid var(--border);color:#374151;
      font-size:12.5px;line-height:1;padding:7px 12px;border-radius:999px;font-weight:500;
      user-select:none;pointer-events:none; /* не клікається */
    }
    .btn{
      border:1px solid var(--border);background:#fff;color:#fg;
      font-size:13px;padding:8px 14px;border-radius:999px;font-weight:700;cursor:pointer;
    }
    .btn:hover{border-color:var(--accent)}
    .btn:active{transform:translateY(.5px)}

    /* модалка */
    .backdrop{position:fixed;inset:0;background:rgba(0,0,0,.35);display:none;align-items:center;justify-content:center;padding:16px;z-index:50}
    .modal{width:min(520px,100%);background:#fff;border:1px solid var(--border);border-radius:14px;box-shadow:var(--shadow);padding:18px}
    .modal h4{margin:0 0 8px;font-size:16px}
    .pill{display:inline-block;padding:4px 10px;border-radius:999px;border:1px solid var(--border);font-weight:700}
    .ok{color:var(--ok);border-color:var(--ok)} .bad{color:var(--danger);border-color:var(--danger)}

    footer{display:flex;justify-content:space-between;align-items:center;margin-top:14px;color:var(--muted);font-size:12px}
    footer b{font-weight:700}
  </style>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4"></script>
</head>
<body>
  <div class="wrap">
    <header>
      <div class="logo">KLD</div>
      <h1>Kenorland Digitizing Server is running 🚀</h1>
      <div class="tag">healthy</div>
    </header>

    <h2>FUNCTIONALITY</h2>
    <div class="cols">
      <section>
        <h3>AR</h3>
        <ul>
          <li>Download AR PDFs for <b>Quebec (GM#)</b>, <b>Ontario</b>, and <b>Manitoba</b></li>
          <li>Create report structure & templates for <b>QC, ON, NB, MB</b>:
            <ul>
              <li>Copy & rename <i>Instructions.xlsx</i></li>
              <li>Copy & rename <i>Geochemistry.gdb</i></li>
              <li>Copy & rename <i>DDH.gdb</i></li>
            </ul>
          </li>
        </ul>
      </section>

      <section>
        <h3>ASX</h3>
        <ul>
          <li>Auto-unlock ASX PDFs</li>
          <li>Removes encryption and copy/print restrictions</li>
        </ul>
      </section>
    </div>

    <div class="actions">
      <span class="chip">Dropbox integrated</span>
      <span class="chip">Google Apps Script integrated</span>
      <span class="chip">Timeouts & retries</span>
      <button class="btn" onclick="checkHealth()">Check health</button>
      <span class="chip">ASX unlock API</span>
    </div>

        <!-- ===== STATS SECTION ===== -->
    <section class="bg-white rounded-2xl shadow p-6" style="margin-top:16px">
      <h2 class="text-xl font-semibold">Статистика проєктів</h2>
      <div class="mt-4">
        <canvas id="projChart" height="140"></canvas>
      </div>
    </section>

    <footer>
      <div>Powered by <b>Flask</b> · <b>Render</b></div>
      <div>Created by <b>Zirka</b> · <b>chatGPT</b></div>
    </footer>
  </div>

  <!-- Health modal -->
  <div id="backdrop" class="backdrop" role="dialog" aria-modal="true" aria-labelledby="healthTitle">
    <div class="modal">
      <h4 id="healthTitle">Service health</h4>
      <div id="healthBody">Checking…</div>
      <div style="margin-top:12px;display:flex;justify-content:flex-end">
        <button class="btn" onclick="closeModal()">Close</button>
      </div>
    </div>
  </div>

  <script>
    const backdrop = document.getElementById('backdrop');
    const bodyEl = document.getElementById('healthBody');

    function openModal(){ backdrop.style.display = 'flex'; }
    function closeModal(){ backdrop.style.display = 'none'; }

    async function checkHealth(){
      openModal();
      bodyEl.innerHTML = 'Checking…';
      try{
        const res = await fetch('/healthz', { cache:'no-store' });
        const txt = (await res.text() || '').trim();
        const ok = res.ok && txt.toLowerCase().includes('ok');
        bodyEl.innerHTML = ok
          ? 'Status: <span class="pill ok">OK</span>'
          : 'Status: <span class="pill bad">Unavailable</span><div style="margin-top:6px;color:#6b7280">Response: <code>'+escapeHtml(txt)+'</code></div>';
      }catch(e){
        bodyEl.innerHTML = 'Status: <span class="pill bad">Error</span><div style="margin-top:6px;"><code>'+escapeHtml(String(e))+'</code></div>';
      }
    }
    function escapeHtml(s){ return s.replace(/[&<>"']/g, m=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m])); }
    backdrop.addEventListener('click', e=>{ if(e.target===backdrop) closeModal(); });
    document.addEventListener('keydown', e=>{ if(e.key==='Escape') closeModal(); });

      // ====== Chart: project stats ======
  async function initChart() {
    try {
      const res = await fetch('/api/stats', { cache: 'no-store' });
      const data = await res.json();
      const labels = data.labels || [];
      const values = data.values || [];

      const ctx = document.getElementById('projChart');
      if (!ctx) return;

      new Chart(ctx, {
        type: 'bar',
        data: {
          labels,
          datasets: [{
            label: 'К-сть завантажених PDF / дій',
            data: values
          }]
        },
        options: {
          responsive: true,
          plugins: { legend: { display: true } },
          scales: { y: { beginAtZero: true, ticks: { precision: 0 } } }
        }
      });
    } catch (e) {
      console.error('Chart init error:', e);
    }
  }

  // Запуск при завантаженні сторінки
  document.addEventListener('DOMContentLoaded', initChart);
    
  </script>
</body>
</html>
""")



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
# PDF helpers
# -----------------------------------------------------------------------------
def _extract_pdf_links(html: str, base: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if href.lower().endswith(".pdf") and urlparse(href).scheme in ("http", "https"):
            links.append(href)
        elif href.lower().endswith(".pdf"):
            links.append(urljoin(base, href))
    return list(dict.fromkeys(links))

def _case_variants(ext: str) -> List[str]:
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

def download_ar_generic(ar_number: str, province: str, project: str,
                        list_page_url: str | None = None,
                        base_url: str | None = None) -> int:
    token = get_dropbox_access_token()
    dbx = dropbox.Dropbox(token)
    base = f"/KENORLAND_DIGITIZING/ASSESSMENT_REPORTS/1 - NEW REPORTS/{province}/{project}/{ar_number}"
    instr = f"{base}/Instructions"
    srcdata = f"{base}/Source Data"
    for p in (base, instr, srcdata):
        ensure_folder(dbx, p)
    try:
        dbx.files_copy_v2("/KENORLAND_DIGITIZING/ASSESSMENT_REPORTS/_Documents/Instructions/01_Instructions.xlsx",
                          f"{instr}/{ar_number}_Instructions.xlsx", autorename=False)
    except dropbox.exceptions.ApiError as e:
        app.logger.warning(f"Instructions copy failed: {e}")
    try:
        dbx.files_copy_v2("/KENORLAND_DIGITIZING/ASSESSMENT_REPORTS/_Documents/Instructions/ReportID_Geochemistry.gdb",
                          f"{base}/{ar_number}_Geochemistry.gdb", autorename=False)
    except dropbox.exceptions.ApiError as e:
        app.logger.warning(f"Geochemistry copy failed: {e}")
    try:
        dbx.files_copy_v2("/KENORLAND_DIGITIZING/ASSESSMENT_REPORTS/_Documents/Instructions/ReportID_DDH.gdb",
                          f"{base}/{ar_number}_DDH.gdb", autorename=False)
    except dropbox.exceptions.ApiError as e:
        app.logger.warning(f"DDH copy failed: {e}")
    if not list_page_url:
        return 0
    resp = session.get(list_page_url, timeout=DEFAULT_TIMEOUT)
    resp.raise_for_status()
    pdf_links = _extract_pdf_links(resp.text, list_page_url)
    more_links: List[str] = []
    if base_url:
        soup = BeautifulSoup(resp.text, "html.parser")
        hrefs = [a["href"].strip() for a in soup.find_all("a", href=True)]
        candidates = []
        for h in hrefs:
            name = os.path.basename(h)
            root, ext = os.path.splitext(name)
            if ext and ext[1:].lower() == "pdf":
                candidates.append(root)
        if not candidates:
            for h in hrefs:
                name = os.path.basename(h)
                root, _ = os.path.splitext(name)
                if root:
                    candidates.append(root)
        candidates = list(dict.fromkeys(candidates))
        for root in candidates:
            for v in _case_variants("pdf"):
                more_links.append(f"{base_url}/{ar_number}/{root}.{v}")
    all_links = list(dict.fromkeys(pdf_links + more_links))
    count = 0
    for url in all_links:
        try:
            content = _try_get(url)
            if not content:
                continue
            filename = os.path.basename(urlparse(url).path) or "file.pdf"
            dbx.files_upload(content, f"{srcdata}/{filename}", mode=WriteMode.overwrite)
            count += 1
        except Exception as e:
            app.logger.error(f"PDF upload error [{url}]: {e}")
    return count


# -------------------- ADD: Manitoba direct-download logic --------------------
def download_ar_manitoba(ar_number: str, province: str, project: str) -> int:
    """
    Manitoba: пряме завантаження одного PDF:
    https://www.gov.mb.ca/data/em/application/assessment/{ar_number}.pdf
    """
    token = get_dropbox_access_token()
    dbx = dropbox.Dropbox(token)

    base = f"/KENORLAND_DIGITIZING/ASSESSMENT_REPORTS/1 - NEW REPORTS/{province}/{project}/{ar_number}"
    instr = f"{base}/Instructions"
    srcdata = f"{base}/Source Data"

    # Створення тек та копіювання шаблонів — ідентично іншим провінціям
    for p in (base, instr, srcdata):
        ensure_folder(dbx, p)
    try:
        dbx.files_copy_v2("/KENORLAND_DIGITIZING/ASSESSMENT_REPORTS/_Documents/Instructions/01_Instructions.xlsx",
                          f"{instr}/{ar_number}_Instructions.xlsx", autorename=False)
    except dropbox.exceptions.ApiError as e:
        app.logger.warning(f"Instructions copy failed: {e}")
    try:
        dbx.files_copy_v2("/KENORLAND_DIGITIZING/ASSESSMENT_REPORTS/_Documents/Instructions/ReportID_Geochemistry.gdb",
                          f"{base}/{ar_number}_Geochemistry.gdb", autorename=False)
    except dropbox.exceptions.ApiError as e:
        app.logger.warning(f"Geochemistry copy failed: {e}")
    try:
        dbx.files_copy_v2("/KENORLAND_DIGITIZING/ASSESSMENT_REPORTS/_Documents/Instructions/ReportID_DDH.gdb",
                          f"{base}/{ar_number}_DDH.gdb", autorename=False)
    except dropbox.exceptions.ApiError as e:
        app.logger.warning(f"DDH copy failed: {e}")

    # Прямий PDF без суфіксів
    url = f"https://www.gov.mb.ca/data/em/application/assessment/{ar_number}.pdf"
    content = _try_get(url)
    if not content:
        return 0

    filename = os.path.basename(urlparse(url).path) or f"{ar_number}.pdf"
    dbx.files_upload(content, f"{srcdata}/{filename}", mode=WriteMode.overwrite)
    return 1
# -----------------------------------------------------------------------------


ASX_DROPBOX_PREFIX = "/KENORLAND_DIGITIZING/ASX/2 - WORKING/"

def _is_allowed_asx_path(path: str) -> bool:
    return isinstance(path, str) and path.startswith(ASX_DROPBOX_PREFIX)

def _check_bearer(req) -> None:
    expected = os.getenv("ASX_UNLOCK_TOKEN", "").strip()
    if not expected:
        return
    if req.headers.get("Authorization", "") != f"Bearer {expected}":
        raise PermissionError("Unauthorized")

def _unlock_pdf_bytes(data: bytes) -> bytes:
    """
    Знімає owner-обмеження та прибирає шифрування.
    Підтримує кейс із порожнім user-password ("").
    Якщо справді потрібен пароль на відкриття (непорожній) або сталася помилка — повертає оригінал.
    """
    import io
    import pikepdf

    # Спробувати без пароля, потім з порожнім паролем
    for pw in (None, ""):
        try:
            pdf = pikepdf.open(io.BytesIO(data), password=pw)
            try:
                out = io.BytesIO()
                # ВАЖЛИВО: зберігаємо БЕЗ encryption= — файл стає нешифрований і без "Enter password"
                pdf.save(out)
                pdf.close()
                return out.getvalue()
            finally:
                try:
                    pdf.close()
                except Exception:
                    pass
        except (pikepdf.PasswordError, getattr(pikepdf, "_qpdf", type("", (), {})) .__dict__.get("PasswordError", Exception)):
            # потрібен справжній user-password (непорожній) — пропускаємо цикл/повернемо оригінал
            continue
        except Exception:
            # будь-яка інша помилка — переходимо до повернення оригіналу
            break

    return data


@app.post("/asx_unlock_upload")
def asx_unlock_upload():
    try:
        _check_bearer(request)
    except PermissionError:
        return jsonify(error="Unauthorized"), 401

    f = request.files.get("file")
    path = request.form.get("dropbox_path", "")
    if not f or not path:
        return jsonify(error="Missing file or dropbox_path"), 400
    if not _is_allowed_asx_path(path):
        return jsonify(error="Path not allowed"), 400

    try:
        data = f.read()
        if not data:
            return jsonify(error="Empty file"), 400

        unlocked = _unlock_pdf_bytes(data)

        token = get_dropbox_access_token()
        dbx = dropbox.Dropbox(token)
        dbx.files_upload(unlocked, path, mode=WriteMode.overwrite)

        return jsonify(message="Uploaded (unlocked if possible)", path=path), 200
    except Exception as e:
        app.logger.error(f"/asx_unlock_upload error: {e}", exc_info=True)
        return jsonify(error=str(e)), 500


# -----------------------------------------------------------------------------
# API route
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
        elif prov == "Manitoba":
            cnt = download_ar_manitoba(num, prov, proj)
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
# Error handler
# -----------------------------------------------------------------------------
@app.errorhandler(Exception)
def all_errors(e):
    if isinstance(e, HTTPException):
        return e
    app.logger.error(f"Unhandled: {e}", exc_info=True)
    return jsonify(error="Internal server error"), 500

# -----------------------------------------------------------------------------
# Local run
# -----------------------------------------------------------------------------

@app.get("/api/stats")
def api_stats():
    """
    Поки що повертаємо прості тестові дані.
    Потім підмінимо на реальні (напр., скільки PDF завантажено по провінціях).
    """
    data = {
        "labels": ["Quebec", "Ontario", "Manitoba", "New Brunswick"],
        "values": [12, 9, 4, 2]  # тимчасові числа
    }
    return jsonify(data), 200



if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port)
