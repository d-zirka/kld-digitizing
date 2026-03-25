# KLD Digitizing Service

> **What this project does (quickly):** this service automates Kenorland geological report digitizing workflows by creating the expected Dropbox folder structure, pulling source PDFs by province, generating template-based XLSX files, and exposing runtime stats/health APIs.

## At a glance

- **Purpose:** speed up intake of geological assessment reports and ASX working files.
- **Input:** report metadata (`ar_number`, `province`, `project`) and optional uploaded PDFs/templates.
- **Output:** standardized Dropbox folders/files + API responses + runtime stats.
- **Core endpoints:** `/download_gm`, `/asx_unlock_upload`, `/asx_create_xlsx_dropbox_test`, `/api/stats`, `/healthz`.

Flask service for Kenorland digitizing workflows. It provides:

- Assessment report intake (`/download_gm`) that creates Dropbox folders, copies templates, and downloads province-specific PDFs.
- ASX utilities:
  - unlock + upload PDF (`/asx_unlock_upload`)
  - create and upload XLSX from a Dropbox template (`/asx_create_xlsx_dropbox_test`)
- Runtime stats API (`/api/stats`) backed by local file or Dropbox JSON storage.
- Health endpoint (`/healthz`).

## Tech Stack

- Python + Flask
- Dropbox API
- Requests + BeautifulSoup (PDF discovery/downloading)
- openpyxl (XLSX generation/editing)
- pikepdf (PDF unlock flow)

## Repository Layout

- `main.py` — Flask app, routes, Dropbox helpers, report download logic, ASX helpers.
- `stats_runtime.py` — stats state model and persistence (`file` / `dropbox`).
- `requirements.txt` — Python dependencies.
- `Procfile` — production command (Gunicorn).

## Requirements

- Python 3.10+
- A Dropbox app with OAuth refresh-token credentials for runtime Dropbox operations.

## Installation

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Configuration

### Required environment variables

For Dropbox-authenticated flows (`/download_gm`, `/asx_unlock_upload`, `/asx_create_xlsx_dropbox_test`):

- `DROPBOX_CLIENT_ID`
- `DROPBOX_CLIENT_SECRET`
- `DROPBOX_REFRESH_TOKEN`

### Optional environment variables

- `PORT` (default: `5000`)
- `ASX_UNLOCK_TOKEN` (if set, required as `Authorization: Bearer <token>` on `/asx_unlock_upload`)
- `STATS_BACKEND` (`file` or `dropbox`, default: `file`)
- `STATS_LOCAL_PATH` (default: `./stats/project_stats.json`)
- `STATS_DROPBOX_PATH` (default: `/KENORLAND_DIGITIZING/ASSESSMENT_REPORTS/_Documents/Stats/project_stats.json`)

## Run Locally

```bash
python main.py
```

App starts on `http://localhost:5000` by default.

## Production (Procfile)

```bash
gunicorn -w 4 --bind 0.0.0.0:$PORT main:app
```

## API Endpoints

### `GET /healthz`
Simple health check.

### `POST /download_gm`
Create report folders/templates in Dropbox and attempt province-specific PDF download.

Request JSON:

```json
{
  "ar_number": "GM12345",
  "province": "Quebec",
  "project": "ProjectName"
}
```

Supported `province` values:
- `Quebec`
- `Ontario`
- `Manitoba`
- `New Brunswick`
- `Nunavut`

### `GET /api/stats?period=all`
Returns aggregated runtime stats and chart-ready data.

Typical period values include `all`, `7d`, and `30d`.

### `POST /asx_unlock_upload`
Multipart upload endpoint:
- form `file`: PDF bytes
- form `dropbox_path`: must begin with `/KENORLAND_DIGITIZING/ASX/2 - WORKING/`

If `ASX_UNLOCK_TOKEN` is set, send header:

```http
Authorization: Bearer <token>
```

### `POST /asx_create_xlsx_dropbox_test`
Creates an XLSX from a Dropbox template, renames sheets based on `report_id`, writes fields/dropdowns, and uploads result.

Request JSON:

```json
{
  "report_id": "ASX-001",
  "template_path": "/path/in/dropbox/template.xlsx",
  "output_path": "/path/in/dropbox/output.xlsx"
}
```

### Test endpoints
- `POST /asx_create_xlsx_test`
- `POST /asx_create_xlsx_rename_test`

## Checks Run

Basic verification commands used during repository check:

```bash
python -m compileall main.py stats_runtime.py
python -m py_compile main.py stats_runtime.py
```

## Notes

- Stats fallback behavior: if stats read fails, the API returns a default payload instead of erroring.
- Manitoba flow uses direct PDF URL pattern: `https://www.gov.mb.ca/data/em/application/assessment/{ar_number}.pdf`.
