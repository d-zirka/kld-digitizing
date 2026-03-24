import json
import os
import threading
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, Dict, Optional

PROVINCES = ["Quebec", "Ontario", "Manitoba", "New Brunswick", "Nunavut"]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _province_template() -> Dict[str, Any]:
    return {
        "reports_downloaded": 0,
        "templates_copied": 0,
        "requests": 0,
        "failed_requests": 0,
        "last_event_at": None,
    }


def default_state() -> Dict[str, Any]:
    now = utc_now_iso()
    return {
        "schema_version": 1,
        "tracking_started_at": now,
        "updated_at": now,
        "totals": {
            "reports_downloaded": 0,
            "templates_copied": 0,
            "requests": 0,
            "failed_requests": 0,
        },
        "by_province": {p: _province_template() for p in PROVINCES},
    }


def normalize_state(raw: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    state = default_state()
    if not isinstance(raw, dict):
        return state

    state["schema_version"] = int(raw.get("schema_version", 1) or 1)
    state["tracking_started_at"] = str(raw.get("tracking_started_at") or state["tracking_started_at"])
    state["updated_at"] = str(raw.get("updated_at") or state["updated_at"])

    raw_totals = raw.get("totals") if isinstance(raw.get("totals"), dict) else {}
    for k in ("reports_downloaded", "templates_copied", "requests", "failed_requests"):
        try:
            state["totals"][k] = int(raw_totals.get(k, 0) or 0)
        except Exception:
            state["totals"][k] = 0

    raw_by_province = raw.get("by_province") if isinstance(raw.get("by_province"), dict) else {}
    for p in PROVINCES:
        incoming = raw_by_province.get(p) if isinstance(raw_by_province.get(p), dict) else {}
        row = _province_template()
        for k in ("reports_downloaded", "templates_copied", "requests", "failed_requests"):
            try:
                row[k] = int(incoming.get(k, 0) or 0)
            except Exception:
                row[k] = 0
        row["last_event_at"] = incoming.get("last_event_at") if incoming.get("last_event_at") else None
        state["by_province"][p] = row

    return state


class StatsStore:
    def __init__(
        self,
        backend: str = "dropbox",
        dropbox_path: str = "/KENORLAND_DIGITIZING/ASSESSMENT_REPORTS/_Documents/Stats/ar_stats_v1.json",
        local_path: str = "ar_stats_v1.local.json",
        token_provider=None,
        logger=None,
    ):
        self.backend = (backend or "dropbox").strip().lower()
        self.dropbox_path = dropbox_path
        self.local_path = local_path
        self.token_provider = token_provider
        self._logger = logger
        self._lock = threading.Lock()

    def _log(self, level: str, msg: str) -> None:
        if self._logger and hasattr(self._logger, level):
            getattr(self._logger, level)(msg)

    def _read_local(self) -> Dict[str, Any]:
        if not os.path.exists(self.local_path):
            return default_state()
        with open(self.local_path, "r", encoding="utf-8") as f:
            return normalize_state(json.load(f))

    def _write_local(self, state: Dict[str, Any]) -> None:
        os.makedirs(os.path.dirname(self.local_path) or ".", exist_ok=True)
        with open(self.local_path, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)

    def _read_dropbox(self, dbx) -> tuple[Dict[str, Any], Optional[str]]:
        from dropbox.exceptions import ApiError

        try:
            md, res = dbx.files_download(self.dropbox_path)
            payload = json.loads(res.content.decode("utf-8"))
            return normalize_state(payload), getattr(md, "rev", None)
        except ApiError:
            return default_state(), None

    def _write_dropbox(self, dbx, state: Dict[str, Any], rev: Optional[str]) -> None:
        from dropbox.files import WriteMode

        payload = json.dumps(state, ensure_ascii=False, indent=2).encode("utf-8")
        mode = WriteMode.update(rev) if rev else WriteMode.overwrite
        dbx.files_upload(payload, self.dropbox_path, mode=mode)

    def _build_dbx(self):
        import dropbox

        token = ""
        if callable(self.token_provider):
            token = str(self.token_provider() or "").strip()
        if not token:
            token = os.getenv("DROPBOX_ACCESS_TOKEN", "").strip()
        if not token:
            raise RuntimeError("DROPBOX_ACCESS_TOKEN not set")
        return dropbox.Dropbox(token)

    def _with_dropbox_mutation(self, mutate_fn):
        dbx = self._build_dbx()
        for _ in range(3):
            state, rev = self._read_dropbox(dbx)
            updated = mutate_fn(deepcopy(state))
            updated["updated_at"] = utc_now_iso()
            try:
                self._write_dropbox(dbx, updated, rev)
                return updated
            except Exception:
                continue
        raise RuntimeError("Unable to update Dropbox stats after retries")

    def load(self) -> Dict[str, Any]:
        with self._lock:
            if self.backend == "file":
                return self._read_local()
            if self.backend == "dropbox":
                dbx = self._build_dbx()
                state, _ = self._read_dropbox(dbx)
                return state
            return self._read_local()

    def apply_download_event(
        self,
        province: str,
        downloaded_pdfs: int,
        templates_copied: int,
        success: bool,
    ) -> Dict[str, Any]:
        province = province if province in PROVINCES else "Quebec"
        downloaded_pdfs = max(0, int(downloaded_pdfs or 0))
        templates_copied = max(0, int(templates_copied or 0))

        def mutate(state: Dict[str, Any]) -> Dict[str, Any]:
            now = utc_now_iso()
            row = state["by_province"][province]

            row["requests"] += 1
            state["totals"]["requests"] += 1

            if not success:
                row["failed_requests"] += 1
                state["totals"]["failed_requests"] += 1

            row["reports_downloaded"] += downloaded_pdfs
            state["totals"]["reports_downloaded"] += downloaded_pdfs

            row["templates_copied"] += templates_copied
            state["totals"]["templates_copied"] += templates_copied

            row["last_event_at"] = now
            return state

        with self._lock:
            if self.backend == "file":
                state = self._read_local()
                out = mutate(state)
                out["updated_at"] = utc_now_iso()
                self._write_local(out)
                return out

            if self.backend == "dropbox":
                return self._with_dropbox_mutation(mutate)

            state = self._read_local()
            out = mutate(state)
            out["updated_at"] = utc_now_iso()
            self._write_local(out)
            return out

    @staticmethod
    def to_api_payload(state: Dict[str, Any]) -> Dict[str, Any]:
        norm = normalize_state(state)
        labels = PROVINCES[:]
        pdf_values = [norm["by_province"][p]["reports_downloaded"] for p in labels]
        template_values = [norm["by_province"][p]["templates_copied"] for p in labels]

        return {
            "labels": labels,
            "values": pdf_values,
            "templates_values": template_values,
            "tracking_started_at": norm["tracking_started_at"],
            "updated_at": norm["updated_at"],
            "totals": norm["totals"],
            "by_province": norm["by_province"],
        }
