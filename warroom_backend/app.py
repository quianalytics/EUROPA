from __future__ import annotations

import gzip
from typing import Any, Dict, Iterable

from flask import Flask, Response, abort, jsonify, request, send_from_directory
from werkzeug.utils import secure_filename

from warroom_backend.config import Settings
from warroom_backend.jobs.manager import JobManager
from warroom_backend.scheduler.manager import ScheduleManager
from warroom_backend.services.appwrite_store import AppwriteStore
from warroom_backend.services.storage import StorageService
from warroom_backend.utils import utc_now


def create_app(settings: Settings | None = None) -> Flask:
    settings = settings or Settings()
    app = Flask(__name__)
    latest_cap_file = "live_NFL_cap_tables.csv"
    frontend_numeric_fields = {
        "cap_year",
        "base_salary",
        "prorated_bonus",
        "roster_bonus",
        "signing_bonus",
        "dead_money",
        "cap_hit",
        "cap_number",
        "guaranteed_cash",
        "prorated_base",
    }

    appwrite_store = AppwriteStore(settings)
    storage = StorageService(settings.upload_dir, settings.live_data_dir)
    job_manager = JobManager(appwrite_store, storage, timeout=settings.scraper_timeout)
    schedule_manager = ScheduleManager(job_manager, timezone=settings.scheduler_timezone)
    if schedule_manager.enabled_status() and settings.auto_schedule_overthecap:
        try:
            schedule_manager.add_cron_schedule(
                name="daily-overthecap",
                scraper_payload={
                    "type": "overthecap_team_csv",
                    "include_player_details": True,
                    "enable_team_fallback": True,
                },
                cron={"hour": "4", "minute": "0"},
            )
        except Exception:
            pass

    @app.get("/health")
    def health() -> Dict[str, Any]:
        return {
            "ok": True,
            "appwrite": appwrite_store.status(),
            "jobs": len(job_manager.list_jobs()),
            "schedules": schedule_manager.job_count(),
            "scheduler_enabled": schedule_manager.enabled_status(),
            "timestamp": utc_now(),
        }

    @app.post("/api/scrape")
    def start_scrape():
        body = request.get_json(silent=True) or {}
        if body.get("type") != "overthecap_team_csv" and not body.get("url"):
            return jsonify({"error": "Missing required field 'url' for generic scraper"}), 400
        if body.get("type") == "generic" or body.get("type") is None:
            body.setdefault("type", "generic")
        body.setdefault("async", True)
        body.setdefault("user_agent", None)
        try:
            limit = body.get("limit")
            if limit is not None:
                body["limit"] = int(limit)
        except (TypeError, ValueError):
            return jsonify({"error": "limit must be a positive integer"}), 400

        job_id = job_manager.enqueue(body, async_mode=body.get("async", True))
        return jsonify({"job_id": job_id, "status": job_manager.get_job(job_id)["status"]}), 202

    @app.post("/api/scrape/overthecap/teams")
    def start_overthecap_team_scrape():
        body = request.get_json(silent=True) or {}
        try:
            max_pages = body.get("max_pages")
            if max_pages is not None:
                body["max_pages"] = int(max_pages)
        except (TypeError, ValueError):
            return jsonify({"error": "max_pages must be a positive integer"}), 400

        body.setdefault("type", "overthecap_team_csv")
        body.setdefault("seed_url", "https://overthecap.com/")
        body.setdefault("include_player_details", True)
        body.setdefault("async", True)
        body.setdefault("user_agent", None)

        job_id = job_manager.enqueue(body, async_mode=body.get("async", True))
        return jsonify({"job_id": job_id, "status": job_manager.get_job(job_id)["status"], "artifact_type": "csv"}), 202

    @app.get("/api/scrape/jobs")
    def list_jobs():
        return jsonify({"jobs": job_manager.list_jobs()})

    @app.get("/api/scrape/jobs/<job_id>")
    def get_job(job_id: str):
        job = job_manager.get_job(job_id)
        if not job:
            return jsonify({"error": "job not found"}), 404
        return jsonify(job)

    @app.get("/api/scrape/overthecap/teams/<job_id>/download")
    def download_overthecap_job_csv(job_id: str):
        job = job_manager.get_job(job_id)
        if not job:
            return jsonify({"error": "job not found"}), 404
        if job.get("status") != "completed":
            return jsonify({"error": "job not completed", "status": job.get("status")}), 409
        if job.get("artifact_dir") != "live_data":
            return jsonify({"error": "requested job does not use live_data artifact"}), 400

        artifact = job.get("artifact")
        if not artifact:
            return jsonify({"error": "artifact unavailable"}), 404

        safe_artifact = secure_filename(artifact)
        if not storage.exists(safe_artifact, artifact_dir="live_data"):
            return jsonify({"error": "artifact file not found"}), 404

        gzipped = gzip.compress(storage.read_binary(safe_artifact, artifact_dir="live_data"))
        return Response(
            gzipped,
            headers={
                "Content-Type": "application/gzip",
                "Content-Disposition": f'attachment; filename="{safe_artifact}.gz"',
                "Content-Encoding": "gzip",
                "Content-Length": str(len(gzipped)),
            },
        )

    @app.get("/api/salary-cap/latest")
    def get_latest_salary_cap_data():
        if not storage.exists(latest_cap_file, artifact_dir="live_data"):
            return jsonify({"error": f"artifact '{latest_cap_file}' not found"}), 404

        path = storage.get_path(latest_cap_file, artifact_dir="live_data")
        rows = storage.read_csv(latest_cap_file, artifact_dir="live_data", deserialize_json=True)

        parsed_rows = []
        for row in rows:
            parsed_rows.append(_coerce_frontend_salary_row(row, frontend_numeric_fields))

        return jsonify(
            {
                "schema": "salary_cap_player_v1",
                "schema_version": "1.0",
                "artifact": latest_cap_file,
                "updated_at": str(path.stat().st_mtime),
                "row_count": len(parsed_rows),
                "rows": parsed_rows,
            }
        )

    @app.get("/api/salary-cap/latest/csv")
    def get_latest_salary_cap_csv():
        if not storage.exists(latest_cap_file, artifact_dir="live_data"):
            return jsonify({"error": f"artifact '{latest_cap_file}' not found"}), 404
        gzipped = gzip.compress(storage.read_binary(latest_cap_file, artifact_dir="live_data"))
        return Response(
            gzipped,
            headers={
                "Content-Type": "application/gzip",
                "Content-Disposition": f'attachment; filename="{latest_cap_file}.gz"',
                "Content-Encoding": "gzip",
                "Content-Length": str(len(gzipped)),
            },
        )

    def _coerce_frontend_salary_row(row: Dict[str, Any], numeric_fields: Iterable[str]) -> Dict[str, Any]:
        normalized: Dict[str, Any] = {}
        for key, value in row.items():
            if key in numeric_fields:
                normalized[key] = _coerce_float(value)
            elif key == "raw_fields" and isinstance(value, (dict, list)):
                normalized[key] = value
            else:
                normalized[key] = value
        return normalized

    @app.get("/files")
    def list_files():
        files = []
        for file_path in sorted(settings.upload_dir.iterdir()):
            if not file_path.is_file():
                continue
            stat = file_path.stat()
            files.append(
                {
                    "name": file_path.name,
                    "size": stat.st_size,
                    "modified_at": str(stat.st_mtime),
                }
            )
        return jsonify({"files": files})

    @app.get("/live-data")
    def list_live_data():
        files = []
        for file_path in sorted(settings.live_data_dir.iterdir()):
            if not file_path.is_file():
                continue
            stat = file_path.stat()
            files.append(
                {
                    "name": file_path.name,
                    "size": stat.st_size,
                    "modified_at": str(stat.st_mtime),
                }
            )
        return jsonify({"files": files})

    @app.post("/files")
    def upload_file():
        uploaded = request.files.get("file")
        if not uploaded or not uploaded.filename:
            return jsonify({"error": "No file uploaded"}), 400

        filename = secure_filename(uploaded.filename)
        if not filename:
            return jsonify({"error": "Invalid file name"}), 400

        destination = settings.upload_dir / filename
        uploaded.save(destination)
        return jsonify({"name": filename, "size": destination.stat().st_size}), 201

    @app.get("/files/<path:filename>")
    def get_file(filename: str):
        safe_name = secure_filename(filename)
        target = settings.upload_dir / safe_name
        if not target.exists() or not target.is_file():
            abort(404)
        return send_from_directory(str(settings.upload_dir), safe_name, as_attachment=True)

    @app.post("/api/schedules")
    def create_schedule():
        body = request.get_json(silent=True) or {}
        if not schedule_manager.enabled_status():
            return jsonify({"error": "Scheduler unavailable. Install APScheduler."}), 503
        schedule_id = body.get("name") or ""
        trigger = body.get("trigger", "cron")
        scraper_payload = body.get("scraper_payload") or {}
        if not isinstance(scraper_payload, dict):
            return jsonify({"error": "scraper_payload must be an object"}), 400

        if trigger == "cron":
            cron = body.get("cron") or {}
            if not isinstance(cron, dict):
                return jsonify({"error": "cron must be an object"}), 400
            created_id = schedule_manager.add_cron_schedule(
                name=schedule_id,
                scraper_payload=scraper_payload,
                cron=cron,
            )
        elif trigger == "interval":
            interval = body.get("interval") or {}
            if not isinstance(interval, dict):
                return jsonify({"error": "interval must be an object"}), 400
            if not interval:
                return jsonify({"error": "interval config required"}), 400
            created_id = schedule_manager.add_interval_schedule(
                name=schedule_id,
                scraper_payload=scraper_payload,
                interval=interval,
            )
        else:
            return jsonify({"error": "trigger must be cron or interval"}), 400

        return jsonify({"id": created_id}), 201

    @app.get("/api/schedules")
    def list_schedules():
        if not schedule_manager.enabled_status():
            return jsonify({"error": "Scheduler unavailable. Install APScheduler.", "schedules": []}), 503
        return jsonify({"schedules": schedule_manager.list_schedules()})

    @app.delete("/api/schedules/<schedule_id>")
    def delete_schedule(schedule_id: str):
        if not schedule_manager.enabled_status():
            return jsonify({"error": "Scheduler unavailable. Install APScheduler."}), 503
        removed = schedule_manager.remove_schedule(schedule_id)
        if not removed:
            return jsonify({"error": "schedule not found"}), 404
        return jsonify({"status": "removed", "id": schedule_id}), 200

    return app


def _coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)

    if isinstance(value, str):
        text = value.strip()
    else:
        text = str(value).strip()

    if not text:
        return None

    lowered = text.lower()
    if lowered in {"n/a", "na", "none", "-", "--", "—", "null"}:
        return None

    cleaned = text.replace(",", "").replace("$", "").strip()
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


app = create_app()

if __name__ == "__main__":  # pragma: no cover
    settings = Settings()
    app.run(host=settings.host, port=settings.port, debug=settings.debug)
