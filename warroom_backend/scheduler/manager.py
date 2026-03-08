from __future__ import annotations

import uuid
from typing import Any, Dict, List

from warroom_backend.jobs.manager import JobManager

try:
    from apscheduler.schedulers.background import BackgroundScheduler
    from apscheduler.triggers.cron import CronTrigger
    from apscheduler.triggers.interval import IntervalTrigger
except Exception:  # pragma: no cover - optional dependency
    BackgroundScheduler = None
    CronTrigger = None
    IntervalTrigger = None


class ScheduleManager:
    def __init__(self, job_manager: JobManager, timezone: str = "UTC") -> None:
        self.job_manager = job_manager
        self.enabled = BackgroundScheduler is not None
        self.error: str | None = None

        if not self.enabled:
            self.error = "APScheduler not installed; schedule endpoints are disabled."
            self.scheduler = None
            return

        self.scheduler = BackgroundScheduler(timezone=timezone)
        self.scheduler.start()

    def add_cron_schedule(self, name: str, scraper_payload: Dict[str, Any], cron: Dict[str, Any]) -> str:
        if not self.enabled:
            raise RuntimeError(self.error or "APScheduler unavailable")
        schedule_id = name or str(uuid.uuid4())
        trigger_kwargs = dict(cron)
        self.scheduler.add_job(
            self._trigger_job,
            CronTrigger(**trigger_kwargs),
            args=[scraper_payload],
            id=schedule_id,
            replace_existing=True,
        )
        return schedule_id

    def add_interval_schedule(self, name: str, scraper_payload: Dict[str, Any], interval: Dict[str, Any]) -> str:
        if not self.enabled:
            raise RuntimeError(self.error or "APScheduler unavailable")
        schedule_id = name or str(uuid.uuid4())
        trigger = IntervalTrigger(**interval)
        self.scheduler.add_job(
            self._trigger_job,
            trigger,
            args=[scraper_payload],
            id=schedule_id,
            replace_existing=True,
        )
        return schedule_id

    def _trigger_job(self, payload: Dict[str, Any]) -> None:
        self.job_manager.enqueue(dict(payload), async_mode=True)

    def list_schedules(self) -> List[Dict[str, Any]]:
        values = []
        for job in self.scheduler.get_jobs():
            values.append(
                {
                    "id": job.id,
                    "next_run_time": str(job.next_run_time) if job.next_run_time else None,
                    "trigger": str(job.trigger),
                }
            )
        return values

    def remove_schedule(self, name: str) -> bool:
        if not self.enabled:
            return False
        job = self.scheduler.get_job(name)
        if not job:
            return False
        self.scheduler.remove_job(name)
        return True

    def shutdown(self) -> None:
        if not self.enabled:
            return
        self.scheduler.shutdown(wait=False)

    def enabled_status(self) -> bool:
        return self.enabled

    def job_count(self) -> int:
        if not self.enabled or not self.scheduler:
            return 0
        return len(self.scheduler.get_jobs())
