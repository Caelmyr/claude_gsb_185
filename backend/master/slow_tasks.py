"""Slow-task analysis: per-job outliers and a cross-job node view.

The monitor page shows raw per-task durations; this module answers the
follow-up questions — *which* tasks were abnormally slow, and on which nodes.

Design notes:

* **Grouping, not a global bar.**  A task is only ever compared against
  finished tasks of the *same job and same stage* (map vs reduce), because
  normal durations differ wildly across job sizes and stages.  Within a
  group the baseline is the median and the spread is the MAD (median
  absolute deviation); a task is slow when its execution time exceeds
  ``max(median * factor, median + 3 * MAD, median + min_abs_ms)`` — a
  relative bar that scales with the job, a robustness bar that absorbs
  naturally skewed groups, and an absolute floor that keeps sub-second
  noise from being flagged on tiny jobs.

* **Wait vs execution.**  Queue wait (task became runnable -> actually
  started) is reported separately from execution (the worker-measured
  ``duration_ms``, i.e. the same value the monitor page shows), so a task
  that merely queued behind a busy cluster is never mislabelled as slow —
  it surfaces in the wait-outlier list instead.  For reduce tasks the
  "runnable" moment is the start of the reduce stage, not task creation,
  otherwise the whole map+shuffle phase would count as queueing.

* **Cross-job view.**  ``analyze_cluster`` normalises every finished task
  against its own group baseline (duration / median) and aggregates per
  worker, exposing nodes whose tasks are *consistently* slower than their
  peers no matter which job they ran.
"""

from __future__ import annotations

import statistics
from typing import Optional

from backend.common import constants as C
from backend.common.ids import partition_name
from backend.common.jsonutil import now_ms

MIN_GROUP_SIZE = 3         # finished tasks required for a credible baseline
MAD_K = 3.0                # robust-spread multiplier (Hampel-style)
DEFAULT_MIN_ABS_MS = 1000  # absolute floor: never flag sub-second noise
TOP_SLOW_LIMIT = 20        # cap on the cluster-wide slow-task list


def _float(value, default: float, lo: float, hi: float) -> float:
    try:
        v = float(value)
    except (TypeError, ValueError):
        return default
    return max(lo, min(hi, v))


def _baseline(values: list[float]) -> tuple[float, float]:
    """Median and MAD of a sample."""
    med = statistics.median(values)
    mad = statistics.median([abs(v - med) for v in values])
    return med, mad


def _threshold(median: float, mad: float, factor: float, min_abs_ms: float) -> float:
    return max(median * factor, median + MAD_K * mad, median + min_abs_ms)


class SlowTaskAnalyzer:
    def __init__(self, job_manager, registry, config) -> None:
        self.job_manager = job_manager
        self.registry = registry
        self.config = config

    # ------------------------------------------------------------------
    def _resolve(self, factor, min_abs_ms) -> tuple[float, float]:
        default_factor = _float(getattr(self.config, "speculation_threshold", 2.0), 2.0, 1.0, 100.0)
        factor = _float(factor, default_factor, 1.0, 100.0)
        min_abs = _float(min_abs_ms, DEFAULT_MIN_ABS_MS, 0.0, 3_600_000.0)
        return factor, min_abs

    def _criteria(self, factor: float, min_abs_ms: float) -> dict:
        return {
            "factor": factor,
            "min_abs_ms": int(min_abs_ms),
            "min_group_size": MIN_GROUP_SIZE,
            "mad_k": MAD_K,
        }

    def _runnable_ms(self, job, task) -> int:
        """When the task could first have been dispatched.

        Reduce tasks are created at submit time but can only run once the
        reduce stage starts, so their queue wait is measured from there.
        """
        if task.kind == C.TASK_REDUCE:
            stats = job.stats or {}
            return int(stats.get("reduce_started_ms")
                       or stats.get("shuffle_started_ms")
                       or task.created_ms)
        return task.created_ms

    def _entry(self, job, task, now: int, baseline_ms: float, threshold_ms: float) -> dict:
        runnable = self._runnable_ms(job, task)
        started = task.started_ms or 0
        assigned = task.assigned_ms or 0
        wait_ms = max(0, started - runnable) if started else 0
        schedule_wait_ms = max(0, (assigned or started) - runnable) if (assigned or started) else 0
        launch_wait_ms = max(0, started - assigned) if (started and assigned) else 0
        if task.status == C.TASK_SUCCEEDED:
            exec_ms = int(task.duration_ms or 0)
        elif task.status == C.TASK_RUNNING and started:
            exec_ms = max(0, now - started)
        else:
            exec_ms = 0
        worker = self.registry.get(task.worker_id) if task.worker_id else None
        shard = task.input_shard if task.kind == C.TASK_MAP else partition_name(task.partition)
        return {
            "task_id": task.task_id,
            "job_id": job.job_id,
            "kind": task.kind,
            "index": task.index,
            "shard": shard,
            "status": task.status,
            "worker_id": task.worker_id or "",
            "worker_name": worker.name if worker else "",
            "attempts": task.attempts,
            "records_processed": task.records_processed,
            # Same value the monitor page shows for this task.
            "duration_ms": int(task.duration_ms or 0),
            # Execution time used for the verdict: duration_ms once finished,
            # elapsed-so-far while still running.
            "exec_ms": exec_ms,
            "wait_ms": wait_ms,
            "schedule_wait_ms": schedule_wait_ms,
            "launch_wait_ms": launch_wait_ms,
            "baseline_ms": int(baseline_ms),
            "threshold_ms": int(threshold_ms),
            "ratio": round(exec_ms / baseline_ms, 2) if baseline_ms > 0 else None,
        }

    # ------------------------------------------------------------------
    def _analyze(self, job, factor: float, min_abs_ms: float, now: int) -> dict:
        tasks = self.job_manager.tasks_for(job.job_id)
        groups: dict = {}
        entries: list[dict] = []      # every finished task with a baseline
        slow: list[dict] = []
        wait_outliers: list[dict] = []
        for kind in (C.TASK_MAP, C.TASK_REDUCE):
            kind_tasks = [t for t in tasks if t.kind == kind]
            finished = [t for t in kind_tasks if t.status == C.TASK_SUCCEEDED]
            group = {"kind": kind, "tasks": len(kind_tasks), "finished": len(finished)}
            durations = [int(t.duration_ms or 0) for t in finished]
            if len(durations) < MIN_GROUP_SIZE:
                group["note"] = f"need >= {MIN_GROUP_SIZE} finished tasks for a baseline"
                groups[kind] = group
                continue

            med, mad = _baseline(durations)
            thr = _threshold(med, mad, factor, min_abs_ms)
            group["median_ms"] = int(med)
            group["mad_ms"] = int(mad)
            group["threshold_ms"] = int(thr)

            for t in finished:
                entry = self._entry(job, t, now, med, thr)
                entries.append(entry)
                if entry["duration_ms"] >= thr:
                    slow.append(entry)
            # Still-running tasks that have already blown past the threshold.
            for t in kind_tasks:
                if t.status == C.TASK_RUNNING and t.started_ms and now - t.started_ms >= thr:
                    slow.append(self._entry(job, t, now, med, thr))

            # Queue-wait outliers: long wait but (usually) normal execution.
            started = [t for t in kind_tasks if t.started_ms]
            waits = [max(0, t.started_ms - self._runnable_ms(job, t)) for t in started]
            if len(waits) >= MIN_GROUP_SIZE:
                wmed, wmad = _baseline(waits)
                wthr = _threshold(wmed, wmad, factor, min_abs_ms)
                group["wait_median_ms"] = int(wmed)
                group["wait_threshold_ms"] = int(wthr)
                for t, w in zip(started, waits):
                    if w >= wthr:
                        entry = self._entry(job, t, now, med, thr)
                        entry["wait_baseline_ms"] = int(wmed)
                        entry["wait_ratio"] = round(w / wmed, 2) if wmed > 0 else None
                        wait_outliers.append(entry)
            groups[kind] = group

        slow.sort(key=lambda e: e["exec_ms"], reverse=True)
        wait_outliers.sort(key=lambda e: e["wait_ms"], reverse=True)
        return {"groups": groups, "entries": entries,
                "slow": slow, "wait_outliers": wait_outliers}

    # ------------------------------------------------------------------
    def analyze_job(self, job_id: str, factor=None, min_abs_ms=None) -> Optional[dict]:
        job = self.job_manager.get_job(job_id)
        if job is None:
            return None
        factor, min_abs = self._resolve(factor, min_abs_ms)
        result = self._analyze(job, factor, min_abs, now_ms())
        return {
            "job_id": job.job_id,
            "job_name": job.name,
            "status": job.status,
            "criteria": self._criteria(factor, min_abs),
            "groups": result["groups"],
            "slow_tasks": result["slow"],
            "wait_outliers": result["wait_outliers"],
        }

    def analyze_cluster(self, factor=None, min_abs_ms=None) -> dict:
        """Cross-job view: which nodes are slow relative to their job's peers."""
        factor, min_abs = self._resolve(factor, min_abs_ms)
        now = now_ms()
        per_worker: dict[str, dict] = {}
        top: list[dict] = []
        jobs_analyzed = 0
        for job in self.job_manager.list_jobs():
            result = self._analyze(job, factor, min_abs, now)
            if not any("median_ms" in g for g in result["groups"].values()):
                continue  # no credible baseline anywhere in this job
            jobs_analyzed += 1
            slow_ids = {e["task_id"] for e in result["slow"]}
            for entry in result["entries"]:
                wid = entry["worker_id"] or "(unassigned)"
                w = per_worker.setdefault(wid, {"tasks": 0, "slow_tasks": 0, "ratios": [],
                                                "max_ratio": 0.0, "exec_ms": 0, "wait_ms": 0})
                w["tasks"] += 1
                w["exec_ms"] += entry["exec_ms"]
                w["wait_ms"] += entry["wait_ms"]
                if entry["ratio"] is not None:
                    w["ratios"].append(entry["ratio"])
                    w["max_ratio"] = max(w["max_ratio"], entry["ratio"])
                if entry["task_id"] in slow_ids:
                    w["slow_tasks"] += 1
            for entry in result["slow"]:
                enriched = dict(entry)
                enriched["job_name"] = job.name
                top.append(enriched)

        workers = []
        for wid, w in per_worker.items():
            worker = self.registry.get(wid)
            ratios = w.pop("ratios")
            workers.append({
                "worker_id": wid,
                "name": worker.name if worker else wid,
                "tasks": w["tasks"],
                "slow_tasks": w["slow_tasks"],
                "slow_rate": round(w["slow_tasks"] / w["tasks"], 3) if w["tasks"] else 0.0,
                "avg_ratio": round(sum(ratios) / len(ratios), 2) if ratios else None,
                "max_ratio": round(w["max_ratio"], 2) if ratios else None,
                "exec_ms": w["exec_ms"],
                "wait_ms": w["wait_ms"],
            })
        workers.sort(key=lambda w: (w["slow_tasks"], w["avg_ratio"] or 0.0), reverse=True)
        top.sort(key=lambda e: e["exec_ms"], reverse=True)
        return {
            "criteria": self._criteria(factor, min_abs),
            "jobs_analyzed": jobs_analyzed,
            "tasks_analyzed": sum(w["tasks"] for w in workers),
            "slow_total": sum(w["slow_tasks"] for w in workers),
            "workers": workers,
            "top_slow_tasks": top[:TOP_SLOW_LIMIT],
        }
