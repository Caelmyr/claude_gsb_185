"""Slow-task analysis: find tasks whose *execution* is genuinely slow.

Why a dedicated analyzer instead of a single duration cutoff
------------------------------------------------------------
A naive "anything over N seconds is slow" rule fails on every real cluster:

* jobs differ in size by orders of magnitude, and a map task and a reduce task
  have very different normal durations, so the baseline must be computed
  **per job and per stage (map / reduce)** from that peer group itself;
* a task may look late simply because it sat in the scheduler queue waiting for
  a free worker.  Queueing time and execution time are different problems
  (capacity vs. a slow node / skewed shard), so they are measured separately;
* a task processing a much bigger shard is *expected* to take longer — that is
  data skew, not a slow node.  We therefore compare both the wall-time ratio
  and the per-record cost ratio against the peer median.

The analysis is purely a read over the authoritative Task records persisted by
the JobManager, so every number it reports (execution time in particular) is
exactly the ``duration_ms`` shown for that task on the monitoring page.

Slow categories
---------------
``exec_slow``   execution wall time far above the same-stage peer median and
                above an absolute floor, without a proportional workload
                (per-record cost also elevated) -> the node/slot is slow.
``data_skew``   wall time far above the median but per-record cost is normal:
                the shard simply held more records (map) / more fetched pairs
                (reduce).  Reported separately so it is never mis-attributed.
``wait_slow``   queueing time (created -> first dispatched) far above the job's
                median queue wait: a scheduling/capacity symptom, not slow
                execution.
"""

from __future__ import annotations

import statistics
from typing import Optional

from backend.common import constants as C
from backend.common.jsonutil import now_ms
from backend.common.models import Job, Task, WorkerRecord

# Below this many records a task's per-record cost is too noisy to be trusted,
# so skew is decided on wall time alone for it.
MIN_RECORDS_FOR_COST = 10

CATEGORY_LABELS = {
    "exec_slow": "执行慢 Slow execution",
    "data_skew": "数据倾斜 Data skew",
    "wait_slow": "排队久 Long queue wait",
}


# ---------------------------------------------------------------------------
# Timing decomposition
# ---------------------------------------------------------------------------
def first_assigned_ms(task: Task) -> int:
    return int((task.stats or {}).get("first_assigned_ms") or task.assigned_ms or 0)


def wait_ms(task: Task) -> Optional[int]:
    """Scheduler queueing time: created -> first dispatch to a worker.

    Returns ``None`` when the timestamps are missing/incoherent (older records)
    so the caller never reports a fabricated wait value.
    """
    first = first_assigned_ms(task)
    if first and task.created_ms and first >= task.created_ms:
        return first - task.created_ms
    return None


def exec_ms(task: Task) -> int:
    """Actual execution wall time.

    Finished tasks: the worker-measured ``duration_ms`` (same value the
    monitoring page shows).  Running tasks: elapsed since their latest attempt
    actually started on the worker.
    """
    if task.status == C.TASK_SUCCEEDED and task.duration_ms > 0:
        return int(task.duration_ms)
    if task.status == C.TASK_RUNNING and task.started_ms:
        return max(0, now_ms() - task.started_ms)
    return 0


def retry_ms(task: Task) -> int:
    """Extra time lost to failure backoff / worker-death reassignment."""
    first = first_assigned_ms(task)
    if not (first and task.finished_ms and task.duration_ms):
        return 0
    return max(0, task.finished_ms - first - int(task.duration_ms))


def total_ms(task: Task) -> int:
    """Whole lifecycle: created -> finished (running: created -> now)."""
    end = task.finished_ms or now_ms()
    return max(0, end - task.created_ms) if (end and task.created_ms) else 0


# ---------------------------------------------------------------------------
# Descriptive helpers
# ---------------------------------------------------------------------------
def shard_label(task: Task) -> str:
    """The data partition this task worked on (e.g. ``in-0003`` / ``part-0001``)."""
    if task.kind == C.TASK_MAP:
        return task.input_shard or f"in-{task.index:04d}"
    return f"part-{task.partition:04d}"


def _median(values: list[float]) -> float:
    return float(statistics.median(values)) if values else 0.0


def _ms_per_record(task: Task) -> Optional[float]:
    records = max(0, int(task.records_processed or 0))
    if records < MIN_RECORDS_FOR_COST:
        return None
    return exec_ms(task) / records


# ---------------------------------------------------------------------------
# Peer-group baselines
# ---------------------------------------------------------------------------
def _group_baseline(tasks: list[Task], cfg) -> Optional[dict]:
    """Median execution / per-record cost / queue wait for one peer group."""
    finished = [t for t in tasks
                if t.status == C.TASK_SUCCEEDED and t.duration_ms > 0]
    if len(finished) < int(cfg.slow_task_min_group):
        return None
    durations = [float(t.duration_ms) for t in finished]
    costs = [
        float(t.duration_ms) / t.records_processed
        for t in finished
        if t.records_processed >= MIN_RECORDS_FOR_COST
    ]
    waits: list[float] = []
    for t in finished:
        w = wait_ms(t)
        if w is not None:
            waits.append(float(w))
    return {
        "n": len(finished),
        "median_exec_ms": round(_median(durations), 1),
        "median_ms_per_record": round(_median(costs), 4) if costs else None,
        "median_wait_ms": round(_median(waits), 1) if waits else None,
    }


class SlowTaskAnalyzer:
    """Computes slow-task reports for one job or across the whole cluster."""

    def __init__(self, job_manager, registry, config) -> None:
        self.job_manager = job_manager
        self.registry = registry
        self.config = config

    # ------------------------------------------------------------------
    def _worker_view(self, worker_id: Optional[str]) -> dict:
        worker = self.registry.get(worker_id) if worker_id else None
        return {
            "worker_id": worker_id or "",
            "worker_name": worker.name if worker else (worker_id or ""),
            "host": worker.host if worker else "",
            "port": worker.port if worker else 0,
        }

    def _classify_finished(self, task: Task, baseline: dict) -> tuple[str, list[str], dict]:
        """Return (primary category, all flags, reasons) for a succeeded task."""
        cfg = self.config
        d = exec_ms(task)
        flags: list[str] = []
        reasons: dict = {}

        # --- execution slowness (wall-time ratio + absolute floor) ---
        med_exec = baseline["median_exec_ms"]
        ratio = round(d / med_exec, 2) if med_exec else 0.0
        reasons["exec_ratio"] = ratio
        reasons["baseline_median_exec_ms"] = med_exec
        exec_far = (
            med_exec > 0
            and d >= float(cfg.slow_task_ratio) * med_exec
            and d >= float(cfg.slow_task_min_exec_ms)
        )

        # --- workload-normalised cost (distinguishes skew from a slow node) ---
        my_cost = _ms_per_record(task)
        med_cost = baseline.get("median_ms_per_record")
        if my_cost is not None and med_cost:
            cost_ratio = round(my_cost / med_cost, 2) if med_cost else 0.0
            reasons["cost_ratio"] = cost_ratio
            reasons["baseline_median_ms_per_record"] = med_cost
            reasons["ms_per_record"] = round(my_cost, 4)
            if exec_far:
                flags.append("exec_slow"
                             if cost_ratio >= float(cfg.slow_task_cost_ratio)
                             else "data_skew")
        elif exec_far:
            # Too few records to normalise (tiny/empty shard): wall time alone.
            flags.append("exec_slow")

        # --- queueing slowness: evaluated independently, so a task can be both
        # slow/skewed in execution AND long in the queue (reported separately). ---
        wait_flag, wait_reasons = self._wait_flag(task, baseline)
        if wait_flag:
            flags.append("wait_slow")
        reasons.update(wait_reasons)

        primary = self._primary(flags)
        return primary, flags, reasons

    def _wait_flag(self, task: Task, baseline: Optional[dict]) -> tuple[str, dict]:
        """Decide queueing slowness from the task's created -> first dispatch gap."""
        if not baseline:
            return "", {}
        med_wait = baseline.get("median_wait_ms")
        w = wait_ms(task)
        if w is None and task.status == C.TASK_PENDING:
            w = total_ms(task)  # never dispatched: waiting since creation
        reasons: dict = {}
        if w is None or not med_wait:
            return "", reasons
        reasons["wait_ratio"] = round(w / med_wait, 2) if med_wait else 0.0
        reasons["baseline_median_wait_ms"] = med_wait
        if (med_wait > 0
                and w >= float(self.config.slow_task_wait_ratio) * med_wait
                and w >= float(self.config.slow_task_min_wait_ms)):
            return "wait_slow", reasons
        return "", reasons

    @staticmethod
    def _primary(flags: list[str]) -> str:
        for cat in ("exec_slow", "data_skew", "wait_slow"):
            if cat in flags:
                return cat
        return ""

    def _classify_active(self, task: Task, baseline: Optional[dict]) -> tuple[str, list[str], dict]:
        """Running / pending tasks: flag what is abnormal *right now*."""
        cfg = self.config
        flags: list[str] = []
        reasons: dict = {}
        if baseline:
            reasons["baseline_median_exec_ms"] = baseline["median_exec_ms"]

        if task.status == C.TASK_RUNNING and task.started_ms:
            elapsed = exec_ms(task)
            if baseline:
                reasons["exec_ratio"] = round(elapsed / baseline["median_exec_ms"], 2) \
                    if baseline["median_exec_ms"] else 0.0
            if (baseline and baseline["median_exec_ms"] > 0
                    and elapsed >= float(cfg.slow_task_ratio) * baseline["median_exec_ms"]
                    and elapsed >= float(cfg.slow_task_min_exec_ms)):
                my_cost = _ms_per_record(task)
                med_cost = baseline.get("median_ms_per_record")
                if my_cost is not None and med_cost:
                    reasons["cost_ratio"] = round(my_cost / med_cost, 2)
                    reasons["ms_per_record"] = round(my_cost, 4)
                    flags.append("exec_slow"
                                 if my_cost / med_cost >= float(cfg.slow_task_cost_ratio)
                                 else "data_skew")
                else:
                    flags.append("exec_slow")
        wait_flag, wait_reasons = self._wait_flag(task, baseline)
        if wait_flag:
            flags.append("wait_slow")
        reasons.update(wait_reasons)
        return self._primary(flags), flags, reasons

    # ------------------------------------------------------------------
    def _task_row(self, job: Job, task: Task, category: str, flags: list[str],
                  reasons: dict, baseline: Optional[dict]) -> dict:
        row = {
            "job_id": job.job_id,
            "job_name": job.name,
            "task_id": task.task_id,
            "kind": task.kind,
            "index": task.index,
            "shard": shard_label(task),
            "status": task.status,
            "attempts": task.attempts,
            "records_processed": task.records_processed,
            "wait_ms": wait_ms(task),
            "exec_ms": exec_ms(task),
            "retry_ms": retry_ms(task),
            "total_ms": total_ms(task),
            "slow_category": category,
            "slow_flags": flags,
            "slow_label": CATEGORY_LABELS.get(category, ""),
            "reasons": reasons,
        }
        row.update(self._worker_view(task.worker_id))
        if baseline:
            row["baseline"] = {
                "n": baseline["n"],
                "median_exec_ms": baseline["median_exec_ms"],
                "median_ms_per_record": baseline.get("median_ms_per_record"),
                "median_wait_ms": baseline.get("median_wait_ms"),
            }
        return row

    # ------------------------------------------------------------------
    # Per-job report
    # ------------------------------------------------------------------
    def analyze_job(self, job: Job) -> dict:
        tasks = self.job_manager.tasks_for(job.job_id)
        by_kind: dict[str, list[Task]] = {C.TASK_MAP: [], C.TASK_REDUCE: []}
        for t in tasks:
            by_kind.setdefault(t.kind, []).append(t)

        baselines: dict[str, Optional[dict]] = {}
        rows: list[dict] = []
        for kind, group in by_kind.items():
            baseline = _group_baseline(group, self.config)
            baselines[kind] = baseline
            for task in group:
                if task.status == C.TASK_SUCCEEDED:
                    if baseline is None:
                        category, flags, reasons = "", [], {}
                    else:
                        category, flags, reasons = self._classify_finished(task, baseline)
                elif task.status in (C.TASK_RUNNING, C.TASK_PENDING,
                                     C.TASK_ASSIGNED, C.TASK_RETRYING):
                    category, flags, reasons = self._classify_active(task, baseline)
                else:  # permanently FAILED: covered by the fault-recovery page
                    category, flags, reasons = "", [], {}
                if category:
                    rows.append(self._task_row(job, task, category, flags, reasons, baseline))

        # Most suspicious first: exec slowness, then skew, then long waits.
        order = {"exec_slow": 0, "data_skew": 1, "wait_slow": 2}
        rows.sort(key=lambda r: (order.get(r["slow_category"], 9), -r["exec_ms"], -r["wait_ms"]))

        counts = {"exec_slow": 0, "data_skew": 0, "wait_slow": 0}
        flag_totals = {"exec_slow": 0, "data_skew": 0, "wait_slow": 0}
        for r in rows:
            counts[r["slow_category"]] = counts.get(r["slow_category"], 0) + 1
            for f in r["slow_flags"]:
                flag_totals[f] = flag_totals.get(f, 0) + 1

        return {
            "job_id": job.job_id,
            "job_name": job.name,
            "status": job.status,
            "criteria": self.criteria(),
            "baselines": {k: v for k, v in baselines.items() if v is not None},
            "counts": {
                "analyzed": sum(1 for t in tasks if t.status != C.TASK_FAILED),
                "slow": len(rows),
                **counts,
            },
            "flag_totals": flag_totals,
            "tasks": rows,
        }

    # ------------------------------------------------------------------
    # Cross-job / per-node report
    # ------------------------------------------------------------------
    def analyze_cluster(self, limit: int = 200) -> dict:
        """Baseline every succeeded task by (job, stage), then aggregate per node.

        Node comparison must not be fooled by "this node happened to run the
        big job", so alongside the raw execution median we aggregate the
        workload-normalised cost (ms per 1k records) relative to each task's
        peer baseline.
        """
        tasks_rows: list[dict] = []
        nodes: dict[str, dict] = {}

        def node_bucket(worker_id: str, name: str) -> dict:
            return nodes.setdefault(worker_id, {
                "worker_id": worker_id,
                "worker_name": name,
                "tasks": 0,
                "jobs": set(),
                "exec_ms": [],
                "cost_index": [],          # task ms/record / baseline ms/record
                "exec_index": [],          # task exec / baseline median exec
                "exec_slow": 0,
                "data_skew": 0,
                "wait_slow": 0,
            })

        for job in self.job_manager.list_jobs():
            report = self.analyze_job(job)
            flagged = {r["task_id"] + "|" + r["kind"]: r for r in report["tasks"]}
            for kind, baseline in report["baselines"].items():
                if not baseline:
                    continue
                for task in self.job_manager.tasks_for(job.job_id, kind):
                    if task.status != C.TASK_SUCCEEDED or task.duration_ms <= 0:
                        continue
                    cost = _ms_per_record(task)
                    med_cost = baseline.get("median_ms_per_record")
                    bucket = node_bucket(task.worker_id or "",
                                         self._worker_view(task.worker_id)["worker_name"])
                    bucket["tasks"] += 1
                    bucket["jobs"].add(job.job_id)
                    bucket["exec_ms"].append(int(task.duration_ms))
                    if baseline["median_exec_ms"]:
                        bucket["exec_index"].append(
                            task.duration_ms / baseline["median_exec_ms"])
                    if cost is not None and med_cost:
                        bucket["cost_index"].append(cost / med_cost)
                    hit = flagged.get(task.task_id + "|" + task.kind)
                    if hit:
                        for f in hit["slow_flags"]:
                            bucket[f] = bucket.get(f, 0) + 1
                        tasks_rows.append(hit)

        node_rows: list[dict] = []
        for wid, b in nodes.items():
            n = b["tasks"]
            med_exec = round(_median(b["exec_ms"]), 1)
            med_cost_idx = round(_median(b["cost_index"]), 2) if b["cost_index"] else None
            med_exec_idx = round(_median(b["exec_index"]), 2) if b["exec_index"] else None
            worker = self.registry.get(wid) if wid else None
            # "Generally slow": enough observations and *both* wall-time and
            # workload-normalised medians are well above the peer baselines.
            generally_slow = (
                n >= int(self.config.slow_node_min_tasks)
                and med_exec_idx is not None
                and med_exec_idx >= float(self.config.slow_node_ratio)
                and (med_cost_idx is None
                     or med_cost_idx >= float(self.config.slow_node_ratio))
            )
            node_rows.append({
                "worker_id": wid,
                "worker_name": b["worker_name"],
                "host": worker.host if worker else "",
                "status": worker.status if worker else "",
                "jobs": len(b["jobs"]),
                "tasks": n,
                "median_exec_ms": med_exec,
                "median_exec_index": med_exec_idx,
                "median_cost_index": med_cost_idx,
                "exec_slow": b["exec_slow"],
                "data_skew": b["data_skew"],
                "wait_slow": b["wait_slow"],
                "generally_slow": generally_slow,
            })

        node_rows.sort(key=lambda r: (not r["generally_slow"],
                                      -(r["median_cost_index"] or 0),
                                      -(r["median_exec_index"] or 0)))
        order = {"exec_slow": 0, "data_skew": 1, "wait_slow": 2}
        tasks_rows.sort(key=lambda r: (order.get(r["slow_category"], 9),
                                       -r["exec_ms"], -r["wait_ms"]))
        tasks_rows = tasks_rows[:limit]

        return {
            "criteria": self.criteria(),
            "jobs_analyzed": len(self.job_manager.list_jobs()),
            "counts": {
                "slow": len(tasks_rows),
                "exec_slow": sum("exec_slow" in r["slow_flags"] for r in tasks_rows),
                "data_skew": sum("data_skew" in r["slow_flags"] for r in tasks_rows),
                "wait_slow": sum("wait_slow" in r["slow_flags"] for r in tasks_rows),
                "slow_nodes": sum(1 for r in node_rows if r["generally_slow"]),
            },
            "nodes": node_rows,
            "tasks": tasks_rows,
        }

    # ------------------------------------------------------------------
    def criteria(self) -> dict:
        """The exact thresholds used, surfaced so the UI can explain itself."""
        cfg = self.config
        return {
            "grouping": "按作业+阶段(map/reduce)取同组已完成任务中位数 per (job, stage) median",
            "slow_task_ratio": float(cfg.slow_task_ratio),
            "slow_task_min_exec_ms": float(cfg.slow_task_min_exec_ms),
            "slow_task_cost_ratio": float(cfg.slow_task_cost_ratio),
            "slow_task_wait_ratio": float(cfg.slow_task_wait_ratio),
            "slow_task_min_wait_ms": float(cfg.slow_task_min_wait_ms),
            "slow_task_min_group": int(cfg.slow_task_min_group),
            "slow_node_min_tasks": int(cfg.slow_node_min_tasks),
            "slow_node_ratio": float(cfg.slow_node_ratio),
        }
