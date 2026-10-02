"""Tests for slow-task analysis (adaptive baselines, wait/exec split, nodes)."""

import tempfile
import unittest

from backend.common import constants as C
from backend.common.config import ClusterConfig
from backend.common.logbus import LogBus
from backend.common.models import WorkerRecord, new_job, new_task
from backend.common.storage import Storage
from backend.master.job_manager import JobManager
from backend.master.slow_tasks import (
    SlowTaskAnalyzer,
    exec_ms,
    total_ms,
    wait_ms,
)


class _FakeRegistry:
    def __init__(self, names=None):
        self.names = names or {}

    def get(self, wid):
        if not wid:
            return None
        return WorkerRecord(worker_id=wid, name=self.names.get(wid, "node-" + wid),
                            host="10.0.0." + wid.lstrip("w"), port=9000,
                            status=C.WORKER_ALIVE)


class SlowTaskTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.storage = Storage(self.tmp)
        self.config = ClusterConfig()
        self.logbus = LogBus(self.storage)
        self.jm = JobManager(self.storage, self.config, self.logbus)
        self.registry = _FakeRegistry()
        self.analyzer = SlowTaskAnalyzer(self.jm, self.registry, self.config)

    def add_job(self, name="j", maps=8, reduces=4):
        job = new_job(name, "wordcount_mapper", "count_reducer", maps, reduces, 12000)
        job.status = C.JOB_MAP
        self.jm._jobs[job.job_id] = job
        self.jm._tasks[job.job_id] = {}
        return job

    def finish(self, job, kind, i, duration_ms, records, wait_ms=100,
               worker="w1", attempt_loops=None):
        t = new_task(job, kind, i)
        t.status = C.TASK_SUCCEEDED
        t.worker_id = worker
        base = job.created_ms
        t.created_ms = base
        t.assigned_ms = base + wait_ms
        t.stats = {"first_assigned_ms": base + wait_ms}
        t.started_ms = base + wait_ms
        t.finished_ms = base + wait_ms + duration_ms
        t.duration_ms = duration_ms
        t.records_processed = records
        self.jm._tasks[job.job_id][t.task_id] = t
        return t

    def flags_of(self, report, task_id):
        for r in report["tasks"]:
            if r["task_id"] == task_id:
                return r["slow_flags"]
        return []


class TestWaitExecSplit(SlowTaskTestBase):
    def test_wait_and_exec_are_measured_independently(self):
        job = self.add_job()
        for i in range(7):
            self.finish(job, C.TASK_MAP, i, 2000, 1500, wait_ms=100)
        # Long in the queue but perfectly normal execution once dispatched.
        self.finish(job, C.TASK_MAP, 7, 2000, 1500, wait_ms=9000, worker="w2")
        rep = self.analyzer.analyze_job(job)
        row = next(r for r in rep["tasks"] if r["task_id"] == "m-0007")
        self.assertIn("wait_slow", row["slow_flags"])
        self.assertNotIn("exec_slow", row["slow_flags"])
        self.assertNotIn("data_skew", row["slow_flags"])
        self.assertEqual(row["wait_ms"], 9000)
        self.assertEqual(row["exec_ms"], 2000)

    def test_total_decomposes_into_wait_exec_retry(self):
        job = self.add_job()
        for i in range(7):
            self.finish(job, C.TASK_MAP, i, 2000, 1500)
        t = self.finish(job, C.TASK_MAP, 7, 2000, 1500)
        t.stats = {"first_assigned_ms": t.created_ms + 5_000}
        t.assigned_ms = t.created_ms + 8_000
        t.finished_ms = t.created_ms + 5_000 + 2_000 + 1_500
        row = self.analyzer._task_row(job, t, "wait_slow", ["wait_slow"], {}, None)
        self.assertEqual(row["wait_ms"], 5_000)
        self.assertEqual(row["exec_ms"], 2_000)
        self.assertEqual(row["retry_ms"], 1_500)
        self.assertEqual(row["total_ms"], 8_500)


class TestAdaptiveBaseline(SlowTaskTestBase):
    def test_scale_adapts_to_job_size(self):
        # A big job whose "normal" tasks are 20s: a 8s task is slow on the small
        # job but perfectly normal here — proving no one-size-fits-all cutoff.
        small = self.add_job("small")
        for i in range(7):
            self.finish(small, C.TASK_MAP, i, 1000, 800)
        slow_small = self.finish(small, C.TASK_MAP, 7, 4000, 800)
        rep_small = self.analyzer.analyze_job(small)
        self.assertIn("exec_slow", self.flags_of(rep_small, slow_small.task_id))

        big = self.add_job("big")
        for i in range(7):
            self.finish(big, C.TASK_MAP, i, 20_000, 16_000)
        big_task = self.finish(big, C.TASK_MAP, 7, 8_000, 6_400)
        rep_big = self.analyzer.analyze_job(big)
        # 8s is below the 20s median -> not slow, regardless of absolute seconds.
        self.assertNotIn("exec_slow", self.flags_of(rep_big, big_task.task_id))

    def test_map_and_reduce_have_separate_baselines(self):
        job = self.add_job()
        # maps are quick, reduces are normally slow (different stage cost)
        for i in range(8):
            self.finish(job, C.TASK_MAP, i, 1000, 1500)
        for p in range(4):
            self.finish(job, C.TASK_REDUCE, p, 6000, 3000)
        rep = self.analyzer.analyze_job(job)
        self.assertIn("map", rep["baselines"])
        self.assertIn("reduce", rep["baselines"])
        # A 6s reduce is normal even though it is 6x the map median.
        flagged = {r["task_id"] for r in rep["tasks"]}
        self.assertTrue(not any(tid.startswith("r-") for tid in flagged))

    def test_absolute_floor_filters_tiny_task_noise(self):
        job = self.add_job()
        for i in range(7):
            self.finish(job, C.TASK_MAP, i, 50, 40)
        # 4x the median wall time, but still only 200ms — below the 1s floor.
        self.finish(job, C.TASK_MAP, 7, 200, 40)
        rep = self.analyzer.analyze_job(job)
        self.assertEqual(rep["counts"]["exec_slow"], 0)


class TestSkewVsSlow(SlowTaskTestBase):
    def test_long_running_with_more_records_is_skew_not_slow(self):
        job = self.add_job()
        for i in range(7):
            self.finish(job, C.TASK_MAP, i, 2000, 1500)
        # 4x wall time but 4x records at the same per-record cost => skew.
        self.finish(job, C.TASK_MAP, 7, 8000, 6000)
        rep = self.analyzer.analyze_job(job)
        flags = self.flags_of(rep, "m-0007")
        self.assertIn("data_skew", flags)
        self.assertNotIn("exec_slow", flags)

    def test_long_running_same_work_is_exec_slow(self):
        job = self.add_job()
        for i in range(7):
            self.finish(job, C.TASK_MAP, i, 2000, 1500)
        self.finish(job, C.TASK_MAP, 7, 8000, 1500, worker="w3")
        rep = self.analyzer.analyze_job(job)
        self.assertIn("exec_slow", self.flags_of(rep, "m-0007"))


class TestInsufficientPeers(SlowTaskTestBase):
    def test_tiny_group_is_not_judged(self):
        # 2 tasks < min_group(4): no baseline, no verdict even if wildly uneven.
        job = self.add_job("tiny", maps=2, reduces=2)
        self.finish(job, C.TASK_MAP, 0, 100, 10)
        self.finish(job, C.TASK_MAP, 1, 20_000, 10)
        rep = self.analyzer.analyze_job(job)
        self.assertEqual(rep["counts"]["slow"], 0)
        self.assertNotIn("map", rep["baselines"])


class TestNodeAggregation(SlowTaskTestBase):
    def test_generally_slow_node_needs_enough_tasks_and_both_indices(self):
        job = self.add_job()
        for i in range(8):
            self.finish(job, C.TASK_MAP, i, 2000, 1500,
                        worker="wgood" if i < 5 else "wbad")
        # wbad has 3 tasks, 4x wall time and 4x per-record cost -> slow node.
        # Recreate the 3 bad ones with slow values.
        for i in range(5, 8):
            t = self.jm.get_task(job.job_id, f"m-{i:04d}")
            t.duration_ms = 8000
            t.worker_id = "wbad"
        rep = self.analyzer.analyze_cluster()
        nodes = {n["worker_id"]: n for n in rep["nodes"]}
        self.assertTrue(nodes["wbad"]["generally_slow"])
        self.assertFalse(nodes["wgood"]["generally_slow"])

    def test_single_outlier_does_not_make_a_slow_node(self):
        job = self.add_job()
        for i in range(7):
            self.finish(job, C.TASK_MAP, i, 2000, 1500, worker="w1")
        self.finish(job, C.TASK_MAP, 7, 8000, 6000, worker="w2")  # skew, 1 task
        rep = self.analyzer.analyze_cluster()
        nodes = {n["worker_id"]: n for n in rep["nodes"]}
        self.assertFalse(nodes["w2"]["generally_slow"])

    def test_skew_does_not_inflate_node_cost_index(self):
        job = self.add_job()
        for i in range(7):
            self.finish(job, C.TASK_MAP, i, 2000, 1500, worker="w1")
        # w2 does the big shards: high wall time but normal per-record cost.
        self.finish(job, C.TASK_MAP, 7, 8000, 6000, worker="w2")
        rep = self.analyzer.analyze_cluster()
        w2 = next(n for n in rep["nodes"] if n["worker_id"] == "w2")
        self.assertIsNotNone(w2["median_cost_index"])
        self.assertLess(w2["median_cost_index"], 1.5)


class TestTimingHelpers(SlowTaskTestBase):
    def test_wait_none_for_incoherent_timestamps(self):
        job = self.add_job()
        t = new_task(job, C.TASK_MAP, 0)
        t.created_ms = 0
        self.assertIsNone(wait_ms(t))

    def test_running_exec_is_elapsed_not_finished_duration(self):
        from backend.common.jsonutil import now_ms
        job = self.add_job()
        t = new_task(job, C.TASK_MAP, 0)
        t.status = C.TASK_RUNNING
        t.created_ms = now_ms() - 5000       # queued 2s, then running for 3s
        t.started_ms = now_ms() - 3000
        self.assertGreaterEqual(exec_ms(t), 3000)
        self.assertGreaterEqual(total_ms(t), 5000)


if __name__ == "__main__":
    unittest.main()
