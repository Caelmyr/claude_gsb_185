"""Tests for slow-task analysis: grouped baselines, wait/exec split, node view."""

import shutil
import tempfile
import unittest

from backend.common import constants as C
from backend.common.config import ClusterConfig
from backend.common.jsonutil import now_ms
from backend.common.logbus import LogBus
from backend.common.storage import Storage
from backend.master.job_manager import JobManager
from backend.master.registry import WorkerRegistry
from backend.master.slow_tasks import MIN_GROUP_SIZE, SlowTaskAnalyzer


class TestSlowTasks(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.storage = Storage(self.tmp)
        self.config = ClusterConfig()
        self.logbus = LogBus(self.storage)
        self.jm = JobManager(self.storage, self.config, self.logbus)
        self.registry = WorkerRegistry(self.storage, self.config)
        self.analyzer = SlowTaskAnalyzer(self.jm, self.registry, self.config)
        self.registry.register({"worker_id": "w1", "name": "node-1", "port": 9001})
        self.registry.register({"worker_id": "w2", "name": "node-2", "port": 9002})

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    # -- helpers ---------------------------------------------------------
    def _submit(self, num_map=4, num_reduce=4):
        return self.jm.submit({
            "name": "t", "mapper": "wordcount_mapper", "reducer": "count_reducer",
            "num_map_tasks": num_map, "num_reduce_tasks": num_reduce,
            "input_rows": 100, "params": {},
        })

    def _finish(self, job, kind, index, duration_ms, wait_ms=100, worker="w1",
                runnable_ms=None):
        """Mark a task succeeded with a controlled wait/execution split."""
        task = self.jm.tasks_for(job.job_id, kind)[index]
        runnable = runnable_ms if runnable_ms is not None else task.created_ms
        started = runnable + wait_ms
        self.jm.update_task(
            job.job_id, task.task_id,
            status=C.TASK_SUCCEEDED, worker_id=worker,
            assigned_ms=runnable + wait_ms // 2,
            started_ms=started, finished_ms=started + duration_ms,
            duration_ms=duration_ms,
        )

    def _finish_all(self, job, kind, durations, worker="w1", wait_ms=100):
        for i, d in enumerate(durations):
            self._finish(job, kind, i, d, wait_ms=wait_ms, worker=worker)

    # -- per-job analysis -------------------------------------------------
    def test_slow_task_detected_against_group_median(self):
        job = self._submit()
        self._finish_all(job, "map", [1000, 1000, 1000, 10000])
        result = self.analyzer.analyze_job(job.job_id)
        slow = result["slow_tasks"]
        self.assertEqual([s["task_id"] for s in slow], ["m-0003"])
        entry = slow[0]
        self.assertEqual(entry["shard"], "in-0003")
        self.assertEqual(entry["worker_name"], "node-1")
        self.assertEqual(entry["baseline_ms"], 1000)
        self.assertEqual(entry["ratio"], 10.0)
        self.assertEqual(result["groups"]["map"]["median_ms"], 1000)

    def test_duration_matches_monitor_page_value(self):
        job = self._submit()
        self._finish_all(job, "map", [1000, 1000, 1000, 9000])
        result = self.analyzer.analyze_job(job.job_id)
        entry = result["slow_tasks"][0]
        task = self.jm.get_task(job.job_id, entry["task_id"])
        # The monitor page renders Task.duration_ms; the analysis must agree.
        self.assertEqual(entry["duration_ms"], task.duration_ms)
        self.assertEqual(entry["exec_ms"], task.duration_ms)

    def test_stage_baselines_are_independent(self):
        job = self._submit()
        # Reduce tasks legitimately take ~5s; a 4s map task is slow for *its*
        # stage even though it is faster than every reduce task.
        self._finish_all(job, "map", [1000, 1000, 1000, 4000])
        self._finish_all(job, "reduce", [5000, 5000, 5000, 5500])
        result = self.analyzer.analyze_job(job.job_id)
        slow_ids = [s["task_id"] for s in result["slow_tasks"]]
        self.assertEqual(slow_ids, ["m-0003"])
        self.assertEqual(result["groups"]["reduce"]["median_ms"], 5000)

    def test_min_group_size_required(self):
        job = self._submit()
        self._finish_all(job, "map", [1000, 1000])  # < MIN_GROUP_SIZE finished
        self.assertLess(2, MIN_GROUP_SIZE)
        result = self.analyzer.analyze_job(job.job_id)
        self.assertEqual(result["slow_tasks"], [])
        self.assertIn("note", result["groups"]["map"])

    def test_long_queue_wait_is_not_misjudged_as_slow(self):
        job = self._submit()
        self._finish_all(job, "map", [1000, 1000, 1000, 1000])
        # One task queued for 60s but executed in the same ~1s as its peers.
        self._finish(job, "map", 0, 1000, wait_ms=60000)
        result = self.analyzer.analyze_job(job.job_id)
        self.assertEqual(result["slow_tasks"], [])
        outliers = result["wait_outliers"]
        self.assertEqual([o["task_id"] for o in outliers], ["m-0000"])
        self.assertEqual(outliers[0]["wait_ms"], 60000)
        self.assertEqual(outliers[0]["exec_ms"], 1000)

    def test_reduce_wait_measured_from_reduce_stage_start(self):
        job = self._submit()
        reduce_started = now_ms()
        self.jm.apply_job(job.job_id, lambda j: j.stats.__setitem__(
            "reduce_started_ms", reduce_started))
        # Tasks were created at submit (long ago) but started right after the
        # reduce stage began: the map+shuffle phase must not count as waiting.
        for i in range(4):
            task = self.jm.tasks_for(job.job_id, "reduce")[i]
            self.jm.update_task(job.job_id, task.task_id,
                                created_ms=reduce_started - 3_600_000)
            self._finish(job, "reduce", i, 5000, wait_ms=100,
                         runnable_ms=reduce_started)
        result = self.analyzer.analyze_job(job.job_id)
        self.assertEqual(result["wait_outliers"], [])
        for entry in self.analyzer._analyze(
                self.jm.get_job(job.job_id), 2.0, 1000, now_ms())["entries"]:
            self.assertLessEqual(entry["wait_ms"], 200)

    def test_running_straggler_flagged(self):
        job = self._submit()
        self._finish_all(job, "map", [1000, 1000, 1000])
        straggler = self.jm.tasks_for(job.job_id, "map")[3]
        self.jm.update_task(job.job_id, straggler.task_id,
                            status=C.TASK_RUNNING, worker_id="w2",
                            started_ms=now_ms() - 9000)
        result = self.analyzer.analyze_job(job.job_id)
        slow = result["slow_tasks"]
        self.assertEqual(len(slow), 1)
        self.assertEqual(slow[0]["task_id"], "m-0003")
        self.assertEqual(slow[0]["status"], C.TASK_RUNNING)
        self.assertGreaterEqual(slow[0]["exec_ms"], 8900)

    def test_factor_and_floor_are_tunable(self):
        job = self._submit()
        self._finish_all(job, "map", [1000, 1000, 1000, 1600])
        # Default criteria: 1600ms is below the 2x median and the 1s floor.
        self.assertEqual(self.analyzer.analyze_job(job.job_id)["slow_tasks"], [])
        # A stricter factor with no absolute floor flags it.
        result = self.analyzer.analyze_job(job.job_id, factor=1.5, min_abs_ms=0)
        self.assertEqual([s["task_id"] for s in result["slow_tasks"]], ["m-0003"])

    # -- cluster-wide view -------------------------------------------------
    def test_cluster_view_ranks_node_with_slow_tasks_first(self):
        job1 = self._submit()
        self._finish_all(job1, "map", [1000, 1000, 1000], worker="w1")
        self._finish(job1, "map", 3, 10000, worker="w2")
        job2 = self._submit()
        self._finish_all(job2, "map", [1000, 1000, 1000], worker="w1")
        self._finish(job2, "map", 3, 12000, worker="w2")

        result = self.analyzer.analyze_cluster()
        self.assertEqual(result["jobs_analyzed"], 2)
        self.assertEqual(result["slow_total"], 2)
        top = result["workers"][0]
        self.assertEqual(top["worker_id"], "w2")
        self.assertEqual(top["name"], "node-2")
        self.assertEqual(top["slow_tasks"], 2)
        self.assertEqual(top["slow_rate"], 1.0)
        other = result["workers"][1]
        self.assertEqual(other["worker_id"], "w1")
        self.assertEqual(other["slow_tasks"], 0)
        # Cross-job list carries job context and is sorted by execution time.
        self.assertEqual(len(result["top_slow_tasks"]), 2)
        self.assertEqual(result["top_slow_tasks"][0]["exec_ms"], 12000)
        self.assertTrue(all("job_name" in t for t in result["top_slow_tasks"]))

    def test_cluster_view_ignores_jobs_without_baseline(self):
        job = self._submit()
        self._finish_all(job, "map", [1000, 5000])  # too few for a baseline
        result = self.analyzer.analyze_cluster()
        self.assertEqual(result["jobs_analyzed"], 0)
        self.assertEqual(result["workers"], [])


if __name__ == "__main__":
    unittest.main()
