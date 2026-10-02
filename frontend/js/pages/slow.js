/* 慢任务分析 Slow task analysis */
Components.init('slow');
const C = Components;

let currentJob = '';

function fmtRatio(r) { return r == null ? '—' : (Number(r).toFixed(1) + '×'); }

function taskCols(extra) {
  const cols = [
    { key: 'task_id', label: '任务 Task', render: r => `<span class="mono">${C.esc(r.task_id)}</span>` },
    { key: 'kind', label: '阶段 Stage', render: r => r.kind },
    { key: 'shard', label: '分片 Shard', render: r => `<span class="mono">${C.esc(r.shard)}</span>` },
    { key: 'worker_name', label: '节点 Node', render: r => C.esc(r.worker_name || r.worker_id || '-') },
    { key: 'status', label: '状态 Status', render: r => C.stateBadge(r.status, true) },
  ];
  return cols.concat(extra);
}

function slowTable(rows) {
  return C.table(taskCols([
    { key: 'wait_ms', label: '等待 Wait', render: r => C.fmtDur(r.wait_ms), num: true },
    { key: 'exec_ms', label: '执行 Exec', render: r => C.fmtDur(r.exec_ms), num: true },
    { key: 'ratio', label: '倍数 ×Med', render: r => fmtRatio(r.ratio), num: true },
    { key: 'threshold_ms', label: '阈值 Threshold', render: r => C.fmtDur(r.threshold_ms), num: true },
    { key: 'records_processed', label: '记录 Records', render: r => C.fmtNum(r.records_processed), num: true },
  ]), rows);
}

function waitTable(rows) {
  return C.table(taskCols([
    { key: 'wait_ms', label: '等待 Wait', render: r => C.fmtDur(r.wait_ms), num: true },
    { key: 'wait_ratio', label: '等待倍数 ×Med', render: r => fmtRatio(r.wait_ratio), num: true },
    { key: 'exec_ms', label: '执行 Exec', render: r => C.fmtDur(r.exec_ms), num: true },
  ]), rows);
}

function groupTable(groups) {
  const rows = ['map', 'reduce'].map(k => Object.assign({ stage: k }, (groups || {})[k] || {}));
  return C.table([
    { key: 'stage', label: '阶段 Stage', render: r => r.stage },
    { key: 'finished', label: '完成任务 Finished', render: r => `${r.finished || 0}/${r.tasks || 0}`, num: true },
    { key: 'median_ms', label: '执行中位数 Median', render: r => r.median_ms == null ? C.esc(r.note || '-') : C.fmtDur(r.median_ms), num: true },
    { key: 'mad_ms', label: 'MAD', render: r => r.mad_ms == null ? '-' : C.fmtDur(r.mad_ms), num: true },
    { key: 'threshold_ms', label: '慢阈值 Threshold', render: r => r.threshold_ms == null ? '-' : C.fmtDur(r.threshold_ms), num: true },
    { key: 'wait_median_ms', label: '等待中位数 WaitMed', render: r => r.wait_median_ms == null ? '-' : C.fmtDur(r.wait_median_ms), num: true },
    { key: 'wait_threshold_ms', label: '等待阈值 WaitThr', render: r => r.wait_threshold_ms == null ? '-' : C.fmtDur(r.wait_threshold_ms), num: true },
  ], rows);
}

async function renderJob() {
  if (!currentJob) return;
  let data;
  try { data = await API.get('/api/jobs/' + currentJob + '/slow-tasks'); } catch (e) { return; }
  const crit = data.criteria || {};
  const slow = data.slow_tasks || [];
  const waits = data.wait_outliers || [];
  document.getElementById('job-summary').innerHTML = `
    <div class="stat-tiles">
      <div class="stat"><div class="label">慢任务 Slow tasks</div><div class="value ${slow.length ? 'bad' : 'good'}">${slow.length}</div></div>
      <div class="stat"><div class="label">等待异常 Wait outliers</div><div class="value ${waits.length ? 'bad' : 'good'}">${waits.length}</div></div>
      <div class="stat"><div class="label">判定标准 Criteria</div>
        <div class="value" style="font-size:16px">≥ ${crit.factor}× 中位数 且 ≥ +${C.fmtNum(crit.min_abs_ms)} ms</div>
        <div class="delta">分组 作业×阶段 · MAD×${crit.mad_k} · 每组 ≥${crit.min_group_size} 个完成任务</div></div>
    </div>
    <div class="card mt">${groupTable(data.groups)}</div>`;
  document.getElementById('slow-tasks').innerHTML = slow.length
    ? slowTable(slow) : C.empty('未发现执行明显偏慢的任务 No slow tasks');
  document.getElementById('wait-outliers').innerHTML = waits.length
    ? waitTable(waits) : C.empty('未发现等待异常的任务 No wait outliers');
}

async function renderCluster() {
  let data;
  try { data = await API.get('/api/slow-tasks'); } catch (e) { return; }
  document.getElementById('cluster-summary').innerHTML = `
    <div class="stat-tiles">
      <div class="stat"><div class="label">分析作业 Jobs</div><div class="value">${data.jobs_analyzed}</div></div>
      <div class="stat"><div class="label">分析任务 Tasks</div><div class="value">${C.fmtNum(data.tasks_analyzed)}</div></div>
      <div class="stat"><div class="label">慢任务合计 Slow</div><div class="value ${data.slow_total ? 'bad' : 'good'}">${data.slow_total}</div></div>
    </div>`;
  const workers = data.workers || [];
  document.getElementById('cluster').innerHTML = workers.length ? C.table([
    { key: 'name', label: '节点 Node', render: r => C.esc(r.name || r.worker_id) },
    { key: 'tasks', label: '任务数 Tasks', render: r => r.tasks, num: true },
    { key: 'slow_tasks', label: '慢任务 Slow', render: r => r.slow_tasks, num: true },
    { key: 'slow_rate', label: '慢任务占比 Rate', render: r => C.fmtPct((r.slow_rate || 0) * 100), num: true },
    { key: 'avg_ratio', label: '平均倍数 Avg×Med', render: r => fmtRatio(r.avg_ratio), num: true },
    { key: 'max_ratio', label: '最大倍数 Max×Med', render: r => fmtRatio(r.max_ratio), num: true },
    { key: 'exec_ms', label: '累计执行 Exec', render: r => C.fmtDur(r.exec_ms), num: true },
    { key: 'wait_ms', label: '累计等待 Wait', render: r => C.fmtDur(r.wait_ms), num: true },
  ], workers) : C.empty('暂无可分析的节点数据 No node data yet');
}

function renderAll() { renderJob(); renderCluster(); }

C.jobPicker('job-picker', (id) => { currentJob = id; renderJob(); });
document.getElementById('refresh').addEventListener('click', renderAll);
C.poll(renderAll, 3000).start();
