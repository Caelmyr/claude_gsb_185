/* 慢任务分析 Slow tasks */
Components.init('slow');
const C = Components;

let scope = 'job';
let currentJob = '';
let poller = null;

// ---------------------------------------------------------------------------
// Small render helpers
// ---------------------------------------------------------------------------
function catBadge(cat) {
  const map = {
    exec_slow: ['bad', '执行慢 Exec slow'],
    data_skew: ['warn', '数据倾斜 Skew'],
    wait_slow: ['aqua', '排队久 Wait'],
  };
  const [cls, label] = map[cat] || ['muted', cat || '-'];
  return `<span class="badge ${cls}">${C.esc(label)}</span>`;
}

function allFlags(row) {
  return (row.slow_flags && row.slow_flags.length ? row.slow_flags : [row.slow_category])
    .filter(Boolean).map(catBadge).join(' ');
}

function durCell(v) {
  return v == null ? '<span class="muted">-</span>'
    : `<span class="tabular">${C.fmtDur(v)}</span>`;
}

function kindCell(k) {
  return `<span class="badge ${k === 'map' ? 'run' : 'aqua'}">${k}</span>`;
}

// Bar indexed around 1.0 (peer median): > ratio thresholds is hot.
function indexCell(v, hotAt) {
  if (v == null) return '<span class="muted">-</span>';
  const cls = v >= hotAt ? 'bad' : (v >= 1.2 ? 'warn' : 'good');
  return `<span class="tabular bold ${cls}">${v.toFixed(2)}×</span>`;
}

function statTile(label, value, cls, delta) {
  return `<div class="stat"><div class="label">${label}</div>
    <div class="value ${cls || ''}">${value}</div>
    ${delta ? `<div class="delta">${delta}</div>` : ''}</div>`;
}

// ---------------------------------------------------------------------------
// Criteria explainer
// ---------------------------------------------------------------------------
function renderCriteria(cr) {
  if (!cr) return;
  document.getElementById('criteria').innerHTML = `
    <ul style="margin:6px 0 0 18px;line-height:1.8">
      <li>分组基线：${C.esc(cr.grouping)}</li>
      <li>执行慢：执行耗时 ≥ 同组中位数的 <b>${cr.slow_task_ratio}×</b> 且 ≥ <b>${C.fmtDur(cr.slow_task_min_exec_ms)}</b>（绝对值兜底，避免小任务噪声）</li>
      <li>区分倾斜：再比较「每条记录耗时」，≥ 中位数 <b>${cr.slow_task_cost_ratio}×</b> 才算节点慢；否则为<b>数据倾斜</b>（分片更大而非节点慢）</li>
      <li>排队久：创建→首次下发 的等待 ≥ 作业等待中位数 <b>${cr.slow_task_wait_ratio}×</b> 且 ≥ <b>${C.fmtDur(cr.slow_task_min_wait_ms)}</b></li>
      <li>基线样本：同组至少 <b>${cr.slow_task_min_group}</b> 个已完成任务才下结论（样本不足只展示、不判慢）</li>
      <li>节点普遍偏慢：节点上 ≥ <b>${cr.slow_node_min_tasks}</b> 个任务，且执行/单记录耗时指数均 ≥ <b>${cr.slow_node_ratio}×</b></li>
    </ul>`;
}

// ---------------------------------------------------------------------------
// Job scope
// ---------------------------------------------------------------------------
function renderBaselines(rep) {
  const bl = rep.baselines || {};
  const cards = Object.entries(bl).map(([kind, b]) => `
    <div class="card">
      <h2>${kindCell(kind)} <span class="sub">基线 Baseline · n=${b.n}</span></h2>
      <div class="small muted" style="line-height:1.9">
        中位执行耗时 median exec：<b class="tabular">${C.fmtDur(b.median_exec_ms)}</b><br>
        中位排队等待 median wait：<b class="tabular">${b.median_wait_ms != null ? C.fmtDur(b.median_wait_ms) : '-'}</b><br>
        中位每条耗时 median / record：<b class="tabular">${b.median_ms_per_record != null ? b.median_ms_per_record.toFixed(3) + ' ms' : '-'}</b>
      </div>
    </div>`).join('');
  document.getElementById('job-baselines').innerHTML = cards
    ? `<div class="grid cols-2">${cards}</div>`
    : `<div class="card"><div class="empty">同阶段已完成任务不足，暂无稳定基线（Not enough finished peers for a baseline yet）。</div></div>`;
}

function renderJob(rep) {
  renderCriteria(rep.criteria);
  renderBaselines(rep);
  const ct = rep.counts || {};
  const ft = rep.flag_totals || ct;
  document.getElementById('job-summary').innerHTML = [
    statTile('分析任务 Analyzed', ct.analyzed ?? '-'),
    statTile('执行慢 Exec slow', ft.exec_slow || 0, ft.exec_slow ? 'bad' : ''),
    statTile('数据倾斜 Skew', ft.data_skew || 0, ft.data_skew ? '' : ''),
    statTile('排队久 Long wait', ft.wait_slow || 0, ft.wait_slow ? '' : ''),
  ].join('');

  const tasks = rep.tasks || [];
  const headers = [
    { key: 'task_id', label: '任务 Task', render: r => `<span class="mono">${C.esc(r.task_id)}</span>` },
    { key: 'kind', label: '阶段 Stage', render: r => kindCell(r.kind) },
    { key: 'shard', label: '分片/分区 Shard', render: r => `<span class="mono">${C.esc(r.shard)}</span>` },
    { key: 'status', label: '状态 Status', render: r => C.stateBadge(r.status, true) },
    { key: 'worker_name', label: '节点 Node', render: r => C.esc(r.worker_name || r.worker_id || '-') },
    { key: 'records', label: '记录 Records', num: true, render: r => C.fmtNum(r.records_processed) },
    { key: 'wait_ms', label: '排队等待 Wait', num: true, render: r => durCell(r.wait_ms) },
    { key: 'exec_ms', label: '执行耗时 Exec', num: true,
      render: r => `<b class="tabular">${C.fmtDur(r.exec_ms)}</b>` },
    { key: 'retry_ms', label: '重试损耗 Retry', num: true, render: r => durCell(r.retry_ms || 0) },
    { key: 'total_ms', label: '总耗时 Total', num: true, render: r => durCell(r.total_ms) },
    { key: 'ratios', label: '对比基线 vs median', num: true,
      render: r => {
        const x = r.reasons || {};
        const parts = [];
        if (x.exec_ratio != null) parts.push(`exec ${x.exec_ratio}×`);
        if (x.cost_ratio != null) parts.push(`/rec ${x.cost_ratio}×`);
        if (x.wait_ratio != null) parts.push(`wait ${x.wait_ratio}×`);
        return `<span class="small tabular">${parts.join(' ') || '-'}</span>`;
      } },
    { key: 'flags', label: '判定 Verdict', render: r => allFlags(r) },
  ];
  document.getElementById('job-tasks').innerHTML = tasks.length
    ? C.table(headers, tasks)
    : C.empty('未发现明显偏慢的任务。No slow tasks detected against the adaptive baseline.');
}

// ---------------------------------------------------------------------------
// Cluster scope
// ---------------------------------------------------------------------------
function renderCluster(rep) {
  renderCriteria(rep.criteria);
  const ct = rep.counts || {};
  document.getElementById('cluster-summary').innerHTML = [
    statTile('分析作业 Jobs', rep.jobs_analyzed ?? '-'),
    statTile('执行慢 Exec slow', ct.exec_slow || 0, ct.exec_slow ? 'bad' : ''),
    statTile('数据倾斜 Skew', ct.data_skew || 0),
    statTile('排队久 Long wait', ct.wait_slow || 0),
    statTile('普遍偏慢节点 Slow nodes', ct.slow_nodes || 0, ct.slow_nodes ? 'bad' : 'good'),
  ].join('');

  const hotAt = rep.criteria ? rep.criteria.slow_node_ratio : 1.5;
  const nodeHeaders = [
    { key: 'worker_name', label: '节点 Node', render: r => C.esc(r.worker_name || r.worker_id) +
        (r.generally_slow ? ' <span class="badge bad">普遍偏慢 Slow node</span>' : '') },
    { key: 'host', label: '主机 Host', render: r => `<span class="mono small">${C.esc(r.host)}</span>` },
    { key: 'status', label: '状态 Status', render: r => C.stateBadge(r.status, true) },
    { key: 'jobs', label: '作业 Jobs', num: true, render: r => r.jobs },
    { key: 'tasks', label: '任务 Tasks', num: true, render: r => r.tasks },
    { key: 'median_exec_ms', label: '中位执行耗时 Median exec', num: true, render: r => C.fmtDur(r.median_exec_ms) },
    { key: 'median_exec_index', label: '执行指数 Exec idx', num: true, render: r => indexCell(r.median_exec_index, hotAt) },
    { key: 'median_cost_index', label: '单记录指数 /rec idx', num: true, render: r => indexCell(r.median_cost_index, hotAt) },
    { key: 'exec_slow', label: '执行慢', num: true, render: r => r.exec_slow || 0 },
    { key: 'data_skew', label: '倾斜', num: true, render: r => r.data_skew || 0 },
    { key: 'wait_slow', label: '排队久', num: true, render: r => r.wait_slow || 0 },
  ];
  document.getElementById('cluster-nodes').innerHTML = (rep.nodes || []).length
    ? C.table(nodeHeaders, rep.nodes)
    : C.empty('暂无已完成任务。No completed tasks yet.');

  const taskHeaders = [
    { key: 'job_name', label: '作业 Job', render: r => C.esc(r.job_name) +
        `<div class="small muted mono">${C.esc(r.job_id)}</div>` },
    { key: 'task_id', label: '任务 Task', render: r => `<span class="mono">${C.esc(r.task_id)}</span>` },
    { key: 'kind', label: '阶段', render: r => kindCell(r.kind) },
    { key: 'shard', label: '分片 Shard', render: r => `<span class="mono">${C.esc(r.shard)}</span>` },
    { key: 'worker_name', label: '节点 Node', render: r => C.esc(r.worker_name || r.worker_id || '-') },
    { key: 'wait_ms', label: '排队 Wait', num: true, render: r => durCell(r.wait_ms) },
    { key: 'exec_ms', label: '执行 Exec', num: true, render: r => `<b class="tabular">${C.fmtDur(r.exec_ms)}</b>` },
    { key: 'flags', label: '判定', render: r => allFlags(r) },
  ];
  const tasks = rep.tasks || [];
  document.getElementById('cluster-tasks').innerHTML = tasks.length
    ? C.table(taskHeaders, tasks)
    : C.empty('跨作业未发现慢任务。No slow tasks across jobs.');
}

// ---------------------------------------------------------------------------
// Scope switching / loading
// ---------------------------------------------------------------------------
async function loadJob() {
  if (!currentJob) return;
  let rep;
  try { rep = await API.get('/api/jobs/' + currentJob + '/slow-tasks'); } catch (e) { return; }
  renderJob(rep);
}

async function loadCluster() {
  let rep;
  try { rep = await API.get('/api/slow-tasks'); } catch (e) { return; }
  renderCluster(rep);
}

function reload() { return scope === 'job' ? loadJob() : loadCluster(); }

function setScope(s) {
  scope = s;
  document.querySelectorAll('#scope-filter .pill').forEach(p =>
    p.classList.toggle('active', p.getAttribute('data-scope') === s));
  document.getElementById('job-view').style.display = s === 'job' ? '' : 'none';
  document.getElementById('cluster-view').style.display = s === 'cluster' ? '' : 'none';
  document.getElementById('job-picker').style.visibility = s === 'job' ? 'visible' : 'hidden';
  reload();
}

document.querySelectorAll('#scope-filter .pill').forEach(p =>
  p.addEventListener('click', () => setScope(p.getAttribute('data-scope'))));
document.getElementById('refresh').addEventListener('click', reload);

C.jobPicker('job-picker', (id) => { currentJob = id; loadJob(); });
setScope('job');
C.poll(reload, 3000).start();
