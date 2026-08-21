import React, { useCallback, useEffect, useRef, useState } from 'react'
import { api } from './api.js'

const STATE_COLORS = {
  queued: 'gray', running: 'blue', succeeded: 'green',
  degraded: 'orange', failed: 'red', canceled: 'gray', interrupted: 'purple',
}

function Chip({ text, color }) {
  return <span className={`chip chip-${color || 'gray'}`}>{text}</span>
}

function fmtTs(ts) {
  return ts ? ts.replace('T', ' ').slice(5, 19) : '—'
}

// ---------- clusters ----------

function ClusterCard({ c, onAction }) {
  const [ttl, setTtl] = useState(120)
  const busy = c.state === 'starting' || c.state === 'stopping'
  return (
    <div className="card">
      <div className="card-head">
        <b>{c.key}</b>
        <Chip text={c.state} color={{
          running: 'green', starting: 'blue', stopping: 'orange',
          terminated: 'gray',
        }[c.state] || 'gray'} />
      </div>
      <div className="card-body">
        <div>{c.vm_count != null ? `${c.vm_count} VMs` : 'VMs: (unread)'}
          {c.burn_usd_per_hr != null && c.state === 'running' &&
            <span className="burn"> · ${c.burn_usd_per_hr.toFixed(2)}/hr</span>}
          {c.session_cost_usd != null &&
            <span className="muted"> · total ${c.session_cost_usd}</span>}
        </div>
        {c.leases.length > 0 && (
          <ul className="leases">
            {c.leases.map(l => (
              <li key={l.id}>
                lease <code>{l.id}</code> ({l.purpose}) —
                {' '}{Math.floor(l.expires_in_s / 60)}m left
                <button className="link" onClick={() => onAction('extend', c, l)}>extend</button>
              </li>
            ))}
          </ul>
        )}
        {c.vms && (
          <div className="vm-grid">
            {Object.entries(c.vms).map(([vm, st]) => (
              <span key={vm} className={`vm vm-${st === 'RUNNING' ? 'up' : 'down'}`}
                    title={`${vm}: ${st}`}>{vm.split('-').pop()}</span>
            ))}
          </div>
        )}
      </div>
      <div className="card-actions">
        {c.state !== 'running' && !busy && (
          <>
            <input type="number" value={ttl} min="1"
                   onChange={e => setTtl(+e.target.value)} title="TTL minutes" />
            <button onClick={() => onAction('up', c, null, ttl)}>up (TTL {ttl}m)</button>
          </>
        )}
        {c.state === 'running' && (
          <button onClick={() => onAction('down', c)}>down</button>
        )}
        <button onClick={() => onAction('refresh', c)}>refresh VMs</button>
        {busy && <span className="muted">waiting for gcloud…</span>}
      </div>
    </div>
  )
}

// ---------- jobs ----------

function SubmitForm({ project, clusters, catalog, onSubmitted }) {
  const [selected, setSelected] = useState([])
  const [freeText, setFreeText] = useState('')
  const [flags, setFlags] = useState('')
  const [priority, setPriority] = useState(0)
  const [retries, setRetries] = useState(2)
  const [after, setAfter] = useState('')          // chain: wait for job #
  const [managedCluster, setManagedCluster] = useState('')  // daemon lease
  const [err, setErr] = useState(null)
  // One-off sweep mode: generic params the project adapter translates.
  const [oneoff, setOneoff] = useState(false)
  const [base, setBase] = useState('')
  const [dims, setDims] = useState('')           // one NAME=v1,v2 per line
  const [rates, setRates] = useState('')
  const [search, setSearch] = useState(false)
  const [trials, setTrials] = useState('')
  const [duration, setDuration] = useState('')
  const [sweeps, setSweeps] = useState([])       // saved presets

  const loadSweeps = useCallback(() => {
    if (project) api.sweeps(project).then(setSweeps).catch(() => setSweeps([]))
  }, [project])
  useEffect(loadSweeps, [loadSweeps])

  function buildSweep() {
    const sweep = {}
    if (base) sweep.base = base
    const dimObj = {}
    for (const line of dims.split('\n').map(s => s.trim()).filter(Boolean)) {
      const [name, vals] = line.split('=')
      if (!name || !vals) { setErr(`bad dim line: ${line}`); return null }
      dimObj[name.trim()] = vals.split(',').map(s => s.trim()).filter(Boolean)
    }
    if (Object.keys(dimObj).length) sweep.dims = dimObj
    const rateList = rates.split(/[\s,]+/).filter(Boolean).map(Number)
    if (rateList.length) sweep.rates = rateList
    if (search) sweep.rate_search = true
    if (trials) sweep.trials = +trials
    if (duration) sweep.duration_secs = +duration
    if (flags.trim()) sweep.extra_flags = flags.split(/\s+/).filter(Boolean)
    if (managedCluster) sweep.cluster = managedCluster
    return sweep
  }

  function applyPreset(params) {
    setBase(params.base || '')
    setDims(Object.entries(params.dims || {})
      .map(([k, v]) => `${k}=${v.join(',')}`).join('\n'))
    setRates((params.rates || []).join(' '))
    setSearch(Boolean(params.rate_search))
    setTrials(params.trials ?? '')
    setDuration(params.duration_secs ?? '')
    setFlags((params.extra_flags || []).join(' '))
    setManagedCluster(params.cluster || '')
  }

  useEffect(() => {
    setSelected([]); setManagedCluster(''); setAfter('')
  }, [project])

  const hasCatalog = catalog && !catalog.error && catalog.experiments.length > 0
  // One cluster per job: selecting an experiment greys out other clusters.
  const selectedQueues = new Set(
    (catalog?.experiments || [])
      .filter(e => selected.includes(e.name) && e.queue)
      .map(e => e.queue))

  const byQueue = {}
  for (const e of catalog?.experiments || []) {
    (byQueue[e.queue || 'other'] ||= []).push(e)
  }

  function toggle(name) {
    setSelected(s => s.includes(name) ? s.filter(x => x !== name) : [...s, name])
  }

  async function submit(e) {
    e.preventDefault()
    setErr(null)
    try {
      const common = { project, priority: +priority, max_retries: +retries }
      if (after) common.after = +after
      if (oneoff) {
        const sweep = buildSweep()
        if (sweep === null) return
        await api.submit({ ...common, sweep })
      } else {
        const experiments = hasCatalog
          ? selected
          : freeText.split(/\s+/).filter(Boolean)
        if (!experiments.length) { setErr('pick at least one experiment'); return }
        if (managedCluster) common.cluster = managedCluster
        await api.submit({
          ...common,
          experiments,
          extra_flags: flags.split(/\s+/).filter(Boolean),
        })
      }
      setSelected([]); setFreeText('')
      onSubmitted()
    } catch (e2) { setErr(String(e2.message || e2)) }
  }

  return (
    <form className="submit-form-block" onSubmit={submit}>
      <div className="submit-form">
        <label title="ad-hoc sweep: override dims/rates/trials; the project adapter builds the command">
          <input type="checkbox" checked={oneoff}
                 onChange={e => setOneoff(e.target.checked)} /> one-off
        </label>
        {(clusters || []).length > 0 && (
          <label title="daemon-managed lease: cluster up before the job, released after — a chained job on the same cluster inherits it without a VM cycle">
            cluster
            <select value={managedCluster}
                    onChange={e => setManagedCluster(e.target.value)}>
              <option value="">(driver-managed)</option>
              {clusters.map(c => <option key={c} value={c}>{c}</option>)}
            </select>
          </label>
        )}
        <label title="chain: run only after this job's retry chain ends with data; a failed chain cancels this job">
          after #<input type="number" min="1" className="after-input"
                        value={after} onChange={e => setAfter(e.target.value)} />
        </label>
        {!oneoff && !hasCatalog && (
          <input placeholder={catalog?.error
                   ? `no catalog (${catalog.error}) — raw driver args`
                   : 'experiments (space-separated)'}
                 value={freeText} onChange={e => setFreeText(e.target.value)} />
        )}
        <input placeholder="extra flags" value={flags}
               onChange={e => setFlags(e.target.value)} />
        <label>prio <input type="number" value={priority}
               onChange={e => setPriority(e.target.value)} /></label>
        <label>retries <input type="number" value={retries} min="0"
               onChange={e => setRetries(e.target.value)} /></label>
        <button type="submit">
          queue job{!oneoff && selected.length > 1 ? ` (${selected.length})` : ''}
        </button>
        {err && <span className="error">{err}</span>}
      </div>

      {oneoff && sweeps.length > 0 && (
        <div className="agg-row">
          {sweeps.map(s => (
            <button key={s.id} type="button" className="agg"
                    title={JSON.stringify(s.params)}
                    onClick={() => applyPreset(s.params)}>
              {s.name}
            </button>
          ))}
          <button type="button" className="agg" title="delete a preset"
                  onClick={() => {
                    const n = prompt('delete which preset?',
                                     sweeps[0]?.name || '')
                    const hit = sweeps.find(s2 => s2.name === n)
                    if (hit) api.deleteSweep(hit.id).then(loadSweeps)
                  }}>✕</button>
        </div>
      )}
      {oneoff && (
        <div className="oneoff">
          <button type="button" className="link"
                  title="save these params as a named preset"
                  onClick={async () => {
                    const sweep = buildSweep()
                    if (sweep === null) return
                    const n = prompt('preset name:')
                    if (!n) return
                    try {
                      await api.saveSweep(project, n.trim(), sweep)
                      loadSweeps()
                    } catch (e2) { setErr(String(e2.message || e2)) }
                  }}>save preset</button>
          <label>base
            <select value={base} onChange={e => setBase(e.target.value)}>
              <option value="">(none)</option>
              {(catalog?.experiments || []).map(e2 => (
                <option key={e2.name} value={e2.name}>{e2.name}</option>
              ))}
            </select>
          </label>
          <label>dims
            <textarea rows="2" placeholder={'payload_size=16,1024\ngamma=1.2'}
                      value={dims} onChange={e => setDims(e.target.value)} />
          </label>
          {(catalog?.display?.dims || []).length > 0 && (
            <span className="agg-row">
              {catalog.display.dims.map(d => (
                <button key={d.name} type="button" className="agg"
                        title={[d.description, d.unit && `unit: ${d.unit}`]
                          .filter(Boolean).join(' — ')}
                        onClick={() => setDims(v => {
                          if (v.split('\n').some(l =>
                            l.trim().startsWith(`${d.name}=`))) return v
                          const line = `${d.name}=${d.example || ''}`
                          return v.trim() ? `${v.trimEnd()}\n${line}` : line
                        })}>
                  +{d.label}
                </button>
              ))}
            </span>
          )}
          <label>rates
            <input placeholder="1000 2000 4000" value={rates}
                   onChange={e => setRates(e.target.value)} />
          </label>
          <label>
            <input type="checkbox" checked={search}
                   onChange={e => setSearch(e.target.checked)} /> rate search
          </label>
          <label>trials
            <input type="number" min="1" placeholder="1" value={trials}
                   onChange={e => setTrials(e.target.value)} />
          </label>
          <label>duration s
            <input type="number" step="any" placeholder="(default)" value={duration}
                   onChange={e => setDuration(e.target.value)} />
          </label>
        </div>
      )}

      {!oneoff && hasCatalog && (
        <div className="catalog">
          {Object.keys(catalog.aggregates).length > 0 && (
            <div className="agg-row">
              {Object.entries(catalog.aggregates).map(([name, members]) => (
                <button key={name} type="button" className="agg"
                        title={members.join(', ')}
                        onClick={() => setSelected(members)}>
                  {name} ({members.length})
                </button>
              ))}
              <button type="button" className="agg" onClick={() => setSelected([])}>
                clear
              </button>
            </div>
          )}
          {Object.entries(byQueue).map(([queue, exps]) => (
            <div key={queue} className="queue-group">
              <div className="queue-name">{queue}</div>
              <div className="exp-grid">
                {exps.map(e => {
                  const disabled = selectedQueues.size > 0 && e.queue &&
                    !selectedQueues.has(e.queue)
                  return (
                    <label key={e.name}
                           className={`exp ${disabled ? 'exp-disabled' : ''}`}
                           title={e.native
                             ? `${e.description}\n\nnative: runs on the SweepEngine as its own job; the daemon leases ${e.queue} around it`
                             : e.description}>
                      <input type="checkbox" disabled={disabled}
                             checked={selected.includes(e.name)}
                             onChange={() => toggle(e.name)} />
                      {e.name}{e.native && <span className="native-mark">⚡</span>}
                    </label>
                  )
                })}
              </div>
            </div>
          ))}
        </div>
      )}
    </form>
  )
}

function fmtEta(s) {
  if (s >= 3600) return `${(s / 3600).toFixed(1)}h`
  if (s >= 60) return `${Math.round(s / 60)}m`
  return `${Math.round(s)}s`
}

function describePoint(c) {
  const dims = Object.entries(c.dims || {}).map(([k, v]) => `${k}=${v}`).join(' ')
  return [c.experiment, dims, c.rate != null ? `rate=${c.rate}` : '',
          `trial ${c.trial}`].filter(Boolean).join(' · ')
}

// Progress folded by the daemon from the run's events.jsonl (native runs
// only; old-driver jobs have no progress and keep the log tail).
function ProgressPanel({ p }) {
  const pts = p.points || {}
  const total = p.totals_final?.points_total ?? p.est_points
  const done = pts.done || 0
  const pct = total ? Math.min(100, (100 * done) / total) : null
  return (
    <div className="progress">
      <div>
        <b>{done}{total ? `/${total}` : ''}</b> points
        {' '}· ok {pts.ok || 0} · dead {pts.dead || 0} · failed {pts.failed || 0}
        {pts.skipped > 0 && <> · resumed {pts.skipped}</>}
        {p.eta_secs != null && <> · ~{fmtEta(p.eta_secs)} left</>}
        {p.run_state === 'interrupted' && <Chip text="interrupted" color="purple" />}
      </div>
      {pct != null && (
        <div className="bar"><div className="bar-fill" style={{ width: `${pct}%` }} /></div>
      )}
      {p.current && <div className="muted">running: {describePoint(p.current)}</div>}
      {p.last_decision && (
        <div className="muted">
          search: {p.last_decision.action} @ {p.last_decision.rate}
          {p.last_decision.note ? ` — ${p.last_decision.note}` : ''}
        </div>
      )}
    </div>
  )
}

function JobRow({ job, onChanged }) {
  const [open, setOpen] = useState(false)
  const [log, setLog] = useState('')
  const timer = useRef(null)

  const fetchLog = useCallback(async () => {
    try { setLog((await api.jobLog(job.id)).log) } catch { /* ignore */ }
  }, [job.id])

  useEffect(() => {
    if (open) {
      fetchLog()
      timer.current = setInterval(fetchLog, 2000)
      return () => clearInterval(timer.current)
    }
  }, [open, fetchLog])

  async function act(fn) {
    try { await fn(); onChanged() } catch (e) { alert(e.message || e) }
  }

  const editable = job.state === 'queued'
  const spec = job.spec

  return (
    <>
      <tr className="job-row" onClick={() => setOpen(!open)}>
        <td>{job.id}</td>
        <td>{job.project}</td>
        <td className="mono">{(spec.experiments || []).join(' ') ||
          (spec.command || []).join(' ')}</td>
        <td>{spec.queue || job.project}
          {spec.cluster && <span title={`daemon-managed lease on ${spec.cluster}`}> ⚙</span>}
        </td>
        <td>{job.priority !== 0 ? job.priority : ''}</td>
        <td><Chip text={job.state} color={STATE_COLORS[job.state]} />
          {job.state === 'queued' && spec.after &&
            <span className="muted" title="waits for that job's retry chain">
              {' '}after #{spec.after}</span>}
          {job.exit_code != null && job.exit_code !== 0 &&
            <span className="muted"> rc={job.exit_code}</span>}
          {job.state === 'running' && job.progress?.points &&
            <span className="muted"> {job.progress.points.done}
              {(job.progress.totals_final?.points_total ?? job.progress.est_points)
                ? `/${job.progress.totals_final?.points_total ?? job.progress.est_points}`
                : ''} pts</span>}
        </td>
        <td className="muted">{fmtTs(job.created_at)}</td>
        <td className="muted">{fmtTs(job.finished_at)}</td>
        <td onClick={e => e.stopPropagation()}>
          {editable && (
            <>
              <button className="link" onClick={() => {
                const exp = prompt('experiments (space-separated):',
                  (spec.experiments || []).join(' '))
                if (exp !== null) {
                  act(() => api.editJob(job.id,
                    { experiments: exp.split(/\s+/).filter(Boolean) }))
                }
              }}>edit</button>
              <button className="link" onClick={() => {
                const p = prompt('priority (higher runs first):',
                  String(job.priority))
                if (p !== null) act(() => api.editJob(job.id, { priority: +p }))
              }}>prio</button>
            </>
          )}
          {(job.state === 'queued' || job.state === 'running') && (
            <button className="link" onClick={() => act(() => api.cancel(job.id))}>
              cancel</button>
          )}
          {['degraded', 'failed', 'interrupted', 'canceled'].includes(job.state) && (
            <button className="link" title="resubmit, resuming into the same run dir"
                    onClick={() => act(() => api.resubmit(job.id))}>resume</button>
          )}
          {spec.sweep && (
            <button className="link" title="save this one-off's params as a reusable preset"
                    onClick={() => {
                      const n = prompt('preset name:')
                      if (n) act(() => api.saveSweep(job.project, n.trim(), spec.sweep))
                    }}>save</button>
          )}
        </td>
      </tr>
      {open && (
        <tr><td colSpan="9" className="log-cell">
          {job.run_dir && <div className="muted">run dir: <code>{job.run_dir}</code></div>}
          {job.progress && <ProgressPanel p={job.progress} />}
          <pre className="log">{log || '(no output yet)'}</pre>
        </td></tr>
      )}
    </>
  )
}

// ---------- runs ledger ----------

// Fallback columns for projects whose adapter advertises no display()
// metadata; adapters that do own their column set and order.
const POINT_METRIC_COLS = [
  ['throughput_msgs_per_sec', 'delivered/s'],
  ['offered_rate', 'offered/s'],
  ['e2e_p50', 'e2e p50'],
  ['e2e_p99', 'e2e p99'],
]

function fmtNum(v) {
  if (v == null || typeof v !== 'number') return v ?? '—'
  return Math.abs(v) >= 1000 ? Math.round(v).toLocaleString()
    : Math.round(v * 100) / 100
}

function RunRow({ run, display, onChanged }) {
  const [open, setOpen] = useState(false)
  const [detail, setDetail] = useState(null)
  const [note, setNote] = useState('')

  useEffect(() => {
    if (open) api.run(run.id).then(setDetail).catch(() => {})
  }, [open, run.id])

  async function act(fn) {
    try {
      await fn()
      setDetail(await api.run(run.id))
      onChanged()
    } catch (e) { alert(e.message || e) }
  }

  const metricCols = display?.metrics?.length
    ? display.metrics.map(m => [m.name, m.unit ? `${m.label} (${m.unit})` : m.label])
    : POINT_METRIC_COLS
  const cols = detail
    ? metricCols.filter(([k]) => detail.points.some(p => k in p.metrics))
    : []

  return (
    <>
      <tr className="job-row" onClick={() => setOpen(!open)}>
        <td>{run.id}</td>
        <td>{run.project}</td>
        <td className="mono">{run.experiment}</td>
        <td><Chip text={run.status || '?'} color={{
          ok: 'green', degraded: 'orange', failed: 'red',
          running: 'blue', interrupted: 'purple',
        }[run.status] || 'gray'} /></td>
        <td>{run.n_points ?? ''}</td>
        <td className="muted">{fmtTs(run.started_at)}</td>
        <td>{(run.tags || []).map(t => <Chip key={t} text={t} color="blue" />)}</td>
        <td className="muted">{run.job_id ? `#${run.job_id}` : ''}
          {!run.dir_exists && <span title="run dir deleted; metrics preserved"> 🗑</span>}
        </td>
      </tr>
      {open && (
        <tr><td colSpan="8" className="log-cell">
          <div className="muted">dir: <code>{run.run_dir}</code>
            {run.git_commit && <> · commit <code>{run.git_commit.slice(0, 10)}</code></>}
          </div>
          {detail === null ? <p className="muted">loading…</p> : (
            <>
              <div className="run-actions" onClick={e => e.stopPropagation()}>
                <input placeholder="add note (why it ran, what it showed)"
                       value={note} onChange={e => setNote(e.target.value)} />
                <button className="link" onClick={() => {
                  if (note.trim()) act(() => api.addNote(run.id, note.trim()).then(() => setNote('')))
                }}>note</button>
                <button className="link" onClick={() => {
                  const t = prompt('tag:')
                  if (t) act(() => api.addTag(run.id, t.trim()))
                }}>tag</button>
                {(detail.tags || []).map(t => (
                  <button key={t} className="link" title="remove tag"
                          onClick={() => act(() => api.removeTag(run.id, t))}>
                    {t} ✕</button>
                ))}
              </div>
              {detail.notes.length > 0 && (
                <ul className="notes">
                  {detail.notes.map(n => (
                    <li key={n.id}><span className="muted">{fmtTs(n.ts)}</span> {n.text}</li>
                  ))}
                </ul>
              )}
              <table className="points">
                <thead><tr>
                  <th>dims</th><th>rate</th><th>trial</th>
                  {cols.map(([k, label]) => <th key={k}>{label}</th>)}
                  <th></th>
                </tr></thead>
                <tbody>
                  {detail.points.map(p => (
                    <tr key={p.id}>
                      <td className="mono">{Object.entries(p.dims)
                        .map(([k, v]) => `${k}=${v}`).join(' ') || '—'}</td>
                      <td>{fmtNum(p.rate)}</td>
                      <td>{p.trial ?? ''}</td>
                      {cols.map(([k]) => <td key={k}>{fmtNum(p.metrics[k])}</td>)}
                      <td>
                        {p.metrics.status && <Chip text={p.metrics.status}
                          color={p.metrics.status === 'dead' ? 'orange' : 'red'} />}
                        <details className="raw"><summary>raw</summary>
                          <pre>{JSON.stringify(p.metrics, null, 1)}</pre>
                        </details>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </>
          )}
        </td></tr>
      )}
    </>
  )
}

function RunsSection({ projects, runs, display, onChanged }) {
  const [scanBusy, setScanBusy] = useState(false)

  async function scan(name) {
    setScanBusy(true)
    try {
      const r = await api.scanRuns(name)
      onChanged()
      alert(`${name}: ${r.added} added, ${r.updated} refreshed`)
    } catch (e) { alert(e.message || e) } finally { setScanBusy(false) }
  }

  return (
    <section>
      <h2>Runs
        {projects.map(p => (
          <button key={p.name} className="link" disabled={scanBusy}
                  title={`index existing run dirs under ${p.name}'s runs roots`}
                  onClick={() => scan(p.name)}>scan {p.name}</button>
        ))}
      </h2>
      {!runs.length
        ? <p className="muted">No runs recorded yet — native jobs land here
            automatically; use scan to backfill old dirs.</p>
        : <table>
            <thead><tr>
              <th>id</th><th>project</th><th>experiment</th><th>status</th>
              <th>points</th><th>started</th><th>tags</th><th>job</th>
            </tr></thead>
            <tbody>
              {runs.map(r => (
                <RunRow key={r.id} run={r} display={display} onChanged={onChanged} />
              ))}
            </tbody>
          </table>}
    </section>
  )
}

// ---------- app ----------

export default function App() {
  const [health, setHealth] = useState(null)
  const [projects, setProjects] = useState([])
  const [jobs, setJobs] = useState([])
  const [clusters, setClusters] = useState([])
  const [runs, setRuns] = useState([])
  const [catalog, setCatalog] = useState(null)   // {experiments, aggregates, display, error}
  const [connected, setConnected] = useState(false)
  const [selected, setSelected] = useState(() => {
    try { return localStorage.getItem('ck-project') || '' } catch { return '' }
  })

  useEffect(() => {
    if (projects.length && !projects.some(p => p.name === selected)) {
      setSelected(projects[0].name)
    }
  }, [projects, selected])

  function pickProject(name) {
    setSelected(name)
    try { localStorage.setItem('ck-project', name) } catch { /* ignore */ }
  }

  // The catalog (experiments + adapter display metadata) feeds both the
  // submit form and the runs table's metric columns.
  useEffect(() => {
    if (!selected) return
    setCatalog(null)
    api.experiments(selected).then(setCatalog)
      .catch(() => setCatalog({ experiments: [], aggregates: {}, error: 'unreachable' }))
  }, [selected])

  const reload = useCallback(async () => {
    try {
      const [j, c, r] = await Promise.all([api.jobs(), api.clusters(), api.runs()])
      setJobs(j); setClusters(c); setRuns(r)
    } catch { /* daemon down; the SSE handler flips the dot */ }
  }, [])

  useEffect(() => {
    api.health().then(setHealth).catch(() => {})
    api.projects().then(setProjects).catch(() => {})
    reload()
    const es = new EventSource('/api/stream')
    es.onopen = () => setConnected(true)
    es.onerror = () => setConnected(false)
    es.onmessage = () => reload()   // any state change → refetch (cheap at this scale)
    const poll = setInterval(reload, 30000)
    return () => { es.close(); clearInterval(poll) }
  }, [reload])

  async function clusterAction(kind, c, lease, ttl) {
    try {
      if (kind === 'up') await api.clusterUp(c.key, ttl || 120)
      else if (kind === 'down') {
        if (c.leases.length &&
            !confirm(`${c.key} has live leases — force down?`)) return
        await api.clusterDown(c.key, c.leases.length > 0)
      } else if (kind === 'refresh') await api.clusterRefresh(c.key)
      else if (kind === 'extend') {
        const m = prompt('extend lease by minutes:', '120')
        if (m) await api.clusterExtend(c.key, lease.id, +m)
      }
      reload()
    } catch (e) { alert(e.message || e) }
  }

  // Everything below the tabs is scoped to one project.
  const projJobs = jobs.filter(j => j.project === selected)
  const active = projJobs.filter(j => ['queued', 'running'].includes(j.state))
  const done = projJobs.filter(j => !['queued', 'running'].includes(j.state))
  const projClusters = clusters.filter(c => c.project === selected)
  const projRuns = runs.filter(r => r.project === selected)
  const projInfo = projects.find(p => p.name === selected)

  return (
    <div className="app">
      <header>
        <h1>cloud-kitchen</h1>
        <nav className="tabs">
          {projects.map(p => {
            const n = jobs.filter(j => j.project === p.name &&
              ['queued', 'running'].includes(j.state)).length
            return (
              <button key={p.name}
                      className={`tab ${p.name === selected ? 'tab-on' : ''}`}
                      onClick={() => pickProject(p.name)}>
                {p.name}{n > 0 && <span className="tab-count">{n}</span>}
              </button>
            )
          })}
        </nav>
        <span className={`dot ${connected ? 'dot-on' : 'dot-off'}`}
              title={connected ? 'live' : 'disconnected'} />
        {health && <span className="muted">v{health.version}</span>}
      </header>

      {projClusters.length > 0 && (
        <section>
          <h2>Clusters</h2>
          <div className="cards">
            {projClusters.map(c => (
              <ClusterCard key={c.key} c={c} onAction={clusterAction} />
            ))}
          </div>
        </section>
      )}

      <section>
        <h2>Queue</h2>
        {selected && (
          <SubmitForm project={selected}
                      clusters={(projInfo?.clusters) || []}
                      catalog={catalog}
                      onSubmitted={reload} />
        )}
        <JobTable jobs={active} onChanged={reload}
                  empty="Nothing queued or running." />
      </section>

      <section>
        <h2>History</h2>
        <JobTable jobs={done} onChanged={reload} empty="No finished jobs yet." />
      </section>

      <RunsSection projects={projects.filter(p => p.name === selected)}
                   runs={projRuns} display={catalog?.display}
                   onChanged={reload} />
    </div>
  )
}

function JobTable({ jobs, onChanged, empty }) {
  if (!jobs.length) return <p className="muted">{empty}</p>
  return (
    <table>
      <thead><tr>
        <th>id</th><th>project</th><th>experiments</th><th>queue</th>
        <th>prio</th><th>state</th><th>created</th><th>finished</th><th></th>
      </tr></thead>
      <tbody>
        {jobs.map(j => <JobRow key={j.id} job={j} onChanged={onChanged} />)}
      </tbody>
    </table>
  )
}
