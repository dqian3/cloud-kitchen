import React, { useCallback, useEffect, useRef, useState } from 'react'
import { api } from './api.js'

// Job states and the outcomes a done job carries, in one map: the queue
// shows whichever applies.
const STATE_COLORS = {
  queued: 'gray', blocked: 'orange', retrying: 'purple',
  running: 'blue', recorded: 'gray',
  ok: 'green', degraded: 'orange', failed: 'red', canceled: 'gray',
}

function Chip({ text, color }) {
  return <span className={`chip chip-${color || 'gray'}`}>{text}</span>
}

// In-page notices and dialogs: the browser's alert/confirm/prompt block the
// whole tab and can't be styled or dismissed together.
const UI = React.createContext({
  notify: () => {}, ask: async () => false, askText: async () => null,
})
const useUI = () => React.useContext(UI)

function UIProvider({ children }) {
  const [toasts, setToasts] = useState([])
  const [dialog, setDialog] = useState(null)   // {kind, message, value, resolve}

  const notify = useCallback((text, kind = 'info') => {
    const id = Math.random().toString(36).slice(2)
    setToasts(ts => [...ts, { id, text: String(text), kind }])
    setTimeout(() => setToasts(ts => ts.filter(t => t.id !== id)),
               kind === 'error' ? 8000 : 4000)
  }, [])
  const ask = useCallback((message, danger = false) =>
    new Promise(resolve => setDialog({ kind: 'confirm', message, danger, resolve })), [])
  const askText = useCallback((message, value = '') =>
    new Promise(resolve => setDialog({ kind: 'prompt', message, value, resolve })), [])

  function close(result) {
    dialog?.resolve(result)
    setDialog(null)
  }

  return (
    <UI.Provider value={{ notify, ask, askText }}>
      {children}
      <div className="toasts">
        {toasts.map(t => (
          <div key={t.id} className={`toast toast-${t.kind}`}
               onClick={() => setToasts(ts => ts.filter(x => x.id !== t.id))}>
            {t.text}
          </div>
        ))}
      </div>
      {dialog && (
        <div className="modal-back" onClick={() => close(dialog.kind === 'prompt' ? null : false)}>
          <div className="modal" onClick={e => e.stopPropagation()}>
            <div className="modal-msg">{dialog.message}</div>
            {dialog.kind === 'prompt' && (
              <input autoFocus defaultValue={dialog.value}
                     onChange={e => { dialog.value = e.target.value }}
                     onKeyDown={e => {
                       if (e.key === 'Enter') close(dialog.value)
                       if (e.key === 'Escape') close(null)
                     }} />
            )}
            <div className="modal-actions">
              <button className="link" onClick={() => close(dialog.kind === 'prompt' ? null : false)}>
                cancel</button>
              <button className={dialog.danger ? 'danger' : ''}
                      onClick={() => close(dialog.kind === 'prompt' ? dialog.value : true)}>
                {dialog.danger ? 'delete' : 'ok'}</button>
            </div>
          </div>
        </div>
      )}
    </UI.Provider>
  )
}

function fmtTs(ts) {
  return ts ? ts.replace('T', ' ').slice(5, 19) : '—'
}

function fmtResultValue(value) {
  if (value == null) return '—'
  if (typeof value !== 'number') return String(value)
  if (!Number.isFinite(value)) return String(value)
  if (Math.abs(value) >= 1000) return Math.round(value).toLocaleString()
  return Number.isInteger(value) ? String(value) : value.toFixed(2)
}

// ---------- clusters ----------

function ClusterCard({ c, onAction }) {
  const [ttl, setTtl] = useState(120)
  const busy = c.state === 'starting' || c.state === 'stopping'
  return (
    <div className="card">
      <div className="card-head">
        <b>{c.key}</b>
        <Chip text={c.state === 'unmanaged'
                ? `unmanaged (${c.vms_running ?? '?'} up)` : c.state}
              color={{
          running: 'green', starting: 'blue', stopping: 'orange',
          terminated: 'gray', unmanaged: 'orange',
        }[c.state] || 'gray'} />
      </div>
      <div className="card-body">
        <div>{c.vm_count != null ? `${c.vm_count} VMs` : 'VMs: (unread)'}
          {c.burn_usd_per_hr != null &&
            (c.state === 'running' || c.state === 'unmanaged') &&
            <span className="burn"> · ${c.burn_usd_per_hr.toFixed(2)}/hr</span>}
          {c.session_cost_usd != null &&
            <span className="muted"> · total ${c.session_cost_usd}</span>}
        </div>
        {c.kept_for_next && (
          <div className="muted" title="no job holds it right now; the scheduler is keeping it up for the next job on this cluster">
            kept up for the next job
          </div>
        )}
        {c.stop_at && (
          <div className="muted">
            stops in {Math.max(1, Math.ceil((c.stop_at * 1000 - Date.now()) / 60000))}m
          </div>
        )}
        {c.used_by && (
          <div className="muted" title="the job or user this cluster is running for">
            in use by {c.used_by}
          </div>
        )}
        {c.vms && Object.keys(c.vms).length > 0 && (
          <ul className="vm-list">
            {Object.entries(c.vms)
              .sort(([a], [b]) => a.localeCompare(b))
              .map(([vm, st]) => (
                <li key={vm} className={
                      st === 'RUNNING' ? 'vm-on'
                        : st === 'TERMINATED' ? 'vm-off' : 'vm-mid'}>
                  <span className="vm-dot" />
                  {vm}
                  <span className="vm-state">{st.toLowerCase()}</span>
                </li>
              ))}
          </ul>
        )}
      </div>
      <div className="card-actions">
        {c.state !== 'running' && !busy && (
          <>
            <input type="number" value={ttl} min="1"
                   onChange={e => setTtl(+e.target.value)} title="TTL minutes" />
            <button onClick={() => onAction('up', c, ttl)}>up (TTL {ttl}m)</button>
          </>
        )}
        {(c.state === 'running' || c.state === 'unmanaged') && (
          <button title={c.state === 'unmanaged'
                   ? 'stop the VMs running outside the daemon'
                   : undefined}
                  onClick={() => onAction('down', c)}>down</button>
        )}
        <button onClick={() => onAction('refresh', c)}>refresh VMs</button>
        {busy && <span className="muted">waiting for gcloud…</span>}
      </div>
    </div>
  )
}

// ---------- jobs ----------

function SubmitForm({ project, clusters, catalog, onSubmitted }) {
  const { notify, askText } = useUI()
  const [selected, setSelected] = useState([])
  const [freeText, setFreeText] = useState('')
  const [flags, setFlags] = useState('')
  const [priority, setPriority] = useState(0)
  const [attempts, setAttempts] = useState(20)
  const [after, setAfter] = useState('')          // chain: wait for job #
  const [managedCluster, setManagedCluster] = useState('')  // daemon-owned
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
  const [expanded, setExpanded] = useState([])   // bases with variants shown

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
    setSelected([]); setManagedCluster(''); setAfter(''); setExpanded([])
  }, [project])

  // Maintenance actions such as figure generation are intentionally not
  // queue experiments. Keep them out of both normal and one-off selectors.
  const selectableExperiments = (catalog?.experiments || [])
    .filter(e => e.name !== 'figures')
  const hasCatalog = catalog && !catalog.error && selectableExperiments.length > 0
  // One cluster per job: selecting an experiment greys out other clusters.
  const selectedQueues = new Set(
    selectableExperiments
      .filter(e => selected.includes(e.name) && e.queue)
      .map(e => e.queue))

  const byQueue = {}
  for (const e of selectableExperiments) {
    (byQueue[e.queue || 'other'] ||= []).push(e)
  }

  function toggle(name) {
    setSelected(s => s.includes(name) ? s.filter(x => x !== name) : [...s, name])
  }

  async function submit(e) {
    e.preventDefault()
    setErr(null)
    try {
      const common = { project, priority: +priority, max_attempts: +attempts }
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
          <label title="daemon-owned: the cluster comes up before the job, and stays up if the next queued job wants it">
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
        <label>attempts <input type="number" value={attempts} min="1"
               onChange={e => setAttempts(e.target.value)} /></label>
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
                  onClick={async () => {
                    const n = await askText('delete which preset?',
                                            sweeps[0]?.name || '')
                    const hit = sweeps.find(s2 => s2.name === n)
                    if (hit) api.deleteSweep(hit.id).then(loadSweeps)
                    else if (n) notify(`no preset named ${n}`, 'error')
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
                    const n = await askText('preset name:')
                    if (!n) return
                    try {
                      await api.saveSweep(project, n.trim(), sweep)
                      loadSweeps()
                      notify(`saved preset ${n.trim()}`)
                    } catch (e2) { setErr(String(e2.message || e2)) }
                  }}>save preset</button>
          <label>base
            <select value={base} onChange={e => setBase(e.target.value)}>
              <option value="">(none)</option>
              {selectableExperiments.map(e2 => (
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
          {Object.entries(byQueue).map(([queue, exps]) => {
            // Variants (group = base experiment) fold behind their base so
            // the *_n4 / *_no_crypto family doesn't crowd the top level. A
            // group whose name is no experiment (a family label like
            // intraregion_latency) folds behind a virtual header instead.
            const variantsOf = {}
            for (const e of exps) if (e.group) (variantsOf[e.group] ||= []).push(e)
            const expNames = new Set(exps.map(e => e.name))
            const bases = exps.filter(e => !e.group)
            const virtualGroups = Object.keys(variantsOf)
              .filter(g => !expNames.has(g))
            const renderExp = (e) => {
              const disabled = selectedQueues.size > 0 && e.queue &&
                !selectedQueues.has(e.queue)
              return (
                <label key={e.name}
                       className={`exp ${disabled ? 'exp-disabled' : ''} ${e.group ? 'exp-variant' : ''}`}
                       title={e.native
                         ? `${e.description}\n\nnative: runs on the SweepEngine as its own job; the daemon owns ${e.queue} around it`
                         : e.description}>
                  <input type="checkbox" disabled={disabled}
                         checked={selected.includes(e.name)}
                         onChange={() => toggle(e.name)} />
                  {e.name}{e.native && <span className="native-mark">⚡</span>}
                </label>
              )
            }
            return (
              <div key={queue} className="queue-group">
                <div className="queue-name">{queue}</div>
                <div className="exp-grid">
                  {bases.map(e => {
                    const variants = variantsOf[e.name] || []
                    const open = expanded.includes(e.name) ||
                      variants.some(v => selected.includes(v.name))
                    return (
                      <React.Fragment key={e.name}>
                        {renderExp(e)}
                        {variants.length > 0 && (
                          <button type="button" className="agg variant-toggle"
                                  title={variants.map(v => v.name).join(', ')}
                                  onClick={() => setExpanded(x =>
                                    x.includes(e.name)
                                      ? x.filter(n => n !== e.name)
                                      : [...x, e.name])}>
                            {open ? '−' : '+'}{variants.length}
                          </button>
                        )}
                        {open && variants.map(renderExp)}
                      </React.Fragment>
                    )
                  })}
                  {virtualGroups.map(g => {
                    const variants = variantsOf[g]
                    const open = expanded.includes(g) ||
                      variants.some(v => selected.includes(v.name))
                    return (
                      <React.Fragment key={g}>
                        <span className="exp exp-virtual">{g}</span>
                        <button type="button" className="agg variant-toggle"
                                title={variants.map(v => v.name).join(', ')}
                                onClick={() => setExpanded(x =>
                                  x.includes(g) ? x.filter(n => n !== g)
                                    : [...x, g])}>
                          {open ? '−' : '+'}{variants.length}
                        </button>
                        {open && variants.map(renderExp)}
                      </React.Fragment>
                    )
                  })}
                </div>
              </div>
            )
          })}
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

// The daemon reports the retry wait as a snapshot taken when the job list was
// fetched. Polling is far slower than a second, so the number has to be run
// down locally or it looks stuck; each fetch resyncs it.
function useCountdown(seconds) {
  const [left, setLeft] = useState(seconds)
  useEffect(() => {
    if (seconds == null) { setLeft(null); return }
    const deadline = Date.now() + seconds * 1000
    setLeft(seconds)
    const t = setInterval(
      () => setLeft(Math.max(0, Math.round((deadline - Date.now()) / 1000))), 1000)
    return () => clearInterval(t)
  }, [seconds])
  return left
}

// The gist of a failure, for a row that has no space for the whole thing.
// Prefers the reason the provisioner gave (appended after an em dash) over
// the wrapper text, which is the same on every bring-up failure and says
// nothing about which one this is.
function shortError(e) {
  if (!e) return ''
  // Unwrap RuntimeError('...') first, so splitting later cannot leave its
  // closing quote behind.
  const inner = e.match(/RuntimeError\((?:'|")([\s\S]+?)(?:'|")\)/)
  let t = (inner ? inner[1] : e).replace(/^cluster bring-up failed:\s*/, '')
  // Prefer the provisioner's own reason over our wrapper text, which reads
  // the same on every bring-up failure.
  if (t.includes(' — ')) t = t.split(' — ').pop()
  // Drop a leading "cluster X failed to start (why): " preamble.
  const after = t.match(/\):\s*([\s\S]+)$/)
  if (after) t = after[1]
  t = t.trim()
  return t.length > 90 ? t.slice(0, 90) + '…' : t
}

function JobRow({ job, isHead, onChanged, onMove, canMoveUp, canMoveDown }) {
  const { notify } = useUI()
  const [open, setOpen] = useState(false)
  const [log, setLog] = useState('')
  const [tall, setTall] = useState(false)
  const timer = useRef(null)
  const logRef = useRef(null)

  // Follow a live log rather than leaving the reader at the top of a file
  // that is still being written.
  useEffect(() => {
    const el = logRef.current
    if (el && job.state === 'running') el.scrollTop = el.scrollHeight
  }, [log, job.state])

  const fetchLog = useCallback(async () => {
    try { setLog((await api.jobLog(job.id, 500)).log) } catch { /* ignore */ }
  }, [job.id])

  useEffect(() => {
    if (open) {
      fetchLog()
      timer.current = setInterval(fetchLog, 2000)
      return () => clearInterval(timer.current)
    }
  }, [open, fetchLog])

  async function act(fn) {
    try { await fn(); onChanged() } catch (e) { notify(e.message || e, 'error') }
  }

  // A job acquiring its cluster is already the active queue head even though
  // its persisted state remains `waiting` until the driver spawns.
  // Includes the job acquiring a cluster: it holds the slot but has not
  // run, and the daemon hands the slot over when it is outranked.
  const waiting = job.state === 'waiting'
  const done = job.state === 'done'
  const retryIn = useCountdown(waiting ? job.retry_in_s : null)
  // Cluster transitions belong to the cluster cards. Queue rows describe
  // only the head job. Everything behind it is simply queued.
  const label = done ? job.outcome
    : isHead && job.state === 'running' ? 'running'
      : isHead && retryIn != null ? 'retrying' : 'queued'
  const spec = job.spec
  // Fallback keeps the indicator correct against a daemon that has not yet
  // restarted onto the derived `will_resume` API field.
  const willResume = job.will_resume ?? Boolean(
    job.run_dir && (spec.resume || job.attempts > 0))

  return (
    <>
      <tr className="job-row" onClick={() => setOpen(!open)}>
        <td>{job.id}</td>
        <td className="mono" title={(spec.command || []).join(' ')}>
          {spec.name || (spec.experiments || []).join(' ') || 'job'}
          {willResume &&
            <span className="muted" title="continues in the existing run directory">
              {' '}· continues existing run</span>}</td>
        <td className="job-status"><div className="job-status-scroll">
            <Chip text={label} color={STATE_COLORS[label]} />
            {isHead && job.attempts > 1 && !done &&
              <span className="muted" title="driver invocations so far">
                {' '}attempt {job.attempts}/{job.max_attempts}</span>}
            {isHead && retryIn != null &&
              <span className="muted" title={job.last_error || 'waiting'}>
                {' '}{retryIn > 0 ? `retrying in ${retryIn}s` : 'retrying…'}</span>}
            {isHead && waiting && job.last_error && (
              <span className="error" title={job.last_error}>
                {' '}· {shortError(job.last_error)}</span>
            )}
            {isHead && waiting && spec.after &&
              <span className="muted" title="waits for that job to finish with data">
                {' '}after #{spec.after}</span>}
          </div>
        </td>
        <td className="muted">{job.priority}</td>
        <td className="muted">{fmtTs(job.created_at)}</td>
        <td onClick={e => e.stopPropagation()}>
          {waiting && (
            <>
              <button className="link" disabled={!canMoveUp}
                      title="raise priority"
                      onClick={() => onMove(job.id, 'up')}>▲</button>
              <button className="link" disabled={!canMoveDown}
                      title="lower priority"
                      onClick={() => onMove(job.id, 'down')}>▼</button>
            </>
          )}
          {!done && (
            <button className="link" onClick={() => act(() => api.cancel(job.id))}>
              cancel</button>
          )}
        </td>
      </tr>
      {open && (
        <tr><td colSpan="6" className="log-cell">
          <div className="muted">queue {spec.queue || job.project}
            {spec.cluster && <> · lease on {spec.cluster}</>}
            {' '}· up to {job.max_attempts} attempts, {spec.retry_delay_secs}s apart
            {willResume && <> · next attempt resumes this directory</>}
          </div>
          <div className="mono command-line">{(spec.command || []).join(' ') ||
            (spec.experiments || []).join(' ') || '(no command)'}</div>
          {job.run_dir && <div className="muted">run dir: <code>{job.run_dir}</code></div>}
          {job.last_error &&
            <div className="error wrap">last error: {job.last_error}</div>}
          {job.progress && <ProgressPanel p={job.progress} />}
          <div className="log-head">
            <strong>driver output</strong>
            <span className="muted">
              {log ? `${log.split('\n').length} lines` : 'nothing yet'}
              {job.state === 'running' && ' · live'}
            </span>
            <button className="link" onClick={() => setTall(t => !t)}>
              {tall ? 'shorter' : 'taller'}</button>
          </div>
          <pre className={`log${tall ? ' log-tall' : ''}`} ref={logRef}>
            {log || '(no output yet)'}</pre>
        </td></tr>
      )}
    </>
  )
}

// ---------- runs ledger ----------

// A result may retain job provenance, but its status belongs to the result.
// A waiting or resumed job must never make completed data look "waiting".
function ResultTable({ detail, display }) {
  const rawPoints = detail.points || []
  if (!rawPoints.length) return <p className="muted">No indexed measurements.</p>
  const value = (point, name) => point.dims?.[name] ?? point.metrics?.[name]
  const dims = (display?.dims || []).filter(d => d.name !== 'max_in_flight' &&
    rawPoints.some(p => value(p, d.name) != null))
  const compare = (a, b) => {
    if (a == null) return b == null ? 0 : 1
    if (b == null) return -1
    if (typeof a === 'number' && typeof b === 'number') return a - b
    return String(a).localeCompare(String(b), undefined, { numeric: true })
  }
  const points = [...rawPoints].sort((a, b) => {
    for (const dim of dims) {
      const order = compare(value(a, dim.name), value(b, dim.name))
      if (order) return order
    }
    return compare(a.rate, b.rate) || compare(a.trial, b.trial) || a.id - b.id
  })
  const metrics = (display?.metrics || [])
    .filter(m => points.some(p => p.metrics?.[m.name] != null))
    .sort((a, b) => (a.name === 'offered_rate' ? -1 : 0) -
      (b.name === 'offered_rate' ? -1 : 0))

  return (
    <div className="table-scroll"><table className="result-points">
      <thead><tr>
        {dims.map(d => <th key={d.name} title={d.description}>{d.label || d.name}</th>)}
        <th>rate</th>
        {metrics.map(m => <th key={m.name}>{m.label || m.name}</th>)}
      </tr></thead>
      <tbody>{points.map(p => (
        <tr key={p.id}>
          {dims.map(d => <td key={d.name}>{fmtResultValue(value(p, d.name))}</td>)}
          <td>{fmtResultValue(p.rate)}</td>
          {metrics.map(m => <td key={m.name}>{fmtResultValue(p.metrics?.[m.name])}</td>)}
        </tr>
      ))}</tbody>
    </table></div>
  )
}

function RunEntry({ entry, onChanged, display }) {
  const { job, run } = entry
  const { notify, ask, askText } = useUI()
  const [open, setOpen] = useState(false)
  const [detail, setDetail] = useState(null)
  const [log, setLog] = useState(null)
  const [note, setNote] = useState('')

  useEffect(() => {
    if (!open) return
    if (run) api.run(run.id).then(setDetail).catch(() => {})
    if (job) api.jobLog(job.id, 500).then(r => setLog(r.log)).catch(() => {})
  }, [open, run?.id, job?.id])

  // `refresh: false` for actions that remove the run — re-reading it would
  // 404 on the row we just deleted.
  async function act(fn, refresh = true) {
    try {
      await fn()
      if (run && refresh) setDetail(await api.run(run.id))
      onChanged()
    } catch (e) { notify(e.message || e, 'error') }
  }

  const state = run.status || 'recorded'
  const chipColor = STATE_COLORS[state] || 'gray'
  const what = run.experiment || 'result'
  const when = run.started_at

  return (
    <>
      <tr className="job-row" onClick={() => setOpen(!open)}>
        <td className="muted">r{run.id}</td>
        <td className="mono">{what}</td>
        <td><Chip text={state || '?'} color={chipColor} />
          {detail?.mixed_build &&
            <span className="chip chip-orange" title={
              (detail.builds || []).map(b =>
                `${b.started_at} ${String(b.git_commit).slice(0, 8)}`
                + (b.git_dirty ? ' (dirty)' : '')).join('\n')
            }>mixed build</span>}
          {!run.dir_exists &&
            <span className="muted" title="result directory deleted; summary retained"> dir gone</span>}
        </td>
        <td className="muted">{fmtTs(when)}</td>
        <td>{(run?.tags || []).map(t => <Chip key={t} text={t} color="blue" />)}</td>
        <td onClick={e => e.stopPropagation()}>
          {job?.state === 'done' && (
            <button className="link" title="submit the same job again in a fresh run dir"
                    onClick={() => act(() => api.resubmit(job.id, false))}>rerun</button>
          )}
          {job?.spec.sweep && (
            <button className="link" title="save this one-off's params as a preset"
                    onClick={async () => {
                      const n = await askText('preset name:')
                      if (n) act(() => api.saveSweep(job.project, n.trim(), job.spec.sweep))
                    }}>save</button>
          )}
          <button className="link" onClick={() => setOpen(!open)}>
            {open ? 'hide' : 'details'}</button>
          <button className="link"
                  title="remove this entry: its ledger run and/or job record (files on disk stay; a scan re-indexes them)"
                  onClick={async () => {
                    const what = `result r${run.id}`
                    const failed = run.status === 'failed'
                    if (!failed && !await ask(
                          `Delete ${what} and its data in ${run.run_dir}? `
                          + 'The measurements are not recoverable.', true)) return
                    setOpen(false)
                    await act(() => api.deleteRun(run.id, true), false)
                    notify(`deleted ${what} and its data`)
                  }}>delete</button>
        </td>
      </tr>
      {open && (
        <tr><td colSpan="6" className="log-cell">
          {job && (
            <div className="muted">job #{job.id} · queue {job.spec.queue || job.project}
              {job.spec.cluster && <> · lease on {job.spec.cluster}</>}
              {job.run_dir && <> · dir <code>{job.run_dir}</code></>}
              <div className="mono command-line">{(job.spec.command || []).join(' ') ||
                (job.spec.experiments || []).join(' ')}</div>
            </div>
          )}
          {!job && <div className="muted">dir: <code>{run.run_dir}</code>
            {run.git_commit && <> · commit <code>{run.git_commit.slice(0, 10)}</code></>}</div>}
          {detail === null && <p className="muted">loading result…</p>}
          {detail && (
            <>
              <div className="run-actions" onClick={e => e.stopPropagation()}>
                <input placeholder="add note (why it ran, what it showed)"
                       value={note} onChange={e => setNote(e.target.value)} />
                <button className="link" onClick={() => {
                  if (note.trim()) act(() => api.addNote(run.id, note.trim()).then(() => setNote('')))
                }}>note</button>
                <button className="link" onClick={async () => {
                  const t = await askText('tag:')
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
              <ResultTable detail={detail} display={display} />
            </>
          )}
          {job && (
            <details className="logs" open={!run}>
              <summary>
                driver output
                <span className="muted">
                  {' · '}{log ? `${log.split('\n').length} lines` : 'none'}</span>
              </summary>
              <pre className="log log-tall">{log || '(no output)'}</pre>
            </details>
          )}
        </td></tr>
      )}
    </>
  )
}

function RunsSection({ projects, entries, onChanged, display }) {
  const [scanBusy, setScanBusy] = useState(false)
  const { notify } = useUI()

  async function scan(name) {
    setScanBusy(true)
    try {
      const r = await api.scanRuns(name)
      onChanged()
      notify(`${name}: ${r.added} added, ${r.updated} refreshed`)
    } catch (e) { notify(e.message || e, 'error') } finally { setScanBusy(false) }
  }

  return (
    <section>
      <h2>Results
        {projects.map(p => (
          <button key={p.name} className="link" disabled={scanBusy}
                  title={`index existing run dirs under ${p.name}'s runs roots`}
                  onClick={() => scan(p.name)}>scan {p.name}</button>
        ))}
      </h2>
      {!entries.length
        ? <p className="muted">No recorded results yet — use scan to index old result directories.</p>
        : <div className="table-scroll"><table>
            <thead><tr>
              <th>id</th><th>experiment</th><th>status</th>
              <th>when</th><th>tags</th><th></th>
            </tr></thead>
            <tbody>
              {entries.map(e => (
                <RunEntry key={`r${e.run.id}`}
                          entry={e} onChanged={onChanged} display={display} />
              ))}
            </tbody>
          </table></div>}
    </section>
  )
}

// ---------- daemon log ----------

// The events table is the daemon's audit trail (every job/cluster/lease
// state change goes through the hub); this is its readable tail.
function DaemonLog() {
  const [open, setOpen] = useState(false)
  const [lines, setLines] = useState([])

  const load = useCallback(() => {
    api.daemonLog(200).then(setLines).catch(() => {})
  }, [])
  useEffect(() => {
    if (!open) return
    load()
    const t = setInterval(load, 5000)
    return () => clearInterval(t)
  }, [open, load])

  const skip = new Set(['id', 'ts', 'type', 'job_id', 'cluster_id', 'cluster'])
  return (
    <section>
      <h2>Daemon log
        <button className="link" onClick={() => setOpen(!open)}>
          {open ? 'hide' : 'show'}</button>
      </h2>
      {open && (
        <pre className="log">
          {lines.map(e => {
            const extra = Object.entries(e)
              .filter(([k]) => !skip.has(k))
              .map(([k, v]) => `${k}=${typeof v === 'object' ? JSON.stringify(v) : v}`)
              .join(' ')
            return `${(e.ts || '').slice(5, 19)}  ${e.type}`
              + (e.job_id != null ? `  job#${e.job_id}` : '')
              + (e.cluster ? `  ${e.cluster}` : '')
              + (extra ? `  ${extra}` : '')
          }).join('\n') || '(no events yet)'}
        </pre>
      )}
    </section>
  )
}

// ---------- app ----------

export default function App() {
  return <UIProvider><Dashboard /></UIProvider>
}

function Dashboard() {
  const { notify, ask, askText } = useUI()
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

  // The catalog feeds the submit form.
  useEffect(() => {
    if (!selected) return
    setCatalog(null)
    api.experiments(selected).then(setCatalog)
      .catch(() => setCatalog({ experiments: [], aggregates: {}, error: 'unreachable' }))
  }, [selected])

  const reload = useCallback(async () => {
    try {
      const [j, c, r, h] = await Promise.all([
        api.jobs(), api.clusters(), api.runs(), api.health()])
      setJobs(j); setClusters(c); setRuns(r); setHealth(h)
    } catch { /* daemon down; the SSE handler flips the dot */ }
  }, [])

  const reloadClusters = useCallback(() => {
    api.clusters().then(setClusters).catch(() => {})
  }, [])

  useEffect(() => {
    api.health().then(setHealth).catch(() => {})
    api.projects().then(setProjects).catch(() => {})
    reload()
    let es = null
    let retry = null
    let stopped = false
    const connect = () => {
      if (stopped) return
      es = new EventSource('/api/stream')
      es.onopen = () => { setConnected(true); reload() }
      es.onmessage = () => reload() // any state change → refetch (cheap at this scale)
      es.onerror = () => {
        setConnected(false)
        // A non-200 (the proxy's 502 while the daemon restarts) closes an
        // EventSource PERMANENTLY — the built-in retry only covers drops of
        // an established stream — so recreate it ourselves.
        if (es.readyState === EventSource.CLOSED) {
          retry = setTimeout(connect, 5000)
        }
      }
    }
    connect()
    const poll = setInterval(reload, 30000)
    const clusterPoll = setInterval(reloadClusters, 5000)
    return () => {
      stopped = true
      if (es) es.close()
      clearTimeout(retry)
      clearInterval(poll)
      clearInterval(clusterPoll)
    }
  }, [reload, reloadClusters])

  async function clusterAction(kind, c, ttl) {
    try {
      if (kind === 'up') await api.clusterUp(c.key, ttl || 120)
      else if (kind === 'down') {
        if (c.used_by &&
            !await ask(`${c.key} is in use by ${c.used_by} — force it down?`, true)) return
        await api.clusterDown(c.key, Boolean(c.used_by))
      } else if (kind === 'refresh') await api.clusterRefresh(c.key)
      reload()
    } catch (e) { notify(e.message || e, 'error') }
  }

  // Everything below the tabs is scoped to one project.
  const projJobs = jobs.filter(j => j.project === selected)
  // Anything not done is work in flight: a job waiting out its next attempt
  // is still the same job, in the queue.
  const inFlight = j => j.state !== 'done'
  // Queue shows dispatch order: running/starting first, then queued by
  // priority (high first) and age.
  const rank = j => (j.state === 'running' || j.bringing_up) ? 0 : 1
  const active = projJobs.filter(inFlight).sort((a, b) =>
    rank(a) - rank(b) || (b.priority - a.priority) || (a.id - b.id))
  const projClusters = clusters.filter(c => c.project === selected)
  const projRuns = runs.filter(r => r.project === selected)
  const projInfo = projects.find(p => p.name === selected)

  // Results are results, not job-history rows. A job reference is retained
  // only as provenance for the details panel.
  const jobById = Object.fromEntries(projJobs.map(j => [j.id, j]))
  const entries = projRuns
    .map(r => ({ job: jobById[r.job_id] || null, run: r }))
    .sort((a, b) => (b.run.started_at || '').localeCompare(a.run.started_at || ''))

  async function moveJob(id, how) {
    // The cluster-acquiring job is included: it owns the queue slot but has
    // not started, so the daemon can still hand the slot (and the fleet) to
    // whoever this reorder puts in front of it. A running job cannot move.
    const queued = active
      .filter(j => j.state === 'waiting')
      .map(j => j.id)
    const i = queued.indexOf(id)
    if (i < 0) return
    let order = [...queued]
    if (how === 'top') { order.splice(i, 1); order.unshift(id) }
    else if (how === 'up' && i > 0) { [order[i - 1], order[i]] = [order[i], order[i - 1]] }
    else if (how === 'down' && i < order.length - 1) {
      [order[i + 1], order[i]] = [order[i], order[i + 1]]
    } else return
    try { await api.reorderJobs(order); reload() }
    catch (e) { notify(e.message || e, 'error') }
  }

  return (
    <div className="app">
      <header>
        <h1>cloud-kitchen</h1>
        <nav className="tabs">
          {projects.map(p => {
            const n = jobs.filter(j => j.project === p.name &&
              j.state !== 'done').length
            return (
              <button key={p.name}
                      className={`tab ${p.name === selected ? 'tab-on' : ''}`}
                      onClick={() => pickProject(p.name)}>
                {p.name}{n > 0 && <span className="tab-count">{n}</span>}
              </button>
            )
          })}
        </nav>
        {projInfo?.publish_url && (
          <a className="pub-link" href={projInfo.publish_url} target="_blank" rel="noreferrer"
             title={`${selected}'s published pages (figures, reports)`}>published ↗</a>
        )}
        <span className={`dot ${connected ? 'dot-on' : 'dot-off'}`}
              title={connected ? 'live' : 'disconnected'} />
        {health && <span className="muted">v{health.version}</span>}
        {health && (
          <button className="link"
                  title={health.paused
                    ? 'start jobs again'
                    : 'stop starting jobs; anything running is left alone'}
                  onClick={async () => {
                    try {
                      setHealth(await api.setPaused(!health.paused)
                        .then(r => ({ ...health, paused: r.paused })))
                    } catch (e) { notify(e.message || e, 'error') }
                  }}>
            {health.paused ? '▶ resume queue' : '❚❚ pause queue'}</button>
        )}
        {health?.paused &&
          <Chip text="paused" color="orange" />}
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
        <h2>Queue
          {selected && (
            <button className="link"
                    title="delete failed, canceled, and interrupted jobs; results are kept"
                    onClick={async () => {
                      if (!await ask(`Delete ${selected}'s failed, canceled, and `
                                     + 'interrupted jobs and their working directories?', true)) return
                      try {
                        const r = await api.purgeJobs(selected); reload()
                        notify(`purged ${r.purged.length} job(s)`
                          + (r.removed_dirs.length ? `, ${r.removed_dirs.length} directories` : ''))
                      } catch (e) { notify(e.message || e, 'error') }
                    }}>purge failed</button>
          )}
        </h2>
        {selected && (
          <SubmitForm project={selected}
                      clusters={(projInfo?.clusters) || []}
                      catalog={catalog}
                      onSubmitted={reload} />
        )}
        <JobTable jobs={active} onChanged={reload}
                  onMove={moveJob} empty="Nothing queued or running." />
      </section>

      <RunsSection projects={projects.filter(p => p.name === selected)}
                   entries={entries} onChanged={reload} display={catalog?.display} />

      <DaemonLog />
    </div>
  )
}

function JobTable({ jobs, onChanged, empty, onMove }) {
  if (!jobs.length) return <p className="muted">{empty}</p>
  const movable = jobs.filter(j => j.state === 'waiting')
  const movableIndex = Object.fromEntries(movable.map((j, i) => [j.id, i]))
  return (
    <div className="queue-table"><table>
      <thead><tr>
        <th>id</th><th>experiment</th>
        <th>state</th><th>priority</th><th>created</th><th></th>
      </tr></thead>
      <tbody>
        {jobs.map((j, i) => <JobRow key={j.id} job={j} isHead={i === 0}
                                  onChanged={onChanged} onMove={onMove}
                                  canMoveUp={movableIndex[j.id] > 0}
                                  canMoveDown={movableIndex[j.id] >= 0 &&
                                    movableIndex[j.id] < movable.length - 1} />)}
      </tbody>
    </table></div>
  )
}
