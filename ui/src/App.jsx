import React, { useCallback, useEffect, useRef, useState } from 'react'
import { api } from './api.js'

const STATE_COLORS = {
  queued: 'gray', starting: 'blue', running: 'blue', succeeded: 'green',
  degraded: 'orange', failed: 'red', canceled: 'gray', interrupted: 'purple',
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

// Minutes until a daemon timestamp ("YYYY-MM-DD HH:MM:SS", UTC).
function fmtUntil(ts) {
  const ms = Date.parse(ts.replace(' ', 'T') + 'Z') - Date.now()
  return ms <= 0 ? 'moments' : `${Math.max(1, Math.round(ms / 60000))}m`
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
        {c.hold_for != null && (
          <div className="muted" title="kept up past its last lease for the next job on this cluster">
            held for job #{c.hold_for}
          </div>
        )}
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
            <button onClick={() => onAction('up', c, null, ttl)}>up (TTL {ttl}m)</button>
          </>
        )}
        {(c.state === 'running' || c.state === 'unmanaged') && (
          <button title={c.state === 'unmanaged'
                   ? 'stop the VMs running outside daemon leases'
                   : undefined}
                  onClick={() => onAction('down', c)}>down</button>
        )}
        <button onClick={() => onAction('refresh', c)}>refresh VMs</button>
        {c.create && c.state !== 'running' && !busy && !c.create.running && (
          <button title="provision the VMs via the repo's setup script — creates instances (money); restartable, existing VMs are skipped"
                  onClick={() => onAction('create', c)}>create VMs</button>
        )}
        {c.create?.running && (
          <>
            <span className="muted">
              provisioning · attempt {c.create.attempt}/{c.create.max_attempts}
              {c.create.missing?.length > 0 && ` · ${c.create.missing.length} VMs missing`}
              {c.create.next_at &&
                ` · next in ${Math.max(1, Math.round((c.create.next_at * 1000 - Date.now()) / 60000))}m`}
            </span>
            <button className="link" onClick={() => onAction('create-cancel', c)}>stop</button>
          </>
        )}
        {busy && <span className="muted">waiting for gcloud…</span>}
      </div>
      {c.create && !c.create.running && c.create.missing?.length > 0 && (
        <div className="muted" title={c.create.missing.join(', ')}>
          provisioning stopped with {c.create.missing.length} VM(s) missing
        </div>
      )}
      {c.create && (c.create.running || c.create.log_tail?.length > 0) && (
        <details className="raw" open={c.create.running}>
          <summary>
            create log{c.create.rc != null && !c.create.running &&
              ` (exited ${c.create.rc})`}
          </summary>
          <pre>{(c.create.log_tail || []).join('\n') || '(no output yet)'}</pre>
        </details>
      )}
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
                         ? `${e.description}\n\nnative: runs on the SweepEngine as its own job; the daemon leases ${e.queue} around it`
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

function JobRow({ job, onChanged, reorder, onMove, inherits }) {
  const { notify, askText } = useUI()
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
    try { await fn(); onChanged() } catch (e) { notify(e.message || e, 'error') }
  }

  const queued = job.state === 'queued'
  const retrying = job.state === 'failed' && job.retry_at
  const spec = job.spec

  return (
    <>
      <tr className="job-row" onClick={() => setOpen(!open)}>
        <td>{job.id}</td>
        <td className="mono">{(spec.experiments || []).join(' ') ||
          (spec.command || []).join(' ')}</td>
        <td>{spec.queue || job.project}
          {spec.cluster && <span title={`daemon-managed lease on ${spec.cluster}`}> ⚙</span>}
        </td>
        <td><Chip text={retrying ? 'retrying' : job.state}
                  color={retrying ? 'orange' : STATE_COLORS[job.state]} />
          {job.state === 'starting' &&
            <span className="muted"> {spec.cluster ? `${spec.cluster} coming up` : 'launching'}</span>}
          {retrying &&
            <span className="muted" title={`retry at ${job.retry_at} UTC`}>
              {' '}in {fmtUntil(job.retry_at)} · {job.retries_left} left</span>}
          {queued && spec.after &&
            <span className="muted" title="waits for that job's retry chain">
              {' '}after #{spec.after}</span>}
          {queued && inherits &&
            <span className="muted" title="same cluster as the job ahead of it: the VMs are handed over, not cycled">
              {' '}↳ inherits lease</span>}
          {job.state === 'running' && job.progress?.points &&
            <span className="muted"> {job.progress.points.done}
              {(job.progress.totals_final?.points_total ?? job.progress.est_points)
                ? `/${job.progress.totals_final?.points_total ?? job.progress.est_points}`
                : ''} pts</span>}
        </td>
        <td className="muted">{fmtTs(job.created_at)}</td>
        <td onClick={e => e.stopPropagation()}>
          {queued && !reorder && (
            <button className="link" title="run this next"
                    onClick={() => onMove(job.id, 'top')}>top</button>
          )}
          {queued && reorder && (
            <>
              <button className="link" onClick={() => onMove(job.id, 'up')}>▲</button>
              <button className="link" onClick={() => onMove(job.id, 'down')}>▼</button>
            </>
          )}
          {queued && !reorder && (
            <button className="link" onClick={async () => {
              const exp = await askText('experiments (space-separated):',
                (spec.experiments || []).join(' '))
              if (exp !== null) {
                act(() => api.editJob(job.id,
                  { experiments: exp.split(/\s+/).filter(Boolean) }))
              }
            }}>edit</button>
          )}
          {(queued || job.state === 'starting' || job.state === 'running' || retrying) && (
            <button className="link" onClick={() => act(() => api.cancel(job.id))}>
              cancel</button>
          )}
        </td>
      </tr>
      {open && (
        <tr><td colSpan="6" className="log-cell">
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

// One entry per piece of work: a finished job joined to its ledger run.
// Either side can be missing — a job that died before producing data has no
// run; a backfilled old run has no job — and the row says which.
function RunEntry({ entry, display, onChanged }) {
  const { job, run } = entry
  const { notify, ask, askText } = useUI()
  const [open, setOpen] = useState(false)
  const [detail, setDetail] = useState(null)
  const [log, setLog] = useState(null)
  const [note, setNote] = useState('')
  const [sortBy, setSortBy] = useState('rate')   // 'rate' | 'time'

  useEffect(() => {
    if (!open) return
    if (run) api.run(run.id).then(setDetail).catch(() => {})
    if (job) api.jobLog(job.id, 60).then(r => setLog(r.log)).catch(() => {})
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

  const metricCols = display?.metrics?.length
    ? display.metrics.map(m => [m.name, m.unit ? `${m.label} (${m.unit})` : m.label])
    : POINT_METRIC_COLS
  const cols = detail
    ? metricCols.filter(([k]) => detail.points.some(p => k in p.metrics))
    : []
  // Points arrive in the order they ran; the natural reading order is by
  // dims, then numeric rate, then trial.
  const points = !detail ? [] : sortBy === 'time' ? detail.points
    : [...detail.points].sort((a, b) => {
        const da = JSON.stringify(a.dims), db = JSON.stringify(b.dims)
        if (da !== db) return da < db ? -1 : 1
        const ra = a.rate ?? -Infinity, rb = b.rate ?? -Infinity
        if (ra !== rb) return ra - rb
        return (a.trial ?? 0) - (b.trial ?? 0)
      })

  const state = job ? job.state : run.status
  const chipColor = job
    ? STATE_COLORS[job.state]
    : { ok: 'green', degraded: 'orange', failed: 'red', running: 'blue',
        interrupted: 'purple' }[run.status] || 'gray'
  const what = job
    ? ((job.spec.experiments || []).join(' ') || (job.spec.command || []).slice(1, 3).join(' '))
    : run.experiment
  const when = job ? (job.finished_at || job.created_at) : run.started_at

  return (
    <>
      <tr className="job-row" onClick={() => setOpen(!open)}>
        <td className="muted">{job ? `#${job.id}` : ''}{run ? ` r${run.id}` : ''}</td>
        <td className="mono">{what}</td>
        <td><Chip text={state || '?'} color={chipColor} />
          {job && job.exit_code != null && job.exit_code !== 0 &&
            <span className="muted"> rc={job.exit_code}</span>}
        </td>
        <td>
          {run
            ? <>{run.n_points ?? 0} pts
                {!run.dir_exists && <span className="muted" title="run directory deleted; metrics kept in the ledger"> · dir gone</span>}</>
            : <span className="muted" title="no ledger entry: the job produced no summaries">no results</span>}
          {!job && <span className="muted" title="indexed from disk; no daemon job record"> · no job record</span>}
        </td>
        <td className="muted">{fmtTs(when)}</td>
        <td>{(run?.tags || []).map(t => <Chip key={t} text={t} color="blue" />)}</td>
        <td onClick={e => e.stopPropagation()}>
          {job && (
            <button className="link" title="submit the same job again in a fresh run dir"
                    onClick={() => act(() => api.resubmit(job.id, false))}>rerun</button>
          )}
          {job && ['degraded', 'failed', 'interrupted', 'canceled'].includes(job.state) && job.run_dir && (
            <button className="link" title="resubmit, resuming into the same run dir"
                    onClick={() => act(() => api.resubmit(job.id, true))}>resume</button>
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
                    const what = [run && `run r${run.id}`, job && `job #${job.id}`]
                      .filter(Boolean).join(' and ')
                    // A failed run is only worth keeping to rerun it, so it
                    // goes on one click; anything that produced measurements
                    // confirms — including an imported run, which has no job
                    // row but is somebody's data.
                    const failed = ['failed', 'canceled', 'interrupted']
                      .includes(job ? job.state : run.status)
                    if (!failed && !await ask(
                          `Delete ${what} and its data in ${job?.run_dir || run?.run_dir}? `
                          + 'The measurements are not recoverable.', true)) return
                    setOpen(false)
                    await act(async () => {
                      if (run) await api.deleteRun(run.id, true)
                      if (job) await api.deleteJob(job.id, true)
                    }, false)
                    notify(`deleted ${what} and its data`)
                  }}>delete</button>
        </td>
      </tr>
      {open && (
        <tr><td colSpan="7" className="log-cell">
          {job && (
            <div className="muted">job #{job.id} · queue {job.spec.queue || job.project}
              {job.spec.cluster && <> · lease on {job.spec.cluster}</>}
              {job.run_dir && <> · dir <code>{job.run_dir}</code></>}
              <div className="mono">{(job.spec.command || []).join(' ') ||
                (job.spec.experiments || []).join(' ')}</div>
            </div>
          )}
          {!job && run && <div className="muted">dir: <code>{run.run_dir}</code>
            {run.git_commit && <> · commit <code>{run.git_commit.slice(0, 10)}</code></>}</div>}
          {job?.progress && <ProgressPanel p={job.progress} />}
          {run && detail === null && <p className="muted">loading results…</p>}
          {run && detail && (
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
              {detail.points.length === 0
                ? <p className="muted">no points recorded</p>
                : <table className="points">
                    <thead><tr>
                      <th>dims</th>
                      <th>rate
                        <button className="link" title="toggle point order: by rate or by when they ran"
                                onClick={e => { e.stopPropagation(); setSortBy(sortBy === 'rate' ? 'time' : 'rate') }}>
                          {sortBy === 'rate' ? '↑' : '⏱'}</button>
                      </th><th>trial</th>
                      {cols.map(([k, label]) => <th key={k}>{label}</th>)}
                      <th></th>
                    </tr></thead>
                    <tbody>
                      {points.map(p => (
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
                  </table>}
            </>
          )}
          {job && (
            <details className="raw" open={!run}>
              <summary>driver output{log === '' ? ' (none)' : ''}</summary>
              <pre className="log">{log || '(no output)'}</pre>
            </details>
          )}
        </td></tr>
      )}
    </>
  )
}

function RunsSection({ projects, entries, display, onChanged }) {
  const [scanBusy, setScanBusy] = useState(false)
  const { notify, ask } = useUI()

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
      <h2>Runs
        {projects.map(p => (
          <button key={p.name} className="link" disabled={scanBusy}
                  title={`index existing run dirs under ${p.name}'s runs roots`}
                  onClick={() => scan(p.name)}>scan {p.name}</button>
        ))}
        {projects.map(p => (
          <button key={`purge-${p.name}`} className="link"
                  title="delete failed, canceled, and interrupted jobs (ledger runs are kept)"
                  onClick={async () => {
                    if (!await ask(`Delete ${p.name}'s failed, canceled, and `
                                   + 'interrupted jobs and everything they wrote?', true)) return
                    try {
                      const r = await api.purgeJobs(p.name); onChanged()
                      notify(`purged ${r.purged.length} job(s)`
                             + (r.removed_dirs.length ? `, ${r.removed_dirs.length} run dir(s)` : ''))
                    }
                    catch (e) { notify(e.message || e, 'error') }
                  }}>purge failed</button>
        ))}
      </h2>
      {!entries.length
        ? <p className="muted">No finished jobs or recorded runs yet — use scan to
            index old run dirs.</p>
        : <table>
            <thead><tr>
              <th>id</th><th>experiment</th><th>status</th>
              <th>results</th><th>when</th><th>tags</th><th></th>
            </tr></thead>
            <tbody>
              {entries.map(e => (
                <RunEntry key={e.job ? `j${e.job.id}` : `r${e.run.id}`}
                          entry={e} display={display} onChanged={onChanged} />
              ))}
            </tbody>
          </table>}
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
    return () => {
      stopped = true
      if (es) es.close()
      clearTimeout(retry)
      clearInterval(poll)
    }
  }, [reload])

  async function clusterAction(kind, c, lease, ttl) {
    try {
      if (kind === 'up') await api.clusterUp(c.key, ttl || 120)
      else if (kind === 'down') {
        if (c.leases.length &&
            !await ask(`${c.key} has live leases — force it down?`, true)) return
        await api.clusterDown(c.key, c.leases.length > 0)
      } else if (kind === 'refresh') await api.clusterRefresh(c.key)
      else if (kind === 'create') {
        if (!await ask(`Provision ${c.key}'s VMs? This CREATES instances `
                       + '(billed). Existing VMs are skipped.')) return
        await api.clusterCreate(c.key)
        notify(`provisioning ${c.key}`)
      } else if (kind === 'create-cancel') {
        await api.clusterCreateCancel(c.key)
      } else if (kind === 'extend') {
        const m = await askText('extend lease by minutes:', '120')
        if (m) await api.clusterExtend(c.key, lease.id, +m)
      }
      reload()
    } catch (e) { notify(e.message || e, 'error') }
  }

  // Everything below the tabs is scoped to one project.
  const projJobs = jobs.filter(j => j.project === selected)
  // A failed job with a pending retry is still work in flight: it stays in
  // the queue until the retry requeues (as a child) or the chain is canceled.
  const inFlight = j => ['queued', 'starting', 'running'].includes(j.state) ||
    (j.state === 'failed' && j.retry_at)
  // Queue shows dispatch order: running/starting first, then queued by
  // priority (high first) and age, then pending retries.
  const rank = j => j.state === 'running' ? 0 : j.state === 'starting' ? 1
    : j.state === 'queued' ? 2 : 3
  const active = projJobs.filter(inFlight).sort((a, b) =>
    rank(a) - rank(b) || (b.priority - a.priority) || (a.id - b.id))
  const done = projJobs.filter(j => !inFlight(j))
  const projClusters = clusters.filter(c => c.project === selected)
  const projRuns = runs.filter(r => r.project === selected)
  const projInfo = projects.find(p => p.name === selected)

  // Finished jobs joined to their ledger runs, plus runs with no job.
  const runByJob = {}
  for (const r of projRuns) if (r.job_id) runByJob[r.job_id] = r
  const entries = [
    ...done.map(j => ({ job: j, run: runByJob[j.id] || null })),
    ...projRuns.filter(r => !r.job_id || !projJobs.some(j => j.id === r.job_id))
      .map(r => ({ job: null, run: r })),
  ].sort((a, b) => {
    const ta = a.job ? (a.job.finished_at || a.job.created_at) : a.run.started_at
    const tb = b.job ? (b.job.finished_at || b.job.created_at) : b.run.started_at
    return (tb || '').localeCompare(ta || '')
  })

  const [reorder, setReorder] = useState(false)
  async function moveJob(id, how) {
    const queued = active.filter(j => j.state === 'queued').map(j => j.id)
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
              ['queued', 'starting', 'running'].includes(j.state)).length
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
        <h2>Queue
          {active.some(j => j.state === 'queued') && (
            <button className="link" onClick={() => setReorder(!reorder)}>
              {reorder ? 'done' : 'reorder'}</button>
          )}
        </h2>
        {selected && (
          <SubmitForm project={selected}
                      clusters={(projInfo?.clusters) || []}
                      catalog={catalog}
                      onSubmitted={reload} />
        )}
        <JobTable jobs={active} onChanged={reload} reorder={reorder}
                  onMove={moveJob} empty="Nothing queued or running." />
      </section>

      <RunsSection projects={projects.filter(p => p.name === selected)}
                   entries={entries} display={catalog?.display}
                   onChanged={reload} />

      <DaemonLog />
    </div>
  )
}

function JobTable({ jobs, onChanged, empty, reorder, onMove }) {
  if (!jobs.length) return <p className="muted">{empty}</p>
  // A queued job inherits the lease when the job right ahead of it in
  // dispatch order runs on the same cluster.
  const inherits = new Set()
  for (let i = 1; i < jobs.length; i++) {
    const prev = jobs[i - 1], cur = jobs[i]
    if (cur.state === 'queued' && cur.spec.cluster &&
        cur.spec.cluster === prev.spec.cluster &&
        ['running', 'starting', 'queued'].includes(prev.state)) inherits.add(cur.id)
  }
  return (
    <table>
      <thead><tr>
        <th>id</th><th>experiments</th><th>queue</th>
        <th>state</th><th>created</th><th></th>
      </tr></thead>
      <tbody>
        {jobs.map(j => <JobRow key={j.id} job={j} onChanged={onChanged}
                               reorder={reorder} onMove={onMove}
                               inherits={inherits.has(j.id)} />)}
      </tbody>
    </table>
  )
}
