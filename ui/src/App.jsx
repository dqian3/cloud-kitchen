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

function SubmitForm({ projects, onSubmitted }) {
  const [project, setProject] = useState('')
  const [catalog, setCatalog] = useState(null)   // {experiments, aggregates, error}
  const [selected, setSelected] = useState([])
  const [freeText, setFreeText] = useState('')
  const [flags, setFlags] = useState('')
  const [priority, setPriority] = useState(0)
  const [retries, setRetries] = useState(2)
  const [err, setErr] = useState(null)

  useEffect(() => {
    if (!project && projects.length) setProject(projects[0].name)
  }, [projects])

  useEffect(() => {
    if (!project) return
    setCatalog(null); setSelected([])
    api.experiments(project).then(setCatalog)
      .catch(() => setCatalog({ experiments: [], aggregates: {}, error: 'unreachable' }))
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
    const experiments = hasCatalog
      ? selected
      : freeText.split(/\s+/).filter(Boolean)
    if (!experiments.length) { setErr('pick at least one experiment'); return }
    try {
      await api.submit({
        project,
        experiments,
        extra_flags: flags.split(/\s+/).filter(Boolean),
        priority: +priority,
        max_retries: +retries,
      })
      setSelected([]); setFreeText('')
      onSubmitted()
    } catch (e2) { setErr(String(e2.message || e2)) }
  }

  return (
    <form className="submit-form-block" onSubmit={submit}>
      <div className="submit-form">
        <select value={project} onChange={e => setProject(e.target.value)}>
          {projects.map(p => <option key={p.name}>{p.name}</option>)}
        </select>
        {!hasCatalog && (
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
          queue job{selected.length > 1 ? ` (${selected.length})` : ''}
        </button>
        {err && <span className="error">{err}</span>}
      </div>

      {hasCatalog && (
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
                           title={e.description}>
                      <input type="checkbox" disabled={disabled}
                             checked={selected.includes(e.name)}
                             onChange={() => toggle(e.name)} />
                      {e.name}
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
        <td>{spec.queue || job.project}</td>
        <td>{job.priority !== 0 ? job.priority : ''}</td>
        <td><Chip text={job.state} color={STATE_COLORS[job.state]} />
          {job.exit_code != null && job.exit_code !== 0 &&
            <span className="muted"> rc={job.exit_code}</span>}
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
        </td>
      </tr>
      {open && (
        <tr><td colSpan="9" className="log-cell">
          {job.run_dir && <div className="muted">run dir: <code>{job.run_dir}</code></div>}
          <pre className="log">{log || '(no output yet)'}</pre>
        </td></tr>
      )}
    </>
  )
}

// ---------- app ----------

export default function App() {
  const [health, setHealth] = useState(null)
  const [projects, setProjects] = useState([])
  const [jobs, setJobs] = useState([])
  const [clusters, setClusters] = useState([])
  const [connected, setConnected] = useState(false)

  const reload = useCallback(async () => {
    try {
      const [j, c] = await Promise.all([api.jobs(), api.clusters()])
      setJobs(j); setClusters(c)
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

  const active = jobs.filter(j => ['queued', 'running'].includes(j.state))
  const done = jobs.filter(j => !['queued', 'running'].includes(j.state))

  return (
    <div className="app">
      <header>
        <h1>cloud-kitchen</h1>
        <span className={`dot ${connected ? 'dot-on' : 'dot-off'}`}
              title={connected ? 'live' : 'disconnected'} />
        {health && <span className="muted">v{health.version}</span>}
      </header>

      <section>
        <h2>Clusters</h2>
        {clusters.length === 0 && <p className="muted">
          No clusters configured — add them to ~/.cloud-kitchen/config.toml.</p>}
        <div className="cards">
          {clusters.map(c => (
            <ClusterCard key={c.key} c={c} onAction={clusterAction} />
          ))}
        </div>
      </section>

      <section>
        <h2>Queue</h2>
        <SubmitForm projects={projects} onSubmitted={reload} />
        <JobTable jobs={active} onChanged={reload}
                  empty="Nothing queued or running." />
      </section>

      <section>
        <h2>History</h2>
        <JobTable jobs={done} onChanged={reload} empty="No finished jobs yet." />
      </section>
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
