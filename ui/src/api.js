async function req(path, opts = {}) {
  const res = await fetch(path, {
    headers: { 'Content-Type': 'application/json' },
    ...opts,
  })
  if (!res.ok) {
    let detail = res.statusText
    try { detail = (await res.json()).detail ?? detail } catch { /* ignore */ }
    throw new Error(detail)
  }
  return res.json()
}

export const api = {
  health: () => req('/api/health'),
  projects: () => req('/api/projects'),
  experiments: (project) => req(`/api/experiments?project=${encodeURIComponent(project)}`),
  jobs: () => req('/api/jobs?limit=100'),
  job: (id) => req(`/api/jobs/${id}`),
  jobLog: (id, tail = 200) => req(`/api/jobs/${id}/log?tail=${tail}`),
  submit: (spec) => req('/api/jobs', { method: 'POST', body: JSON.stringify(spec) }),
  editJob: (id, updates) => req(`/api/jobs/${id}`, { method: 'PATCH', body: JSON.stringify(updates) }),
  purgeJobs: (project) => req('/api/jobs/purge', { method: 'POST', body: JSON.stringify({ project }) }),
  reorderJobs: (ids) => req('/api/jobs/reorder', { method: 'POST', body: JSON.stringify({ ids }) }),
  cancel: (id) => req(`/api/jobs/${id}/cancel`, { method: 'POST', body: '{}' }),
  resubmit: (id, resume = true) =>
    req(`/api/jobs/${id}/resubmit`, { method: 'POST', body: JSON.stringify({ resume }) }),
  sweeps: (project) => req(`/api/sweeps?project=${encodeURIComponent(project)}`),
  saveSweep: (project, name, params) =>
    req('/api/sweeps', { method: 'POST', body: JSON.stringify({ project, name, params }) }),
  deleteSweep: (id) => req(`/api/sweeps/${id}`, { method: 'DELETE' }),
  runs: (project) =>
    req(`/api/runs?limit=100${project ? `&project=${encodeURIComponent(project)}` : ''}`),
  run: (id) => req(`/api/runs/${id}`),
  deleteJob: (id, files = false) =>
    req(`/api/jobs/${id}?delete_files=${files}`, { method: 'DELETE' }),
  deleteRun: (id, files = false) =>
    req(`/api/runs/${id}?delete_files=${files}`, { method: 'DELETE' }),
  addNote: (id, text) =>
    req(`/api/runs/${id}/notes`, { method: 'POST', body: JSON.stringify({ text }) }),
  addTag: (id, name) =>
    req(`/api/runs/${id}/tags`, { method: 'POST', body: JSON.stringify({ name }) }),
  removeTag: (id, name) =>
    req(`/api/runs/${id}/tags/${encodeURIComponent(name)}`, { method: 'DELETE' }),
  scanRuns: (project) =>
    req('/api/runs/scan', { method: 'POST', body: JSON.stringify({ project }) }),
  clusters: () => req('/api/clusters'),
  clusterUp: (key, ttl) =>
    req(`/api/clusters/${key}/up`, { method: 'POST', body: JSON.stringify({ ttl_minutes: ttl }) }),
  clusterDown: (key, force = false) =>
    req(`/api/clusters/${key}/down`, { method: 'POST', body: JSON.stringify({ force }) }),
  clusterRefresh: (key) => req(`/api/clusters/${key}/refresh`, { method: 'POST', body: '{}' }),
  clusterCreate: (key) => req(`/api/clusters/${key}/create`, { method: 'POST', body: '{}' }),
  clusterCreateCancel: (key) => req(`/api/clusters/${key}/create/cancel`, { method: 'POST', body: '{}' }),
  daemonLog: (limit = 200) => req(`/api/logs?limit=${limit}`),
  clusterExtend: (key, leaseId, ttl) =>
    req(`/api/clusters/${key}/extend`, {
      method: 'POST',
      body: JSON.stringify({ lease_id: leaseId, ttl_minutes: ttl }),
    }),
}
