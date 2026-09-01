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
  jobLog: (id, tail = 200) => req(`/api/jobs/${id}/log?tail=${tail}`),
  submit: (spec) => req('/api/jobs', { method: 'POST', body: JSON.stringify(spec) }),
  purgeJobs: (project) => req('/api/jobs/purge', { method: 'POST', body: JSON.stringify({ project }) }),
  reorderJobs: (ids) => req('/api/jobs/reorder', { method: 'POST', body: JSON.stringify({ ids }) }),
  cancel: (id) => req(`/api/jobs/${id}/cancel`, { method: 'POST', body: '{}' }),
  setPaused: (paused) => req('/api/pause', {
    method: 'POST', body: JSON.stringify({ paused }) }),
  resubmit: (id, resume = true) =>
    req(`/api/jobs/${id}/resubmit`, { method: 'POST', body: JSON.stringify({ resume }) }),
  runs: (project) =>
    req(`/api/runs?limit=100${project ? `&project=${encodeURIComponent(project)}` : ''}`),
  run: (id) => req(`/api/runs/${id}`),
  addTrials: (id, trials) =>
    req(`/api/runs/${id}/trials`, {
      method: 'POST', body: JSON.stringify({ trials }) }),
  retryPoint: (runId, pointId) =>
    req(`/api/runs/${runId}/points/${pointId}/retry`, {
      method: 'POST', body: '{}' }),
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
  daemonLog: (limit = 200) => req(`/api/logs?limit=${limit}`),
}
