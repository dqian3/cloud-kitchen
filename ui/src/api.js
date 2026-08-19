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
  jobs: () => req('/api/jobs?limit=100'),
  job: (id) => req(`/api/jobs/${id}`),
  jobLog: (id, tail = 200) => req(`/api/jobs/${id}/log?tail=${tail}`),
  submit: (spec) => req('/api/jobs', { method: 'POST', body: JSON.stringify(spec) }),
  editJob: (id, updates) => req(`/api/jobs/${id}`, { method: 'PATCH', body: JSON.stringify(updates) }),
  cancel: (id) => req(`/api/jobs/${id}/cancel`, { method: 'POST', body: '{}' }),
  resubmit: (id, resume = true) =>
    req(`/api/jobs/${id}/resubmit`, { method: 'POST', body: JSON.stringify({ resume }) }),
  clusters: () => req('/api/clusters'),
  clusterUp: (key, ttl) =>
    req(`/api/clusters/${key}/up`, { method: 'POST', body: JSON.stringify({ ttl_minutes: ttl }) }),
  clusterDown: (key, force = false) =>
    req(`/api/clusters/${key}/down`, { method: 'POST', body: JSON.stringify({ force }) }),
  clusterRefresh: (key) => req(`/api/clusters/${key}/refresh`, { method: 'POST', body: '{}' }),
  clusterExtend: (key, leaseId, ttl) =>
    req(`/api/clusters/${key}/extend`, {
      method: 'POST',
      body: JSON.stringify({ lease_id: leaseId, ttl_minutes: ttl }),
    }),
}
