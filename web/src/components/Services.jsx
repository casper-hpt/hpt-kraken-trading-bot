import { useState, useEffect } from 'react'

const DOT_COLOR = {
  running: '#1a9950',
  exited: '#d93025',
  stopped: '#d93025',
  paused: '#f5a623',
  restarting: '#f5a623',
}

export default function Services() {
  const [services, setServices] = useState([])
  const [busy, setBusy] = useState({})

  const load = () =>
    fetch('/api/services')
      .then(r => r.json())
      .then(setServices)
      .catch(() => {})

  useEffect(() => {
    load()
    const id = setInterval(load, 5000)
    return () => clearInterval(id)
  }, [])

  const act = async (name, action) => {
    setBusy(b => ({ ...b, [name]: action }))
    try {
      await fetch(`/api/services/${name}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ action }),
      })
    } finally {
      setTimeout(() => {
        load()
        setBusy(b => { const n = { ...b }; delete n[name]; return n })
      }, 800)
    }
  }

  return (
    <div className="card">
      <h2>Services</h2>
      {services.length === 0 && (
        <p style={{ fontSize: 14, color: '#999' }}>No service info available — is the Docker socket mounted?</p>
      )}
      {services.map(svc => (
        <div key={svc.name} className="service-row">
          <div className="service-info">
            <span
              className="status-dot"
              style={{ background: DOT_COLOR[svc.status] ?? '#bbb' }}
            />
            <span className="service-name">{svc.name}</span>
            <span className="service-status">{svc.status}</span>
          </div>
          <div className="service-actions">
            {['start', 'stop', 'restart'].map(a => (
              <button
                key={a}
                className="btn btn-sm btn-outline"
                disabled={!!busy[svc.name]}
                onClick={() => act(svc.name, a)}
              >
                {busy[svc.name] === a ? '…' : a}
              </button>
            ))}
          </div>
        </div>
      ))}
    </div>
  )
}
