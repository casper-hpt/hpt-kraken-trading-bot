import { useState, useEffect } from 'react'

const FIELDS = [
  { key: 'KRAKEN_API_KEY', label: 'API Key', type: 'password', placeholder: 'Kraken API key' },
  { key: 'KRAKEN_API_SECRET', label: 'API Secret', type: 'password', placeholder: 'Kraken API secret' },
  { key: 'MAX_POSITIONS', label: 'Max Positions', type: 'number', placeholder: '5' },
  { key: 'STOP_LOSS_PCT', label: 'Stop Loss %', type: 'number', placeholder: '0.08' },
]

export default function Config() {
  const [form, setForm] = useState({})
  const [dryRun, setDryRun] = useState(false)
  const [status, setStatus] = useState(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    fetch('/api/config')
      .then(r => r.json())
      .then(data => {
        setForm(data)
        setDryRun(data.DRY_RUN === 'true')
        setLoading(false)
      })
      .catch(() => setLoading(false))
  }, [])

  const set = (key, value) => {
    setForm(f => ({ ...f, [key]: value }))
    setStatus(null)
  }

  const handleSubmit = async e => {
    e.preventDefault()
    setStatus(null)
    try {
      const payload = { ...form, DRY_RUN: String(dryRun) }
      const res = await fetch('/api/config', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      })
      if (!res.ok) throw new Error(await res.text())
      setStatus('saved')
      setTimeout(() => setStatus(null), 3000)
    } catch (err) {
      setStatus('error:' + err.message)
    }
  }

  if (loading) return null

  return (
    <div className="card">
      <h2>Bot Settings</h2>
      <form onSubmit={handleSubmit}>
        {FIELDS.map(f => (
          <div key={f.key} className="form-group">
            <label>{f.label}</label>
            <input
              type={f.type}
              placeholder={
                f.type === 'password' && form[f.key] === '***'
                  ? '(already set — leave blank to keep)'
                  : f.placeholder
              }
              value={
                form[f.key] === null || form[f.key] === undefined || form[f.key] === '***'
                  ? ''
                  : form[f.key]
              }
              onChange={e => set(f.key, e.target.value)}
              autoComplete="off"
              spellCheck={false}
            />
          </div>
        ))}

        <div className="form-group">
          <label>Dry Run</label>
          <div className="toggle-row">
            <label className="toggle">
              <input
                type="checkbox"
                checked={dryRun}
                onChange={e => { setDryRun(e.target.checked); setStatus(null) }}
              />
              <span className="toggle-slider" />
            </label>
            <span className="toggle-label">
              {dryRun ? 'Paper trading — no real orders sent' : 'Live trading'}
            </span>
          </div>
        </div>

        <div className="msg-row">
          <button type="submit" className="btn btn-primary">Save</button>
          {status === 'saved' && <span className="success-msg">Saved</span>}
          {status?.startsWith('error:') && (
            <span className="error-msg">{status.slice(6)}</span>
          )}
        </div>
      </form>
    </div>
  )
}
