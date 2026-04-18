import { useState, useEffect } from 'react'

export default function Watchlist() {
  const [universe, setUniverse] = useState([])
  const [active, setActive] = useState(new Set())
  const [status, setStatus] = useState(null)

  useEffect(() => {
    fetch('/api/watchlist')
      .then(r => r.json())
      .then(({ universe, active }) => {
        setUniverse(universe)
        setActive(new Set(active))
      })
      .catch(() => {})
  }, [])

  const toggle = async sym => {
    const next = new Set(active)
    if (next.has(sym)) next.delete(sym)
    else next.add(sym)
    setActive(next)
    setStatus(null)
    try {
      const res = await fetch('/api/watchlist', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ symbols: [...next] }),
      })
      if (!res.ok) throw new Error(await res.text())
      setStatus('saved')
      setTimeout(() => setStatus(null), 1500)
    } catch (err) {
      setStatus('error:' + err.message)
    }
  }

  const colSize = Math.ceil(universe.length / 3)
  const columns = [
    universe.slice(0, colSize),
    universe.slice(colSize, colSize * 2),
    universe.slice(colSize * 2),
  ]

  return (
    <div className="card">
      <h2>
        Watchlist
        <span className="watchlist-count">{active.size} / {universe.length} active</span>
      </h2>

      <div className="sym-grid">
        {columns.map((col, i) => (
          <div key={i} className="sym-col">
            {col.map(sym => (
              <button
                key={sym}
                className={`sym-pill ${active.has(sym) ? 'sym-on' : 'sym-off'}`}
                onClick={() => toggle(sym)}
                title={active.has(sym) ? `Remove ${sym}` : `Re-add ${sym}`}
              >
                <span className="sym-dot" />
                {sym}
              </button>
            ))}
          </div>
        ))}
      </div>

      {status === 'saved' && <p className="success-msg">Saved</p>}
      {status?.startsWith('error:') && <p className="error-msg">{status.slice(6)}</p>}
    </div>
  )
}
