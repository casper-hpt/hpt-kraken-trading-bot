import { useState } from 'react'
import Config from './components/Config'
import Watchlist from './components/Watchlist'
import Services from './components/Services'
import './App.css'

const TABS = ['Settings', 'Watchlist', 'Services']

export default function App() {
  const [tab, setTab] = useState(0)

  return (
    <div className="app">
      <header>
        <h1>HPT Kraken Bot</h1>
        <nav>
          {TABS.map((t, i) => (
            <button
              key={t}
              className={i === tab ? 'active' : ''}
              onClick={() => setTab(i)}
            >
              {t}
            </button>
          ))}
        </nav>
      </header>
      <main>
        {tab === 0 && <Config />}
        {tab === 1 && <Watchlist />}
        {tab === 2 && <Services />}
      </main>
    </div>
  )
}
