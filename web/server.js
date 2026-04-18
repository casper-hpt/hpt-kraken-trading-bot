import express from 'express'
import { readFile, writeFile, mkdir } from 'fs/promises'
import { existsSync } from 'fs'
import path from 'path'
import { fileURLToPath } from 'url'
import Docker from 'dockerode'

const __dirname = path.dirname(fileURLToPath(import.meta.url))
const app = express()
app.use(express.json())

const WATCHLIST_PATH = process.env.WATCHLIST_PATH
  ?? path.join(__dirname, '../auto/data-collector/crypto_watchlist.json')
const ENV_PATH       = process.env.ENV_PATH       ?? path.join(__dirname, '../auto/.env')
const COMPOSE_PROJECT = process.env.COMPOSE_PROJECT ?? 'auto'
const PORT           = process.env.PORT            ?? 3001

const KNOWN_SERVICES = ['questdb', 'data-collector', 'trader', 'prometheus', 'grafana']
const SECRET_KEYS    = new Set(['KRAKEN_API_KEY', 'KRAKEN_API_SECRET'])
const CONFIG_KEYS    = ['KRAKEN_API_KEY', 'KRAKEN_API_SECRET', 'DRY_RUN', 'MAX_POSITIONS', 'STOP_LOSS_PCT']

// Universe is read from the file once and cached for the process lifetime so
// removed symbols can still be shown as inactive in the UI.
let _universe = null
async function getUniverse() {
  if (!_universe) _universe = JSON.parse(await readFile(WATCHLIST_PATH, 'utf8'))
  return _universe
}

// ── .env helpers ──────────────────────────────────────────────────────────────

function parseEnv(text) {
  const env = {}
  for (const raw of text.split('\n')) {
    const line = raw.trim()
    if (!line || line.startsWith('#') || !line.includes('=')) continue
    const [key, ...rest] = line.split('=')
    env[key.trim()] = rest.join('=').split('#')[0].trim()
  }
  return env
}

function serializeEnv(env) {
  return Object.entries(env).map(([k, v]) => `${k}=${v}`).join('\n') + '\n'
}

async function readEnv() {
  try { return parseEnv(await readFile(ENV_PATH, 'utf8')) } catch { return {} }
}

async function writeEnv(env) {
  await mkdir(path.dirname(ENV_PATH), { recursive: true })
  await writeFile(ENV_PATH, serializeEnv(env))
}

// ── Watchlist ─────────────────────────────────────────────────────────────────

app.get('/api/watchlist', async (req, res) => {
  try {
    const universe = await getUniverse()
    const active = JSON.parse(await readFile(WATCHLIST_PATH, 'utf8'))
    res.json({ universe, active })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

app.post('/api/watchlist', async (req, res) => {
  const symbols = [...new Set(
    (req.body.symbols ?? []).map(s => s.toUpperCase().trim()).filter(Boolean)
  )].sort()
  await writeFile(WATCHLIST_PATH, JSON.stringify(symbols, null, 4) + '\n')
  res.json({ symbols })
})

// ── Config (.env) ─────────────────────────────────────────────────────────────

app.get('/api/config', async (req, res) => {
  const env = await readEnv()
  const result = {}
  for (const k of CONFIG_KEYS) {
    result[k] = SECRET_KEYS.has(k) && env[k] ? '***' : (env[k] ?? null)
  }
  res.json(result)
})

app.post('/api/config', async (req, res) => {
  const env = await readEnv()
  for (const [key, value] of Object.entries(req.body)) {
    if (value === null || value === undefined) continue
    if (SECRET_KEYS.has(key) && (value === '***' || value === '')) continue
    env[key] = value
  }
  await writeEnv(env)
  res.json({ ok: true })
})

// ── Services (Docker) ─────────────────────────────────────────────────────────

const docker = new Docker({ socketPath: '/var/run/docker.sock' })

app.get('/api/services', async (req, res) => {
  try {
    const list = await docker.listContainers({ all: true })
    const byName = Object.fromEntries(
      list.flatMap(c => c.Names.map(n => [n.replace(/^\//, ''), c]))
    )
    res.json(KNOWN_SERVICES.map(svc => ({
      name: svc,
      status: byName[`${COMPOSE_PROJECT}-${svc}-1`]?.State ?? 'not found',
    })))
  } catch (err) {
    res.json(KNOWN_SERVICES.map(name => ({ name, status: 'unavailable', error: err.message })))
  }
})

app.post('/api/services/:service', async (req, res) => {
  const { service } = req.params
  const { action } = req.body
  if (!KNOWN_SERVICES.includes(service)) return res.status(400).json({ error: 'Unknown service' })
  if (!['start', 'stop', 'restart'].includes(action)) return res.status(400).json({ error: 'Unknown action' })
  try {
    const container = docker.getContainer(`${COMPOSE_PROJECT}-${service}-1`)
    await container[action]()
    res.json({ ok: true })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// ── Serve built frontend in production ────────────────────────────────────────

const dist = path.join(__dirname, 'dist')
if (existsSync(dist)) {
  app.use(express.static(dist))
  app.get('*', (req, res) => res.sendFile(path.join(dist, 'index.html')))
}

app.listen(PORT, () => console.log(`api  http://localhost:${PORT}`))
