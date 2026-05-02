# AGENTS.md — BitShares Networks

Visualizes BitShares DEX liquidity pools and node latency as interactive HTML maps. Connects to BitShares via WebSocket RPC, renders with PyVis and matplotlib.

## Commands

```bash
pip install -r requirements.txt   # README has typo: missing "install"
cd pools
python3 pool_mapper.py            # interactive CLI menu; optional arg = output filename
python3 latency_test.py           # several minutes; pings hundreds of nodes
python3 main.py                   # CI orchestrator: runs all three tools in parallel
```

## Architecture

- All Python code lives in `pools/` — run from that directory (bare imports: `from config import ...`).
- No test framework, linter, formatter, or type checker exists.
- `rpc.py` uses `websocket-client` (sync) and `aiohttp` (async for latency test).
- `utilities.py:json_ipc()` — custom concurrent read/write cache in `pools/pipe/*.txt`. Auto-creates `pipe/` dir.
- `main.py` spawns three processes and hard-terminates them after ~1 hour. CI-only, not interactive.
- `config.py:DEV = False` — set `True` for verbose `dprint()` output.

## CI / GitHub Pages

- Workflow: `.github/workflows/main.yml` (daily at midnight UTC)
- Python 3.11, caches pip dependencies
- Accumulates images from `gh-pages` branch before running `main.py`
- Deploys by copying `pools/website/*` to root of `gh-pages` branch (not `pools/` itself)
- Uses `git push --force-with-lease` — local gh-pages changes will be overwritten

## Config (`config.py`)

- `NODES` — 9 WSS endpoints (shuffled, first responsive wins)
- `DETACH` — asset IDs to exclude from map
- `ATTACH` — specific pool IDs to include exclusively
- `CHUNK = 10` — RPC batch size
- `SCALE_WEIGHT = 80` — edge thickness divisor
- `DETACH_UNFUNDED = False` — set `True` to hide empty pools

## Gotchas

- `pipe/` cache directory **is committed** — serves as an aggregate for 3rd party readers. Never gitignore it.
- `pool_mapper.py` and `latency_test.py` use bare module imports — must `cd pools` before running.
- Elasticsearch query in `rpc.py` hits `https://es.bitshares.dev` to find max pool object ID (live external dependency).
