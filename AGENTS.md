# AGENTS.md — BitShares Networks

## What This Repo Does

Visualizes the BitShares DEX liquidity pool network and node latency as interactive HTML maps. Connects to BitShares blockchain via WebSocket to public RPC nodes, fetches pool/asset data, and renders with PyVis (network graph) and matplotlib (latency map).

## Repo Structure

```
/
  README.md
  requirements.txt
  images/
  pools/
    pool_mapper.py      # Main entry: liquidity pool network visualization
    latency_test.py     # BitShares node latency testing + geolocation
    main.py             # CI orchestrator: runs pool_mapper + latency_test + awesome_scraper in parallel
    config.py           # Node list, colors, DETACH/ATTACH pool config, visual settings
    rpc.py              # WebSocket RPC + Elasticsearch queries for BitShares chain
    utilities.py        # JSON IPC (concurrent read/write cache), helpers
    bitshares_nodes.py  # Hardcoded lists of known BitShares public nodes
    awesome_scraper.py  # Generates animated GIF from latency map history
    pipe/               # Runtime cache directory (created at runtime, contains .txt JSON files)
    latency_maps/       # Saved latency map PNGs
    *.html / *.css      # Generated output + styles
```

## Commands

### Setup
```bash
pip install -r requirements.txt
cd pools
```
Note: README says `pip3 -r requirements.txt` — this is a typo, missing `install`.

### Run Pool Mapper (interactive CLI menu)
```bash
cd pools
python3 pool_mapper.py
```
Optional: pass an output filename as arg 1 (default: `liquidity_pools.html`).

### Run Latency Test
```bash
cd pools
python3 latency_test.py
```
Takes several minutes — pings hundreds of BitShares nodes via WebSocket.

### Run Full CI Update (pool map + latency + scraper)
```bash
cd pools
python3 main.py
```
Runs all three visualizers in parallel with timeouts, then terminates. Used by the daily GitHub Actions workflow.

## Architecture Notes

- **All Python code lives in `pools/`** — the root is just docs, images, and config.
- **No test framework, linter, formatter, or type checker** exists. Do not invent one without asking.
- **RPC layer** (`rpc.py`) uses `websocket-client` for sync connections and `aiohttp` for async (latency_test uses async).
- **Elasticsearch query** in `rpc.py` hits `https://es.bitshares.dev` to find the max pool object ID — this is a live external dependency.
- **JSON IPC** (`utilities.py:json_ipc`) is a custom concurrent read/write mechanism that stores cache as `.txt` files in `pools/pipe/`. It uses exponential-backoff retries and a tag-based clipping protocol. Auto-creates the `pipe/` dir if missing.
- **Caching** — pool data, asset names, tickers, and share assets are cached to `pools/pipe/*.txt` across runs. The cache persists and is incrementally updated.
- **`main.py`** runs three processes concurrently and hard-terminates them after ~1 hour. Designed for CI, not interactive use.
- **`config.py:DEV = False`** — set to `True` to enable verbose `dprint()` output.

## CI / GitHub Pages

- Workflow: `.github/workflows/main.yml`
- Triggers: daily at midnight UTC + manual dispatch
- Python 3.9 on ubuntu-latest
- Accumulates images from `gh-pages` branch before running `main.py`
- Force-pushes to `gh-pages` and deploys via GitHub Pages Actions
- The generated HTML output is served from the `pools/` directory on GitHub Pages

## Config Quirks (`config.py`)

- `NODES` — 9 WSS endpoints used by pool_mapper (shuffled, first responsive wins)
- `DETACH` — asset IDs to exclude from map in "DETACH" menu mode
- `ATTACH` — specific pool IDs to include in "ATTACH only" menu mode
- `CHUNK = 10` — RPC calls fetch 10 objects at a time
- `SCALE_WEIGHT = 80` — divisor for edge thickness scaling
- `DETACH_UNFUNDED = False` — set True to hide empty pools

## Gotchas

- The `pipe/` cache directory **is committed** — it serves as an aggregate that 3rd party programs read. Keep it in version control; never gitignore it.
- `pool_mapper.py` imports from `config`, `rpc`, `utilities` as bare names — must be run from `pools/` directory (or have it on `PYTHONPATH`).
- `latency_test.py` has the same issue — it imports `bitshares_nodes` as a bare module.
- The workflow uses `git push --force-with-lease` to `gh-pages` — any local gh-pages changes will be overwritten.
