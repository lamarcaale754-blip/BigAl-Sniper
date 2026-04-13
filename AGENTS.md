# BigAl Sniper & Rug Puller

## Architecture

Python strategy brain + Rust execution engine for Solana Pump.fun meme coin trading and counter-MEV operations.

| Layer | Language | Purpose |
|-------|----------|---------|
| Strategy | Python (`bigalbot_customizable.py`) | Entry filters, exit logic, moonbag, anti-manipulation, dashboard WS server |
| Execution | Rust (`rust/bigal-executor/`) | Raw Pump.fun instructions, Jito bundles, dynamic fees, kill chain |
| IPC | ZeroMQ (ports 5555/5556) | Python PUSH → Rust PULL (signals), Rust PUB → Python SUB (results) |
| Dashboard | HTML (`bigal_dashboard_customizable.html`) | Dark UI, 30+ sliders, Chart.js, startup modal, real-time positions |
| Hunter Pipeline | Python (5 files) | Wallet profiling → pattern cracking → honeypot planning → puppet wallets → kill |

## Execution Modes

Selected via dashboard startup modal (no CLI prompts):
1. **Paper** — `PaperExecutor`, simulated, no wallet needed
2. **Live (Legacy)** — `LegacyExecutor`, PumpPortal HTTP API
3. **Live (Rust)** — `ZmqExecutor` → Rust binary, raw instructions + Jito bundles
4. **Rug Puller (Test)** — Stages 1-3 analysis only, simulated deployments
5. **Rug Puller (Live)** — Full 5-stage pipeline with real transactions

## File Map

### Core Bot
| File | Role |
|------|------|
| `bigalbot_customizable.py` | Main bot — strategy, state, dashboard WS server (1883 lines) |
| `executor.py` | `ExecutorBackend` ABC + `PaperExecutor`, `LegacyExecutor`, `ZmqExecutor` |
| `zmq_bridge.py` | ZMQ PUSH/SUB layer — Python ↔ Rust IPC |
| `bigal_dashboard_customizable.html` | Full dashboard UI with config modal |

### Hunter Pipeline (Rug Puller)
| File | Stage | Role |
|------|-------|------|
| `wallet_profiler.py` | 1 | Passive wallet profiling + Sybil co-occurrence detection |
| `pattern_cracker.py` | 2 | ML-based bot filter reverse engineering (RandomForest) |
| `honeypot_planner.py` | 3 | Bait token plan generation from solved bot signatures |
| `puppet_master.py` | 4 | Multi-wallet orchestration for organic-looking volume |
| `rug_orchestrator.py` | 1-5 | Coordination loop across all stages |

### Rust Executor
| File | Role |
|------|------|
| `rust/bigal-executor/src/main.rs` | Tokio runtime, async ZMQ signal loop |
| `rust/bigal-executor/src/config.rs` | Env var loading, keypair parsing (base58 + JSON) |
| `rust/bigal-executor/src/zmq_bridge.rs` | Async PULL/PUB via pure-Rust `zeromq` crate |
| `rust/bigal-executor/src/rpc.rs` | Lightweight Solana JSON-RPC client (no `solana-client` dep) |
| `rust/bigal-executor/src/instruction_builder.rs` | Pump.fun buy/sell with PDA derivation + bonding curve math |
| `rust/bigal-executor/src/jito_bundler.rs` | Jito bundle via JSON-RPC, tip account rotation |
| `rust/bigal-executor/src/fee_estimator.rs` | Polls `getRecentPrioritizationFees`, returns p90 × 1.1 |
| `rust/bigal-executor/src/kill_chain.rs` | Atomic sell-all + tip via Jito (0ms reaction window) |
| `rust/bigal-executor/src/grpc_observer.rs` | Yellowstone gRPC stub (awaiting SDK 4.x compatible release) |

### Config & Data
| File | Role |
|------|------|
| `custom_presets.json` | 4 strategy parameter presets |
| `blacklist_creators.json` | Known rug creator wallets with loss amounts |
| `trade_journal.csv` | Trade history log |
| `FILTER_GUIDE.md` | Dashboard parameter explanations |

## Critical Patterns

- **No awaits between global state changes** in `handle_mode_select()` — build executor first, then swap `MODE_LIVE`/`EXECUTOR_MODE`/`executor` atomically
- **`pos.closing` flag** prevents double-close race between `position_monitor` and `handle_trade_event`
- **Inner try/catch** around message handlers in PumpPortal WS loop — one bad message must not crash the reconnect loop
- **ZMQ sockets are not thread-safe** — `probe_rust()` uses a separate temporary SUB socket, never shares with `_listen_loop`
- **`mode_selected_event`** is set in `finally` block to prevent `run_bot` from hanging forever
- **Rust signal loop is sequential** — one signal at a time matches Python's 3-second wait timeout

## ZMQ Protocol

**Signal (Python → Rust):** `{"id":"uuid", "action":"buy|sell|ping|kill", "mint":"...", "amount":0.5, "amount_type":"sol|pct", "slippage_bps":1000, "priority":"normal|urgent"}`

**Result (Rust → Python):** `{"id":"uuid", "status":"confirmed|failed|pong", "signature":"...", "method":"jito|rpc", "latency":{"zmq_recv_ms":1, "build_ms":150, "land_ms":2100, "total_ms":2250}}`

## Data Flow

```
PumpPortal WS → handle_new_token → Candidate → maybe_open_from_candidate → Position
                                                    ↓ (buy signal)
                                              executor.buy() → ZMQ → Rust → chain
Position → handle_trade_event (price updates, velocity check, TP/SL)
                ↓ (exit signal)
          executor.sell() → ZMQ → Rust → chain
```

## Don'ts

- Don't use `asyncio.to_thread()` for `get_live_balance` — it's already async via executor
- Don't read the shared ZMQ SUB socket from multiple threads
- Don't add `await` calls between setting `MODE_LIVE`/`EXECUTOR_MODE`/`executor` globals
- Don't catch `Exception` with bare `pass` — always log the error
- Don't assume `live_sell_pump` return value is checked — callers fire-and-forget
- Don't add `solana-client` to Cargo.toml — use `rpc.rs` instead (avoids beta dep conflicts)
- Don't enable `grpc` feature until `yellowstone-grpc-proto` supports `solana-sdk` 4.x

## Rust Build

```bash
cd rust/bigal-executor
cargo build --release
```

Requires: Rust toolchain (`rustup.rs`) + MSVC Build Tools (Windows).

### Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `HELIUS_RPC_URL` | Yes | — | Solana RPC endpoint |
| `BIGAL_PRIVATE_KEY` | Yes* | — | Base58 or JSON array wallet key (*or use `keypair.txt`) |
| `JITO_BLOCK_ENGINE` | No | NY mainnet | Jito block engine URL |
| `GRPC_ENDPOINT` | No | — | Yellowstone gRPC URL |
| `GRPC_X_TOKEN` | No | — | gRPC auth token |
| `ZMQ_SIGNAL_PORT` | No | 5555 | ZMQ signal port |
| `ZMQ_RESULT_PORT` | No | 5556 | ZMQ result port |

## External Dependencies

- **Helius RPC** — Solana blockchain access (`HELIUS_RPC_URL`)
- **PumpPortal WebSocket** — `wss://pumpportal.fun/api/data` for token creation + trade events
- **Jito Block Engine** — MEV-protected bundle submission (NY default)
- **Yellowstone gRPC** — Pre-confirmation transaction streaming (optional, not yet SDK 4.x compatible)
