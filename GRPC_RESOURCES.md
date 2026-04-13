# gRPC and low-latency streaming — reference repos

## Which stream is “best”? What BigAl should use

There is no single winner — they solve slightly different jobs:

| Approach | Strengths | Weaknesses |
|----------|-----------|------------|
| **Yellowstone (Geyser gRPC)** | Open standard; same client patterns across **Triton, QuickNode, self-hosted** validators; very flexible filters (accounts, txs, slots). | Official Rust `yellowstone-grpc-*` releases have historically lagged **Solana SDK major versions**. Proto pulls a **second** Solana stack (e.g. 2.x) and **OpenSSL** on some targets — painful on **Windows MSVC** without vcpkg/OpenSSL. |
| **Helius LaserStream** | **Already integrated** in BigAl (`sidecar/sidecar_grpc.js` → ZMQ → Python). Optimized **Helius** path; one API key; no Rust dependency churn for streaming. | **Vendor-specific** — tied to Helius endpoints and product shape. |

**Recommendation for this repo**

1. **Treat Helius LaserStream as the primary “fast gRPC” path** for strategy ingestion (what you run on the server today). It *is* gRPC and is appropriate for competing on reaction time when you use Helius RPC anyway.
2. **Treat Yellowstone as the portable standard** to add later if you need the **same** subscription code against **non-Helius** Geyser endpoints, or want streaming **inside the Rust binary**. Enable it only after pinning compatible `yellowstone-grpc-*` + Solana SDK versions and accepting **Linux + libssl-dev** (or OpenSSL setup) for that build.

**Execution** (signing, Jito, JSON-RPC) stays separate from either stream — streaming only helps you **see** state earlier, not replace `instruction_builder` / Jito.

---

How this maps to **BigAl**:

| Path | Role |
|------|------|
| **`sidecar/sidecar_grpc.js`** | **Helius LaserStream** (gRPC) → parses Pump.fun txs → ZMQ → Python `grpc_zmq_listener`. **Preferred default** for low-latency events. |
| **`rust/bigal-executor/src/grpc_observer.rs`** | Optional **Yellowstone** stub for Rust-side Geyser. **Not wired in default `Cargo.toml`** (dependency/version/OpenSSL friction). |
| **Rust `instruction_builder.rs`** | **Not gRPC** — raw Pump.fun instructions over JSON-RPC + Jito. Validate against official IDL/docs below. |

---

## Yellowstone Geyser gRPC (Solana-wide streaming)

- **[rpcpool/yellowstone-grpc](https://github.com/rpcpool/yellowstone-grpc)** — Triton-maintained Geyser plugin + proto + **Rust examples** (`examples/rust`): subscribe to accounts, txs, slots; unary helpers.
- **[helius-labs/yellowstone-grpc](https://github.com/helius-labs/yellowstone-grpc)** — Helius fork; useful if you standardize on Helius infra.
- **[QuickNode Yellowstone gRPC (Rust) overview](https://www.quicknode.com/docs/solana/yellowstone-grpc/overview/rust)** — third-party walkthrough for the same client patterns.

**BigAl:** optional Rust integration is stubbed in `grpc_observer.rs`. Uncomment deps + `grpc` feature in `rust/bigal-executor/Cargo.toml` only when you accept a **second** Solana SDK in the tree and OpenSSL (typical on **Ubuntu** with `libssl-dev`, not default on Windows MSVC).

---

## Helius LaserStream (what the Node sidecar uses)

- **[helius-labs/laserstream-sdk](https://github.com/helius-labs/laserstream-sdk)** — multi-language SDK; **JavaScript** client: [`javascript/`](https://github.com/helius-labs/laserstream-sdk/tree/main/javascript) (npm `helius-laserstream`).
- **[Helius LaserStream docs](https://www.helius.dev/docs/laserstream/clients)** — endpoints, auth, subscription types.

**BigAl:** `sidecar/package.json` depends on `helius-laserstream`; set `HELIUS_API_KEY`, optional `HELIUS_ENDPOINT`.

---

## Pump.fun program — correctness for Rust txs

Use these to verify **discriminators, account order, and args** (not for streaming):

- **[pump-fun/pump-public-docs](https://github.com/pump-fun/pump-public-docs)** — [`docs/PUMP_PROGRAM_README.md`](https://github.com/pump-fun/pump-public-docs/blob/main/docs/PUMP_PROGRAM_README.md) (program id, global PDA, bonding curve, instruction semantics).
- **Official Anchor IDL:** [`pump-public-docs/idl`](https://github.com/pump-fun/pump-public-docs/tree/main/idl) — authoritative account layouts for `buy` / `sell` / `create`.

**Community Rust SDKs** (cross-check only; verify against IDL above):

- [0xfnzero/pumpfun-sdk](https://github.com/0xfnzero/pumpfun-sdk) — Rust + bundling notes.
- [crlosif/pumpfun-solana-sdk](https://github.com/crlosif/pumpfun-solana-sdk) — instruction builders.

---

## Jito (bundles; HTTP JSON-RPC to block engine)

- **[jito-foundation/jito-repos](https://github.com/jito-foundation)** — official clients/docs; BigAl uses HTTP `sendBundle` in `rust/bigal-executor/src/jito_bundler.rs`.

---

### Sanity checklist for “are our txs correct?”

1. Program id `6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P` matches docs.
2. `buy` ix: token `amount` + `max_sol_cost`; `sell` ix: token `amount` + `min_sol_output` (lamports).
3. `fee_recipient` is one of the pubkeys allowed by **Global** (see pump docs — BigAl default in `config.rs` matches a listed recipient).
4. Bonding curve PDA seeds `["bonding-curve", mint]`; global `["global"]` — match IDL.

When Pump.fun upgrades the program, diff the **IDL** first, then adjust `instruction_builder.rs`.
