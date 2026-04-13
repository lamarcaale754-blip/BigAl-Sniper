//! Streaming observers → ZMQ bridge (sidecar-compatible JSON for Python `grpc_zmq_listener`).
//!
//! Two backends (feature-gated, mutually exclusive at runtime — LaserStream preferred):
//!
//! **LaserStream** (`--features laserstream`): Helius native Rust client, 4x faster than Yellowstone,
//! auto-reconnect + slot replay, replaces the Node.js sidecar entirely.
//! Env: `LASERSTREAM_API_KEY` (or `HELIUS_API_KEY`), `LASERSTREAM_ENDPOINT`.
//!
//! **Yellowstone gRPC** (`--features grpc`): Standard Geyser client.
//! Env: `GRPC_ENDPOINT`, `GRPC_X_TOKEN`.

use zeromq::PubSocket;

#[cfg(feature = "grpc")]
use {
    futures::{SinkExt, StreamExt},
    solana_sdk::pubkey::Pubkey,
    std::collections::HashMap,
    std::str::FromStr,
    std::time::Duration,
    tonic::transport::ClientTlsConfig,
    tracing::{debug, error, info, warn},
    yellowstone_grpc_client::GeyserGrpcClient,
    yellowstone_grpc_proto::prelude::{
        subscribe_update::UpdateOneof, CommitmentLevel, CompiledInstruction, Message,
        SubscribeRequest, SubscribeRequestFilterTransactions, SubscribeRequestPing,
        SubscribeUpdateTransaction, TransactionStatusMeta,
    },
    zeromq::{SocketSend, ZmqMessage},
};

#[cfg(feature = "grpc")]
const DISC_CREATE: [u8; 8] = [24, 30, 200, 40, 5, 28, 7, 119];
#[cfg(feature = "grpc")]
const DISC_BUY: [u8; 8] = [102, 6, 61, 18, 1, 218, 235, 234];
#[cfg(feature = "grpc")]
const DISC_SELL: [u8; 8] = [51, 230, 133, 164, 1, 127, 131, 173];

/// Connect to Geyser with the same builder pattern as Yellowstone upstream examples.
#[cfg(feature = "grpc")]
async fn connect_geyser(endpoint: &str, x_token: &str) -> anyhow::Result<yellowstone_grpc_client::GeyserGrpcClient<impl tonic::service::Interceptor>> {
    let mut builder = GeyserGrpcClient::build_from_shared(endpoint.to_string())?
        .x_token(Some(x_token.to_string()))?
        .connect_timeout(Duration::from_secs(20))
        .tcp_nodelay(true)
        .http2_keep_alive_interval(Duration::from_secs(15))
        .keep_alive_while_idle(true)
        .max_decoding_message_size(256 * 1024 * 1024);

    if endpoint.starts_with("https://") {
        builder = builder.tls_config(ClientTlsConfig::new().with_native_roots())?;
    }

    Ok(builder.connect().await?)
}

#[cfg(feature = "grpc")]
fn account_keys_for_message(msg: &Message, meta: Option<&TransactionStatusMeta>) -> Vec<Vec<u8>> {
    let mut keys: Vec<Vec<u8>> = msg.account_keys.clone();
    if msg.versioned {
        if let Some(m) = meta {
            keys.extend(m.loaded_writable_addresses.iter().cloned());
            keys.extend(m.loaded_readonly_addresses.iter().cloned());
        }
    }
    keys
}

#[cfg(feature = "grpc")]
fn key_b58(keys: &[Vec<u8>], idx: usize) -> Option<String> {
    keys.get(idx).map(|b| bs58::encode(b).into_string())
}

#[cfg(feature = "grpc")]
fn ix_idx(ix: &CompiledInstruction, pos: usize) -> Option<usize> {
    ix.accounts.get(pos).map(|b| *b as usize)
}

/// Map Yellowstone tx + meta → same JSON keys as `sidecar_grpc.js` / Python handlers.
#[cfg(feature = "grpc")]
fn pump_event_sidecar_shape(
    pump_program: &Pubkey,
    slot: u64,
    update: &SubscribeUpdateTransaction,
) -> Option<serde_json::Value> {
    let info = update.transaction.as_ref()?;
    let inner = info.transaction.as_ref()?;
    let msg = inner.message.as_ref()?;
    let meta = info.meta.as_ref();

    let keys = account_keys_for_message(msg, meta);
    let pump_bytes = pump_program.to_bytes();

    for ix in &msg.instructions {
        let pidx = ix.program_id_index as usize;
        let prog = keys.get(pidx)?;
        if prog.len() != 32 || prog.as_slice() != pump_bytes.as_slice() {
            continue;
        }
        let data = &ix.data;
        if data.len() < 8 {
            continue;
        }
        let disc: [u8; 8] = data[0..8].try_into().ok()?;

        let tx_type = if disc == DISC_CREATE {
            "create"
        } else if disc == DISC_BUY {
            "buy"
        } else if disc == DISC_SELL {
            "sell"
        } else {
            continue;
        };

        let (mint, trader) = if tx_type == "create" {
            let mint = key_b58(&keys, ix_idx(ix, 0)?)?;
            let trader = key_b58(&keys, ix_idx(ix, 7)?)?;
            (mint, trader)
        } else {
            let mint = key_b58(&keys, ix_idx(ix, 2)?)?;
            let trader = key_b58(&keys, ix_idx(ix, 6)?)?;
            (mint, trader)
        };

        let mut sol_amount = 0.0_f64;
        let mut v_sol_bc = 0.0_f64;
        if let Some(m) = meta {
            if let Some(ti) = keys.iter().position(|k| k.len() == 32 && bs58::encode(k).into_string() == trader) {
                if ti < m.pre_balances.len() && ti < m.post_balances.len() {
                    let d = m.post_balances[ti] as i128 - m.pre_balances[ti] as i128;
                    sol_amount = (d.unsigned_abs() as f64) / 1e9;
                }
            }
            if let Some(bc_i) = if tx_type == "create" {
                ix_idx(ix, 2)
            } else {
                ix_idx(ix, 3)
            } {
                if bc_i < m.post_balances.len() {
                    v_sol_bc = m.post_balances[bc_i] as f64 / 1e9;
                }
            }
        }

        let mut v_tokens = 0.0_f64;
        if let (Some(m), Some(mint_ref)) = (meta, Some(mint.as_str())) {
            for tb in &m.post_token_balances {
                if tb.mint == mint_ref {
                    if let Some(s) = tb.ui_token_amount.as_ref().and_then(|u| u.ui_amount_string.parse::<f64>().ok()) {
                        if s > v_tokens {
                            v_tokens = s;
                        }
                    }
                }
            }
        }

        return Some(serde_json::json!({
            "txType": tx_type,
            "mint": mint,
            "traderPublicKey": trader,
            "solAmount": sol_amount,
            "vSolInBondingCurve": v_sol_bc,
            "vTokensInBondingCurve": v_tokens,
            "slot": slot,
            "_source": "yellowstone",
        }));
    }
    None
}

#[cfg(feature = "grpc")]
fn subscribe_request(
    program_id: &str,
    watch_wallets: &[String],
) -> anyhow::Result<SubscribeRequest> {
    let pump = Pubkey::from_str(program_id)?;
    let mut account_include = vec![program_id.to_string()];
    for w in watch_wallets {
        if !w.is_empty() && !account_include.iter().any(|x| x == w) {
            account_include.push(w.clone());
        }
    }

    let mut transactions = HashMap::new();
    transactions.insert(
        "bigal_pump".to_string(),
        SubscribeRequestFilterTransactions {
            vote: Some(false),
            failed: Some(false),
            account_include,
            ..Default::default()
        },
    );

    Ok(SubscribeRequest {
        transactions,
        commitment: Some(CommitmentLevel::Processed as i32),
        ..Default::default()
    })
}

/// Run Yellowstone stream; forwards sidecar-compatible JSON to ZMQ PUB.
#[cfg(feature = "grpc")]
pub async fn start_grpc_observer(
    endpoint: &str,
    x_token: &str,
    program_id: &str,
    watch_wallets: Vec<String>,
    zmq_pub: std::sync::Arc<tokio::sync::Mutex<PubSocket>>,
) -> anyhow::Result<()> {
    let pump_pk = Pubkey::from_str(program_id)?;
    let mut client = connect_geyser(endpoint, x_token).await?;

    let request = subscribe_request(program_id, &watch_wallets)?;
    info!(
        "Yellowstone subscribe | endpoint={} | program={} | extra_wallets={}",
        endpoint,
        program_id,
        watch_wallets.len()
    );

    let (mut sink, mut stream) = client.subscribe_with_request(Some(request)).await?;

    while let Some(message) = stream.next().await {
        match message {
            Ok(msg) => {
                if let Some(oneof) = msg.update_oneof {
                    match oneof {
                        UpdateOneof::Ping(_) => {
                            let keepalive = SubscribeRequest {
                                ping: Some(SubscribeRequestPing { id: 0 }),
                                ..Default::default()
                            };
                            if let Err(e) = sink.send(keepalive).await {
                                debug!("Yellowstone ping reply send failed: {}", e);
                            }
                        }
                        UpdateOneof::Transaction(tx) => {
                            let slot = tx.slot;
                            if let Some(json) = pump_event_sidecar_shape(&pump_pk, slot, &tx) {
                                let line = json.to_string();
                                let mut sock = zmq_pub.lock().await;
                                if let Err(e) = sock.send(ZmqMessage::from(line)).await {
                                    debug!("Yellowstone ZMQ send failed: {}", e);
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
            Err(e) => {
                error!("Yellowstone stream error: {}", e);
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }

    warn!("Yellowstone stream ended");
    Ok(())
}

/// Without `--features grpc`, Yellowstone is unavailable — caller should not spawn a busy loop.
#[cfg(not(feature = "grpc"))]
pub async fn start_grpc_observer(
    _endpoint: &str,
    _x_token: &str,
    _program_id: &str,
    _watch_wallets: Vec<String>,
    _zmq_pub: std::sync::Arc<tokio::sync::Mutex<PubSocket>>,
) -> anyhow::Result<()> {
    anyhow::bail!(
        "bigal-executor was built without Yellowstone; rebuild with: cargo build --release --features grpc"
    );
}

// ─────────────────────────────────────────────
//  Helius LaserStream — native Rust, 4x faster than Yellowstone, auto-reconnect.
//  Build: cargo build --release --features laserstream
//  Replaces the Node.js sidecar (sidecar/sidecar_grpc.js) entirely.
// ─────────────────────────────────────────────

/// Start LaserStream observer — streams Pump.fun txs and forwards sidecar-compatible JSON via ZMQ.
#[cfg(feature = "laserstream")]
pub async fn start_laserstream_observer(
    api_key: &str,
    endpoint: &str,
    program_id: &str,
    zmq_pub: std::sync::Arc<tokio::sync::Mutex<PubSocket>>,
) -> anyhow::Result<()> {
    use helius_laserstream::{subscribe, LaserstreamConfig, ChannelOptions};
    use helius_laserstream::grpc::{
        subscribe_update::UpdateOneof,
        SubscribeRequest, SubscribeRequestFilterTransactions, CommitmentLevel,
    };
    use futures::StreamExt;
    use zeromq::{SocketSend, ZmqMessage};
    use std::collections::HashMap;
    use std::str::FromStr;
    use tracing::{info, warn, debug};

    let pump_program = program_id.to_string();
    let pump_bytes: [u8; 32] = solana_sdk::pubkey::Pubkey::from_str(program_id)?.to_bytes();

    let config = LaserstreamConfig {
        api_key: api_key.to_string(),
        endpoint: endpoint.to_string(),
        replay: true,
        max_reconnect_attempts: Some(240),
        channel_options: ChannelOptions {
            tcp_nodelay: Some(true),
            tcp_keepalive_secs: Some(15),
            keep_alive_while_idle: Some(true),
            http2_keep_alive_interval_secs: Some(15),
            ..Default::default()
        },
    };

    let mut transactions = HashMap::new();
    transactions.insert(
        "bigal_pump".to_string(),
        SubscribeRequestFilterTransactions {
            vote: Some(false),
            failed: Some(false),
            account_include: vec![pump_program.clone()],
            ..Default::default()
        },
    );

    let request = SubscribeRequest {
        transactions,
        commitment: Some(CommitmentLevel::Processed as i32),
        ..Default::default()
    };

    info!(
        "LaserStream connecting | endpoint={} | program={}",
        endpoint, pump_program
    );

    let (mut stream, _handle) = subscribe(config, request);

    // Pump.fun discriminators
    const DISC_CREATE: [u8; 8] = [24, 30, 200, 40, 5, 28, 7, 119];
    const DISC_BUY: [u8; 8] = [102, 6, 61, 18, 1, 218, 235, 234];
    const DISC_SELL: [u8; 8] = [51, 230, 133, 164, 1, 127, 131, 173];

    while let Some(result) = stream.next().await {
        match result {
            Ok(update) => {
                let oneof = match update.update_oneof {
                    Some(o) => o,
                    None => continue,
                };
                if let UpdateOneof::Transaction(tx) = oneof {
                    let slot = tx.slot;
                    let info = match tx.transaction.as_ref() {
                        Some(i) => i,
                        None => continue,
                    };
                    let inner = match info.transaction.as_ref() {
                        Some(t) => t,
                        None => continue,
                    };
                    let msg = match inner.message.as_ref() {
                        Some(m) => m,
                        None => continue,
                    };
                    let meta = info.meta.as_ref();

                    // Resolve account keys (including v0 loaded addresses)
                    let mut keys: Vec<Vec<u8>> = msg.account_keys.clone();
                    if msg.versioned {
                        if let Some(m) = meta {
                            keys.extend(m.loaded_writable_addresses.iter().cloned());
                            keys.extend(m.loaded_readonly_addresses.iter().cloned());
                        }
                    }

                    // Find Pump.fun instructions
                    for ix in &msg.instructions {
                        let pidx = ix.program_id_index as usize;
                        let prog = match keys.get(pidx) {
                            Some(p) if p.len() == 32 && p.as_slice() == pump_bytes.as_slice() => p,
                            _ => continue,
                        };
                        let data = &ix.data;
                        if data.len() < 8 { continue; }
                        let disc: [u8; 8] = match data[0..8].try_into() {
                            Ok(d) => d,
                            Err(_) => continue,
                        };

                        let tx_type = if disc == DISC_CREATE { "create" }
                            else if disc == DISC_BUY { "buy" }
                            else if disc == DISC_SELL { "sell" }
                            else { continue };

                        let key_b58 = |idx: usize| -> Option<String> {
                            keys.get(idx).map(|b| bs58::encode(b).into_string())
                        };
                        let ix_idx = |pos: usize| -> Option<usize> {
                            ix.accounts.get(pos).map(|b| *b as usize)
                        };

                        let (mint, trader) = if tx_type == "create" {
                            match (ix_idx(0).and_then(&key_b58), ix_idx(7).and_then(&key_b58)) {
                                (Some(m), Some(t)) => (m, t),
                                _ => continue,
                            }
                        } else {
                            match (ix_idx(2).and_then(&key_b58), ix_idx(6).and_then(&key_b58)) {
                                (Some(m), Some(t)) => (m, t),
                                _ => continue,
                            }
                        };

                        // SOL amount from balance diff
                        let mut sol_amount = 0.0_f64;
                        let mut v_sol_bc = 0.0_f64;
                        if let Some(m) = meta {
                            if let Some(ti) = keys.iter().position(|k| k.len() == 32 && bs58::encode(k).into_string() == trader) {
                                if ti < m.pre_balances.len() && ti < m.post_balances.len() {
                                    let d = m.post_balances[ti] as i128 - m.pre_balances[ti] as i128;
                                    sol_amount = (d.unsigned_abs() as f64) / 1e9;
                                }
                            }
                            let bc_idx = if tx_type == "create" { ix_idx(2) } else { ix_idx(3) };
                            if let Some(bi) = bc_idx {
                                if bi < m.post_balances.len() {
                                    v_sol_bc = m.post_balances[bi] as f64 / 1e9;
                                }
                            }
                        }

                        // Token balance from post_token_balances
                        let mut v_tokens = 0.0_f64;
                        if let Some(m) = meta {
                            for tb in &m.post_token_balances {
                                if tb.mint == mint {
                                    if let Some(s) = tb.ui_token_amount.as_ref()
                                        .and_then(|u| u.ui_amount_string.parse::<f64>().ok())
                                    {
                                        if s > v_tokens { v_tokens = s; }
                                    }
                                }
                            }
                        }

                        let json = serde_json::json!({
                            "txType": tx_type,
                            "mint": mint,
                            "traderPublicKey": trader,
                            "solAmount": sol_amount,
                            "vSolInBondingCurve": v_sol_bc,
                            "vTokensInBondingCurve": v_tokens,
                            "slot": slot,
                            "_source": "laserstream",
                        });

                        let line = json.to_string();
                        let mut sock = zmq_pub.lock().await;
                        if let Err(e) = sock.send(ZmqMessage::from(line)).await {
                            debug!("LaserStream ZMQ send failed: {}", e);
                        }
                        break; // one pump ix per tx
                    }
                }
            }
            Err(e) => {
                warn!("LaserStream stream error: {} (auto-reconnecting)", e);
            }
        }
    }

    warn!("LaserStream stream ended");
    Ok(())
}

#[cfg(not(feature = "laserstream"))]
pub async fn start_laserstream_observer(
    _api_key: &str,
    _endpoint: &str,
    _program_id: &str,
    _zmq_pub: std::sync::Arc<tokio::sync::Mutex<PubSocket>>,
) -> anyhow::Result<()> {
    anyhow::bail!(
        "bigal-executor was built without LaserStream; rebuild with: cargo build --release --features laserstream"
    );
}
