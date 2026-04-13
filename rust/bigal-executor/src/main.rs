mod balance_cache;
mod blockhash_cache;
mod config;
mod fee_estimator;
mod grpc_observer;
mod instruction_builder;
mod jito_bundler;
mod kill_chain;
mod rpc;
mod zmq_bridge;

use anyhow::{Context, Result};
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;
use tracing::{error, info, warn};

use balance_cache::BalanceCache;
use blockhash_cache::{BlockhashCache, BlockhashHandle};
use config::Config;
use fee_estimator::FeeEstimator;
use instruction_builder::InstructionBuilder;
use jito_bundler::{race_land_submit, JitoBundler, TipEstimator};
use zmq_bridge::{LatencyInfo, Signal, TradeResult, ZmqBridge};

use solana_sdk::signer::{keypair::Keypair, Signer};

/// Reconstruct a Keypair from raw bytes (32 or 64), matching config.rs pattern.
fn keypair_from_raw(bytes: &[u8]) -> Result<Keypair> {
    match bytes.len() {
        64 => {
            let mut secret = [0u8; 32];
            secret.copy_from_slice(&bytes[..32]);
            Ok(Keypair::new_from_array(secret))
        }
        32 => {
            let mut secret = [0u8; 32];
            secret.copy_from_slice(bytes);
            Ok(Keypair::new_from_array(secret))
        }
        n => anyhow::bail!("Invalid keypair length: {} bytes (expected 32 or 64)", n),
    }
}

/// Resolve the transaction signer from the signal's keypair_b58 field,
/// falling back to the default config keypair if not provided.
fn resolve_signer(signal: &Signal, default: &Keypair) -> Result<Keypair> {
    if let Some(ref kb58) = signal.keypair_b58 {
        let bytes = bs58::decode(kb58).into_vec()
            .context("Failed to decode keypair_b58 base58")?;
        keypair_from_raw(&bytes)
    } else {
        keypair_from_raw(&default.to_bytes())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Load .env file if present
    dotenv::dotenv().ok();

    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "bigal_executor=info".into()),
        )
        .with_target(false)
        .compact()
        .init();

    info!("BigAl Executor starting...");

    // Load configuration
    let cfg = Config::from_env().context("Failed to load configuration")?;

    info!("Wallet: {}", cfg.pubkey_str());
    info!("RPC: {}", cfg.rpc_url);
    if cfg.rpc_read_url != cfg.rpc_url {
        info!("RPC (read / sell balance): {}", cfg.rpc_read_url);
    }
    info!("Jito: {}", cfg.jito_block_engine);
    info!(
        "ZMQ: signal_port={} result_port={} | addrs: pull←{} pub→{}",
        cfg.zmq_signal_port,
        cfg.zmq_result_port,
        cfg.zmq_signal_addr,
        cfg.zmq_result_addr
    );

    if cfg.grpc_endpoint.is_some() {
        info!(
            "gRPC: {} (enabled)",
            cfg.grpc_endpoint.as_ref().unwrap()
        );
    } else {
        info!("gRPC: disabled (set GRPC_ENDPOINT to enable)");
    }

    // Initialize ZMQ bridge (async)
    let mut bridge = ZmqBridge::new(&cfg.zmq_signal_addr, &cfg.zmq_result_addr)
        .await
        .context("Failed to initialize ZMQ bridge")?;

    // Initialize instruction builder
    let builder = InstructionBuilder::new(
        &cfg.rpc_url,
        &cfg.rpc_read_url,
        cfg.pump_program_id,
        cfg.fee_recipient,
    );

    // Initialize Jito bundler
    let mut bundler = JitoBundler::new(&cfg.jito_block_engine);

    // Try to refresh Jito tip accounts
    if let Err(e) = bundler.refresh_tip_accounts().await {
        warn!(
            "Failed to refresh Jito tip accounts (using fallback): {}",
            e
        );
    }

    // Start fee estimator (background task)
    let fee_handle = FeeEstimator::start_polling(
        cfg.rpc_url.clone(),
        cfg.pump_program_id.to_string(),
        cfg.default_priority_fee,
        500, // poll every 500ms
    );

    // Start dynamic Jito tip estimator (background task)
    let tip_handle = TipEstimator::start_polling(
        cfg.jito_tip_floor_url.clone(),
        config::DEFAULT_JITO_TIP_LAMPORTS,
        5_000, // poll every 5s (tips change slowly)
    );

    // Balance cache: stores expected token amounts from confirmed buys
    // so sells can skip the RPC polling loop entirely (Phase 2)
    let balance_cache = Arc::new(BalanceCache::new(300)); // 5-minute expiry

    // Rolling blockhash pre-fetch — fresher hashes mean fewer cache misses under sell pressure
    let bh_poll_ms: u64 = std::env::var("BIGAL_BLOCKHASH_POLL_MS")
        .ok().and_then(|s| s.parse().ok()).unwrap_or(1_500);
    let blockhash_handle = BlockhashCache::start_polling(cfg.rpc_url.clone(), bh_poll_ms);

    // Pre-warm TCP+TLS connections to RPC and Jito — eliminates 100-200ms cold-start on first trade
    {
        let warmup_start = Instant::now();
        let (rpc_w, rpc_read_w) = tokio::join!(
            builder.rpc().get_health(),
            builder.rpc_read().get_health(),
        );
        if let Err(e) = rpc_w { warn!("RPC warmup failed (non-fatal): {}", e); }
        if let Err(e) = rpc_read_w { warn!("RPC-read warmup failed (non-fatal): {}", e); }
        info!("RPC connections warmed in {:.0}ms", warmup_start.elapsed().as_secs_f64() * 1000.0);
    }

    // Start streaming observer — only bind ZMQ PUB if streaming is configured.
    let needs_streaming = cfg.laserstream_api_key.is_some()
        || (cfg.grpc_endpoint.is_some() && cfg.grpc_x_token.is_some());

    if needs_streaming {
        let streaming_zmq_pub = zmq_bridge::create_yellowstone_pub(&cfg.zmq_yellowstone_bind)
            .await
            .context("Failed to create streaming ZMQ PUB socket")?;
        let streaming_zmq_pub = Arc::new(tokio::sync::Mutex::new(streaming_zmq_pub));

        if let Some(ls_key) = &cfg.laserstream_api_key {
            let ls_endpoint = cfg.laserstream_endpoint.clone()
                .unwrap_or_else(|| "https://laserstream-mainnet-ewr.helius-rpc.com".to_string());
            let key = ls_key.clone();
            let program = cfg.pump_program_id.to_string();
            let pub_sock = streaming_zmq_pub.clone();

            tokio::spawn(async move {
                loop {
                    if let Err(e) = grpc_observer::start_laserstream_observer(
                        &key, &ls_endpoint, &program, pub_sock.clone(),
                    ).await {
                        error!("LaserStream error: {} — restarting in 3s", e);
                        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                    }
                }
            });
            info!("Streaming: LaserStream (native Rust, replaces Node.js sidecar)");
        } else if let (Some(grpc_endpoint), Some(grpc_token)) = (&cfg.grpc_endpoint, &cfg.grpc_x_token) {
            let endpoint = grpc_endpoint.clone();
            let token = grpc_token.clone();
            let program = cfg.pump_program_id.to_string();
            let pub_sock = streaming_zmq_pub.clone();

            tokio::spawn(async move {
                loop {
                    if let Err(e) = grpc_observer::start_grpc_observer(
                        &endpoint, &token, &program, vec![], pub_sock.clone(),
                    ).await {
                        error!("gRPC observer error: {} — reconnecting in 5s", e);
                        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    }
                }
            });
            info!("Streaming: Yellowstone gRPC");
        }
    } else {
        info!("Streaming: none (set LASERSTREAM_API_KEY or GRPC_ENDPOINT to enable)");
    }

    info!(
        "BigAl Executor ready — waiting for signals on port {}",
        cfg.zmq_signal_port
    );

    // Main signal loop (fully async)
    loop {
        let (signal, recv_time) = match bridge.recv_signal().await {
            Ok(s) => s,
            Err(e) => {
                error!("ZMQ recv error: {} — retrying", e);
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                continue;
            }
        };

        match signal.action.as_str() {
            "ping" => {
                let result = TradeResult::pong(signal.id);
                if let Err(e) = bridge.send_result(&result).await {
                    error!("Failed to send pong: {}", e);
                }
            }

            "buy" => {
                let signer = match resolve_signer(&signal, &cfg.keypair) {
                    Ok(kp) => kp,
                    Err(e) => {
                        let result = TradeResult::failed(signal.id, format!("Signer error: {}", e));
                        bridge.send_result(&result).await.ok();
                        continue;
                    }
                };
                let result = handle_buy(
                    &builder,
                    &bundler,
                    &signer,
                    &fee_handle,
                    &tip_handle,
                    &balance_cache,
                    &blockhash_handle,
                    signal,
                    recv_time,
                )
                .await;
                if let Err(e) = bridge.send_result(&result).await {
                    error!("Failed to send buy result: {}", e);
                }
            }

            "sell" => {
                let signer = match resolve_signer(&signal, &cfg.keypair) {
                    Ok(kp) => kp,
                    Err(e) => {
                        let result = TradeResult::failed(signal.id, format!("Signer error: {}", e));
                        bridge.send_result(&result).await.ok();
                        continue;
                    }
                };
                let result = handle_sell(
                    &builder,
                    &bundler,
                    &signer,
                    &fee_handle,
                    &tip_handle,
                    &balance_cache,
                    &blockhash_handle,
                    signal,
                    recv_time,
                )
                .await;
                if let Err(e) = bridge.send_result(&result).await {
                    error!("Failed to send sell result: {}", e);
                }
            }

            "kill" => {
                let signer = match resolve_signer(&signal, &cfg.keypair) {
                    Ok(kp) => kp,
                    Err(e) => {
                        let result = TradeResult::failed(signal.id, format!("Signer error: {}", e));
                        bridge.send_result(&result).await.ok();
                        continue;
                    }
                };
                let mint = match Pubkey::from_str(&signal.mint) {
                    Ok(m) => m,
                    Err(e) => {
                        let result =
                            TradeResult::failed(signal.id, format!("Invalid mint for kill: {}", e));
                        bridge.send_result(&result).await.ok();
                        continue;
                    }
                };

                let tip = if signal.amount > 0.0 {
                    (signal.amount * 1e9) as u64
                } else {
                    tip_handle.load(Ordering::Relaxed) * 3 // 3x dynamic tip for kill urgency
                };

                let result = kill_chain::execute_kill(
                    &builder,
                    &bundler,
                    &signer,
                    &mint,
                    tip,
                    &balance_cache,
                    &blockhash_handle,
                    signal.id,
                    recv_time,
                )
                .await;
                if let Err(e) = bridge.send_result(&result).await {
                    error!("Failed to send kill result: {}", e);
                }
            }

            "create" => {
                let result = handle_create(
                    &builder,
                    &bundler,
                    &cfg.keypair,
                    &fee_handle,
                    &tip_handle,
                    signal,
                    recv_time,
                )
                .await;
                if let Err(e) = bridge.send_result(&result).await {
                    error!("Failed to send create result: {}", e);
                }
            }

            "config" => {
                if let Some(config_data) = &signal.config {
                    info!("Received runtime config update: {}", config_data);
                }
                let result = TradeResult {
                    id: signal.id,
                    status: "ok".to_string(),
                    signature: None,
                    method: None,
                    latency: None,
                    error: None,
                };
                bridge.send_result(&result).await.ok();
            }

            other => {
                warn!("Unknown signal action: {}", other);
                let result = TradeResult::failed(signal.id, format!("Unknown action: {}", other));
                bridge.send_result(&result).await.ok();
            }
        }
    }
}

/// Handle a buy signal
async fn handle_buy(
    builder: &InstructionBuilder,
    bundler: &JitoBundler,
    keypair: &solana_sdk::signer::keypair::Keypair,
    fee_handle: &Arc<std::sync::atomic::AtomicU64>,
    tip_handle: &Arc<std::sync::atomic::AtomicU64>,
    balance_cache: &Arc<BalanceCache>,
    blockhash_handle: &BlockhashHandle,
    signal: Signal,
    recv_time: Instant,
) -> TradeResult {
    let zmq_recv_ms = recv_time.elapsed().as_secs_f64() * 1000.0;

    let mint = match Pubkey::from_str(&signal.mint) {
        Ok(m) => m,
        Err(e) => return TradeResult::failed(signal.id, format!("Invalid mint: {}", e)),
    };

    // Prefer Python-resolved fee if provided, else use local estimate
    let priority_fee = if signal.priority_fee_lamports > 0 {
        signal.priority_fee_lamports
    } else {
        fee_handle.load(Ordering::Relaxed)
    };
    let build_start = Instant::now();

    // Read cached blockhash for both build_buy (signing) and Jito tip tx
    let cached_bh = blockhash_cache::read_cached_blockhash(blockhash_handle, blockhash_max_age_secs()).await;

    // Build buy transaction — returns (tx, expected_tokens) for balance cache
    let (tx, tokens_out) = match builder
        .build_buy(keypair, &mint, signal.amount, signal.slippage_bps, priority_fee, cached_bh)
        .await
    {
        Ok(pair) => pair,
        Err(e) => return TradeResult::failed(signal.id, format!("Build failed: {}", e)),
    };

    let build_ms = build_start.elapsed().as_secs_f64() * 1000.0;
    let land_start = Instant::now();

    // Tip tx blockhash: reuse cached or fetch fresh
    let blockhash = match cached_bh {
        Some(bh) => bh,
        None => match builder.rpc().get_latest_blockhash().await {
            Ok(bh) => bh,
            Err(e) => {
                return TradeResult::failed(signal.id, format!("Blockhash failed: {}", e))
            }
        },
    };

    let base_tip = if signal.priority_fee_lamports > 0 {
        signal.priority_fee_lamports
    } else {
        tip_handle.load(Ordering::Relaxed)
    };
    let tip = if signal.priority == "urgent" { base_tip * 2 } else { base_tip };

    // Phase 4: Race Jito + direct RPC simultaneously
    let jito_fut = bundler.submit_bundle(vec![tx.clone()], tip, keypair, blockhash);
    let rpc_fut = JitoBundler::send_via_rpc(builder.rpc(), &tx);

    match race_land_submit(jito_fut, rpc_fut, "jito", "rpc").await {
        Ok((sig_or_id, method)) => {
            let mut tx_ref = sig_or_id.clone();
            let mut method_ref = method.to_string();
            if !looks_like_tx_signature(&tx_ref) {
                if let Ok(rpc_sig) = JitoBundler::send_via_rpc(builder.rpc(), &tx).await {
                    tx_ref = rpc_sig;
                    method_ref = "jito+rpc".to_string();
                }
            }
            if looks_like_tx_signature(&tx_ref) {
                if let Err(e) = await_chain_confirmation(
                    builder.rpc(),
                    &tx_ref,
                    trade_confirm_timeout_secs(),
                    require_finalized(),
                )
                .await
                {
                    return TradeResult::failed(signal.id, format!("BUY chain confirm failed: {}", e));
                }
            } else {
                return TradeResult::failed(
                    signal.id,
                    format!(
                        "BUY landed via {} but no tx signature to verify ({})",
                        method_ref, tx_ref
                    ),
                );
            }
            let land_ms = land_start.elapsed().as_secs_f64() * 1000.0;
            let total_ms = recv_time.elapsed().as_secs_f64() * 1000.0;

            // Cache expected token balance for fast sell
            balance_cache.insert(mint, tokens_out);

            let tip_used = if method_ref.contains("jito") { tip } else { 0 };
            info!(
                "BUY confirmed via {} | mint={} | {:.3} SOL | ~{} tokens cached | fee={} tip={} | {} | total={:.0}ms",
                method_ref, signal.mint, signal.amount, tokens_out, priority_fee, tip_used, tx_ref, total_ms
            );

            TradeResult::confirmed(
                signal.id,
                tx_ref,
                method_ref,
                LatencyInfo {
                    zmq_recv_ms,
                    build_ms,
                    land_ms,
                    total_ms,
                    fee_used_lamports: priority_fee,
                    tip_used_lamports: tip_used,
                },
            )
        }
        Err(e) => {
            error!("BUY FAILED | {}", e);
            TradeResult::failed(signal.id, format!("{}", e))
        }
    }
}

/// Milliseconds to pause before a sell land retry (default 0 — retry immediately with fresh blockhash).
fn sell_retry_stagger_ms() -> u64 {
    std::env::var("BIGAL_SELL_RETRY_STAGGER_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0)
}

/// Milliseconds to pause before a sell build retry (default 50 — brief pause for RPC state to settle).
fn sell_build_retry_ms() -> u64 {
    std::env::var("BIGAL_SELL_BUILD_RETRY_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(50)
}

/// Blockhash cache max age in seconds (default 40; Solana TTL ~60s).
fn blockhash_max_age_secs() -> u64 {
    std::env::var("BIGAL_BLOCKHASH_MAX_AGE_SEC")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(40)
}

fn trade_confirm_timeout_secs() -> u64 {
    std::env::var("BIGAL_CONFIRM_TIMEOUT_SEC")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(14)
}

fn require_finalized() -> bool {
    std::env::var("BIGAL_REQUIRE_FINALIZED")
        .map(|v| matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
        .unwrap_or(false)
}

fn looks_like_tx_signature(s: &str) -> bool {
    (80..=100).contains(&s.len())
        && s
            .chars()
            .all(|c| c.is_ascii_alphanumeric() && !"0OIl".contains(c))
}

/// Returns true if the error message indicates a permanent on-chain failure
/// that won't be fixed by retrying the same transaction parameters.
fn is_permanent_chain_error(err_msg: &str) -> bool {
    let lower = err_msg.to_ascii_lowercase();
    // 6024 / 0x1788 = Overflow in Pump.fun sell math
    // 3012 = AccountNotInitialized (ATA doesn't exist — buy didn't land)
    // "depleted" = our own overflow guard rejection
    lower.contains("overflow")
        || lower.contains("6024")
        || lower.contains("0x1788")
        || lower.contains("accountnotinitialized")
        || lower.contains("3012")
        || lower.contains("depleted")
        || lower.contains("insufficient funds")
        || lower.contains("insufficient lamports")
}

async fn await_chain_confirmation(
    rpc: &rpc::RpcClient,
    signature: &str,
    timeout_secs: u64,
    finalized: bool,
) -> Result<()> {
    let deadline = Instant::now() + std::time::Duration::from_secs(timeout_secs);
    while Instant::now() < deadline {
        if let Some((status, err)) = rpc.get_signature_status(signature, true).await? {
            if let Some(e) = err {
                if !e.is_null() {
                    anyhow::bail!("on-chain tx failed: {}", e);
                }
            }
            if finalized {
                if status.as_deref() == Some("finalized") {
                    return Ok(());
                }
            } else if matches!(status.as_deref(), Some("confirmed") | Some("finalized")) {
                return Ok(());
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(220)).await;
    }
    anyhow::bail!(
        "tx not {} within {}s",
        if finalized { "finalized" } else { "confirmed" },
        timeout_secs
    );
}

/// Handle a sell signal.
///
/// Tip tx uses the same recent blockhash as the signed sell (see [`InstructionBuilder::build_sell`]).
/// Up to three rebuild+land rounds; optional short stagger between attempts (see [`sell_retry_stagger_ms`]).
/// Attempt 1 uses balance cache + blockhash cache; later attempts poll fresh.
async fn handle_sell(
    builder: &InstructionBuilder,
    bundler: &JitoBundler,
    keypair: &solana_sdk::signer::keypair::Keypair,
    fee_handle: &Arc<std::sync::atomic::AtomicU64>,
    tip_handle: &Arc<std::sync::atomic::AtomicU64>,
    balance_cache: &Arc<BalanceCache>,
    blockhash_handle: &BlockhashHandle,
    signal: Signal,
    recv_time: Instant,
) -> TradeResult {
    let zmq_recv_ms = recv_time.elapsed().as_secs_f64() * 1000.0;

    let mint = match Pubkey::from_str(&signal.mint) {
        Ok(m) => m,
        Err(e) => return TradeResult::failed(signal.id, format!("Invalid mint: {}", e)),
    };

    let mut priority_fee = if signal.priority_fee_lamports > 0 {
        signal.priority_fee_lamports
    } else {
        fee_handle.load(Ordering::Relaxed)
    };
    // Optional extra micro-lamports on urgent sells (default off = strategy/Python fee only).
    if signal.priority == "urgent" {
        let bps: u128 = std::env::var("BIGAL_SELL_PRIORITY_MULT_BPS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(10_000);
        if bps > 0 && bps != 10_000 {
            priority_fee = ((priority_fee as u128).saturating_mul(bps) / 10_000).min(u64::MAX as u128) as u64;
        }
    }

    let pct = signal.amount;

    let sell_rpc_only = std::env::var("BIGAL_SELL_RPC_ONLY")
        .map(|v| {
            matches!(
                v.to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false);

    let base_tip = if signal.priority_fee_lamports > 0 {
        signal.priority_fee_lamports
    } else {
        tip_handle.load(Ordering::Relaxed)
    };
    let tip = if signal.priority == "urgent" {
        base_tip * 2
    } else {
        base_tip
    };

    let mut last_failure = String::new();

    for attempt in 1..=3u8 {
        let build_start = Instant::now();

        // Attempt 1: use balance cache + blockhash cache for maximum speed
        // Attempt 2: force fresh RPC polls (cache may have been wrong)
        let cached_bal = if attempt == 1 {
            balance_cache.get(&mint)
        } else {
            None
        };
        let cached_bh = blockhash_cache::read_cached_blockhash(blockhash_handle, blockhash_max_age_secs()).await;

        let (tx, tip_blockhash) = match builder
            .build_sell(keypair, &mint, pct, signal.slippage_bps, priority_fee, cached_bal, cached_bh)
            .await
        {
            Ok(x) => x,
            Err(e) => {
                let err_str = format!("{}", e);
                // Don't retry permanent build errors (depleted curve, zero balance after all retries)
                if is_permanent_chain_error(&err_str) {
                    error!("SELL build permanent failure: {}", err_str);
                    balance_cache.remove(&mint);
                    return TradeResult::failed(signal.id, format!("Build failed: {}", e));
                }
                if attempt < 3 {
                    warn!(
                        "SELL build attempt {} failed: {}, retrying fresh (no cache)",
                        attempt, e
                    );
                    let ms = sell_build_retry_ms();
                    if ms > 0 {
                        tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
                    }
                    continue;
                }
                return TradeResult::failed(signal.id, format!("Build failed: {}", e));
            }
        };

        let build_ms = build_start.elapsed().as_secs_f64() * 1000.0;
        let land_start = Instant::now();

        let land_result = if sell_rpc_only {
            JitoBundler::send_via_rpc_fast(builder.rpc(), &tx)
                .await
                .map(|sig| (sig, "rpc"))
        } else {
            // Race Jito bundle + fast RPC (maxRetries=0, base64). Same tx → Solana dedupes.
            let jito_fut = bundler.submit_bundle(vec![tx.clone()], tip, keypair, tip_blockhash);
            let rpc_fut = JitoBundler::send_via_rpc_fast(builder.rpc(), &tx);
            race_land_submit(jito_fut, rpc_fut, "jito", "rpc").await
        };

        match land_result {
            Ok((sig_or_id, method)) => {
                let mut tx_ref = sig_or_id.clone();
                let mut method_ref = method.to_string();
                if !looks_like_tx_signature(&tx_ref) {
                    if let Ok(rpc_sig) = JitoBundler::send_via_rpc_fast(builder.rpc(), &tx).await {
                        tx_ref = rpc_sig;
                        method_ref = "jito+rpc".to_string();
                    }
                }
                if looks_like_tx_signature(&tx_ref) {
                    if let Err(e) = await_chain_confirmation(
                        builder.rpc(),
                        &tx_ref,
                        trade_confirm_timeout_secs(),
                        require_finalized(),
                    )
                    .await
                    {
                        last_failure = format!("SELL chain confirm failed: {}", e);
                        error!("SELL chain confirm failed attempt {} | {}", attempt, last_failure);

                        // Don't retry permanent on-chain errors (Overflow, AccountNotInitialized, etc.)
                        // Retrying the same amount will just burn more priority fees.
                        if is_permanent_chain_error(&last_failure) {
                            error!("SELL permanent chain error — not retrying: {}", last_failure);
                            balance_cache.remove(&mint);
                            break;
                        }

                        if attempt < 3 {
                            let ms = sell_retry_stagger_ms();
                            if ms > 0 {
                                tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
                            }
                            continue;
                        }
                        break;
                    }
                } else {
                    last_failure = format!("SELL landed via {} but no tx signature to verify ({})", method_ref, tx_ref);
                    error!("SELL unverifiable attempt {} | {}", attempt, last_failure);
                    if attempt < 3 {
                        let ms = sell_retry_stagger_ms();
                        if ms > 0 {
                            tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
                        }
                        continue;
                    }
                    break;
                }
                let land_ms = land_start.elapsed().as_secs_f64() * 1000.0;
                let total_ms = recv_time.elapsed().as_secs_f64() * 1000.0;

                balance_cache.remove(&mint);

                let tip_used = if method_ref.contains("jito") { tip } else { 0 };
                info!(
                    "SELL confirmed via {} | mint={} | {}% | fee={} tip={} | {} | total={:.0}ms | attempt={}",
                    method_ref, signal.mint, pct, priority_fee, tip_used, tx_ref, total_ms, attempt
                );

                return TradeResult::confirmed(
                    signal.id,
                    tx_ref,
                    method_ref,
                    LatencyInfo {
                        zmq_recv_ms,
                        build_ms,
                        land_ms,
                        total_ms,
                        fee_used_lamports: priority_fee,
                        tip_used_lamports: tip_used,
                    },
                );
            }
            Err(e) => {
                last_failure = format!("{}", e);
                error!("SELL land failed attempt {} | {}", attempt, last_failure);
                if attempt < 3 {
                    let ms = sell_retry_stagger_ms();
                    if ms > 0 {
                        tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
                    }
                    continue;
                }
            }
        }
    }

    TradeResult::failed(
        signal.id,
        if last_failure.is_empty() {
            "SELL: exhausted attempts".to_string()
        } else {
            last_failure
        },
    )
}

/// Handle a create-token signal (deploy new Pump.fun token)
async fn handle_create(
    builder: &InstructionBuilder,
    bundler: &JitoBundler,
    default_keypair: &Keypair,
    fee_handle: &Arc<std::sync::atomic::AtomicU64>,
    tip_handle: &Arc<std::sync::atomic::AtomicU64>,
    signal: Signal,
    recv_time: Instant,
) -> TradeResult {
    let zmq_recv_ms = recv_time.elapsed().as_secs_f64() * 1000.0;

    // Resolve deployer keypair (from signal or default)
    let deployer = match resolve_signer(&signal, default_keypair) {
        Ok(kp) => kp,
        Err(e) => return TradeResult::failed(signal.id, format!("Signer error: {}", e)),
    };

    // Parse token metadata from signal.config
    let config_data = match &signal.config {
        Some(c) => c.clone(),
        None => return TradeResult::failed(signal.id, "Missing config for create".to_string()),
    };
    let name = config_data.get("name").and_then(|v| v.as_str()).unwrap_or("Token").to_string();
    let symbol = config_data.get("symbol").and_then(|v| v.as_str()).unwrap_or("TKN").to_string();
    let uri = config_data.get("uri").and_then(|v| v.as_str()).unwrap_or("").to_string();

    let priority_fee = if signal.priority_fee_lamports > 0 {
        signal.priority_fee_lamports
    } else {
        fee_handle.load(Ordering::Relaxed)
    };

    let build_start = Instant::now();

    // Generate fresh mint keypair for the new token
    let mint_keypair = Keypair::new();
    let mint_pubkey = mint_keypair.pubkey();

    // Build create transaction
    let tx = match builder
        .build_create(&deployer, &mint_keypair, name.clone(), symbol.clone(), uri, priority_fee)
        .await
    {
        Ok(tx) => tx,
        Err(e) => return TradeResult::failed(signal.id, format!("Build create failed: {}", e)),
    };

    let build_ms = build_start.elapsed().as_secs_f64() * 1000.0;
    let land_start = Instant::now();

    // Submit via Jito bundle
    let blockhash = match builder.rpc().get_latest_blockhash().await {
        Ok(bh) => bh,
        Err(e) => return TradeResult::failed(signal.id, format!("Blockhash failed: {}", e)),
    };

    let base_tip = tip_handle.load(Ordering::Relaxed);
    let tip = base_tip * 2; // 2x tip for token creation

    match bundler
        .submit_bundle(vec![tx.clone()], tip, &deployer, blockhash)
        .await
    {
        Ok(bundle_id) => {
            let land_ms = land_start.elapsed().as_secs_f64() * 1000.0;
            let total_ms = recv_time.elapsed().as_secs_f64() * 1000.0;

            info!(
                "CREATE confirmed via Jito | {} ({}) | mint={} | bundle={} | total={:.0}ms",
                name, symbol, mint_pubkey, bundle_id, total_ms
            );

            // Return mint pubkey as the "signature" so Python records the mint address
            TradeResult::confirmed(
                signal.id,
                mint_pubkey.to_string(),
                "jito_create".to_string(),
                LatencyInfo {
                    zmq_recv_ms,
                    build_ms,
                    land_ms,
                    total_ms,
                    fee_used_lamports: priority_fee,
                    tip_used_lamports: tip,
                },
            )
        }
        Err(jito_err) => {
            warn!("Create Jito failed ({}), trying direct RPC...", jito_err);

            match JitoBundler::send_via_rpc(builder.rpc(), &tx).await {
                Ok(_sig) => {
                    let land_ms = land_start.elapsed().as_secs_f64() * 1000.0;
                    let total_ms = recv_time.elapsed().as_secs_f64() * 1000.0;

                    TradeResult::confirmed(
                        signal.id,
                        mint_pubkey.to_string(),
                        "rpc_create".to_string(),
                        LatencyInfo {
                            zmq_recv_ms,
                            build_ms,
                            land_ms,
                            total_ms,
                            fee_used_lamports: priority_fee,
                            tip_used_lamports: 0,
                        },
                    )
                }
                Err(rpc_err) => {
                    error!("CREATE FAILED | jito: {} | rpc: {}", jito_err, rpc_err);
                    TradeResult::failed(
                        signal.id,
                        format!("jito: {} | rpc: {}", jito_err, rpc_err),
                    )
                }
            }
        }
    }
}
