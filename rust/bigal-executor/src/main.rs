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

use config::Config;
use fee_estimator::FeeEstimator;
use instruction_builder::InstructionBuilder;
use jito_bundler::{JitoBundler, TipEstimator};
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

    // Start gRPC observer (if configured)
    if let (Some(grpc_endpoint), Some(grpc_token)) = (&cfg.grpc_endpoint, &cfg.grpc_x_token) {
        let grpc_pub = zmq_bridge::create_grpc_pub(cfg.zmq_grpc_port)
            .await
            .context("Failed to create gRPC PUB socket")?;
        let grpc_pub = Arc::new(tokio::sync::Mutex::new(grpc_pub));

        let endpoint = grpc_endpoint.clone();
        let token = grpc_token.clone();
        let program = cfg.pump_program_id.to_string();

        tokio::spawn(async move {
            loop {
                if let Err(e) = grpc_observer::start_grpc_observer(
                    &endpoint,
                    &token,
                    &program,
                    vec![],
                    grpc_pub.clone(),
                )
                .await
                {
                    error!("gRPC observer error: {} — reconnecting in 5s", e);
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                }
            }
        });
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

    // Build buy transaction
    let tx = match builder
        .build_buy(keypair, &mint, signal.amount, signal.slippage_bps, priority_fee)
        .await
    {
        Ok(tx) => tx,
        Err(e) => return TradeResult::failed(signal.id, format!("Build failed: {}", e)),
    };

    let build_ms = build_start.elapsed().as_secs_f64() * 1000.0;
    let land_start = Instant::now();

    // Try Jito bundle first, fall back to direct RPC
    let blockhash = match builder.rpc().get_latest_blockhash().await {
        Ok(bh) => bh,
        Err(e) => {
            return TradeResult::failed(signal.id, format!("Blockhash failed: {}", e))
        }
    };

    let base_tip = if signal.priority_fee_lamports > 0 {
        signal.priority_fee_lamports
    } else {
        tip_handle.load(Ordering::Relaxed)
    };
    let tip = if signal.priority == "urgent" { base_tip * 2 } else { base_tip };

    match bundler
        .submit_bundle(vec![tx.clone()], tip, keypair, blockhash)
        .await
    {
        Ok(bundle_id) => {
            let land_ms = land_start.elapsed().as_secs_f64() * 1000.0;
            let total_ms = recv_time.elapsed().as_secs_f64() * 1000.0;

            info!(
                "BUY confirmed via Jito | mint={} | {:.3} SOL | fee={} tip={} | bundle={} | total={:.0}ms",
                signal.mint, signal.amount, priority_fee, tip, bundle_id, total_ms
            );

            TradeResult::confirmed(
                signal.id,
                bundle_id,
                "jito".to_string(),
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
            warn!("Jito failed ({}), trying direct RPC...", jito_err);

            match JitoBundler::send_via_rpc(builder.rpc(), &tx).await {
                Ok(sig) => {
                    let land_ms = land_start.elapsed().as_secs_f64() * 1000.0;
                    let total_ms = recv_time.elapsed().as_secs_f64() * 1000.0;

                    info!(
                        "BUY confirmed via RPC | mint={} | {:.3} SOL | sig={} | total={:.0}ms",
                        signal.mint, signal.amount, sig, total_ms
                    );

                    TradeResult::confirmed(
                        signal.id,
                        sig.to_string(),
                        "rpc".to_string(),
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
                    error!("BUY FAILED | jito: {} | rpc: {}", jito_err, rpc_err);
                    TradeResult::failed(
                        signal.id,
                        format!("jito: {} | rpc: {}", jito_err, rpc_err),
                    )
                }
            }
        }
    }
}

/// Handle a sell signal.
///
/// Tip tx uses the same recent blockhash as the signed sell (see [`InstructionBuilder::build_sell`]).
/// Up to two full rebuild+land rounds for transient failures.
async fn handle_sell(
    builder: &InstructionBuilder,
    bundler: &JitoBundler,
    keypair: &solana_sdk::signer::keypair::Keypair,
    fee_handle: &Arc<std::sync::atomic::AtomicU64>,
    tip_handle: &Arc<std::sync::atomic::AtomicU64>,
    signal: Signal,
    recv_time: Instant,
) -> TradeResult {
    let zmq_recv_ms = recv_time.elapsed().as_secs_f64() * 1000.0;

    let mint = match Pubkey::from_str(&signal.mint) {
        Ok(m) => m,
        Err(e) => return TradeResult::failed(signal.id, format!("Invalid mint: {}", e)),
    };

    let priority_fee = if signal.priority_fee_lamports > 0 {
        signal.priority_fee_lamports
    } else {
        fee_handle.load(Ordering::Relaxed)
    };

    let pct = signal.amount;

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

    for attempt in 1..=2u8 {
        let build_start = Instant::now();

        let (tx, tip_blockhash) = match builder
            .build_sell(keypair, &mint, pct, signal.slippage_bps, priority_fee)
            .await
        {
            Ok(x) => x,
            Err(e) => {
                if attempt < 2 {
                    warn!(
                        "SELL build attempt {} failed: {}, retrying after 250ms",
                        attempt, e
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
                    continue;
                }
                return TradeResult::failed(signal.id, format!("Build failed: {}", e));
            }
        };

        let build_ms = build_start.elapsed().as_secs_f64() * 1000.0;
        let land_start = Instant::now();

        match bundler
            .submit_bundle(vec![tx.clone()], tip, keypair, tip_blockhash)
            .await
        {
            Ok(bundle_id) => {
                let land_ms = land_start.elapsed().as_secs_f64() * 1000.0;
                let total_ms = recv_time.elapsed().as_secs_f64() * 1000.0;

                info!(
                    "SELL confirmed via Jito | mint={} | {}% | fee={} tip={} | bundle={} | total={:.0}ms | attempt={}",
                    signal.mint, pct, priority_fee, tip, bundle_id, total_ms, attempt
                );

                return TradeResult::confirmed(
                    signal.id,
                    bundle_id,
                    "jito".to_string(),
                    LatencyInfo {
                        zmq_recv_ms,
                        build_ms,
                        land_ms,
                        total_ms,
                        fee_used_lamports: priority_fee,
                        tip_used_lamports: tip,
                    },
                );
            }
            Err(jito_err) => {
                warn!(
                    "Jito failed ({}) attempt {}, trying direct RPC...",
                    jito_err, attempt
                );

                match JitoBundler::send_via_rpc(builder.rpc(), &tx).await {
                    Ok(sig) => {
                        let land_ms = land_start.elapsed().as_secs_f64() * 1000.0;
                        let total_ms = recv_time.elapsed().as_secs_f64() * 1000.0;

                        info!(
                            "SELL confirmed via RPC | mint={} | {}% | sig={} | total={:.0}ms | attempt={}",
                            signal.mint, pct, sig, total_ms, attempt
                        );

                        return TradeResult::confirmed(
                            signal.id,
                            sig.to_string(),
                            "rpc".to_string(),
                            LatencyInfo {
                                zmq_recv_ms,
                                build_ms,
                                land_ms,
                                total_ms,
                                fee_used_lamports: priority_fee,
                                tip_used_lamports: 0,
                            },
                        );
                    }
                    Err(rpc_err) => {
                        last_failure = format!("jito: {} | rpc: {}", jito_err, rpc_err);
                        error!("SELL land failed attempt {} | {}", attempt, last_failure);
                        if attempt < 2 {
                            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                            continue;
                        }
                    }
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
