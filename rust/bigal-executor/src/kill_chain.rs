use anyhow::Result;
use solana_sdk::{
    pubkey::Pubkey,
    signer::keypair::Keypair,
};
use std::sync::Arc;
use std::time::Instant;
use tracing::{error, info, warn};

use crate::balance_cache::BalanceCache;
use crate::blockhash_cache::{self, BlockhashHandle};
use crate::instruction_builder::InstructionBuilder;
use crate::jito_bundler::JitoBundler;
use crate::zmq_bridge::{LatencyInfo, TradeResult};

/// Execute an atomic rug pull via Jito bundle.
///
/// This bundles the sell-all transaction with a Jito tip into a single
/// atomic execution. The target bot has 0ms to react because the
/// transactions never hit the public mempool.
///
/// Flow:
///   1. Build sell-100% instruction (sell all tokens for the mint)
///   2. Bundle with Jito tip transaction
///   3. Submit atomically via Jito block engine
///   4. Return result with extracted SOL amount
pub async fn execute_kill(
    builder: &InstructionBuilder,
    bundler: &JitoBundler,
    keypair: &Keypair,
    mint: &Pubkey,
    tip_lamports: u64,
    balance_cache: &Arc<BalanceCache>,
    blockhash_handle: &BlockhashHandle,
    signal_id: String,
    recv_time: Instant,
) -> TradeResult {
    let zmq_recv_ms = recv_time.elapsed().as_secs_f64() * 1000.0;

    info!(
        "KILL CHAIN initiated | mint={} | tip={} lamports",
        mint, tip_lamports
    );

    let build_start = Instant::now();

    // Use caches for maximum speed on kill chain
    let cached_bal = balance_cache.get(mint);
    let cached_bh = blockhash_cache::read_cached_blockhash(blockhash_handle, 30).await;

    // Step 1: Build sell-all transaction (100% of holdings) + blockhash aligned with sell signature
    let (sell_tx, tip_blockhash) = match builder.build_sell(
        keypair,
        mint,
        100.0,         // sell everything
        2000,          // 20% slippage (aggressive for kills — we want it to land)
        500_000,       // high priority fee
        cached_bal,
        cached_bh,
    ).await {
        Ok(pair) => pair,
        Err(e) => {
            error!("Kill chain build_sell failed: {}", e);
            return TradeResult::failed(signal_id, format!("build_sell failed: {}", e));
        }
    };

    let build_ms = build_start.elapsed().as_secs_f64() * 1000.0;
    let land_start = Instant::now();

    // Step 2: Race Jito bundle + direct RPC simultaneously for maximum kill speed.
    // Same signed tx → Solana deduplicates. First to land wins.
    let jito_fut = bundler.submit_bundle(
        vec![sell_tx.clone()],
        tip_lamports,
        keypair,
        tip_blockhash,
    );
    let rpc_fut = JitoBundler::send_via_rpc(builder.rpc(), &sell_tx);
    tokio::pin!(jito_fut);
    tokio::pin!(rpc_fut);

    let submission_result: anyhow::Result<(String, &str)> = tokio::select! {
        jito_result = &mut jito_fut => {
            match jito_result {
                Ok(id) => Ok((id, "jito_kill")),
                Err(jito_err) => {
                    warn!("Kill Jito failed ({}), awaiting RPC...", jito_err);
                    match rpc_fut.await {
                        Ok(sig) => Ok((sig, "rpc_kill")),
                        Err(rpc_err) => Err(anyhow::anyhow!("jito: {} | rpc: {}", jito_err, rpc_err)),
                    }
                }
            }
        }
        rpc_result = &mut rpc_fut => {
            match rpc_result {
                Ok(sig) => Ok((sig, "rpc_kill")),
                Err(rpc_err) => {
                    warn!("Kill RPC failed ({}), awaiting Jito...", rpc_err);
                    match jito_fut.await {
                        Ok(id) => Ok((id, "jito_kill")),
                        Err(jito_err) => Err(anyhow::anyhow!("jito: {} | rpc: {}", jito_err, rpc_err)),
                    }
                }
            }
        }
    };

    match submission_result {
        Ok((sig_or_id, method)) => {
            let land_ms = land_start.elapsed().as_secs_f64() * 1000.0;
            let total_ms = recv_time.elapsed().as_secs_f64() * 1000.0;

            balance_cache.remove(mint);

            let tip_used = if method == "jito_kill" { tip_lamports } else { 0 };
            info!(
                "KILL CHAIN SUCCESS via {} | {} | build={:.0}ms land={:.0}ms total={:.0}ms",
                method, sig_or_id, build_ms, land_ms, total_ms
            );

            TradeResult::confirmed(
                signal_id,
                sig_or_id,
                method.to_string(),
                LatencyInfo {
                    zmq_recv_ms,
                    build_ms,
                    land_ms,
                    total_ms,
                    fee_used_lamports: 500_000,
                    tip_used_lamports: tip_used,
                },
            )
        }
        Err(e) => {
            error!("Kill chain FAILED (both Jito and RPC): {}", e);
            TradeResult::failed(signal_id, format!("{}", e))
        }
    }
}

/// RPC fallback for kill chain when Jito bundle fails — always polls fresh (no cache)
async fn attempt_rpc_kill(
    builder: &InstructionBuilder,
    keypair: &Keypair,
    mint: &Pubkey,
) -> Result<String> {
    let (sell_tx, _) = builder.build_sell(
        keypair,
        mint,
        100.0,
        3000,      // even more slippage for fallback
        1_000_000, // maximum priority
        None,      // no cached balance — poll fresh
        None,      // no cached blockhash — fetch fresh
    ).await?;

    let sig = JitoBundler::send_via_rpc(builder.rpc(), &sell_tx).await?;
    Ok(sig.to_string())
}
