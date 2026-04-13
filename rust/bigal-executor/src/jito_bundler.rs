use anyhow::{Context, Result};
use rand::seq::SliceRandom;
use solana_sdk::{
    instruction::{AccountMeta, Instruction},
    pubkey::Pubkey,
    signer::keypair::Keypair,
    signer::Signer,
    transaction::Transaction,
};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info, warn};

/// Known Jito tip accounts (these rotate — fetched dynamically)
const FALLBACK_TIP_ACCOUNTS: &[&str] = &[
    "96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5",
    "HFqU5x63VTqvQss8hp11i4bPYDKRG3kRPg2E9K1KqYSR",
    "Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY",
    "ADaUMid9yfUytqMBgopwjb2DTLSGi4ABJEFwniGPMr3z",
    "DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh",
    "ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt",
    "DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL",
    "3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT",
];

/// Jito bundle submission via JSON-RPC API.
///
/// Uses HTTP POST to the Jito block engine instead of gRPC,
/// avoiding complex protobuf dependencies.
pub struct JitoBundler {
    bundle_url: String,
    /// Optional secondary Jito endpoint for geographic redundancy.
    /// When set, `submit_bundle` races both endpoints simultaneously.
    secondary_bundle_url: Option<String>,
    tip_accounts: Vec<Pubkey>,
    http: reqwest::Client,
}

impl JitoBundler {
    pub fn new(block_engine_url: &str) -> Self {
        // Construct the bundle API endpoint
        let bundle_url = if block_engine_url.ends_with("/api/v1/bundles") {
            block_engine_url.to_string()
        } else {
            format!("{}/api/v1/bundles", block_engine_url.trim_end_matches('/'))
        };

        // Check for secondary Jito endpoint (multi-region redundancy)
        let secondary_bundle_url = std::env::var("JITO_BLOCK_ENGINE_SECONDARY")
            .ok()
            .map(|url| {
                if url.ends_with("/api/v1/bundles") {
                    url
                } else {
                    format!("{}/api/v1/bundles", url.trim_end_matches('/'))
                }
            });

        // Initialize with fallback tip accounts
        let tip_accounts: Vec<Pubkey> = FALLBACK_TIP_ACCOUNTS
            .iter()
            .filter_map(|s| Pubkey::from_str(s).ok())
            .collect();

        if let Some(ref sec) = secondary_bundle_url {
            info!("JitoBundler initialized | primary={} | secondary={} | tip_accounts={}", bundle_url, sec, tip_accounts.len());
        } else {
            info!("JitoBundler initialized | url={} | tip_accounts={}", bundle_url, tip_accounts.len());
        }

        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(12))
            .connect_timeout(Duration::from_secs(8))
            .build()
            .expect("reqwest client build (Jito)");

        JitoBundler {
            bundle_url,
            secondary_bundle_url,
            tip_accounts,
            http,
        }
    }

    /// Fetch current tip accounts from Jito and update the local cache.
    pub async fn refresh_tip_accounts(&mut self) -> Result<()> {
        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getTipAccounts",
            "params": []
        });

        let resp = self.http
            .post(&self.bundle_url)
            .json(&request)
            .send()
            .await
            .context("Failed to fetch tip accounts")?
            .json::<serde_json::Value>()
            .await?;

        if let Some(accounts) = resp["result"].as_array() {
            let parsed: Vec<Pubkey> = accounts
                .iter()
                .filter_map(|a| a.as_str())
                .filter_map(|s| Pubkey::from_str(s).ok())
                .collect();

            if !parsed.is_empty() {
                info!("Refreshed {} tip accounts from Jito", parsed.len());
                self.tip_accounts = parsed;
            }
        }

        Ok(())
    }

    /// Pick a random tip account for load distribution
    fn random_tip_account(&self) -> Pubkey {
        let mut rng = rand::thread_rng();
        *self.tip_accounts.choose(&mut rng).unwrap_or(&self.tip_accounts[0])
    }

    /// Build a SOL transfer tip transaction
    fn build_tip_tx(
        &self,
        payer: &Keypair,
        tip_lamports: u64,
        recent_blockhash: solana_sdk::hash::Hash,
    ) -> Transaction {
        let tip_account = self.random_tip_account();
        let system_program = Pubkey::from_str("11111111111111111111111111111111").unwrap();

        // System Program Transfer instruction (index = 2)
        let mut data = vec![2u8, 0, 0, 0]; // Transfer instruction discriminator
        data.extend_from_slice(&tip_lamports.to_le_bytes());

        let ix = Instruction::new_with_bytes(
            system_program,
            &data,
            vec![
                AccountMeta::new(payer.pubkey(), true),
                AccountMeta::new(tip_account, false),
            ],
        );

        let message = solana_sdk::message::Message::new(&[ix], Some(&payer.pubkey()));
        let mut tx = Transaction::new_unsigned(message);
        tx.sign(&[payer], recent_blockhash);
        tx
    }

    /// Submit a bundle of transactions to Jito block engine.
    ///
    /// The bundle includes all provided transactions plus a tip transaction.
    /// When a secondary endpoint is configured (JITO_BLOCK_ENGINE_SECONDARY),
    /// races both endpoints simultaneously — first success wins.
    /// Returns the bundle UUID on success.
    pub async fn submit_bundle(
        &self,
        transactions: Vec<Transaction>,
        tip_lamports: u64,
        payer: &Keypair,
        recent_blockhash: solana_sdk::hash::Hash,
    ) -> Result<String> {
        // Build tip transaction
        let tip_tx = self.build_tip_tx(payer, tip_lamports, recent_blockhash);

        // Serialize all transactions to base58
        let mut encoded_txs: Vec<String> = Vec::new();
        for tx in &transactions {
            let bytes = bincode::serialize(tx)
                .context("Failed to serialize transaction")?;
            encoded_txs.push(bs58::encode(&bytes).into_string());
        }
        // Tip is the last transaction in the bundle
        let tip_bytes = bincode::serialize(&tip_tx)
            .context("Failed to serialize tip transaction")?;
        encoded_txs.push(bs58::encode(&tip_bytes).into_string());

        debug!("Submitting Jito bundle: {} txs + tip ({} lamports)", transactions.len(), tip_lamports);

        // Build JSON-RPC request (shared by both endpoints)
        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "sendBundle",
            "params": [encoded_txs]
        });

        // Multi-region: race primary + secondary if configured
        if let Some(ref secondary_url) = self.secondary_bundle_url {
            let primary_fut = self.post_bundle(&self.bundle_url, &request);
            let secondary_fut = self.post_bundle(secondary_url, &request);
            tokio::pin!(primary_fut);
            tokio::pin!(secondary_fut);

            tokio::select! {
                primary_result = &mut primary_fut => {
                    match primary_result {
                        Ok(id) => {
                            info!("Jito bundle submitted (primary): {}", id);
                            Ok(id)
                        }
                        Err(primary_err) => {
                            warn!("Jito primary failed ({}), awaiting secondary...", primary_err);
                            match secondary_fut.await {
                                Ok(id) => {
                                    info!("Jito bundle submitted (secondary): {}", id);
                                    Ok(id)
                                }
                                Err(sec_err) => {
                                    Err(anyhow::anyhow!("primary: {} | secondary: {}", primary_err, sec_err))
                                }
                            }
                        }
                    }
                }
                secondary_result = &mut secondary_fut => {
                    match secondary_result {
                        Ok(id) => {
                            info!("Jito bundle submitted (secondary won): {}", id);
                            Ok(id)
                        }
                        Err(sec_err) => {
                            warn!("Jito secondary failed ({}), awaiting primary...", sec_err);
                            match primary_fut.await {
                                Ok(id) => {
                                    info!("Jito bundle submitted (primary): {}", id);
                                    Ok(id)
                                }
                                Err(primary_err) => {
                                    Err(anyhow::anyhow!("primary: {} | secondary: {}", primary_err, sec_err))
                                }
                            }
                        }
                    }
                }
            }
        } else {
            // Single endpoint
            let bundle_id = self.post_bundle(&self.bundle_url, &request).await?;
            info!("Jito bundle submitted: {}", bundle_id);
            Ok(bundle_id)
        }
    }

    /// Post a bundle to a single Jito endpoint and return the bundle UUID.
    async fn post_bundle(&self, url: &str, request: &serde_json::Value) -> Result<String> {
        let resp = self.http
            .post(url)
            .json(request)
            .send()
            .await
            .context("Jito bundle submission failed")?
            .json::<serde_json::Value>()
            .await?;

        if let Some(err) = resp.get("error") {
            let err_msg = err.get("message")
                .and_then(|m| m.as_str())
                .unwrap_or("Unknown Jito error");
            anyhow::bail!("Jito bundle rejected: {}", err_msg);
        }

        let bundle_id = resp["result"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("No bundle ID in Jito response"))?
            .to_string();

        Ok(bundle_id)
    }

    /// Check bundle status (fire-and-forget, for logging)
    pub async fn check_bundle_status(&self, bundle_id: &str) -> Result<String> {
        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getBundleStatuses",
            "params": [[bundle_id]]
        });

        let resp = self.http
            .post(&self.bundle_url)
            .json(&request)
            .send()
            .await?
            .json::<serde_json::Value>()
            .await?;

        let status = resp["result"]["value"]
            .as_array()
            .and_then(|arr| arr.first())
            .and_then(|s| s["confirmation_status"].as_str())
            .unwrap_or("unknown")
            .to_string();

        Ok(status)
    }

    /// Direct RPC fallback: send a single transaction via standard RPC
    /// (used when Jito bundle submission fails)
    pub async fn send_via_rpc(
        rpc: &crate::rpc::RpcClient,
        tx: &Transaction,
    ) -> Result<String> {
        rpc.send_transaction(tx).await
            .context("RPC send_and_confirm failed")
    }
}

/// Race Jito bundle submission against direct RPC; same signed tx → chain dedupes.
///
/// `jito_method` / `rpc_method` are returned to Python (e.g. `"jito"` vs `"rpc"`, or
/// `"jito_kill"` / `"rpc_kill"` for kill chain).
///
/// Bounded by `BIGAL_LAND_TIMEOUT_SEC` (default 28). Uses `biased` polling so RPC is
/// checked first when both complete — more `rpc` wins when timings are similar.
pub async fn race_land_submit(
    jito_fut: impl std::future::Future<Output = Result<String>> + Send,
    rpc_fut: impl std::future::Future<Output = Result<String>> + Send,
    jito_method: &'static str,
    rpc_method: &'static str,
) -> Result<(String, &'static str)> {
    let secs: u64 = std::env::var("BIGAL_LAND_TIMEOUT_SEC")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(28);
    let land_deadline = Duration::from_secs(secs);

    tokio::time::timeout(land_deadline, async {
        tokio::pin!(jito_fut);
        tokio::pin!(rpc_fut);

        tokio::select! {
            biased;
            rpc_result = &mut rpc_fut => {
                match rpc_result {
                    Ok(sig) => Ok((sig, rpc_method)),
                    Err(rpc_err) => {
                        warn!("RPC lost race ({}), awaiting Jito...", rpc_err);
                        match jito_fut.await {
                            Ok(id) => Ok((id, jito_method)),
                            Err(jito_err) => Err(anyhow::anyhow!("jito: {} | rpc: {}", jito_err, rpc_err)),
                        }
                    }
                }
            }
            jito_result = &mut jito_fut => {
                match jito_result {
                    Ok(id) => Ok((id, jito_method)),
                    Err(jito_err) => {
                        warn!("Jito lost race ({}), awaiting RPC...", jito_err);
                        match rpc_fut.await {
                            Ok(sig) => Ok((sig, rpc_method)),
                            Err(rpc_err) => Err(anyhow::anyhow!("jito: {} | rpc: {}", jito_err, rpc_err)),
                        }
                    }
                }
            }
        }
    })
    .await
    .map_err(|_| {
        anyhow::anyhow!(
            "Jito+RPC land timed out after {}s (raise BIGAL_LAND_TIMEOUT_SEC or ZMQ_SELL_WAIT_SEC if needed)",
            secs
        )
    })?
}

// ─────────────────────────────────────────────
//  Dynamic Jito Tip Estimator
//  Polls the Jito tip_floor API for the 75th percentile
//  landed tip, applies 1.2x multiplier, floor at 10,000 lamports.
// ─────────────────────────────────────────────

/// Default Jito tip floor API endpoint
pub const DEFAULT_TIP_FLOOR_URL: &str = "https://bundles.jito.wtf/api/v1/bundles/tip_floor";

/// Minimum tip floor (10,000 lamports = 0.00001 SOL)
const MIN_TIP_LAMPORTS: u64 = 10_000;

/// Dynamic tip estimator — mirrors FeeEstimator pattern.
pub struct TipEstimator;

impl TipEstimator {
    /// Start polling the Jito tip floor API as a background tokio task.
    /// Returns a handle to the tip value (in lamports) readable from any thread.
    pub fn start_polling(
        tip_floor_url: String,
        default_tip: u64,
        poll_interval_ms: u64,
    ) -> Arc<AtomicU64> {
        let tip = Arc::new(AtomicU64::new(default_tip));
        let tip_clone = tip.clone();

        tokio::spawn(async move {
            let client = reqwest::Client::new();
            let interval = Duration::from_millis(poll_interval_ms);

            info!("Jito TipEstimator started | polling every {}ms | url={}", poll_interval_ms, tip_floor_url);

            loop {
                match poll_tip_floor(&client, &tip_floor_url).await {
                    Ok(tip_lamports) => {
                        let adjusted = ((tip_lamports as f64) * 1.2) as u64;
                        let adjusted = adjusted.max(MIN_TIP_LAMPORTS);
                        tip_clone.store(adjusted, Ordering::Relaxed);
                        debug!("Tip update: raw_p75={} adjusted={} lamports", tip_lamports, adjusted);
                    }
                    Err(e) => {
                        warn!("Tip floor poll failed (using current): {}", e);
                    }
                }

                tokio::time::sleep(interval).await;
            }
        });

        tip
    }
}

/// Poll the Jito tip floor API and return the 75th percentile tip in lamports.
async fn poll_tip_floor(client: &reqwest::Client, url: &str) -> Result<u64> {
    let resp = client
        .get(url)
        .send()
        .await
        .context("Jito tip floor request failed")?
        .json::<serde_json::Value>()
        .await
        .context("Jito tip floor parse failed")?;

    // Response is an array of objects with percentile fields
    let entry = if let Some(arr) = resp.as_array() {
        arr.first()
            .ok_or_else(|| anyhow::anyhow!("Empty tip floor response"))?
    } else {
        &resp
    };

    let tip_sol = entry
        .get("landed_tips_75th_percentile")
        .and_then(|v| v.as_f64())
        .ok_or_else(|| anyhow::anyhow!("Missing landed_tips_75th_percentile in response"))?;

    // Convert SOL to lamports
    let tip_lamports = (tip_sol * 1_000_000_000.0) as u64;

    Ok(tip_lamports)
}
