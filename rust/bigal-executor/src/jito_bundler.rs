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

        // Initialize with fallback tip accounts
        let tip_accounts: Vec<Pubkey> = FALLBACK_TIP_ACCOUNTS
            .iter()
            .filter_map(|s| Pubkey::from_str(s).ok())
            .collect();

        info!("JitoBundler initialized | url={} | tip_accounts={}", bundle_url, tip_accounts.len());

        JitoBundler {
            bundle_url,
            tip_accounts,
            http: reqwest::Client::new(),
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

        // Submit via JSON-RPC
        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "sendBundle",
            "params": [encoded_txs]
        });

        let resp = self.http
            .post(&self.bundle_url)
            .json(&request)
            .send()
            .await
            .context("Jito bundle submission failed")?
            .json::<serde_json::Value>()
            .await?;

        // Check for errors
        if let Some(err) = resp.get("error") {
            let err_msg = err.get("message")
                .and_then(|m| m.as_str())
                .unwrap_or("Unknown Jito error");
            anyhow::bail!("Jito bundle rejected: {}", err_msg);
        }

        // Extract bundle UUID
        let bundle_id = resp["result"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("No bundle ID in Jito response"))?
            .to_string();

        info!("Jito bundle submitted: {}", bundle_id);
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
        rpc.send_and_confirm_transaction(tx).await
            .context("RPC send_and_confirm failed")
    }
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
