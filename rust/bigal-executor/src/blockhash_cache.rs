/// Rolling blockhash pre-fetch.
///
/// Background tokio task polls `getLatestBlockhash` every 2 seconds and stores
/// the result for instant access during trade execution. Consumers reject
/// blockhashes older than 30 seconds (Solana TTL is ~60 seconds).
///
/// Follows the same pattern as `fee_estimator.rs`.

use anyhow::Result;
use solana_sdk::hash::Hash;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

pub struct CachedBlockhash {
    pub hash: Hash,
    pub fetched_at: Instant,
}

/// Shared handle to the cached blockhash.
pub type BlockhashHandle = Arc<RwLock<Option<CachedBlockhash>>>;

pub struct BlockhashCache;

impl BlockhashCache {
    /// Start the polling loop as a background tokio task.
    /// Returns a handle that can be read from any async context.
    pub fn start_polling(rpc_url: String, poll_interval_ms: u64) -> BlockhashHandle {
        let handle: BlockhashHandle = Arc::new(RwLock::new(None));
        let handle_clone = handle.clone();

        tokio::spawn(async move {
            let client = reqwest::Client::new();
            let interval = Duration::from_millis(poll_interval_ms);

            info!(
                "Blockhash cache started | polling every {}ms",
                poll_interval_ms
            );

            loop {
                match poll_blockhash(&client, &rpc_url).await {
                    Ok(hash) => {
                        let mut lock = handle_clone.write().await;
                        *lock = Some(CachedBlockhash {
                            hash,
                            fetched_at: Instant::now(),
                        });
                        debug!("Blockhash cache updated: {}", hash);
                    }
                    Err(e) => {
                        warn!("Blockhash poll failed (stale cache remains): {}", e);
                    }
                }

                tokio::time::sleep(interval).await;
            }
        });

        handle
    }
}

/// Read a non-stale blockhash from the cache, returning None if missing or too old.
pub async fn read_cached_blockhash(handle: &BlockhashHandle, max_age_secs: u64) -> Option<Hash> {
    let lock = handle.read().await;
    match &*lock {
        Some(cached) if cached.fetched_at.elapsed().as_secs() < max_age_secs => {
            Some(cached.hash)
        }
        _ => None,
    }
}

/// Fetch latest blockhash from RPC.
async fn poll_blockhash(client: &reqwest::Client, rpc_url: &str) -> Result<Hash> {
    let request = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getLatestBlockhash",
        "params": [{"commitment": "processed"}]
    });

    let resp = client
        .post(rpc_url)
        .json(&request)
        .send()
        .await?
        .json::<serde_json::Value>()
        .await?;

    let hash_str = resp["result"]["value"]["blockhash"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("No blockhash in response"))?;

    let hash = hash_str
        .parse::<Hash>()
        .map_err(|e| anyhow::anyhow!("Failed to parse blockhash: {}", e))?;

    Ok(hash)
}
