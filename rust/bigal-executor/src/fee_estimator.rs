use anyhow::Result;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info, warn};

/// Dynamic priority fee estimator.
///
/// Polls `getRecentPrioritizationFees` for the Pump.fun program every 500ms
/// and maintains the 90th percentile fee * 1.1 (10% above the fastest bot).
pub struct FeeEstimator;

impl FeeEstimator {
    /// Start the polling loop as a background tokio task.
    /// Returns a handle to the fee value that can be read from any thread.
    pub fn start_polling(
        rpc_url: String,
        pump_program_id: String,
        default_fee: u64,
        poll_interval_ms: u64,
    ) -> Arc<AtomicU64> {
        let fee = Arc::new(AtomicU64::new(default_fee));
        let fee_clone = fee.clone();

        tokio::spawn(async move {
            let client = reqwest::Client::new();
            let interval = Duration::from_millis(poll_interval_ms);

            info!("Fee estimator started | polling every {}ms", poll_interval_ms);

            loop {
                match poll_fees(&client, &rpc_url, &pump_program_id).await {
                    Ok(estimated) => {
                        let adjusted = (estimated as f64 * 1.1) as u64;
                        let adjusted = adjusted.max(1_000); // minimum floor
                        fee_clone.store(adjusted, Ordering::Relaxed);
                        debug!("Fee update: raw_p90={} adjusted={} ulamports", estimated, adjusted);
                    }
                    Err(e) => {
                        warn!("Fee poll failed (using default): {}", e);
                        fee_clone.store(default_fee, Ordering::Relaxed);
                    }
                }

                tokio::time::sleep(interval).await;
            }
        });

        fee
    }
}

/// Poll recent prioritization fees and return the 90th percentile
async fn poll_fees(client: &reqwest::Client, rpc_url: &str, program_id: &str) -> Result<u64> {
    let request = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getRecentPrioritizationFees",
        "params": [[program_id]]
    });

    let resp = client
        .post(rpc_url)
        .json(&request)
        .send()
        .await?
        .json::<serde_json::Value>()
        .await?;

    let fees = resp["result"]
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("No result array in fee response"))?;

    if fees.is_empty() {
        anyhow::bail!("Empty fee response");
    }

    let mut fee_values: Vec<u64> = fees
        .iter()
        .rev()
        .take(5)
        .filter_map(|f| f["prioritizationFee"].as_u64())
        .collect();

    if fee_values.is_empty() {
        anyhow::bail!("No valid fee values");
    }

    fee_values.sort_unstable();

    let p90_idx = (fee_values.len() as f64 * 0.9).ceil() as usize;
    let p90 = fee_values[p90_idx.min(fee_values.len() - 1)];

    Ok(p90)
}
