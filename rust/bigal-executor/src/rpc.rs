/// Lightweight Solana JSON-RPC client using reqwest.
///
/// Replaces solana-client dependency to avoid beta/version conflicts.
/// Only implements the RPC methods needed by the executor.

use anyhow::{Context, Result};
use solana_sdk::{
    hash::Hash,
    pubkey::Pubkey,
    transaction::Transaction,
};
use tracing::debug;

pub struct RpcClient {
    url: String,
    http: reqwest::Client,
}

impl RpcClient {
    pub fn new(url: &str) -> Self {
        RpcClient {
            url: url.to_string(),
            http: reqwest::Client::new(),
        }
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    /// Call a JSON-RPC method
    async fn call(&self, method: &str, params: serde_json::Value) -> Result<serde_json::Value> {
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params
        });

        let resp = self.http
            .post(&self.url)
            .json(&body)
            .send()
            .await
            .with_context(|| format!("RPC {} request failed", method))?
            .json::<serde_json::Value>()
            .await
            .with_context(|| format!("RPC {} response parse failed", method))?;

        if let Some(err) = resp.get("error") {
            let msg = err.get("message").and_then(|m| m.as_str()).unwrap_or("Unknown RPC error");
            anyhow::bail!("RPC error in {}: {}", method, msg);
        }

        Ok(resp["result"].clone())
    }

    /// Get the latest blockhash
    pub async fn get_latest_blockhash(&self) -> Result<Hash> {
        let result = self.call("getLatestBlockhash", serde_json::json!([
            {"commitment": "processed"}
        ])).await?;

        let hash_str = result["value"]["blockhash"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("No blockhash in response"))?;

        hash_str.parse().context("Invalid blockhash format")
    }

    /// Get account data (for reading bonding curve state)
    pub async fn get_account_data(&self, pubkey: &Pubkey) -> Result<Vec<u8>> {
        let result = self.call("getAccountInfo", serde_json::json!([
            pubkey.to_string(),
            {"encoding": "base64", "commitment": "processed"}
        ])).await?;

        let data_arr = result["value"]["data"]
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("Account not found: {}", pubkey))?;

        let b64_data = data_arr[0]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("Invalid account data format"))?;

        use base64::Engine;
        base64::engine::general_purpose::STANDARD
            .decode(b64_data)
            .context("Failed to decode account data from base64")
    }

    /// Get SPL token account balance via `getTokenAccountBalance`.
    /// **Always uses `processed` commitment** — default RPC commitment can lag badly behind a
    /// just-landed buy, which caused `No tokens to sell (after N retries)` while Legacy still worked.
    pub async fn get_token_account_balance(&self, ata: &Pubkey) -> Result<u64> {
        let result = self.call("getTokenAccountBalance", serde_json::json!([
            ata.to_string(),
            {"commitment": "processed"}
        ]))
        .await?;

        let amount_str = result["value"]["amount"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("No token balance for {}", ata))?;

        amount_str.parse().context("Invalid token balance amount")
    }

    /// Fallback: read raw amount from `getAccountInfo` + `jsonParsed` (some nodes return balance here
    /// when `getTokenAccountBalance` errors or lags).
    pub async fn get_token_balance_json_parsed(&self, ata: &Pubkey) -> Result<u64> {
        let result = self
            .call(
                "getAccountInfo",
                serde_json::json!([
                    ata.to_string(),
                    {"encoding": "jsonParsed", "commitment": "processed"}
                ]),
            )
            .await?;

        let value = &result["value"];
        if value.is_null() {
            anyhow::bail!("ATA not found: {}", ata);
        }

        let amount_str = value
            .pointer("/data/parsed/info/tokenAmount/amount")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("jsonParsed token amount missing for {}", ata))?;

        amount_str.parse().context("Invalid parsed token amount")
    }

    /// Get SOL balance in lamports
    pub async fn get_balance(&self, pubkey: &Pubkey) -> Result<u64> {
        let result = self.call("getBalance", serde_json::json!([
            pubkey.to_string(),
            {"commitment": "processed"}
        ])).await?;

        result["value"]
            .as_u64()
            .ok_or_else(|| anyhow::anyhow!("Invalid balance response"))
    }

    /// Send a signed transaction and wait for confirmation
    pub async fn send_and_confirm_transaction(&self, tx: &Transaction) -> Result<String> {
        let tx_bytes = bincode::serialize(tx)
            .context("Failed to serialize transaction")?;
        let encoded = bs58::encode(&tx_bytes).into_string();

        let result = self.call("sendTransaction", serde_json::json!([
            encoded,
            {
                "encoding": "base58",
                "skipPreflight": true,
                "preflightCommitment": "processed",
                "maxRetries": 3
            }
        ])).await?;

        let sig = result
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("No signature in sendTransaction response"))?
            .to_string();

        debug!("TX sent: {}", sig);
        Ok(sig)
    }
}
