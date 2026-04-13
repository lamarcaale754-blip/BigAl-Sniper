/// Yellowstone gRPC Observer — The "Geyser Eye"
///
/// Connects to a Yellowstone gRPC endpoint and subscribes to Pump.fun
/// program transactions at the "Processed" commitment level.
///
/// This sees transactions as validators are streaming shreds — before
/// they appear in the public WebSocket feeds, giving ~200-500ms advantage.
///
/// Parsed events are forwarded to Python via a dedicated ZMQ PUB socket (port 5557).
///
/// Compile with `--features grpc` to enable this module.

use serde::Serialize;
use tracing::warn;
use zeromq::PubSocket;

/// A parsed gRPC transaction event forwarded to Python
#[derive(Debug, Serialize)]
pub struct GrpcEvent {
    pub event_type: String,     // "new_token", "buy", "sell", "transaction"
    pub signature: String,      // transaction signature
    pub mint: Option<String>,   // token mint address
    pub trader: Option<String>, // wallet that initiated
    pub sol_amount: Option<f64>,
    pub slot: u64,
    pub timestamp_ms: u64,
}

/// Start the gRPC observer stream.
///
/// This function requires the `grpc` feature flag to be enabled.
/// Without it, this is a no-op stub.
#[cfg(feature = "grpc")]
pub async fn start_grpc_observer(
    endpoint: &str,
    x_token: &str,
    program_id: &str,
    watch_wallets: Vec<String>,
    zmq_pub: std::sync::Arc<tokio::sync::Mutex<PubSocket>>,
) -> anyhow::Result<()> {
    use futures::StreamExt;
    use std::collections::HashMap;
    use yellowstone_grpc_client::GeyserGrpcClient;
    use yellowstone_grpc_proto::prelude::*;
    use zeromq::{SocketSend, ZmqMessage};

    info!("Connecting to Yellowstone gRPC: {}", endpoint);

    let mut client = GeyserGrpcClient::build_from_shared(endpoint)?
        .x_token(Some(x_token.to_string()))?
        .connect()
        .await?;

    // Build subscription request
    let mut transactions = HashMap::new();
    let mut account_include = vec![program_id.to_string()];
    for wallet in &watch_wallets {
        account_include.push(wallet.clone());
    }

    transactions.insert(
        "pump_fun_txs".to_string(),
        SubscribeRequestFilterTransactions {
            vote: Some(false),
            failed: Some(false),
            account_include,
            ..Default::default()
        },
    );

    let request = SubscribeRequest {
        transactions,
        commitment: Some(CommitmentLevel::Processed as i32),
        ..Default::default()
    };

    info!(
        "gRPC subscription active | program={} | watching {} wallets",
        program_id,
        watch_wallets.len()
    );

    let (_, mut stream) = client.subscribe_with_request(Some(request)).await?;

    while let Some(message) = stream.next().await {
        match message {
            Ok(msg) => {
                if let Some(update) = msg.update_oneof {
                    if let subscribe_update::UpdateOneof::Transaction(tx_info) = update {
                        let slot = tx_info.slot;
                        if let Some(event) = extract_grpc_event(&tx_info, slot) {
                            let json = serde_json::to_string(&event).unwrap_or_default();
                            let mut sock = zmq_pub.lock().await;
                            if let Err(e) = sock.send(ZmqMessage::from(json)).await {
                                debug!("gRPC->ZMQ send failed: {}", e);
                            }
                        }
                    }
                }
            }
            Err(e) => {
                error!("gRPC stream error: {}", e);
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }
    }

    warn!("gRPC stream ended — reconnecting...");
    Ok(())
}

/// Extract a GrpcEvent from a raw gRPC transaction update.
#[cfg(feature = "grpc")]
fn extract_grpc_event(
    tx_info: &yellowstone_grpc_proto::prelude::SubscribeUpdateTransaction,
    slot: u64,
) -> Option<GrpcEvent> {
    let tx_detail = tx_info.transaction.as_ref()?;
    let inner_tx = tx_detail.transaction.as_ref()?;

    if inner_tx.signatures.is_empty() {
        return None;
    }

    let sig = bs58::encode(&inner_tx.signatures[0]).into_string();

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    let trader = inner_tx
        .message
        .as_ref()
        .and_then(|msg| msg.account_keys.first().map(|k| bs58::encode(k).into_string()));

    Some(GrpcEvent {
        event_type: "transaction".to_string(),
        signature: sig,
        mint: None,
        trader,
        sol_amount: None,
        slot,
        timestamp_ms: now_ms,
    })
}

/// Stub implementation when gRPC feature is not enabled
#[cfg(not(feature = "grpc"))]
pub async fn start_grpc_observer(
    _endpoint: &str,
    _x_token: &str,
    _program_id: &str,
    _watch_wallets: Vec<String>,
    _zmq_pub: std::sync::Arc<tokio::sync::Mutex<PubSocket>>,
) -> anyhow::Result<()> {
    warn!("gRPC observer not available — compile with `--features grpc` to enable");
    warn!("Falling back to WebSocket-based observation (higher latency)");
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
    }
}
