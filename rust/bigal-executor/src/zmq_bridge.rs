use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::time::Instant;
use tracing::{debug, info, warn};
use zeromq::{PubSocket, PullSocket, Socket, SocketRecv, SocketSend, ZmqMessage};

/// Inbound signal from Python (trade command or ping)
#[derive(Debug, Deserialize)]
pub struct Signal {
    pub id: String,
    #[serde(default)]
    pub seq: u64,
    pub action: String,
    #[serde(default)]
    pub mint: String,
    #[serde(default)]
    pub amount: f64,
    #[serde(default)]
    pub amount_type: String,
    #[serde(default)]
    pub slippage_bps: u16,
    #[serde(default)]
    pub priority: String,
    #[serde(default)]
    pub timestamp_ms: u64,
    /// Python-resolved priority fee (lamports). If > 0, Rust prefers this over its own estimate.
    #[serde(default)]
    pub priority_fee_lamports: u64,
    /// Optional base58-encoded keypair for puppet wallet signing.
    /// When present, the executor signs with this keypair instead of the main wallet.
    #[serde(default)]
    pub keypair_b58: Option<String>,
    /// Optional config payload for runtime updates
    #[serde(default)]
    pub config: Option<serde_json::Value>,
}

/// Latency breakdown returned to Python
#[derive(Debug, Serialize, Default)]
pub struct LatencyInfo {
    pub zmq_recv_ms: f64,
    pub build_ms: f64,
    pub land_ms: f64,
    pub total_ms: f64,
    /// Actual priority fee used (lamports) — fed back to Python's AdaptiveState
    #[serde(default)]
    pub fee_used_lamports: u64,
    /// Actual Jito tip used (lamports) — fed back to Python's AdaptiveState
    #[serde(default)]
    pub tip_used_lamports: u64,
}

/// Outbound result to Python
#[derive(Debug, Serialize)]
pub struct TradeResult {
    pub id: String,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub method: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latency: Option<LatencyInfo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl TradeResult {
    pub fn pong(id: String) -> Self {
        TradeResult {
            id,
            status: "pong".to_string(),
            signature: None,
            method: None,
            latency: None,
            error: None,
        }
    }

    pub fn confirmed(id: String, signature: String, method: String, latency: LatencyInfo) -> Self {
        TradeResult {
            id,
            status: "confirmed".to_string(),
            signature: Some(signature),
            method: Some(method),
            latency: Some(latency),
            error: None,
        }
    }

    pub fn failed(id: String, error: String) -> Self {
        TradeResult {
            id,
            status: "failed".to_string(),
            signature: None,
            method: None,
            latency: None,
            error: Some(error),
        }
    }
}

/// ZMQ communication bridge between Python strategy and Rust executor.
///
/// Socket layout (matches zmq_bridge.py):
///   Python PUSH binds on signal_port → Rust PULL connects
///   Rust PUB binds on result_port   → Python SUB connects
pub struct ZmqBridge {
    pull: PullSocket,
    pub_sock: PubSocket,
    signal_addr: String,
    result_addr: String,
}

impl ZmqBridge {
    /// Create and connect a new ZMQ bridge.
    /// `signal_connect` — Python PUSH bind address (Rust PULL connects).
    /// `result_bind` — address Rust PUB binds (Python SUB connects).
    ///
    /// Retries connection for up to 60s if Python hasn't bound the IPC sockets yet,
    /// so startup order doesn't matter.
    pub async fn new(signal_connect: &str, result_bind: &str) -> Result<Self> {
        let signal_addr = signal_connect.to_string();
        let result_addr = result_bind.to_string();

        let max_wait_secs: u64 = std::env::var("ZMQ_CONNECT_WAIT_SEC")
            .ok().and_then(|s| s.parse().ok()).unwrap_or(60);
        let poll_ms = 500u64;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(max_wait_secs);

        // Wait for IPC socket files to exist (Python binds them on mode select)
        if signal_addr.starts_with("ipc://") {
            let path = &signal_addr["ipc://".len()..];
            if !std::path::Path::new(path).exists() {
                info!("Waiting for Python to bind ZMQ (select Live Rust in dashboard)...");
                while !std::path::Path::new(path).exists() {
                    if std::time::Instant::now() > deadline {
                        anyhow::bail!(
                            "Timed out waiting {}s for Python ZMQ socket at {}",
                            max_wait_secs, path
                        );
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(poll_ms)).await;
                }
                info!("Python ZMQ socket detected — connecting");
            }
        }

        // PULL socket: receive signals from Python
        let mut pull = PullSocket::new();
        pull.connect(&signal_addr)
            .await
            .with_context(|| format!("Failed to connect PULL to {}", signal_addr))?;

        // PUB socket: publish results to Python — retry if address in use (stale socket)
        let mut pub_sock = PubSocket::new();
        let mut bound = false;
        while std::time::Instant::now() < deadline {
            match pub_sock.bind(&result_addr).await {
                Ok(_) => { bound = true; break; }
                Err(e) => {
                    let msg = format!("{}", e);
                    if msg.contains("Address already in use") || msg.contains("os error 98") {
                        // Stale IPC socket from previous run — remove and retry
                        if result_addr.starts_with("ipc://") {
                            let path = &result_addr["ipc://".len()..];
                            let _ = std::fs::remove_file(path);
                            warn!("Removed stale IPC socket {}, retrying bind...", path);
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(poll_ms)).await;
                        pub_sock = PubSocket::new();
                        continue;
                    }
                    return Err(anyhow::anyhow!("Failed to bind PUB on {}: {}", result_addr, e));
                }
            }
        }
        if !bound {
            anyhow::bail!("Timed out binding PUB on {}", result_addr);
        }

        info!(
            "ZMQ PULL connected to {}, PUB bound on {}",
            signal_addr, result_addr
        );

        Ok(ZmqBridge {
            pull,
            pub_sock,
            signal_addr,
            result_addr,
        })
    }

    /// Async receive — waits for next signal from Python.
    /// Returns (Signal, receive_timestamp) for latency tracking.
    pub async fn recv_signal(&mut self) -> Result<(Signal, Instant)> {
        let msg = self.pull.recv().await
            .map_err(|e| anyhow::anyhow!("ZMQ recv error: {}", e))?;

        let recv_time = Instant::now();

        // Extract first frame as string
        let data = msg
            .into_vec()
            .into_iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("Empty ZMQ message"))?;
        let text = String::from_utf8(data.to_vec())
            .context("ZMQ message is not valid UTF-8")?;

        debug!("ZMQ recv: {}", &text[..text.len().min(200)]);

        let signal: Signal = serde_json::from_str(&text)
            .with_context(|| format!("Bad JSON signal: {}", &text[..text.len().min(100)]))?;

        Ok((signal, recv_time))
    }

    /// Send a result back to Python via PUB socket.
    pub async fn send_result(&mut self, result: &TradeResult) -> Result<()> {
        let json = serde_json::to_string(result).context("Failed to serialize TradeResult")?;

        debug!("ZMQ send: {}", &json[..json.len().min(200)]);

        let msg = ZmqMessage::from(json);
        self.pub_sock
            .send(msg)
            .await
            .map_err(|e| anyhow::anyhow!("ZMQ send error: {}", e))?;

        Ok(())
    }

    /// Addresses for logging
    pub fn signal_addr(&self) -> &str {
        &self.signal_addr
    }

    pub fn result_addr(&self) -> &str {
        &self.result_addr
    }
}

/// Create a PUB socket for Yellowstone → Python (same JSON shape as `sidecar/sidecar_grpc.js`).
/// Bind address: `ZMQ_YELLOWSTONE_IPC` (default `ipc:///tmp/bigal_yellowstone.ipc` on Unix) or
/// `tcp://127.0.0.1:ZMQ_GRPC_PORT` on Windows — must match `grpc_zmq_listener` in bigalbot.
pub async fn create_yellowstone_pub(bind_addr: &str) -> Result<PubSocket> {
    let mut pub_sock = PubSocket::new();
    pub_sock
        .bind(bind_addr)
        .await
        .with_context(|| format!("Failed to bind Yellowstone ZMQ PUB on {}", bind_addr))?;
    info!("Yellowstone ZMQ PUB bound on {}", bind_addr);
    Ok(pub_sock)
}
