use anyhow::{Context, Result};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signer::keypair::Keypair;
use solana_sdk::signer::Signer;
use std::env;
use std::str::FromStr;

/// Pump.fun program ID — verify against live program before mainnet use
pub const PUMP_FUN_PROGRAM_ID: &str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";

/// Known Pump.fun fee recipient
pub const PUMP_FUN_FEE_RECIPIENT: &str = "CebN5WGQ4jvEPvsVU4EoHEpgzq1VV7AbicfhtW4xC9iM";

/// SPL Token Program
pub const TOKEN_PROGRAM_ID: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";

/// Associated Token Program
pub const ASSOCIATED_TOKEN_PROGRAM_ID: &str = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL";

/// System Program
pub const SYSTEM_PROGRAM_ID: &str = "11111111111111111111111111111111";

/// Rent Sysvar
pub const RENT_SYSVAR_ID: &str = "SysvarRent111111111111111111111111111111111";

/// Default Jito block engine (NY — US East, pairs with `us-east-1` deployments)
pub const DEFAULT_JITO_BLOCK_ENGINE: &str = "https://ny.mainnet.block-engine.jito.wtf";

/// Default Jito bundle endpoint
pub const DEFAULT_JITO_BUNDLE_URL: &str = "https://ny.mainnet.block-engine.jito.wtf/api/v1/bundles";

/// ZMQ IPC paths (Linux/macOS) — must match Python `zmq_bridge.py` / `bigalbot_customizable.py`
pub const DEFAULT_ZMQ_SIGNAL_IPC: &str = "ipc:///tmp/bigal_signal.ipc";
pub const DEFAULT_ZMQ_RESULT_IPC: &str = "ipc:///tmp/bigal_result.ipc";

/// Rust Yellowstone → Python SUB (must match `ZMQ_YELLOWSTONE_IPC` in bigalbot on Unix)
pub const DEFAULT_ZMQ_YELLOWSTONE_IPC: &str = "ipc:///tmp/bigal_yellowstone.ipc";

/// Default Jito tip (5000 lamports = 0.000005 SOL)
pub const DEFAULT_JITO_TIP_LAMPORTS: u64 = 5_000_000; // 0.005 SOL

/// Application configuration loaded from environment variables.
pub struct Config {
    /// Solana RPC endpoint (Helius or other) — sends, blockhash, curve state
    pub rpc_url: String,

    /// JSON-RPC URL used for post-buy token balance polling on sells (defaults to `rpc_url`).
    /// Set `HELIUS_READ_RPC_URL` to your paid Helius HTTP endpoint so balance reads match buy landing speed.
    pub rpc_read_url: String,

    /// Wallet keypair for signing transactions
    pub keypair: Keypair,

    /// Yellowstone gRPC endpoint (optional — enables Geyser streaming)
    pub grpc_endpoint: Option<String>,

    /// gRPC auth token
    pub grpc_x_token: Option<String>,

    /// Jito block engine URL
    pub jito_block_engine: String,

    /// Jito bundle submission URL
    pub jito_bundle_url: String,

    /// ZMQ signal port (Python PUSH → Rust PULL)
    pub zmq_signal_port: u16,

    /// ZMQ result port (Rust PUB → Python SUB)
    pub zmq_result_port: u16,

    /// ZMQ gRPC forward port (Rust PUB → Python SUB for gRPC events, Windows TCP fallback)
    pub zmq_grpc_port: u16,

    /// Bind address for Rust Yellowstone PUB (`ZMQ_YELLOWSTONE_IPC` or Unix default IPC / Windows TCP)
    pub zmq_yellowstone_bind: String,

    /// ZMQ signal endpoint (Rust PULL connects — Python PUSH binds here)
    pub zmq_signal_addr: String,

    /// ZMQ result endpoint (Rust PUB binds — Python SUB connects here)
    pub zmq_result_addr: String,

    /// Pump.fun program ID
    pub pump_program_id: Pubkey,

    /// Fee recipient address
    pub fee_recipient: Pubkey,

    /// Default priority fee in micro-lamports (fallback if estimator fails)
    pub default_priority_fee: u64,

    /// Jito tip floor API URL (for dynamic tip estimation)
    pub jito_tip_floor_url: String,
}

impl Config {
    /// Load configuration from environment variables.
    /// Call `dotenv::dotenv().ok()` before this to load `.env` file.
    pub fn from_env() -> Result<Self> {
        let rpc_url = env::var("HELIUS_RPC_URL")
            .context("HELIUS_RPC_URL not set — provide your Solana RPC endpoint")?;

        let rpc_read_url = env::var("HELIUS_READ_RPC_URL")
            .or_else(|_| env::var("BIGAL_READ_RPC_URL"))
            .ok()
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| rpc_url.clone());

        let keypair = load_keypair()?;

        let grpc_endpoint = env::var("GRPC_ENDPOINT").ok().filter(|s| !s.is_empty());
        let grpc_x_token = env::var("GRPC_X_TOKEN").ok().filter(|s| !s.is_empty());

        let jito_block_engine = env::var("JITO_BLOCK_ENGINE")
            .unwrap_or_else(|_| DEFAULT_JITO_BLOCK_ENGINE.to_string());
        let jito_bundle_url = env::var("JITO_BUNDLE_URL")
            .unwrap_or_else(|_| DEFAULT_JITO_BUNDLE_URL.to_string());

        let zmq_signal_port = env::var("ZMQ_SIGNAL_PORT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(5555);
        let zmq_result_port = env::var("ZMQ_RESULT_PORT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(5556);
        let zmq_grpc_port = env::var("ZMQ_GRPC_PORT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(5557);

        let zmq_yellowstone_bind = env::var("ZMQ_YELLOWSTONE_IPC").unwrap_or_else(|_| {
            #[cfg(unix)]
            {
                DEFAULT_ZMQ_YELLOWSTONE_IPC.to_string()
            }
            #[cfg(not(unix))]
            {
                format!("tcp://127.0.0.1:{}", zmq_grpc_port)
            }
        });

        let (zmq_signal_addr, zmq_result_addr) = resolve_zmq_endpoints(zmq_signal_port, zmq_result_port);

        let pump_program_id = env::var("PUMP_FUN_PROGRAM_ID")
            .ok()
            .and_then(|s| Pubkey::from_str(&s).ok())
            .unwrap_or_else(|| Pubkey::from_str(PUMP_FUN_PROGRAM_ID).unwrap());

        let fee_recipient = env::var("PUMP_FUN_FEE_RECIPIENT")
            .ok()
            .and_then(|s| Pubkey::from_str(&s).ok())
            .unwrap_or_else(|| Pubkey::from_str(PUMP_FUN_FEE_RECIPIENT).unwrap());

        let default_priority_fee = env::var("DEFAULT_PRIORITY_FEE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(10_000); // 10k micro-lamports

        let jito_tip_floor_url = env::var("JITO_TIP_FLOOR_URL")
            .unwrap_or_else(|_| crate::jito_bundler::DEFAULT_TIP_FLOOR_URL.to_string());

        Ok(Config {
            rpc_url,
            rpc_read_url,
            keypair,
            grpc_endpoint,
            grpc_x_token,
            jito_block_engine,
            jito_bundle_url,
            zmq_signal_port,
            zmq_result_port,
            zmq_grpc_port,
            zmq_yellowstone_bind,
            zmq_signal_addr,
            zmq_result_addr,
            pump_program_id,
            fee_recipient,
            default_priority_fee,
            jito_tip_floor_url,
        })
    }

    /// Wallet public key as string
    pub fn pubkey_str(&self) -> String {
        self.keypair.pubkey().to_string()
    }
}

/// Prefer `/tmp/` IPC on Unix (matches Python); TCP loopback on Windows.
fn resolve_zmq_endpoints(signal_port: u16, result_port: u16) -> (String, String) {
    #[cfg(unix)]
    {
        let _ = (signal_port, result_port);
        let signal = env::var("ZMQ_SIGNAL_IPC")
            .unwrap_or_else(|_| DEFAULT_ZMQ_SIGNAL_IPC.to_string());
        let result = env::var("ZMQ_RESULT_IPC")
            .unwrap_or_else(|_| DEFAULT_ZMQ_RESULT_IPC.to_string());
        (signal, result)
    }
    #[cfg(not(unix))]
    {
        let signal = env::var("ZMQ_SIGNAL_IPC").unwrap_or_else(|_| {
            format!("tcp://127.0.0.1:{}", signal_port)
        });
        let result = env::var("ZMQ_RESULT_IPC").unwrap_or_else(|_| {
            format!("tcp://127.0.0.1:{}", result_port)
        });
        (signal, result)
    }
}

/// Convert raw bytes to a Keypair (handles both 64-byte and 32-byte formats)
fn keypair_from_raw_bytes(bytes: &[u8]) -> Result<Keypair> {
    match bytes.len() {
        64 => {
            // Old format: 32-byte secret key + 32-byte public key
            let mut secret = [0u8; 32];
            secret.copy_from_slice(&bytes[..32]);
            Ok(Keypair::new_from_array(secret))
        }
        32 => {
            // Just the secret key
            let mut secret = [0u8; 32];
            secret.copy_from_slice(bytes);
            Ok(Keypair::new_from_array(secret))
        }
        n => anyhow::bail!("Invalid keypair length: {} bytes (expected 32 or 64)", n),
    }
}

/// Load keypair from BIGAL_PRIVATE_KEY env var (base58) or keypair.txt file
fn load_keypair() -> Result<Keypair> {
    // Try env var first
    if let Ok(key_str) = env::var("BIGAL_PRIVATE_KEY") {
        let key_str = key_str.trim();

        // Try base58 format
        if let Ok(bytes) = bs58::decode(key_str).into_vec() {
            return keypair_from_raw_bytes(&bytes);
        }

        // Try JSON array format [1,2,3,...]
        if key_str.starts_with('[') {
            let bytes: Vec<u8> = serde_json::from_str(key_str)
                .context("Invalid JSON array keypair")?;
            return keypair_from_raw_bytes(&bytes);
        }

        anyhow::bail!("BIGAL_PRIVATE_KEY is set but not valid base58 or JSON array");
    }

    // Try keypair.txt file
    let keypair_path = env::var("KEYPAIR_PATH").unwrap_or_else(|_| "keypair.txt".to_string());
    if let Ok(contents) = std::fs::read_to_string(&keypair_path) {
        let contents = contents.trim();
        if contents.starts_with('[') {
            let bytes: Vec<u8> = serde_json::from_str(contents)
                .context("Invalid JSON in keypair.txt")?;
            return keypair_from_raw_bytes(&bytes);
        }
        if let Ok(bytes) = bs58::decode(contents).into_vec() {
            return keypair_from_raw_bytes(&bytes);
        }
    }

    anyhow::bail!(
        "No keypair found. Set BIGAL_PRIVATE_KEY env var (base58 or JSON array) \
         or place a keypair.txt file in the working directory."
    )
}
