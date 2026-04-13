use anyhow::{Context, Result};
use borsh::BorshSerialize;
use solana_sdk::{
    hash::Hash,
    instruction::{AccountMeta, Instruction},
    message::Message,
    pubkey::Pubkey,
    signer::keypair::Keypair,
    signer::Signer,
    transaction::Transaction,
};
use std::env;
use std::str::FromStr;
use tracing::{debug, info};

/// Well-known program IDs
const COMPUTE_BUDGET_PROGRAM: &str = "ComputeBudget111111111111111111111111111111";
const SYSTEM_PROGRAM: &str = "11111111111111111111111111111111";

/// Build a SetComputeUnitLimit instruction
fn compute_unit_limit_ix(units: u32) -> Instruction {
    let mut data = vec![2u8]; // instruction index
    data.extend_from_slice(&units.to_le_bytes());
    Instruction::new_with_bytes(
        Pubkey::from_str(COMPUTE_BUDGET_PROGRAM).unwrap(),
        &data,
        vec![],
    )
}

/// Build a SetComputeUnitPrice instruction
fn compute_unit_price_ix(micro_lamports: u64) -> Instruction {
    let mut data = vec![3u8]; // instruction index
    data.extend_from_slice(&micro_lamports.to_le_bytes());
    Instruction::new_with_bytes(
        Pubkey::from_str(COMPUTE_BUDGET_PROGRAM).unwrap(),
        &data,
        vec![],
    )
}

use crate::config;
use crate::rpc::RpcClient;

/// Build an SPL Token CloseAccount instruction (index 9).
/// Reclaims ~0.002 SOL rent from an empty token account.
fn close_account_ix(account: &Pubkey, destination: &Pubkey, owner: &Pubkey) -> Instruction {
    let token_program = Pubkey::from_str(config::TOKEN_PROGRAM_ID).unwrap();
    Instruction::new_with_bytes(
        token_program,
        &[9u8], // CloseAccount instruction index
        vec![
            AccountMeta::new(*account, false),      // account to close
            AccountMeta::new(*destination, false),   // rent destination
            AccountMeta::new_readonly(*owner, true), // owner (signer)
        ],
    )
}

// ─────────────────────────────────────────────
//  Pump.fun instruction discriminators (Anchor)
//  buy  = sha256("global:buy")[..8]
//  sell = sha256("global:sell")[..8]
// ─────────────────────────────────────────────

const BUY_DISCRIMINATOR: [u8; 8] = [102, 6, 61, 18, 1, 218, 235, 234];
const SELL_DISCRIMINATOR: [u8; 8] = [51, 230, 133, 164, 1, 127, 131, 173];
const CREATE_DISCRIMINATOR: [u8; 8] = [24, 30, 200, 40, 5, 28, 7, 119];
const METAPLEX_METADATA_PROGRAM: &str = "metaqbxxUoLd1PNsHi4FpQRVKuoLY1U8kbBhtNNTfPAv";

/// Create instruction data (Anchor-encoded: discriminator + borsh-serialized args)
#[derive(BorshSerialize)]
struct CreateInstructionData {
    discriminator: [u8; 8],
    name: String,
    symbol: String,
    uri: String,
}

/// Bonding curve account state (deserialized from on-chain data)
#[derive(Debug)]
pub struct BondingCurveState {
    pub virtual_token_reserves: u64,
    pub virtual_sol_reserves: u64,
    pub real_token_reserves: u64,
    pub real_sol_reserves: u64,
    pub token_total_supply: u64,
    pub complete: bool,
}

impl BondingCurveState {
    /// Deserialize from raw account data (skip 8-byte Anchor discriminator)
    fn from_account_data(data: &[u8]) -> Result<Self> {
        if data.len() < 49 {
            anyhow::bail!("Bonding curve data too small: {} bytes", data.len());
        }
        let d = &data[8..]; // skip discriminator
        Ok(BondingCurveState {
            virtual_token_reserves: u64::from_le_bytes(d[0..8].try_into()?),
            virtual_sol_reserves: u64::from_le_bytes(d[8..16].try_into()?),
            real_token_reserves: u64::from_le_bytes(d[16..24].try_into()?),
            real_sol_reserves: u64::from_le_bytes(d[24..32].try_into()?),
            token_total_supply: u64::from_le_bytes(d[32..40].try_into()?),
            complete: d[40] != 0,
        })
    }
}

/// Buy instruction data
#[derive(BorshSerialize)]
struct BuyInstructionData {
    discriminator: [u8; 8],
    amount: u64,
    max_sol_cost: u64,
}

/// Sell instruction data
#[derive(BorshSerialize)]
struct SellInstructionData {
    discriminator: [u8; 8],
    amount: u64,
    min_sol_output: u64,
}

/// Builds raw Pump.fun instructions for buy/sell operations.
pub struct InstructionBuilder {
    program_id: Pubkey,
    global_config: Pubkey,
    fee_recipient: Pubkey,
    event_authority: Pubkey,
    rpc: RpcClient,
    /// Dedicated client for sell-side token balance polling (same URL as `rpc` unless overridden).
    rpc_read: RpcClient,
}

impl InstructionBuilder {
    pub fn new(rpc_url: &str, rpc_read_url: &str, program_id: Pubkey, fee_recipient: Pubkey) -> Self {
        let rpc = RpcClient::new(rpc_url);
        let rpc_read = RpcClient::new(rpc_read_url);

        // Derive PDAs
        let (global_config, _) = Pubkey::find_program_address(&[b"global"], &program_id);
        let (event_authority, _) =
            Pubkey::find_program_address(&[b"__event_authority"], &program_id);

        info!(
            "InstructionBuilder | program={} | global={} | fee_recip={}",
            program_id, global_config, fee_recipient
        );
        if rpc_read_url != rpc_url {
            info!("InstructionBuilder | sell balance polls use dedicated read RPC (HELIUS_READ_RPC_URL)");
        }

        InstructionBuilder {
            program_id,
            global_config,
            fee_recipient,
            event_authority,
            rpc,
            rpc_read,
        }
    }

    /// Derive the bonding curve PDA for a given mint
    pub fn derive_bonding_curve(&self, mint: &Pubkey) -> (Pubkey, u8) {
        Pubkey::find_program_address(&[b"bonding-curve", mint.as_ref()], &self.program_id)
    }

    /// Get the associated token account address
    fn get_ata(wallet: &Pubkey, mint: &Pubkey) -> Pubkey {
        let token_program = Pubkey::from_str(config::TOKEN_PROGRAM_ID).unwrap();
        spl_associated_token_account::get_associated_token_address_with_program_id(
            wallet,
            mint,
            &token_program,
        )
    }

    /// Fetch and deserialize the bonding curve state
    pub async fn fetch_bonding_curve(&self, bonding_curve: &Pubkey) -> Result<BondingCurveState> {
        let data = self.rpc.get_account_data(bonding_curve).await
            .with_context(|| format!("Failed to fetch bonding curve {}", bonding_curve))?;

        let state = BondingCurveState::from_account_data(&data)?;

        debug!(
            "Bonding curve: vTokens={} vSol={} complete={}",
            state.virtual_token_reserves, state.virtual_sol_reserves, state.complete
        );

        Ok(state)
    }

    /// Calculate tokens received for a given SOL input (constant-product formula)
    pub fn calculate_buy_tokens(curve: &BondingCurveState, sol_lamports: u64) -> u64 {
        if curve.virtual_sol_reserves == 0 {
            return 0;
        }
        let numerator = (sol_lamports as u128) * (curve.virtual_token_reserves as u128);
        let denominator = (curve.virtual_sol_reserves as u128) + (sol_lamports as u128);
        (numerator / denominator) as u64
    }

    /// Calculate SOL received for a given token input
    pub fn calculate_sell_sol(curve: &BondingCurveState, token_amount: u64) -> u64 {
        if curve.virtual_token_reserves == 0 {
            return 0;
        }
        let numerator = (token_amount as u128) * (curve.virtual_sol_reserves as u128);
        let denominator = (curve.virtual_token_reserves as u128) + (token_amount as u128);
        (numerator / denominator) as u64
    }

    /// Build a complete buy transaction.
    ///
    /// Returns `(transaction, tokens_out)` where `tokens_out` is the expected token amount
    /// from the bonding curve math. Callers should cache this for fast sell-side balance lookup.
    pub async fn build_buy(
        &self,
        keypair: &Keypair,
        mint: &Pubkey,
        sol_amount: f64,
        slippage_bps: u16,
        priority_fee: u64,
    ) -> Result<(Transaction, u64)> {
        let user = keypair.pubkey();
        let sol_lamports = (sol_amount * 1e9) as u64;

        let (bonding_curve, _) = self.derive_bonding_curve(mint);
        let assoc_bonding_curve = Self::get_ata(&bonding_curve, mint);
        let user_ata = Self::get_ata(&user, mint);

        let curve_state = self.fetch_bonding_curve(&bonding_curve).await?;

        if curve_state.complete {
            anyhow::bail!("Bonding curve already graduated");
        }

        let tokens_out = Self::calculate_buy_tokens(&curve_state, sol_lamports);
        if tokens_out == 0 {
            anyhow::bail!("Calculated 0 tokens for {} lamports", sol_lamports);
        }

        let max_sol_cost = sol_lamports + (sol_lamports * slippage_bps as u64) / 10_000;

        debug!(
            "BUY: {} SOL -> ~{} tokens | max_cost={} | slippage={}bps",
            sol_amount, tokens_out, max_sol_cost, slippage_bps
        );

        let ix_data = BuyInstructionData {
            discriminator: BUY_DISCRIMINATOR,
            amount: tokens_out,
            max_sol_cost,
        };
        let ix_bytes = borsh::to_vec(&ix_data)?;

        let token_program = Pubkey::from_str(config::TOKEN_PROGRAM_ID)?;
        let rent_sysvar = Pubkey::from_str(config::RENT_SYSVAR_ID)?;

        let accounts = vec![
            AccountMeta::new_readonly(self.global_config, false),
            AccountMeta::new(self.fee_recipient, false),
            AccountMeta::new_readonly(*mint, false),
            AccountMeta::new(bonding_curve, false),
            AccountMeta::new(assoc_bonding_curve, false),
            AccountMeta::new(user_ata, false),
            AccountMeta::new(user, true),
            AccountMeta::new_readonly(Pubkey::from_str(SYSTEM_PROGRAM).unwrap(), false),
            AccountMeta::new_readonly(token_program, false),
            AccountMeta::new_readonly(rent_sysvar, false),
            AccountMeta::new_readonly(self.event_authority, false),
            AccountMeta::new_readonly(self.program_id, false),
        ];

        let buy_ix = Instruction::new_with_bytes(self.program_id, &ix_bytes, accounts);

        let create_ata_ix =
            spl_associated_token_account::instruction::create_associated_token_account_idempotent(
                &user, &user, mint, &token_program,
            );

        let compute_limit_ix = compute_unit_limit_ix(200_000);
        let compute_price_ix = compute_unit_price_ix(priority_fee);

        let recent_blockhash = self.rpc.get_latest_blockhash().await?;

        let message = Message::new(
            &[compute_limit_ix, compute_price_ix, create_ata_ix, buy_ix],
            Some(&user),
        );

        let mut tx = Transaction::new_unsigned(message);
        tx.sign(&[keypair], recent_blockhash);

        Ok((tx, tokens_out))
    }

    /// Build a complete sell transaction.
    ///
    /// Returns the transaction and the **recent blockhash** used to sign it. The Jito tip tx in a
    /// bundle must use a blockhash from the same "freshness window"; `handle_sell` passes this
    /// hash to [`JitoBundler::submit_bundle`] instead of fetching a second unrelated hash.
    ///
    /// `cached_balance` — if provided (from buy-confirmation cache), skips the RPC polling loop.
    /// `cached_blockhash` — if provided (from rolling pre-fetch), skips the blockhash RPC call.
    pub async fn build_sell(
        &self,
        keypair: &Keypair,
        mint: &Pubkey,
        pct: f64,
        slippage_bps: u16,
        priority_fee: u64,
        cached_balance: Option<u64>,
        cached_blockhash: Option<Hash>,
    ) -> Result<(Transaction, Hash)> {
        let user = keypair.pubkey();

        let (bonding_curve, _) = self.derive_bonding_curve(mint);
        let assoc_bonding_curve = Self::get_ata(&bonding_curve, mint);
        let user_ata = Self::get_ata(&user, mint);

        // ── Phase 1+2+3: Parallel RPC fetches with cache fast-paths ──
        //
        // If cached_balance is available (from buy confirmation), skip the entire poll loop.
        // If cached_blockhash is available (from rolling pre-fetch), skip the blockhash RPC call.
        // Bonding curve fetch runs in parallel with whatever balance resolution we need.
        let (total_tokens, curve_state, recent_blockhash) = if let Some(cached) = cached_balance {
            info!("SELL: using cached balance {} tokens (skipping poll loop)", cached);
            // Cache hit: only need curve + optional blockhash
            if let Some(bh) = cached_blockhash {
                let curve_state = self.fetch_bonding_curve(&bonding_curve).await?;
                (cached, curve_state, bh)
            } else {
                let (curve_result, bh_result) = tokio::join!(
                    self.fetch_bonding_curve(&bonding_curve),
                    self.rpc.get_latest_blockhash()
                );
                (cached, curve_result?, bh_result?)
            }
        } else if let Some(bh) = cached_blockhash {
            // No balance cache, but have blockhash: parallel poll + curve only
            let (balance_result, curve_result) = tokio::join!(
                poll_token_balance(&self.rpc_read, &user_ata),
                self.fetch_bonding_curve(&bonding_curve)
            );
            (balance_result?, curve_result?, bh)
        } else {
            // No caches: parallel poll + curve + blockhash (Phase 1 core improvement)
            let (balance_result, curve_result, bh_result) = tokio::join!(
                poll_token_balance(&self.rpc_read, &user_ata),
                self.fetch_bonding_curve(&bonding_curve),
                self.rpc.get_latest_blockhash()
            );
            (balance_result?, curve_result?, bh_result?)
        };

        if total_tokens == 0 {
            anyhow::bail!("No tokens to sell for mint {}", mint);
        }

        let sell_amount = if pct >= 100.0 {
            total_tokens
        } else {
            ((total_tokens as f64) * pct / 100.0) as u64
        };

        if sell_amount == 0 {
            anyhow::bail!("Calculated 0 tokens to sell ({}% of {})", pct, total_tokens);
        }

        let expected_sol = Self::calculate_sell_sol(&curve_state, sell_amount);
        let min_sol_output = expected_sol - (expected_sol * slippage_bps as u64) / 10_000;

        debug!(
            "SELL: {}% ({} tokens) -> ~{} lamports | min_out={}",
            pct, sell_amount, expected_sol, min_sol_output
        );

        let ix_data = SellInstructionData {
            discriminator: SELL_DISCRIMINATOR,
            amount: sell_amount,
            min_sol_output,
        };
        let ix_bytes = borsh::to_vec(&ix_data)?;

        let token_program = Pubkey::from_str(config::TOKEN_PROGRAM_ID)?;
        let ata_program = Pubkey::from_str(config::ASSOCIATED_TOKEN_PROGRAM_ID)?;

        let accounts = vec![
            AccountMeta::new_readonly(self.global_config, false),
            AccountMeta::new(self.fee_recipient, false),
            AccountMeta::new_readonly(*mint, false),
            AccountMeta::new(bonding_curve, false),
            AccountMeta::new(assoc_bonding_curve, false),
            AccountMeta::new(user_ata, false),
            AccountMeta::new(user, true),
            AccountMeta::new_readonly(Pubkey::from_str(SYSTEM_PROGRAM).unwrap(), false),
            AccountMeta::new_readonly(ata_program, false),
            AccountMeta::new_readonly(token_program, false),
            AccountMeta::new_readonly(self.event_authority, false),
            AccountMeta::new_readonly(self.program_id, false),
        ];

        let sell_ix = Instruction::new_with_bytes(self.program_id, &ix_bytes, accounts);

        // Bump compute units for 100% sells that include CloseAccount
        let cu_limit = if pct >= 100.0 { 250_000 } else { 200_000 };
        let compute_limit_ix = compute_unit_limit_ix(cu_limit);
        let compute_price_ix = compute_unit_price_ix(priority_fee);

        // Build instruction list — append CloseAccount on full sells to reclaim ~0.002 SOL rent
        let mut ixs = vec![compute_limit_ix, compute_price_ix, sell_ix];
        if pct >= 100.0 {
            ixs.push(close_account_ix(&user_ata, &user, &user));
            debug!("SELL 100%: appending CloseAccount for rent recovery on {}", user_ata);
        }

        let message = Message::new(&ixs, Some(&user));

        let mut tx = Transaction::new_unsigned(message);
        tx.sign(&[keypair], recent_blockhash);

        Ok((tx, recent_blockhash))
    }

    /// Build a Pump.fun token creation transaction.
    ///
    /// Creates a new token mint, bonding curve, and associated accounts.
    /// Both `deployer` and `mint_keypair` must sign the transaction.
    pub async fn build_create(
        &self,
        deployer: &Keypair,
        mint_keypair: &Keypair,
        name: String,
        symbol: String,
        uri: String,
        priority_fee: u64,
    ) -> Result<Transaction> {
        let user = deployer.pubkey();
        let mint = mint_keypair.pubkey();

        // Derive PDAs
        let (bonding_curve, _) = self.derive_bonding_curve(&mint);
        let assoc_bonding_curve = Self::get_ata(&bonding_curve, &mint);
        let metaplex_program = Pubkey::from_str(METAPLEX_METADATA_PROGRAM).unwrap();
        let (metadata_account, _) = Pubkey::find_program_address(
            &[b"metadata", metaplex_program.as_ref(), mint.as_ref()],
            &metaplex_program,
        );

        // Derive mint authority (Pump.fun program's authority over the mint)
        let (mint_authority, _) =
            Pubkey::find_program_address(&[b"mint-authority"], &self.program_id);

        let token_program = Pubkey::from_str(config::TOKEN_PROGRAM_ID).unwrap();
        let assoc_token_program = Pubkey::from_str(config::ASSOCIATED_TOKEN_PROGRAM_ID).unwrap();
        let system_program = Pubkey::from_str(config::SYSTEM_PROGRAM_ID).unwrap();
        let rent_sysvar = Pubkey::from_str(config::RENT_SYSVAR_ID).unwrap();

        // Build instruction data
        let ix_data = CreateInstructionData {
            discriminator: CREATE_DISCRIMINATOR,
            name: name.clone(),
            symbol: symbol.clone(),
            uri,
        };
        let mut data = Vec::new();
        borsh::to_writer(&mut data, &ix_data)
            .context("Failed to serialize create instruction data")?;

        // Account layout for Pump.fun create instruction
        let accounts = vec![
            AccountMeta::new(mint, true),                              // mint (signer)
            AccountMeta::new_readonly(mint_authority, false),           // mint authority PDA
            AccountMeta::new(bonding_curve, false),                    // bonding curve PDA
            AccountMeta::new(assoc_bonding_curve, false),              // associated bonding curve
            AccountMeta::new_readonly(self.global_config, false),      // global config
            AccountMeta::new_readonly(metaplex_program, false),        // Metaplex Token Metadata
            AccountMeta::new(metadata_account, false),                 // metadata PDA
            AccountMeta::new(user, true),                              // user/deployer (signer)
            AccountMeta::new_readonly(system_program, false),          // System Program
            AccountMeta::new_readonly(token_program, false),           // Token Program
            AccountMeta::new_readonly(assoc_token_program, false),     // Associated Token Program
            AccountMeta::new_readonly(rent_sysvar, false),             // Rent Sysvar
            AccountMeta::new_readonly(self.event_authority, false),    // event authority
            AccountMeta::new_readonly(self.program_id, false),         // Pump.fun program
        ];

        let create_ix = Instruction::new_with_bytes(self.program_id, &data, accounts);

        // Build transaction with compute budget + create instruction
        let instructions = vec![
            compute_unit_limit_ix(250_000),
            compute_unit_price_ix(priority_fee),
            create_ix,
        ];

        let recent_blockhash = self.rpc.get_latest_blockhash().await
            .context("Failed to get blockhash for create")?;

        let message = Message::new_with_blockhash(
            &instructions,
            Some(&user),
            &recent_blockhash,
        );

        // Both deployer and mint keypair must sign
        let mut tx = Transaction::new_unsigned(message);
        tx.sign(&[deployer, mint_keypair], recent_blockhash);

        info!(
            "Built CREATE tx | {} ({}) | mint={} | deployer={}",
            name, symbol, mint, user
        );

        Ok(tx)
    }

    /// Get the RPC client reference
    pub fn rpc(&self) -> &RpcClient {
        &self.rpc
    }
}

/// Poll token balance from RPC with retries and backoff.
///
/// Extracted as a standalone function so it can run concurrently via `tokio::join!`
/// alongside bonding curve and blockhash fetches.
///
/// Override defaults with `BIGAL_SELL_BALANCE_RETRIES` (default 20) and
/// `BIGAL_SELL_BALANCE_POLL_MS` (default 200ms base).
async fn poll_token_balance(rpc_read: &RpcClient, user_ata: &Pubkey) -> Result<u64> {
    let max_attempts: u32 = env::var("BIGAL_SELL_BALANCE_RETRIES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(20);
    let base_poll_ms: u64 = env::var("BIGAL_SELL_BALANCE_POLL_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(200);

    for attempt in 0..max_attempts {
        let bal = match rpc_read.get_token_account_balance(user_ata).await {
            Ok(b) if b > 0 => b,
            Ok(_) | Err(_) => rpc_read
                .get_token_balance_json_parsed(user_ata)
                .await
                .unwrap_or(0),
        };
        if bal > 0 {
            return Ok(bal);
        }
        if attempt < max_attempts - 1 {
            let extra = (attempt as u64).saturating_mul(20).min(350);
            let delay_ms = (base_poll_ms + extra).min(1200);
            debug!(
                "SELL: token balance 0, retrying in {}ms (attempt {}/{})",
                delay_ms,
                attempt + 1,
                max_attempts
            );
            tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
        }
    }

    anyhow::bail!(
        "No tokens found at {} (after {} retries)",
        user_ata,
        max_attempts
    );
}
