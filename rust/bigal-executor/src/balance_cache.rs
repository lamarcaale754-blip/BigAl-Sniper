/// Buy-confirmation balance cache.
///
/// After a confirmed buy, stores the expected token amount so that the
/// subsequent sell can skip the entire RPC balance-polling loop.
///
/// Self-correcting: if the cached value is wrong (buy didn't actually land),
/// the sell will fail on attempt 1 and retry 2 will poll fresh from RPC.

use dashmap::DashMap;
use solana_sdk::pubkey::Pubkey;
use std::time::Instant;
use tracing::{debug, info};

pub struct BalanceCache {
    inner: DashMap<Pubkey, (u64, Instant)>,
    max_age_secs: u64,
}

impl BalanceCache {
    pub fn new(max_age_secs: u64) -> Self {
        BalanceCache {
            inner: DashMap::new(),
            max_age_secs,
        }
    }

    /// Cache the expected token balance from a confirmed buy.
    pub fn insert(&self, mint: Pubkey, tokens: u64) {
        info!(
            "Balance cache: stored {} tokens for mint {}",
            tokens, mint
        );
        self.inner.insert(mint, (tokens, Instant::now()));
    }

    /// Retrieve cached balance if present and not expired.
    pub fn get(&self, mint: &Pubkey) -> Option<u64> {
        let entry = self.inner.get(mint)?;
        let (tokens, inserted_at) = entry.value();
        if inserted_at.elapsed().as_secs() > self.max_age_secs {
            debug!("Balance cache: expired entry for mint {}", mint);
            drop(entry);
            self.inner.remove(mint);
            return None;
        }
        Some(*tokens)
    }

    /// Remove a mint from the cache (after confirmed sell).
    pub fn remove(&self, mint: &Pubkey) {
        if self.inner.remove(mint).is_some() {
            debug!("Balance cache: removed mint {}", mint);
        }
    }
}
