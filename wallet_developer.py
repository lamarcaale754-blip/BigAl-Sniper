"""
WALLET DEVELOPER — warm a pool of Solana wallets with age + micro-activity
before they are used as puppets or operational identities.

Each wallet must reach ~5 minutes of age and complete a few randomized
0.001 SOL transfers between pool members (simulated organic history).
"""

from __future__ import annotations

import asyncio
import base64
import random
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import aiohttp
from solders.keypair import Keypair
from solders.message import Message
from solders.pubkey import Pubkey
from solders.system_program import TransferParams, transfer
from solders.transaction import Transaction, VersionedTransaction
from solana.rpc.async_api import AsyncClient
from solana.rpc.commitment import Processed
from solana.rpc.types import TxOpts


MIN_WARM_AGE_SEC = 300.0
MICRO_LAMPORTS = 1_000_000  # 0.001 SOL
TARGET_MICRO_TXS = 3
# Randomized spacing between micro-actions (seconds)
MICRO_INTERVAL_MIN = 25.0
MICRO_INTERVAL_MAX = 90.0
WSOL_MINT = "So11111111111111111111111111111111111111112"
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"


@dataclass
class _PoolEntry:
    keypair: Keypair
    created_at: float = field(default_factory=time.time)
    micro_tx_count: int = 0
    last_micro_at: float = 0.0
    last_error: str = ""

    @property
    def age_sec(self) -> float:
        return time.time() - self.created_at

    def status(self) -> str:
        if self.warm_progress >= 1.0:
            return "ready"
        if self.age_sec < MIN_WARM_AGE_SEC * 0.25:
            return "new"
        return "warming"

    @property
    def warm_progress(self) -> float:
        age_ok = min(1.0, self.age_sec / MIN_WARM_AGE_SEC)
        tx_ok = min(1.0, self.micro_tx_count / float(TARGET_MICRO_TXS))
        return round(0.5 * age_ok + 0.5 * tx_ok, 3)

    def to_dict(self) -> dict:
        pk = str(self.keypair.pubkey())
        return {
            "pubkey_short": pk[:8] + "..." + pk[-4:],
            "pubkey_full": pk,
            "status": self.status(),
            "age_sec": round(self.age_sec, 1),
            "age_target_sec": MIN_WARM_AGE_SEC,
            "micro_tx_count": self.micro_tx_count,
            "micro_tx_target": TARGET_MICRO_TXS,
            "warm_progress": self.warm_progress,
            "last_error": self.last_error,
        }


class WalletDeveloper:
    """
    Maintains `pool_size` wallets; funds them from `funder`, then runs
    randomized 0.001 SOL transfers between pool addresses until warm.
    """

    def __init__(
        self,
        rpc_url: str,
        funder: Keypair,
        pool_size: int = 4,
        fund_each_sol: float = 0.02,
    ):
        self._rpc_url = rpc_url
        self._funder = funder
        self._pool_size = max(2, pool_size)
        self._fund_lamports = int(fund_each_sol * 1_000_000_000)
        self._pool: List[_PoolEntry] = []
        self._running = False
        self._client: Optional[AsyncClient] = None
        self._lock = asyncio.Lock()

    async def _ensure_client(self) -> AsyncClient:
        if self._client is None:
            self._client = AsyncClient(self._rpc_url)
        return self._client

    async def close(self) -> None:
        self._running = False
        if self._client:
            await self._client.close()
            self._client = None

    def ready_count(self) -> int:
        """How many wallets are warmed and available."""
        return sum(1 for e in self._pool if e.status() == "ready")

    async def claim_ready_wallets(self, count: int) -> List[Keypair]:
        """Claim up to `count` warmed wallets, removing them from the pool.

        Returns keypairs that have met the minimum age and micro-tx thresholds.
        If fewer than `count` are ready, returns whatever is available.
        Caller owns the keypairs after claiming.
        """
        async with self._lock:
            ready = [e for e in self._pool if e.status() == "ready"]
            claimed = ready[:count]
            for e in claimed:
                self._pool.remove(e)
            return [e.keypair for e in claimed]

    async def replenish(self, count: int = 0) -> None:
        """Add fresh wallets to the pool to replace claimed ones.

        If count is 0, fills back to the original pool_size.
        """
        client = await self._ensure_client()
        async with self._lock:
            target = count if count > 0 else max(0, self._pool_size - len(self._pool))
            for _ in range(target):
                entry = _PoolEntry(keypair=Keypair())
                self._pool.append(entry)
                await self._fund_entry(client, entry)

    def get_pool_snapshot(self) -> Dict[str, Any]:
        return {
            "enabled": True,
            "pool_size": len(self._pool),
            "entries": [e.to_dict() for e in self._pool],
            "min_age_sec": MIN_WARM_AGE_SEC,
            "micro_lamports": MICRO_LAMPORTS,
        }

    async def _fund_entry(self, client: AsyncClient, entry: _PoolEntry) -> bool:
        try:
            bh = await client.get_latest_blockhash(commitment=Processed)
            blockhash = bh.value.blockhash
            to_pub = entry.keypair.pubkey()
            ix = transfer(
                TransferParams(
                    from_pubkey=self._funder.pubkey(),
                    to_pubkey=to_pub,
                    lamports=self._fund_lamports,
                )
            )
            msg = Message.new_with_blockhash([ix], self._funder.pubkey(), blockhash)
            tx = Transaction.new_unsigned(msg)
            tx.sign([self._funder], blockhash)
            raw = bytes(tx)
            await client.send_raw_transaction(
                raw,
                opts=TxOpts(skip_preflight=True, preflight_commitment=Processed),
            )
            return True
        except Exception as e:
            entry.last_error = str(e)[:120]
            return False

    async def _micro_transfer(
        self, client: AsyncClient, from_kp: Keypair, to_pub: Pubkey
    ) -> bool:
        try:
            bh = await client.get_latest_blockhash(commitment=Processed)
            blockhash = bh.value.blockhash
            ix = transfer(
                TransferParams(
                    from_pubkey=from_kp.pubkey(),
                    to_pubkey=to_pub,
                    lamports=MICRO_LAMPORTS,
                )
            )
            msg = Message.new_with_blockhash([ix], from_kp.pubkey(), blockhash)
            tx = Transaction.new_unsigned(msg)
            tx.sign([from_kp], blockhash)
            await client.send_raw_transaction(
                bytes(tx),
                opts=TxOpts(skip_preflight=True, preflight_commitment=Processed),
            )
            return True
        except Exception:
            return False

    async def _micro_jupiter_swap(self, wallet_kp: Keypair, amount_lamports: int = MICRO_LAMPORTS) -> bool:
        quote_url = (
            "https://quote-api.jup.ag/v6/quote"
            f"?inputMint={WSOL_MINT}&outputMint={USDC_MINT}"
            f"&amount={int(amount_lamports)}&slippageBps=50"
        )
        timeout = aiohttp.ClientTimeout(total=10)
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(quote_url) as quote_resp:
                    if quote_resp.status != 200:
                        return False
                    quote_data = await quote_resp.json()

                swap_payload = {
                    "quoteResponse": quote_data,
                    "userPublicKey": str(wallet_kp.pubkey()),
                    "wrapAndUnwrapSol": True,
                    "dynamicComputeUnitLimit": True,
                    "prioritizationFeeLamports": 50_000,
                }
                async with session.post("https://quote-api.jup.ag/v6/swap", json=swap_payload) as swap_resp:
                    if swap_resp.status != 200:
                        return False
                    swap_json = await swap_resp.json()
        except Exception:
            return False

        tx_b64 = swap_json.get("swapTransaction")
        if not tx_b64:
            return False

        try:
            raw_tx = base64.b64decode(tx_b64)
            unsigned_tx = VersionedTransaction.from_bytes(raw_tx)
            signed_tx = VersionedTransaction(unsigned_tx.message, [wallet_kp])
            client = await self._ensure_client()
            await client.send_raw_transaction(
                bytes(signed_tx),
                opts=TxOpts(skip_preflight=True, preflight_commitment=Processed, max_retries=2),
            )
            return True
        except Exception:
            return False

    async def _bootstrap_pool(self, client: AsyncClient) -> None:
        async with self._lock:
            if self._pool:
                return
            for _ in range(self._pool_size):
                self._pool.append(_PoolEntry(keypair=Keypair()))
        for e in self._pool:
            if not await self._fund_entry(client, e):
                await asyncio.sleep(2.0)

    async def _run_micro_round(self, client: AsyncClient) -> None:
        if len(self._pool) < 2:
            return
        # Prefer wallets still warming
        candidates = [e for e in self._pool if e.warm_progress < 1.0]
        if not candidates:
            candidates = self._pool
        a = random.choice(candidates)
        others = [e for e in self._pool if e.keypair.pubkey() != a.keypair.pubkey()]
        if not others:
            return
        b = random.choice(others)
        mode = random.choice(["swap", "transfer", "bounce"])
        ok = False
        used_swap_fallback_transfer = False
        if mode == "swap":
            ok = await self._micro_jupiter_swap(a.keypair)
            if ok:
                a.micro_tx_count += 1
                a.last_micro_at = time.time()
                a.last_error = ""
            else:
                # Keep existing transfer warmup path as resilient fallback.
                ok = await self._micro_transfer(client, a.keypair, b.keypair.pubkey())
                used_swap_fallback_transfer = bool(ok)
        elif mode == "bounce":
            ok = await self._micro_transfer(client, a.keypair, b.keypair.pubkey())
            if ok:
                await asyncio.sleep(1.0)
                ok = await self._micro_transfer(client, b.keypair, a.keypair.pubkey())
        else:
            ok = await self._micro_transfer(client, a.keypair, b.keypair.pubkey())
        if ok:
            if mode != "swap" or used_swap_fallback_transfer:
                a.micro_tx_count += 1
                a.last_micro_at = time.time()
            if mode == "bounce":
                b.micro_tx_count += 1
                b.last_micro_at = time.time()

    async def run_forever(self) -> None:
        self._running = True
        try:
            client = await self._ensure_client()
            await self._bootstrap_pool(client)
            while self._running:
                await self._run_micro_round(client)
                delay = random.uniform(MICRO_INTERVAL_MIN, MICRO_INTERVAL_MAX)
                await asyncio.sleep(delay)
        except asyncio.CancelledError:
            pass
        finally:
            self._running = False
