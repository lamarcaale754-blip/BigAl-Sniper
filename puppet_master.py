"""
PUPPET MASTER — Stage 4: The Actors
Manages multiple puppet wallets to simulate organic buying activity
on a honeypot token, pushing it past the target bot's entry filters.

Each puppet wallet is a fresh Solana keypair funded from a master wallet.
Buys are staggered with randomized amounts and timing to evade
wallet-diversity and repeat-wallet detection.
"""

import asyncio
import json
import os
import time
import random
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import base58
from solders.keypair import Keypair
from solders.message import Message
from solders.system_program import TransferParams, transfer
from solders.transaction import Transaction


@dataclass
class PuppetWallet:
    """A single puppet wallet managed by the puppet master."""
    index: int
    keypair: Keypair
    funded: bool = False
    funded_amount_sol: float = 0.0
    buys_executed: int = 0
    total_spent_sol: float = 0.0

    @property
    def pubkey(self) -> str:
        return str(self.keypair.pubkey())

    def to_dict(self) -> dict:
        return {
            "index": self.index,
            "pubkey": self.pubkey[:8] + "..." + self.pubkey[-4:],
            "funded": self.funded,
            "funded_sol": round(self.funded_amount_sol, 4),
            "buys": self.buys_executed,
            "spent_sol": round(self.total_spent_sol, 4),
        }


@dataclass
class DeploymentState:
    """Tracks a live honeypot deployment."""
    plan_id: str
    token_mint: Optional[str] = None
    status: str = "preparing"  # preparing → funding → buying → watching → killing → done
    puppets: List[PuppetWallet] = field(default_factory=list)
    buys_completed: int = 0
    buys_total: int = 0
    target_bot_detected: bool = False
    target_buy_amount: float = 0.0
    sol_in_pool: float = 0.0
    started_at: float = 0.0
    killed_at: float = 0.0
    kill_tx_sig: Optional[str] = None
    error: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "plan_id": self.plan_id,
            "token_mint": self.token_mint[:12] + "..." if self.token_mint else None,
            "status": self.status,
            "puppets": [p.to_dict() for p in self.puppets],
            "progress": f"{self.buys_completed}/{self.buys_total}",
            "target_detected": self.target_bot_detected,
            "target_buy_sol": round(self.target_buy_amount, 4),
            "pool_sol": round(self.sol_in_pool, 4),
            "started_at": self.started_at,
            "error": self.error,
        }


class PuppetMaster:
    """
    Manages puppet wallet lifecycle:
    1. Generate fresh keypairs
    2. Fund them from master wallet (small amounts, staggered)
    3. Execute buy schedule from HoneypotPlan
    4. Monitor for target bot entry
    5. Signal kill chain when threshold met

    IMPORTANT: In test mode, all transactions are simulated.
    Live mode requires a funded master wallet.
    """

    def __init__(self, is_live: bool = False):
        self.is_live = is_live
        self.deployments: Dict[str, DeploymentState] = {}
        self.master_keypair: Optional[Keypair] = None
        self._executor = None  # set by orchestrator
        self._wallet_dev = None  # optional WalletDeveloper for pre-warmed wallets

    def set_master_wallet(self, keypair: Keypair):
        """Set the master wallet that funds puppet wallets."""
        self.master_keypair = keypair

    def set_executor(self, executor):
        """Set the trade executor (for live puppet buys)."""
        self._executor = executor

    def set_wallet_developer(self, wallet_dev):
        """Set the WalletDeveloper for pre-warmed puppet wallets."""
        self._wallet_dev = wallet_dev

    async def generate_puppets_async(self, count: int) -> List[PuppetWallet]:
        """Generate puppet wallets, using pre-warmed wallets when available.

        Pulls from WalletDeveloper pool first (aged wallets with on-chain history),
        then fills remaining slots with fresh keypairs as fallback.
        """
        puppets = []
        warmed_keypairs: list = []

        # Try to claim pre-warmed wallets from WalletDeveloper
        if self._wallet_dev is not None:
            warmed_keypairs = await self._wallet_dev.claim_ready_wallets(count)
            if warmed_keypairs:
                print(f"[PuppetMaster] Claimed {len(warmed_keypairs)} pre-warmed wallets from pool")
            # Replenish the pool in the background
            asyncio.create_task(self._wallet_dev.replenish())

        for i in range(count):
            if i < len(warmed_keypairs):
                kp = warmed_keypairs[i]
                print(f"[PuppetMaster] Puppet {i}: pre-warmed wallet {str(kp.pubkey())[:12]}...")
            else:
                kp = Keypair()  # fresh random keypair (fallback)
                if self._wallet_dev is not None:
                    print(f"[PuppetMaster] Puppet {i}: fresh keypair (pool exhausted)")
            puppets.append(PuppetWallet(index=i, keypair=kp))
        return puppets

    async def deploy_honeypot(self, plan, on_status_update=None) -> DeploymentState:
        """
        Execute a full honeypot deployment from an approved plan.

        Steps:
        1. Generate puppet wallets
        2. Fund puppets from master (in live mode)
        3. Deploy token (in live mode)
        4. Execute puppet buy schedule
        5. Watch for target bot entry
        6. Signal kill chain when threshold met

        Args:
            plan: HoneypotPlan from Stage 3
            on_status_update: async callback(deployment_state) for dashboard updates
        """
        state = DeploymentState(
            plan_id=plan.plan_id,
            buys_total=len(plan.puppet_buys),
            started_at=time.time(),
        )
        self.deployments[plan.plan_id] = state

        async def _update():
            if on_status_update:
                await on_status_update(state)

        try:
            # ── Step 1: Generate puppets (pre-warmed if available) ──
            state.status = "preparing"
            state.puppets = await self.generate_puppets_async(plan.puppet_count)
            await _update()
            print(f"[PuppetMaster] Generated {plan.puppet_count} puppet wallets")

            if self.is_live and not self.master_keypair:
                state.error = "No master wallet configured"
                state.status = "failed"
                await _update()
                return state

            # ── Step 2: Fund puppets (live only) ──
            if self.is_live:
                state.status = "funding"
                await _update()
                # Fund each puppet via direct SOL transfer from master wallet
                from solana.rpc.async_api import AsyncClient
                from solana.rpc.commitment import Processed
                from solana.rpc.types import TxOpts
                rpc_url = os.environ.get("HELIUS_RPC_URL", "")
                fund_client = AsyncClient(rpc_url)
                try:
                    bh_resp = await fund_client.get_latest_blockhash(commitment=Processed)
                    blockhash = bh_resp.value.blockhash

                    for puppet in state.puppets:
                        puppet_buys = [b for b in plan.puppet_buys if b.wallet_index == puppet.index]
                        ata_rent = 0.00204  # standard ATA rent-exempt minimum
                        needed = sum(b.sol_amount for b in puppet_buys) + ata_rent + (plan.jito_tip_lamports / 1e9)
                        lamports = int(needed * 1_000_000_000)

                        ix = transfer(TransferParams(
                            from_pubkey=self.master_keypair.pubkey(),
                            to_pubkey=puppet.keypair.pubkey(),
                            lamports=lamports,
                        ))
                        msg = Message.new_with_blockhash(
                            [ix], self.master_keypair.pubkey(), blockhash
                        )
                        tx = Transaction.new_unsigned(msg)
                        tx.sign([self.master_keypair], blockhash)
                        await fund_client.send_raw_transaction(
                            bytes(tx),
                            opts=TxOpts(skip_preflight=True, preflight_commitment=Processed),
                        )
                        puppet.funded = True
                        puppet.funded_amount_sol = needed
                        print(f"[PuppetMaster] Funded puppet {puppet.index}: {needed:.4f} SOL")
                        await asyncio.sleep(0.3)  # stagger funding
                except Exception as e:
                    print(f"[PuppetMaster] Puppet funding FAILED: {e}")
                    state.error = f"Puppet funding failed: {e}"
                    state.status = "failed"
                    await _update()
                    await fund_client.close()
                    return state
                await fund_client.close()
            else:
                # Test mode — mark all as funded
                for puppet in state.puppets:
                    puppet.funded = True
                    puppet.funded_amount_sol = 0.01

            # ── Step 2.5: Warm-up puppets (build trade history) ──
            if self.is_live and self._executor:
                state.status = "warming_up"
                await _update()
                print(f"[PuppetMaster] Warming up puppets with micro-trades...")
                # Each puppet does 1-2 tiny buys on random established tokens
                # This builds a trade history so puppets don't look like fresh burners
                for puppet in state.puppets[:min(5, len(state.puppets))]:
                    try:
                        # Buy and immediately sell a micro amount on a random active token
                        # This creates on-chain history for the puppet wallet
                        warmup_amount = random.uniform(0.001, 0.003)
                        # TODO: Pick a random active token mint from profiler snapshots
                        # For now, this is logged as planned
                        print(f"[PuppetMaster] Puppet {puppet.index} warming up ({warmup_amount:.4f} SOL)")
                        await asyncio.sleep(random.uniform(1.0, 3.0))
                    except Exception as e:
                        print(f"[PuppetMaster] Warm-up error puppet {puppet.index}: {e}")
            else:
                print(f"[PuppetMaster] [TEST] Skipping warm-up in test mode")

            # ── Step 3: Deploy token (live only) ──
            if self.is_live:
                state.status = "deploying"
                await _update()
                if self._executor and hasattr(self._executor, 'create_token'):
                    mint_address = await self._executor.create_token(
                        name=plan.token_name,
                        symbol=plan.token_symbol,
                        uri=plan.token_description,
                        priority_fee=0.003,
                    )
                    if not mint_address:
                        state.error = "Token deployment failed — no mint returned from executor"
                        state.status = "failed"
                        await _update()
                        return state
                    state.token_mint = mint_address
                else:
                    state.error = "No executor with create_token support"
                    state.status = "failed"
                    await _update()
                    return state
                print(f"[PuppetMaster] Token deployed: {plan.token_name} ({plan.token_symbol}) mint={state.token_mint[:16]}...")
            else:
                state.token_mint = "SIMULATED_MINT_" + plan.plan_id
                print(f"[PuppetMaster] [TEST] Simulated token: {plan.token_name}")

            # ── Step 4: Execute puppet buy schedule ──
            state.status = "buying"
            await _update()

            sorted_buys = sorted(plan.puppet_buys, key=lambda b: b.delay_sec)
            swarm_lock = asyncio.Lock()
            swarm_start = time.time()

            async def _staggered_puppet_buy(buy) -> bool:
                puppet = state.puppets[buy.wallet_index]
                randomized_size = max(0.0001, buy.sol_amount * random.uniform(0.9, 1.1))
                planned_wait = max(0.0, float(buy.delay_sec) - (time.time() - swarm_start))
                jitter = random.uniform(0.05, 0.60)
                await asyncio.sleep(planned_wait + jitter)

                success = False
                if self.is_live and self._executor:
                    try:
                        puppet_b58 = base58.b58encode(bytes(puppet.keypair)).decode()
                        sig = await self._executor.buy(
                            state.token_mint, randomized_size, 15, 0.003,
                            keypair_b58=puppet_b58,
                        )
                        success = bool(sig)
                    except Exception as e:
                        print(f"[PuppetMaster] Puppet buy failed {puppet.index}: {e}")
                        success = False
                else:
                    success = True

                async with swarm_lock:
                    state.buys_completed += 1
                    if success:
                        puppet.buys_executed += 1
                        puppet.total_spent_sol += randomized_size
                        state.sol_in_pool += randomized_size
                    await _update()
                    print(
                        f"[PuppetMaster] Buy {state.buys_completed}/{state.buys_total} "
                        f"| puppet {puppet.index} | {randomized_size:.4f} SOL"
                    )
                return success

            swarm_results = await asyncio.gather(
                *[_staggered_puppet_buy(buy) for buy in sorted_buys],
                return_exceptions=True,
            )
            swarm_success = sum(1 for r in swarm_results if r is True)
            print(f"[PuppetMaster] Swarm complete: {swarm_success}/{len(sorted_buys)} successful")

            # ── Step 5: Watch for target bot ──
            state.status = "watching"
            await _update()
            print(f"[PuppetMaster] All buys complete. Watching for target bot...")

            # The orchestrator handles the watching via the profiler/gRPC stream
            # When the target bot is detected, orchestrator calls trigger_kill()

        except asyncio.CancelledError:
            state.status = "aborted"
            state.error = "Deployment cancelled"
            await _update()
        except Exception as e:
            state.status = "failed"
            state.error = str(e)
            await _update()
            print(f"[PuppetMaster] Deployment failed: {e}")

        return state

    def mark_target_detected(self, plan_id: str, buy_amount_sol: float):
        """Called by orchestrator when the target bot buys the honeypot."""
        state = self.deployments.get(plan_id)
        if state:
            state.target_bot_detected = True
            state.target_buy_amount = buy_amount_sol
            state.sol_in_pool += buy_amount_sol
            print(f"[PuppetMaster] TARGET BOT DETECTED! Bought {buy_amount_sol:.4f} SOL")

    def get_deployment(self, plan_id: str) -> Optional[DeploymentState]:
        return self.deployments.get(plan_id)

    def get_all_deployments(self) -> List[dict]:
        return [d.to_dict() for d in self.deployments.values()]
