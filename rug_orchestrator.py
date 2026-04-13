"""
RUG ORCHESTRATOR — The Hunter Loop
Coordinates all 5 stages of the rug puller pipeline:

  Stage 1: Wallet Profiler (observation) — runs continuously
  Stage 2: Pattern Cracker (analysis)   — triggered when bot confirmed
  Stage 3: Honeypot Planner (planning)  — triggered when filters solved
  Stage 4: Puppet Master (deployment)   — triggered when plan approved
  Stage 5: Kill Chain (execution)       — triggered when target buys

The orchestrator runs as an async background task inside the main bot.
"""

import asyncio
import time
from typing import Optional, Callable, Awaitable

from wallet_profiler import WalletProfiler
from pattern_cracker import PatternCracker, BotSignature
from honeypot_planner import HoneypotPlanner, HoneypotPlan
from puppet_master import PuppetMaster


def _existing_active_plan_for_target(planner: HoneypotPlanner, wallet: str) -> bool:
    return any(
        p.target_wallet == wallet
        for p in planner.plans.values()
        if p.status in ("planned", "approved", "deploying", "active")
    )


class RugOrchestrator:
    """
    Coordinates the 5-stage hunter pipeline.

    In TEST mode: Stages 1-3 run fully. Stage 4 simulates buys. Stage 5 simulates kill.
    In LIVE mode: All stages execute real transactions via the executor.
    """

    def __init__(self, profiler: WalletProfiler, is_live: bool = False,
                 creator_blacklist: Optional[dict] = None,
                 rug_settings: Optional[dict] = None):
        self.profiler = profiler
        self.is_live = is_live
        self._creator_blacklist = creator_blacklist or {}
        self._rug_settings_data = rug_settings or {}

        # Stage 2
        self.cracker = PatternCracker()

        # Stage 3
        self.planner = HoneypotPlanner()

        # Stage 4
        self.puppet_master = PuppetMaster(is_live=is_live)

        # Orchestrator state
        self.running = False
        self.auto_crack = True      # auto-crack confirmed bots
        self.auto_plan = True       # auto-generate plans from solved bots
        self.auto_deploy = False    # require manual approval for deployment
        self._task: Optional[asyncio.Task] = None

        # Callbacks for dashboard updates
        self._on_crack: Optional[Callable] = None
        self._on_plan: Optional[Callable] = None
        self._on_deploy: Optional[Callable] = None
        self._on_kill: Optional[Callable] = None

        # Success ratio tracking (economic attrition monitor)
        self.stats_deployments_total: int = 0
        self.stats_deployments_success: int = 0  # target bot bought
        self.stats_deployments_missed: int = 0   # target didn't buy / timed out
        self.stats_sol_spent: float = 0.0        # total puppet + gas costs
        self.stats_sol_extracted: float = 0.0     # total SOL pulled from kills

        # Active deployments watching for target bots
        self._watching: dict = {}  # plan_id → target_wallet

    def set_executor(self, executor):
        """Pass the trade executor to puppet master."""
        self.puppet_master.set_executor(executor)

    def set_master_wallet(self, keypair):
        """Set the master wallet for funding puppets."""
        self.puppet_master.set_master_wallet(keypair)

    def set_creator_blacklist(self, blacklist: dict):
        """Update creator blacklist reference for runtime changes."""
        self._creator_blacklist = blacklist

    def set_rug_settings(self, settings: dict):
        """Update rug puller settings from dashboard."""
        self._rug_settings_data = settings

    def set_callbacks(self, on_crack=None, on_plan=None, on_deploy=None, on_kill=None):
        """Set async callbacks for dashboard updates."""
        self._on_crack = on_crack
        self._on_plan = on_plan
        self._on_deploy = on_deploy
        self._on_kill = on_kill

    def _intel_for_wallet(self, wallet: str) -> tuple:
        """Sybil + blacklist context for plan generation (matches auto_plan loop)."""
        sybil_info: dict = {}
        if self.profiler and hasattr(self.profiler, "sybil_detector"):
            sd = self.profiler.sybil_detector
            cid = sd.get_cluster_id(wallet)
            if cid:
                members = [w for w, c in sd._clusters.items() if c == cid]
                sybil_info["cluster_size"] = len(members)
            if hasattr(sd, "funding") and wallet in sd.funding._source_map:
                sybil_info["parent_wallet"] = sd.funding._source_map[wallet]

        # Tight cluster bonus from profiler
        prof = self.profiler.get_profile(wallet) if self.profiler else None
        if prof and getattr(prof, "cluster_bonus", 0) > 0:
            sybil_info["is_tight_cluster"] = True
            sybil_info["cluster_bonus"] = prof.cluster_bonus

        blacklist_info: dict = {}
        if wallet in self._creator_blacklist:
            bl = self._creator_blacklist[wallet]
            blacklist_info["dev_blacklisted"] = True
            blacklist_info["dev_rug_count"] = bl.get("losses", 0)
        return sybil_info, blacklist_info

    def _rug_settings(self) -> dict:
        return self._rug_settings_data

    def _apply_aggression_to_plan(self, plan: HoneypotPlan) -> None:
        """Scale projected PnL from dashboard aggression preset."""
        rs = self._rug_settings()
        lvl = str(rs.get("aggression_level", "standard")).lower()
        mult = {
            "conservative": 0.72,
            "standard": 1.0,
            "degenerate": 1.52,
        }.get(lvl, 1.0)
        total_puppet = sum(b.sol_amount for b in plan.puppet_buys)
        costs = total_puppet + plan.initial_liquidity_sol + plan.jito_tip_lamports / 1e9
        raw = float(plan.projected_pnl_sol or 0.0)
        if raw <= 0:
            raw = max(0.0, plan.kill_threshold_sol - costs)
        plan.projected_pnl_sol = round(max(0.0, raw) * mult, 4)

    def generate_plan_for_wallet(self, wallet: str, *, force: bool = False) -> dict:
        """
        Crack + plan a single suspect on demand (dashboard Plan button).
        TEST / paper (`not is_live`): always builds a mock plan from profiler data.
        LIVE: uses ML when crack succeeds with confidence > 50%; otherwise, with
        `force=True` (dashboard), falls back to a heuristic mock plan so planning
        always proceeds when profiler has the wallet.
        Returns {ok, plan?, error?, mock?, wallet_requested}.
        """
        wallet = (wallet or "").strip()
        out_base = {"wallet_requested": wallet}
        if len(wallet) < 32:
            return {**out_base, "ok": False, "error": "Invalid wallet address"}

        prof = self.profiler.get_profile(wallet) if self.profiler else None
        if not prof:
            return {
                **out_base,
                "ok": False,
                "error": "No profiler data for this wallet yet",
            }
        if not force and len(prof.buy_trades) < 1:
            return {
                **out_base,
                "ok": False,
                "error": "No buy trades recorded — wait for activity",
            }

        sybil_info, blacklist_info = self._intel_for_wallet(wallet)

        if _existing_active_plan_for_target(self.planner, wallet):
            for p in self.planner.plans.values():
                if p.target_wallet == wallet and p.status in (
                    "planned", "approved", "deploying", "active",
                ):
                    return {
                        **out_base,
                        "ok": True,
                        "message": "Plan already exists",
                        "plan": p.to_dict(),
                        "mock": getattr(p, "is_mock", False),
                    }

        def _mock_plan(tag_note: str = "") -> dict:
            plan = self.planner.generate_mock_plan(
                wallet, prof, sybil_info, blacklist_info
            )
            if tag_note:
                plan.trigger_reason = f"{tag_note} {plan.trigger_reason}"
            self._apply_aggression_to_plan(plan)
            return {**out_base, "ok": True, "plan": plan.to_dict(), "mock": True}

        # Rug Puller test / paper: mock plan so the planning UI works without ML
        if not self.is_live:
            print(f"[Orchestrator] MOCK PLAN for {wallet[:12]}... → (generating)")
            r = _mock_plan("[Test/paper]")
            print(f"[Orchestrator] MOCK PLAN → {r['plan']['plan_id']}")
            return r

        sig = self.cracker.crack_bot(wallet, self.profiler)
        if sig and sig.confidence > 0.5:
            plan = self.planner.generate_plan(
                sig, sybil_info=sybil_info, blacklist_info=blacklist_info
            )
            self._apply_aggression_to_plan(plan)
            print(f"[Orchestrator] Manual PLAN for {wallet[:12]}... → {plan.plan_id}")
            return {**out_base, "ok": True, "plan": plan.to_dict(), "mock": False}

        if not force:
            if not sig:
                return {
                    **out_base,
                    "ok": False,
                    "error": "Pattern crack failed — need more tokens / buys for ML",
                }
            return {
                **out_base,
                "ok": False,
                "error": f"ML confidence too low ({sig.confidence:.0%}); need >50%",
            }

        # Forced dashboard path on live: always produce a heuristic plan
        note = "[Live · manual override — ML crack failed or ≤50% conf] "
        if not sig:
            note = "[Live · manual override — ML crack unavailable] "
        print(f"[Orchestrator] Heuristic MOCK (forced) for {wallet[:12]}...")
        r = _mock_plan(note)
        print(f"[Orchestrator] Heuristic plan → {r['plan']['plan_id']}")
        return r

    # ──────────────────────────────────────────────
    #  MAIN LOOP
    # ──────────────────────────────────────────────

    async def start(self):
        """Start the orchestrator background loop."""
        self.running = True
        self._task = asyncio.create_task(self._loop())
        print("[Orchestrator] Hunter loop started")

    async def stop(self):
        """Stop the orchestrator."""
        self.running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        print("[Orchestrator] Hunter loop stopped")

    async def _loop(self):
        """
        Main orchestrator loop — runs every 10 seconds.
        Checks profiler for new confirmed bots, cracks their patterns,
        generates plans, and manages active deployments.
        """
        while self.running:
            try:
                await asyncio.sleep(10)

                # ── Stage 2: Auto-crack confirmed bots ──
                if self.auto_crack:
                    rs = self._rug_settings()
                    try:
                        min_age_m = float(rs.get("min_wallet_age_minutes", 0) or 0)
                    except (TypeError, ValueError):
                        min_age_m = 0.0
                    min_age_sec = max(0.0, min_age_m) * 60.0
                    confirmed = self.profiler.get_suspected_bots(
                        min_score=80, min_wallet_age_sec=min_age_sec
                    )
                    for bot in confirmed:
                        wallet = bot["wallet_full"]
                        if wallet not in self.cracker.solved_signatures:
                            sig = self.cracker.crack_bot(wallet, self.profiler)
                            if sig and sig.confidence > 0.3:
                                print(f"[Orchestrator] CRACKED bot {wallet[:12]}... "
                                      f"| vol={sig.min_volume_sol:.3f}-{sig.max_volume_sol:.3f} "
                                      f"| buyers={sig.min_buyers}-{sig.max_buyers} "
                                      f"| bc={sig.min_bc_pct:.1f}-{sig.max_bc_pct:.1f}% "
                                      f"| confidence={sig.confidence:.0%}")
                                if self._on_crack:
                                    await self._on_crack(sig)

                # ── Stage 3: Auto-plan from solved bots ──
                if self.auto_plan:
                    rs = self._rug_settings()
                    try:
                        score_thr = float(rs.get("auto_plan_threshold", 70))
                    except (TypeError, ValueError):
                        score_thr = 70.0
                    score_thr = max(0.0, min(100.0, score_thr))
                    auto_ap = bool(rs.get("auto_approve", False))

                    for wallet, sig in self.cracker.solved_signatures.items():
                        if _existing_active_plan_for_target(self.planner, wallet):
                            continue
                        if sig.confidence <= 0.5:
                            continue
                        prof = self.profiler.get_profile(wallet)
                        if not prof:
                            continue
                        if prof._score_dirty:
                            prof.compute_bot_score()
                        if prof._bot_score < score_thr:
                            continue

                        sybil_info, blacklist_info = self._intel_for_wallet(wallet)

                        plan = self.planner.generate_plan(
                            sig, sybil_info=sybil_info, blacklist_info=blacklist_info
                        )
                        self._apply_aggression_to_plan(plan)
                        if auto_ap:
                            self.planner.approve_plan(plan.plan_id)
                        cluster_tag = " [CLUSTERED]" if sybil_info.get("is_tight_cluster") else ""
                        print(f"[Orchestrator] PLAN generated: {plan.plan_id} "
                              f"| token={plan.token_name} [{plan.trigger_tag}] "
                              f"| puppets={plan.puppet_count} "
                              f"| kill@{plan.kill_threshold_sol:.3f} SOL"
                              f"{cluster_tag}"
                              f"{' [auto-approved]' if auto_ap else ''}")
                        if self._on_plan:
                            await self._on_plan(plan)

                # ── Stage 4: Deploy approved plans ──
                for plan_id, plan in list(self.planner.plans.items()):
                    if plan.status == "approved":
                        plan.status = "deploying"
                        asyncio.create_task(self._run_deployment(plan))

            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"[Orchestrator] Loop error: {e}")

    async def _run_deployment(self, plan: HoneypotPlan):
        """Execute a honeypot deployment (Stage 4) and set up watching."""
        async def on_update(state):
            if self._on_deploy:
                await self._on_deploy(state)

        state = await self.puppet_master.deploy_honeypot(plan, on_status_update=on_update)

        if state.status == "watching" and state.token_mint:
            # Register for target bot detection
            self._watching[plan.plan_id] = {
                "target_wallet": plan.target_wallet,
                "token_mint": state.token_mint,
                "kill_threshold": plan.kill_threshold_sol,
                "kill_timeout": plan.kill_timeout_sec,
                "started_at": time.time(),
            }
            print(f"[Orchestrator] Deployment {plan.plan_id} active — watching for target")

    def on_trade_event(self, msg: dict):
        """
        Called from the main bot's trade stream for every buy/sell.
        Checks if any watched honeypot is being bought by its target bot.
        """
        if not self._watching:
            return None

        trader = (msg.get("traderPublicKey")
                  or msg.get("user")
                  or msg.get("owner", ""))
        mint = msg.get("mint", "")
        tx_type = msg.get("txType", "")

        if tx_type != "buy":
            return None

        for plan_id, watch in list(self._watching.items()):
            if mint == watch["token_mint"] and trader == watch["target_wallet"]:
                # TARGET BOT BOUGHT OUR HONEYPOT
                sol_amount = abs(float(msg.get("solAmount", 0) or 0))
                self.puppet_master.mark_target_detected(plan_id, sol_amount)

                deployment = self.puppet_master.get_deployment(plan_id)
                if deployment and deployment.sol_in_pool >= watch["kill_threshold"]:
                    # Track success
                    self.stats_deployments_success += 1
                    self.stats_sol_extracted += deployment.sol_in_pool
                    self._watching.pop(plan_id, None)

                    return {
                        "action": "kill",
                        "plan_id": plan_id,
                        "mint": mint,
                        "target_wallet": trader,
                        "sol_in_pool": deployment.sol_in_pool,
                    }

        # Check for timed-out deployments
        now = time.time()
        for plan_id, watch in list(self._watching.items()):
            if now - watch["started_at"] > watch.get("kill_timeout", 120):
                self.stats_deployments_missed += 1
                puppet_cost = sum(
                    b.sol_amount for b in self.planner.plans[plan_id].puppet_buys
                ) if plan_id in self.planner.plans else 0
                self.stats_sol_spent += puppet_cost
                self._watching.pop(plan_id, None)
                print(f"[Orchestrator] Deployment {plan_id} TIMED OUT — target didn't buy")

        return None

    # ──────────────────────────────────────────────
    #  MANUAL CONTROLS (from dashboard)
    # ──────────────────────────────────────────────

    def crack_specific_bot(self, wallet: str) -> Optional[dict]:
        """Manually trigger pattern cracking for a specific wallet."""
        sig = self.cracker.crack_bot(wallet, self.profiler)
        if sig:
            return sig.to_dict()
        return None

    def approve_plan(self, plan_id: str) -> bool:
        """Approve a honeypot plan for deployment."""
        return self.planner.approve_plan(plan_id)

    def reject_plan(self, plan_id: str) -> bool:
        """Reject a honeypot plan."""
        return self.planner.reject_plan(plan_id)

    # ──────────────────────────────────────────────
    #  DASHBOARD API
    # ──────────────────────────────────────────────

    def get_status(self) -> dict:
        """Full orchestrator status for dashboard."""
        total = self.stats_deployments_success + self.stats_deployments_missed
        success_rate = (self.stats_deployments_success / total * 100) if total > 0 else 0
        net_pnl = self.stats_sol_extracted - self.stats_sol_spent

        return {
            "running": self.running,
            "is_live": self.is_live,
            "auto_crack": self.auto_crack,
            "auto_plan": self.auto_plan,
            "auto_deploy": self.auto_deploy,
            "solved_bots": len(self.cracker.solved_signatures),
            "pending_plans": len(self.planner.get_pending_plans()),
            "active_deployments": len([
                d for d in self.puppet_master.deployments.values()
                if d.status in ("buying", "watching")
            ]),
            "watching": len(self._watching),
            "signatures": self.cracker.get_solved_signatures(),
            "plans": self.planner.get_all_plans(),
            "deployments": self.puppet_master.get_all_deployments(),
            # Economics
            "economics": {
                "deployments_total": total,
                "deployments_success": self.stats_deployments_success,
                "deployments_missed": self.stats_deployments_missed,
                "success_rate_pct": round(success_rate, 1),
                "sol_spent": round(self.stats_sol_spent, 4),
                "sol_extracted": round(self.stats_sol_extracted, 4),
                "net_pnl": round(net_pnl, 4),
            },
        }
