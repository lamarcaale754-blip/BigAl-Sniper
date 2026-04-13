"""
HONEYPOT PLANNER — Stage 3: The Bait
Takes a solved BotSignature and generates a honeypot token deployment plan
designed to trigger the target bot's entry filters.

The plan includes:
- Token metadata (name, symbol, description) that passes common filters
- Deployment parameters (initial liquidity, puppet buy schedule)
- Kill switch threshold (total SOL to accumulate before rugging)
- Puppet wallet count and buy schedule to simulate organic activity

Output: HoneypotPlan consumed by Stage 4 (puppet master)
"""

import random
import statistics
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Dict, List, Optional

if TYPE_CHECKING:
    from wallet_profiler import WalletProfile


# ── Safe token name pools (pass common blacklist filters) ──

NAME_PREFIXES = [
    "Mega", "Ultra", "Based", "Chad", "Moon", "Degen", "Cosmic", "Alpha",
    "Sigma", "Turbo", "Hyper", "Nova", "Pixel", "Retro", "Neon", "Laser",
    "Quantum", "Astro", "Cyber", "Galaxy", "Solar", "Blaze", "Thunder",
]

NAME_SUFFIXES = [
    "Cat", "Dog", "Frog", "Pepe", "Inu", "Coin", "Token", "AI", "Protocol",
    "Swap", "Finance", "Punk", "Ape", "Bear", "Bull", "Duck", "Owl", "Fox",
    "Labs", "DAO", "Cult", "Gang", "Army", "Club", "World", "Verse",
]

DESCRIPTIONS = [
    "The next generation of decentralized meme culture.",
    "Community-driven token for the people.",
    "Built different. No roadmap needed.",
    "Fair launch. No presale. No team tokens.",
    "We're all gonna make it.",
]


@dataclass
class PuppetBuy:
    """A single planned puppet wallet buy."""
    wallet_index: int        # which puppet wallet to use
    delay_sec: float         # seconds after token creation
    sol_amount: float        # SOL to spend
    purpose: str             # "volume", "buyers", "curve_push"


@dataclass
class HoneypotPlan:
    """Complete deployment plan for a honeypot token."""
    plan_id: str
    target_wallet: str       # the bot we're targeting
    target_bot_score: float

    # Token metadata
    token_name: str
    token_symbol: str
    token_description: str

    # Deployment params
    initial_liquidity_sol: float    # SOL to seed the bonding curve
    puppet_count: int               # number of puppet wallets needed
    puppet_buys: List[PuppetBuy] = field(default_factory=list)

    # Kill switch
    kill_threshold_sol: float = 5.0      # rug when this much SOL accumulated
    kill_timeout_sec: float = 120.0      # max wait before aborting
    jito_tip_lamports: int = 5_000_000   # 0.005 SOL tip for kill bundle

    # Status
    status: str = "planned"   # planned → approved → deploying → active → killed / aborted
    created_at: float = 0.0
    approved_at: float = 0.0
    deployed_at: float = 0.0

    # Results
    total_extracted_sol: float = 0.0
    bot_bought: bool = False
    bot_buy_amount_sol: float = 0.0

    # ── Intelligence fields (populated by planner) ──
    trigger_reason: str = ""               # e.g. "5-wallet Sybil cluster within 400ms"
    trigger_tag: str = ""                  # e.g. "SYBIL", "VELOCITY", "PATTERN"
    confidence: float = 0.0                # 0-1 from BotSignature
    parent_wallet: str = ""                # funding source of the cluster
    cluster_sol_stake: float = 0.0         # total SOL across the puppet cluster
    cluster_size: int = 0                  # number of wallets in Sybil cluster
    dev_blacklisted: bool = False          # creator in blacklist_creators.json
    dev_rug_count: int = 0                 # past rug count from blacklist
    prev_score: float = 0.0               # previous score (for delta display)
    expected_bot_buy_sol: float = 0.0      # bot's avg buy size
    projected_pnl_sol: float = 0.0        # estimated extraction minus costs
    slippage_bps: int = 2000              # planned exit slippage
    is_mock: bool = False                 # TEST / paper honeypot preview
    threat_summary: str = ""              # human-readable risk narrative
    mock_entry_summary: str = ""          # simulated entry conditions
    mock_exit_summary: str = ""           # simulated exit / kill conditions

    def to_dict(self) -> dict:
        total_puppet = round(sum(b.sol_amount for b in self.puppet_buys), 4)
        costs = total_puppet + self.initial_liquidity_sol + (self.jito_tip_lamports / 1e9)
        projected = round(self.kill_threshold_sol - costs, 4) if self.kill_threshold_sol > costs else 0.0

        return {
            "plan_id": self.plan_id,
            "target": self.target_wallet[:8] + "..." + self.target_wallet[-4:],
            "target_full": self.target_wallet,
            "target_score": self.target_bot_score,
            "token_name": self.token_name,
            "token_symbol": self.token_symbol,
            "initial_liq": round(self.initial_liquidity_sol, 4),
            "puppet_count": self.puppet_count,
            "puppet_buys": len(self.puppet_buys),
            "total_puppet_sol": total_puppet,
            "kill_threshold": self.kill_threshold_sol,
            "kill_timeout": self.kill_timeout_sec,
            "status": self.status,
            "created_at": self.created_at,
            "bot_bought": self.bot_bought,
            "extracted_sol": round(self.total_extracted_sol, 4),
            # Intelligence
            "trigger_reason": self.trigger_reason,
            "trigger_tag": self.trigger_tag,
            "confidence": round(self.confidence, 2),
            "parent_wallet": self.parent_wallet[:8] + "..." + self.parent_wallet[-4:] if self.parent_wallet else "",
            "parent_wallet_full": self.parent_wallet,
            "cluster_sol_stake": round(self.cluster_sol_stake, 4),
            "cluster_size": self.cluster_size,
            "dev_blacklisted": self.dev_blacklisted,
            "dev_rug_count": self.dev_rug_count,
            "score_delta": round(self.target_bot_score - self.prev_score, 1),
            "expected_bot_buy": round(self.expected_bot_buy_sol, 4),
            "projected_pnl": round(self.projected_pnl_sol or projected, 4),
            "slippage_bps": self.slippage_bps,
            "jito_tip_sol": round(self.jito_tip_lamports / 1e9, 6),
            "is_mock": self.is_mock,
            "threat_summary": self.threat_summary,
            "mock_entry_summary": self.mock_entry_summary,
            "mock_exit_summary": self.mock_exit_summary,
        }


class HoneypotPlanner:
    """
    Generates honeypot deployment plans from solved bot signatures.

    The plan is designed to:
    1. Create a token with metadata that passes common name/quality filters
    2. Schedule puppet buys that push volume, buyer count, and BC% just past
       the target bot's entry thresholds
    3. Set a kill switch threshold for the Jito rug bundle
    """

    def __init__(self):
        self.plans: Dict[str, HoneypotPlan] = {}

    def generate_plan(self, bot_signature, sybil_info=None, blacklist_info=None,
                      jito_tip_lamports: int = 5_000_000) -> HoneypotPlan:
        """Generate a honeypot plan targeting a specific bot.

        Args:
            bot_signature: Solved BotSignature from PatternCracker
            sybil_info: Optional dict with cluster_size, parent_wallet, cluster_sol_stake
            blacklist_info: Optional dict with dev_blacklisted, dev_rug_count
        """
        target = bot_signature.wallet
        plan_id = f"hp-{int(time.time())}-{random.randint(1000,9999)}"
        sybil_info = sybil_info or {}
        blacklist_info = blacklist_info or {}

        # ── Generate token metadata ──
        name = random.choice(NAME_PREFIXES) + random.choice(NAME_SUFFIXES)
        symbol = name[:4].upper() if len(name) >= 4 else name.upper()
        description = random.choice(DESCRIPTIONS)

        # ── Calculate puppet schedule from bot signature ──
        target_vol = bot_signature.min_volume_sol * 1.15   # 15% above threshold
        target_buyers = bot_signature.min_buyers + 2       # 2 extra for safety
        target_bc_pct = bot_signature.min_bc_pct + 1.0     # slightly above

        # Number of puppets = target buyer count (each puppet = 1 unique buyer)
        puppet_count = max(3, min(15, target_buyers))

        # Split volume across puppets with randomized amounts
        base_amount = target_vol / puppet_count
        puppet_buys = []

        for i in range(puppet_count):
            # Randomize amount +-30% to avoid uniform-amount detection
            jitter = random.uniform(0.7, 1.3)
            amount = base_amount * jitter

            # Stagger timing to look organic (within bot's entry window)
            entry_window = bot_signature.avg_entry_time_sec
            delay = random.uniform(1.0, max(2.0, entry_window * 0.8))

            puppet_buys.append(PuppetBuy(
                wallet_index=i,
                delay_sec=round(delay, 1),
                sol_amount=round(max(0.001, amount), 4),
                purpose="volume" if i < puppet_count // 2 else "buyers",
            ))

        # Sort by delay so they execute in order
        puppet_buys.sort(key=lambda b: b.delay_sec)

        # ── Kill switch: extract when enough SOL accumulated ──
        total_puppet_sol = sum(b.sol_amount for b in puppet_buys)
        # Bot's typical buy size + some buffer
        expected_bot_buy = bot_signature.avg_sol_amount
        kill_threshold = total_puppet_sol + expected_bot_buy + 0.5

        # ── Build trigger reason from detection method ──
        cluster_size = sybil_info.get("cluster_size", 0)
        if cluster_size >= 2:
            trigger_tag = "SYBIL"
            trigger_reason = f"{cluster_size}-wallet Sybil cluster detected"
        elif bot_signature.confidence >= 0.7:
            trigger_tag = "PATTERN"
            trigger_reason = f"ML pattern match ({bot_signature.confidence:.0%} confidence)"
        else:
            trigger_tag = "HEURISTIC"
            trigger_reason = f"Heuristic filter match (score {bot_signature.bot_score:.0f})"

        plan = HoneypotPlan(
            plan_id=plan_id,
            target_wallet=target,
            target_bot_score=bot_signature.bot_score,
            token_name=name,
            token_symbol=symbol,
            token_description=description,
            initial_liquidity_sol=round(max(0.5, target_bc_pct * 0.85 / 100), 4),
            puppet_count=puppet_count,
            puppet_buys=puppet_buys,
            kill_threshold_sol=round(kill_threshold, 4),
            kill_timeout_sec=max(60, bot_signature.max_entry_time_sec * 3),
            created_at=time.time(),
            jito_tip_lamports=jito_tip_lamports,
            # Intelligence
            trigger_reason=trigger_reason,
            trigger_tag=trigger_tag,
            confidence=bot_signature.confidence,
            parent_wallet=sybil_info.get("parent_wallet", ""),
            cluster_sol_stake=sybil_info.get("cluster_sol_stake", 0.0),
            cluster_size=cluster_size,
            dev_blacklisted=blacklist_info.get("dev_blacklisted", False),
            dev_rug_count=blacklist_info.get("dev_rug_count", 0),
            expected_bot_buy_sol=expected_bot_buy,
        )

        self.plans[plan_id] = plan
        return plan

    def generate_mock_plan(
        self,
        wallet: str,
        profile: "WalletProfile",
        sybil_info: Optional[Dict] = None,
        blacklist_info: Optional[Dict] = None,
        jito_tip_lamports: int = 5_000_000,
    ) -> HoneypotPlan:
        """
        TEST / paper-mode plan from profiler stats only (no ML crack required).
        Populates entry/exit summaries, threat narrative, and projected PnL for UI demos.
        """
        sybil_info = sybil_info or {}
        blacklist_info = blacklist_info or {}

        if profile._score_dirty:
            profile.compute_bot_score()
        score = profile._bot_score
        buys = profile.buy_trades
        avg_sol = round(statistics.mean([t.sol_amount for t in buys]), 4) if buys else 0.05
        t_after = [t.time_after_create for t in buys if t.time_after_create > 0]
        avg_t = round(statistics.mean(t_after), 1) if t_after else 5.0
        bc_obs = [t.bc_progress for t in buys if t.bc_progress > 0]
        mean_bc = statistics.mean(bc_obs) if bc_obs else 12.0
        mean_bc = min(mean_bc, 99.0)  # BC progress can't exceed 100%
        min_bc = max(0.5, mean_bc - 4.0)
        max_bc = min(45.0, mean_bc + 6.0)
        min_vol = max(0.15, avg_sol * len(buys) * 0.08)
        min_buyers = max(3, min(12, len(buys) // 3 + 2))

        plan_id = f"mock-{int(time.time())}-{random.randint(1000, 9999)}"
        name = random.choice(NAME_PREFIXES) + random.choice(NAME_SUFFIXES)
        symbol = name[:4].upper() if len(name) >= 4 else name.upper()

        puppet_count = max(4, min(10, min_buyers + 2))
        base = max(0.02, min_vol / puppet_count)
        puppet_buys: List[PuppetBuy] = []
        for i in range(puppet_count):
            jitter = random.uniform(0.75, 1.25)
            delay = round(random.uniform(1.5, max(3.0, avg_t * 0.6)), 1)
            puppet_buys.append(
                PuppetBuy(
                    wallet_index=i,
                    delay_sec=delay,
                    sol_amount=round(max(0.008, base * jitter), 4),
                    purpose="volume" if i % 2 == 0 else "buyers",
                )
            )
        puppet_buys.sort(key=lambda b: b.delay_sec)

        total_puppet_sol = sum(b.sol_amount for b in puppet_buys)
        expected_bot = max(0.02, avg_sol * 0.85)
        initial_liq = round(max(0.35, min(2.0, min_bc / 25.0)), 4)
        kill_threshold = round(initial_liq + total_puppet_sol + expected_bot, 4)
        costs = total_puppet_sol + initial_liq + jito_tip_lamports / 1e9
        projected = round(kill_threshold - costs, 4)

        cluster_size = int(sybil_info.get("cluster_size", 0) or 0)
        if cluster_size >= 2:
            tag, reason = "SYBIL", f"{cluster_size}-wallet cluster (mock intel)"
        elif score >= 70:
            tag, reason = "PATTERN", f"High bot score {score:.0f}% — mock pattern bait"
        else:
            tag, reason = "MOCK", "Paper / test honeypot preview (no ML crack)"

        threat = (
            f"Target {score:.0f}% bot probability, {len(buys)} buys across "
            f"{profile.unique_tokens} tokens; avg entry ~{avg_t}s, ~{avg_sol} SOL/trade."
        )
        if blacklist_info.get("dev_blacklisted"):
            threat += " Linked wallet on creator blacklist (simulated elevated risk)."

        entry_txt = (
            f"Bait entry band: BC {min_bc:.1f}-{max_bc:.1f}%, "
            f"aggregate vol ≥{min_vol:.2f} SOL, unique buyers ≥{min_buyers}. "
            f"Puppet ladder over {puppet_count} wallets staggers ~{puppet_buys[0].delay_sec}-{puppet_buys[-1].delay_sec}s."
        )
        exit_txt = (
            f"Simulated kill: Jito bundle when pool ≥{kill_threshold} SOL "
            f"or after {max(90.0, avg_t * 8):.0f}s watch — est. net {projected:+.4f} SOL after puppet + seed + tip."
        )

        plan = HoneypotPlan(
            plan_id=plan_id,
            target_wallet=wallet,
            target_bot_score=score,
            token_name=name,
            token_symbol=symbol,
            token_description=random.choice(DESCRIPTIONS),
            initial_liquidity_sol=initial_liq,
            puppet_count=puppet_count,
            puppet_buys=puppet_buys,
            kill_threshold_sol=kill_threshold,
            kill_timeout_sec=max(90.0, avg_t * 8),
            created_at=time.time(),
            jito_tip_lamports=jito_tip_lamports,
            trigger_reason=reason + " [MOCK PLAN]",
            trigger_tag=tag,
            confidence=0.72 if score >= 60 else 0.55,
            parent_wallet=sybil_info.get("parent_wallet", "") or "",
            cluster_sol_stake=float(sybil_info.get("cluster_sol_stake", 0.0) or 0.0),
            cluster_size=cluster_size,
            dev_blacklisted=bool(blacklist_info.get("dev_blacklisted", False)),
            dev_rug_count=int(blacklist_info.get("dev_rug_count", 0) or 0),
            expected_bot_buy_sol=expected_bot,
            projected_pnl_sol=projected,
            is_mock=True,
            threat_summary=threat,
            mock_entry_summary=entry_txt,
            mock_exit_summary=exit_txt,
        )
        self.plans[plan_id] = plan
        return plan

    def approve_plan(self, plan_id: str) -> bool:
        """User approves a plan for deployment."""
        plan = self.plans.get(plan_id)
        if plan and plan.status == "planned":
            plan.status = "approved"
            plan.approved_at = time.time()
            return True
        return False

    def reject_plan(self, plan_id: str) -> bool:
        """User rejects a plan."""
        plan = self.plans.get(plan_id)
        if plan and plan.status == "planned":
            plan.status = "rejected"
            return True
        return False

    def get_pending_plans(self) -> List[dict]:
        """Return plans awaiting approval."""
        return [
            p.to_dict() for p in self.plans.values()
            if p.status == "planned"
        ]

    def get_all_plans(self) -> List[dict]:
        """Return all plans for dashboard."""
        return [p.to_dict() for p in self.plans.values()]

    def get_plan(self, plan_id: str) -> Optional[HoneypotPlan]:
        """Get a specific plan."""
        return self.plans.get(plan_id)
