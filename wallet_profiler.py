"""
WALLET PROFILER — Stage 1: The Eyes
Passively monitors PumpPortal trade stream and builds behavioral
profiles for every wallet observed. Scores wallets on "bot probability"
using timing consistency, amount patterns, trade frequency,
entry-condition fingerprinting, and Sybil co-occurrence analysis.

Part of BigAl Auto Rug Puller pipeline.
"""

import time
import statistics
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Callable, Tuple


# =====================================================
#  DATA STRUCTURES
# =====================================================

@dataclass
class TradeObservation:
    """Single observed trade event for a wallet."""
    timestamp: float
    token_mint: str
    token_name: str
    token_symbol: str
    tx_type: str              # "buy" or "sell"
    sol_amount: float
    time_after_create: float  # seconds after token was created (-1 if unknown)
    bc_progress: float        # bonding curve % at moment of trade
    unique_buyers: int        # unique buyer count at moment of trade
    volume_sol: float         # cumulative buy volume at moment
    sell_ratio: float         # sell/total ratio at moment
    price: float              # token price at moment


@dataclass
class TokenSnapshot:
    """Tracks a token's lifecycle for timing and market-condition analysis."""
    mint: str
    name: str
    symbol: str
    creator: str
    created_at: float
    initial_vsol: float
    initial_vtokens: float
    initial_price: float
    last_vsol: float
    last_vtokens: float
    unique_buyers: Set[str] = field(default_factory=set)
    buy_volume: float = 0.0
    sell_volume: float = 0.0


@dataclass
class WalletProfile:
    """Behavioral profile for a single on-chain wallet."""
    wallet: str
    first_seen: float
    trades: List[TradeObservation] = field(default_factory=list)

    # Cached scoring
    _bot_score: float = 0.0
    _last_scored: float = 0.0
    _score_dirty: bool = True
    _component_scores: Dict[str, float] = field(default_factory=dict)

    # Sybil detection
    sybil_cluster_id: Optional[str] = None  # set if part of a Sybil group
    sybil_score: float = 0.0  # 0-100, how likely this is a Sybil sub-wallet
    cluster_bonus: float = 0.0  # +15 if in tight Sybil cluster (set by profiler)

    # Transaction density — bots trade at unnaturally regular intervals
    _trade_gaps: List[float] = field(default_factory=list)  # seconds between consecutive trades

    def add_trade(self, obs: TradeObservation):
        # Track inter-trade gaps for density scoring
        if self.trades:
            gap = obs.timestamp - self.trades[-1].timestamp
            if gap > 0:
                self._trade_gaps.append(gap)
                if len(self._trade_gaps) > 150:
                    self._trade_gaps = self._trade_gaps[-100:]
        self.trades.append(obs)
        self._score_dirty = True
        # Rolling window — keep last 200 trades, trim to 150
        if len(self.trades) > 200:
            self.trades = self.trades[-150:]

    @property
    def buy_trades(self) -> List[TradeObservation]:
        return [t for t in self.trades if t.tx_type == "buy"]

    @property
    def sell_trades(self) -> List[TradeObservation]:
        return [t for t in self.trades if t.tx_type == "sell"]

    @property
    def unique_tokens(self) -> int:
        return len(set(t.token_mint for t in self.trades))

    def compute_bot_score(self) -> float:
        """
        Compute 0–100 bot probability score from behavioral fingerprints.

        Components (weighted):
          timing      18%  — std dev of seconds-after-create across buys
          amount      14%  — coefficient of variation of SOL amounts
          frequency   12%  — trades per hour
          diversity   10%  — unique tokens traded
          buy_dom      8%  — buy-to-total ratio (bots rarely manual-sell)
          pattern     13%  — consistency of entry conditions (BC%, buyers)
          sybil       13%  — co-occurrence with other wallets (Sybil clusters)
          density     12%  — regularity of inter-trade gaps (bots trade like clockwork)
        """
        buys = self.buy_trades
        if len(buys) < 8:
            self._bot_score = 0.0
            self._score_dirty = False
            return 0.0

        scores: Dict[str, float] = {}

        # ── 1. Timing consistency ──
        timings = [t.time_after_create for t in buys if t.time_after_create > 0]
        if len(timings) >= 3:
            mean_t = statistics.mean(timings)
            std_t = statistics.stdev(timings) if len(timings) > 1 else 0
            cv_t = std_t / mean_t if mean_t > 0 else 1.0
            scores["timing"] = max(0, min(100, (1.0 - min(cv_t, 1.0)) * 100))
        else:
            scores["timing"] = 0

        # ── 2. Amount consistency ──
        amounts = [t.sol_amount for t in buys if t.sol_amount > 0]
        if len(amounts) >= 3:
            mean_a = statistics.mean(amounts)
            std_a = statistics.stdev(amounts) if len(amounts) > 1 else 0
            cv_a = std_a / mean_a if mean_a > 0 else 1.0
            scores["amount"] = max(0, min(100, (1.0 - min(cv_a, 1.0)) * 100))
        else:
            scores["amount"] = 0

        # ── 3. Trade frequency ──
        if len(buys) >= 2:
            duration = buys[-1].timestamp - buys[0].timestamp
            if duration > 0:
                tph = len(buys) / (duration / 3600)
                scores["frequency"] = max(0, min(100, tph * 10))
            else:
                scores["frequency"] = 50
        else:
            scores["frequency"] = 0

        # ── 4. Cross-token diversity ──
        unique_t = len(set(t.token_mint for t in buys))
        scores["diversity"] = max(0, min(100, unique_t * 8))

        # ── 5. Buy dominance ──
        total = len(self.trades)
        buy_ratio = len(buys) / total if total > 0 else 0.5
        scores["buy_dom"] = max(0, min(100, max(0, buy_ratio - 0.5) * 200))

        # ── 6. Entry pattern consistency ──
        bc_progs = [t.bc_progress for t in buys if t.bc_progress > 0]
        buyer_counts = [t.unique_buyers for t in buys if t.unique_buyers > 0]
        pattern_score = 0

        if len(bc_progs) >= 3:
            mean_bc = statistics.mean(bc_progs)
            std_bc = statistics.stdev(bc_progs) if len(bc_progs) > 1 else 0
            cv_bc = std_bc / mean_bc if mean_bc > 0 else 1.0
            pattern_score += max(0, min(50, (1.0 - min(cv_bc, 1.0)) * 50))

        if len(buyer_counts) >= 3:
            mean_bk = statistics.mean(buyer_counts)
            std_bk = statistics.stdev(buyer_counts) if len(buyer_counts) > 1 else 0
            cv_bk = std_bk / mean_bk if mean_bk > 0 else 1.0
            pattern_score += max(0, min(50, (1.0 - min(cv_bk, 1.0)) * 50))

        scores["pattern"] = pattern_score

        # ── 7. Sybil score (set externally by SybilDetector) ──
        scores["sybil"] = self.sybil_score

        # ── 8. Transaction density — regularity of inter-trade gaps ──
        if len(self._trade_gaps) >= 5:
            mean_gap = statistics.mean(self._trade_gaps)
            std_gap = statistics.stdev(self._trade_gaps) if len(self._trade_gaps) > 1 else 0
            cv_gap = std_gap / mean_gap if mean_gap > 0 else 1.0
            # Low CV = very regular intervals = bot-like
            # Also: very short mean gap (< 2s between trades) = bot
            regularity = max(0, min(100, (1.0 - min(cv_gap, 1.0)) * 80))
            speed_bonus = max(0, min(20, (5.0 - mean_gap) * 10)) if mean_gap < 5 else 0
            scores["density"] = min(100, regularity + speed_bonus)
        else:
            scores["density"] = 0

        # ── Weighted composite ──
        weighted = (
            scores["timing"]    * 0.18 +
            scores["amount"]    * 0.14 +
            scores["frequency"] * 0.12 +
            scores["diversity"] * 0.10 +
            scores["buy_dom"]   * 0.08 +
            scores["pattern"]   * 0.13 +
            scores["sybil"]     * 0.13 +
            scores["density"]   * 0.12
        )

        # ── Cluster bonus (set by WalletProfiler._detect_tight_clusters) ──
        weighted += self.cluster_bonus
        scores["cluster_bonus"] = self.cluster_bonus

        self._bot_score = round(min(100, max(0, weighted)), 1)
        self._component_scores = scores
        self._score_dirty = False
        self._last_scored = time.time()
        return self._bot_score

    def get_status(self) -> str:
        n = len(self.buy_trades)
        if n < 8:
            return "WATCHING"
        if self._bot_score >= 80:
            return "CONFIRMED"
        if self._bot_score >= 60:
            return "SUSPECTED"
        return "SCORING"

    def to_dashboard_dict(
        self,
        now: Optional[float] = None,
        min_wallet_age_sec: float = 0.0,
    ) -> Optional[dict]:
        """
        Serialize for dashboard. Returns None if the wallet is younger than
        `min_wallet_age_sec` since first_seen (Rug Puller age filter).
        """
        tnow = time.time() if now is None else now
        if min_wallet_age_sec > 0 and (tnow - self.first_seen) < min_wallet_age_sec:
            return None

        buys = self.buy_trades
        avg_sol = round(statistics.mean([t.sol_amount for t in buys]), 4) if buys else 0

        valid_timings = [t.time_after_create for t in buys if t.time_after_create > 0]
        avg_timing = round(statistics.mean(valid_timings), 1) if valid_timings else 0

        return {
            "wallet": self.wallet[:8] + "..." + self.wallet[-4:],
            "wallet_full": self.wallet,
            "confidence_pct": round(self._bot_score, 1),
            "trades": len(self.trades),
            "buys": len(buys),
            "unique_tokens": self.unique_tokens,
            "bot_score": self._bot_score,
            "status": self.get_status(),
            "avg_sol": avg_sol,
            "avg_timing": avg_timing,
            "first_seen": self.first_seen,
            "last_seen": self.trades[-1].timestamp if self.trades else self.first_seen,
            "components": dict(self._component_scores),
            "sybil_cluster": self.sybil_cluster_id,
            "sybil_score": self.sybil_score,
            "is_clustered": self.sybil_cluster_id is not None,
        }


# =====================================================
#  WALLET PROFILER ENGINE
# =====================================================

BC_GRADUATION_SOL = 85.0  # standard PumpFun graduation threshold

# Suspect retention: evict only when idle >30m AND bot score <25 (0–100).
# High-confidence wallets (score ≥70) are not dropped in the normal memory-pressure pass.
SUSPECT_INACTIVE_SEC = 30 * 60
SUSPECT_LOW_SCORE_CUTOFF = 25.0
SUSPECT_HIGH_CONFIDENCE_MIN = 70.0

# Minimum buy volume — filter out dust/noise wallets
MIN_BUY_VOLUME_SOL = 0.005

# Activity decay — evict wallets stuck below threshold after this duration
ACTIVITY_DECAY_SEC = 300
ACTIVITY_DECAY_SCORE = 60.0


class WalletProfiler:
    """
    Passive wallet profiler — sits on the PumpPortal trade stream
    and builds behavioral fingerprints to detect automated trading bots.

    Usage:
        profiler = WalletProfiler()

        # Feed events from PumpPortal
        profiler.on_token_create(msg)   # on "create" events
        profiler.on_trade(msg)          # on "buy"/"sell" events

        # Query results
        suspects = profiler.get_suspected_bots(min_score=60)
        stats    = profiler.get_profiler_stats()
    """

    def __init__(self, score_interval: float = 5.0,
                 max_profiles: int = 5000,
                 min_trades_to_score: int = 8):
        self.profiles: Dict[str, WalletProfile] = {}
        self.token_snapshots: Dict[str, TokenSnapshot] = {}

        self.score_interval = score_interval
        self.max_profiles = max_profiles
        self.min_trades = min_trades_to_score

        self.last_score_time: float = 0.0
        self.events_processed: int = 0
        self.start_time: float = time.time()

        # Batch scoring — only re-score wallets that got new data
        self._dirty_wallets: Set[str] = set()

        # Sybil detection — co-occurrence graph
        self.sybil_detector = SybilDetector()

    # ──────────────────────────────────────────────
    #  EVENT HANDLERS (called from main bot loop)
    # ──────────────────────────────────────────────

    def on_token_create(self, msg: dict):
        """Record a new token creation for timing and market-state tracking."""
        mint = msg.get("mint")
        if not mint:
            return

        try:
            vsol = float(msg.get("vSolInBondingCurve", 0))
            vtokens = float(msg.get("vTokensInBondingCurve", 0))
        except (ValueError, TypeError):
            return

        creator = (msg.get("traderPublicKey")
                   or msg.get("user")
                   or msg.get("owner")
                   or "")

        self.token_snapshots[mint] = TokenSnapshot(
            mint=mint,
            name=msg.get("name", "Unknown"),
            symbol=msg.get("symbol", "???"),
            creator=creator,
            created_at=time.time(),
            initial_vsol=vsol,
            initial_vtokens=vtokens,
            initial_price=vsol / vtokens if vtokens > 0 else 0,
            last_vsol=vsol,
            last_vtokens=vtokens,
        )

        # Evict old snapshots to cap memory
        if len(self.token_snapshots) > 2000:
            cutoff = time.time() - 600  # 10 min window
            self.token_snapshots = {
                k: v for k, v in self.token_snapshots.items()
                if v.created_at > cutoff
            }

    def on_trade(self, msg: dict):
        """Process a buy/sell event and update the responsible wallet's profile."""
        mint = msg.get("mint")
        tx_type = msg.get("txType")
        trader = (msg.get("traderPublicKey")
                  or msg.get("user")
                  or msg.get("owner"))

        if not mint or not tx_type or not trader:
            return
        if tx_type not in ("buy", "sell"):
            return

        self.events_processed += 1
        now = time.time()

        # ── Resolve token context ──
        snap = self.token_snapshots.get(mint)
        time_after_create = (now - snap.created_at) if snap else -1.0

        # ── Parse trade numbers ──
        try:
            sol_amount = abs(float(msg.get("solAmount", 0) or 0))
            vsol = float(msg.get("vSolInBondingCurve", 0))
            vtokens = float(msg.get("vTokensInBondingCurve", 0))
            price = vsol / vtokens if vtokens > 0 else 0
        except (ValueError, TypeError):
            sol_amount = 0
            vsol = vtokens = price = 0

        # ── Compute market state at moment of trade ──
        bc_progress = 0.0
        unique_buyers = 0
        buy_volume = 0.0
        sell_ratio = 0.0

        if snap:
            denom = BC_GRADUATION_SOL - snap.initial_vsol
            if denom > 0:
                bc_progress = max(0, (vsol - snap.initial_vsol) / denom * 100.0)

            if tx_type == "buy":
                snap.unique_buyers.add(trader)
                snap.buy_volume += sol_amount
            else:
                snap.sell_volume += sol_amount

            snap.last_vsol = vsol
            snap.last_vtokens = vtokens

            unique_buyers = len(snap.unique_buyers)
            buy_volume = snap.buy_volume
            total_vol = snap.buy_volume + snap.sell_volume
            sell_ratio = snap.sell_volume / total_vol if total_vol > 0 else 0

        # ── Build observation ──
        obs = TradeObservation(
            timestamp=now,
            token_mint=mint,
            token_name=snap.name if snap else "Unknown",
            token_symbol=snap.symbol if snap else "???",
            tx_type=tx_type,
            sol_amount=sol_amount,
            time_after_create=time_after_create,
            bc_progress=bc_progress,
            unique_buyers=unique_buyers,
            volume_sol=buy_volume,
            sell_ratio=sell_ratio,
            price=price,
        )

        # ── Update wallet profile ──
        if trader not in self.profiles:
            self.profiles[trader] = WalletProfile(wallet=trader, first_seen=now)

        self.profiles[trader].add_trade(obs)
        self._dirty_wallets.add(trader)

        # ── Feed buys to Sybil detector ──
        if tx_type == "buy":
            self.sybil_detector.on_buy(trader, mint, now)

        # ── Periodic batch scoring ──
        if now - self.last_score_time >= self.score_interval:
            self._batch_score()
            self.last_score_time = now

        # ── Memory management ──
        if len(self.profiles) > self.max_profiles:
            self._evict_profiles()

    # ──────────────────────────────────────────────
    #  INTERNAL
    # ──────────────────────────────────────────────

    def _batch_score(self):
        """Re-score dirty wallets with sybil data, cluster bonus, and decay."""
        # Phase 1: Update sybil data for dirty wallets
        for wallet in list(self._dirty_wallets):
            profile = self.profiles.get(wallet)
            if profile and len(profile.buy_trades) >= self.min_trades:
                profile.sybil_score = self.sybil_detector.get_sybil_score(wallet)
                profile.sybil_cluster_id = self.sybil_detector.get_cluster_id(wallet)

        # Phase 2: Detect tight clusters and apply +15 bonus
        tight_wallets = self._detect_tight_clusters()
        for wallet, profile in self.profiles.items():
            profile.cluster_bonus = 15.0 if wallet in tight_wallets else 0.0

        # Phase 3: Compute composite scores for dirty wallets
        for wallet in list(self._dirty_wallets):
            profile = self.profiles.get(wallet)
            if profile and len(profile.buy_trades) >= self.min_trades:
                profile.compute_bot_score()
        self._dirty_wallets.clear()

        # Phase 4: Proactive decay
        self._decay_stale()

    def _detect_tight_clusters(self) -> Set[str]:
        """
        Identify wallets in tight Sybil clusters: 3+ wallets with
        buy sizes within 5% CV and coordinated activity (3+ distinct
        wallets trading within a 10s window).
        Returns wallet addresses qualifying for the +15 cluster bonus.
        """
        boosted: Set[str] = set()

        # Group profiles by sybil cluster
        cluster_groups: Dict[str, List[str]] = defaultdict(list)
        for wallet, profile in self.profiles.items():
            if profile.sybil_cluster_id:
                cluster_groups[profile.sybil_cluster_id].append(wallet)

        for cid, members in cluster_groups.items():
            if len(members) < 3:
                continue

            # Per-wallet average buy size (skip wallets with no buys)
            wallet_means = []
            for w in members:
                p = self.profiles.get(w)
                if not p or not p.buy_trades:
                    continue
                wallet_means.append(
                    statistics.mean([b.sol_amount for b in p.buy_trades])
                )

            if len(wallet_means) < 3:
                continue

            # Buy size variance: CV <= 0.05 (within 5%)
            mean_amt = statistics.mean(wallet_means)
            if mean_amt <= 0:
                continue
            std_amt = statistics.stdev(wallet_means) if len(wallet_means) > 1 else 0
            if std_amt / mean_amt > 0.05:
                continue

            # Timestamp proximity: 3+ trades from distinct wallets within 10s
            trade_events = []
            for w in members:
                p = self.profiles.get(w)
                if not p:
                    continue
                for b in p.buy_trades[-5:]:
                    trade_events.append((b.timestamp, w))

            trade_events.sort()
            tight = False
            for i in range(len(trade_events)):
                window_end = trade_events[i][0] + 10.0
                distinct = set()
                for j in range(i, len(trade_events)):
                    if trade_events[j][0] > window_end:
                        break
                    distinct.add(trade_events[j][1])
                if len(distinct) >= 3:
                    tight = True
                    break

            if tight:
                boosted.update(members)

        return boosted

    def _decay_stale(self):
        """Proactively evict wallets active >300s with <60% confidence."""
        now = time.time()
        to_remove = []
        for wallet, profile in self.profiles.items():
            if (now - profile.first_seen) <= ACTIVITY_DECAY_SEC:
                continue
            if len(profile.buy_trades) < self.min_trades:
                continue
            if profile._bot_score < ACTIVITY_DECAY_SCORE:
                to_remove.append(wallet)
        for w in to_remove:
            self.profiles.pop(w, None)
            self._dirty_wallets.discard(w)

    def _evict_profiles(self):
        """
        Stay under memory cap without timer-dropping high-confidence suspects.

        Primary eviction: inactive >30m AND bot score <25 (same as <25% confidence).
        Secondary: lowest scores first, skipping score ≥70 until unavoidable.
        """
        now = time.time()
        target = len(self.profiles) - int(self.max_profiles * 0.8)
        if target <= 0:
            return

        def last_active(p: WalletProfile) -> float:
            return p.trades[-1].timestamp if p.trades else p.first_seen

        def score_of(p: WalletProfile) -> float:
            if p._score_dirty and len(p.buy_trades) >= self.min_trades:
                p.compute_bot_score()
            return p._bot_score

        # 0) Activity decay: scored wallets active >300s with <60% confidence
        decayed = [
            w for w, p in list(self.profiles.items())
            if (now - p.first_seen) > ACTIVITY_DECAY_SEC
            and len(p.buy_trades) >= self.min_trades
            and score_of(p) < ACTIVITY_DECAY_SCORE
        ]
        for w in decayed:
            if target <= 0:
                break
            if self.profiles.pop(w, None):
                target -= 1

        # 1) Stale + low confidence only
        stale_low = [
            w for w, p in list(self.profiles.items())
            if (now - last_active(p)) > SUSPECT_INACTIVE_SEC
            and score_of(p) < SUSPECT_LOW_SCORE_CUTOFF
        ]
        for w in stale_low:
            if target <= 0:
                break
            if self.profiles.pop(w, None):
                target -= 1

        # 2) Pressure: lowest bot scores, protect high-confidence (≥70) until last
        ranked = [(w, p, score_of(p), last_active(p)) for w, p in self.profiles.items()]
        ranked.sort(key=lambda x: (x[2], -(now - x[3])))
        for w, p, sc, _ in ranked:
            if target <= 0:
                break
            if sc >= SUSPECT_HIGH_CONFIDENCE_MIN:
                continue
            if self.profiles.pop(w, None):
                target -= 1

        # 3) Still over cap — drop oldest-activity first (includes inactive high-conf)
        if target > 0:
            rest = [(w, last_active(p)) for w, p in self.profiles.items()]
            rest.sort(key=lambda x: x[1])
            for w, _ in rest[:target]:
                self.profiles.pop(w, None)

    # ──────────────────────────────────────────────
    #  QUERY API (for dashboard + pattern cracker)
    # ──────────────────────────────────────────────

    def get_suspected_bots(self, min_score: float = 50.0,
                           min_trades: int = 8,
                           limit: int = 50,
                           min_wallet_age_sec: float = 0.0) -> List[dict]:
        """Return suspected bots sorted by score (descending)."""
        now = time.time()
        results = []
        for wallet, profile in self.profiles.items():
            total_buy_vol = sum(t.sol_amount for t in profile.buy_trades)
            if (len(profile.buy_trades) >= min_trades
                    and profile._bot_score >= min_score
                    and total_buy_vol >= MIN_BUY_VOLUME_SOL):
                row = profile.to_dashboard_dict(
                    now=now, min_wallet_age_sec=min_wallet_age_sec,
                )
                if row is not None:
                    results.append(row)
        results.sort(key=lambda x: x["bot_score"], reverse=True)
        return results[:limit]

    def get_all_scored(self, limit: int = 100,
                       min_wallet_age_sec: float = 0.0) -> List[dict]:
        """Return all wallets that have enough data for display, sorted by score."""
        now = time.time()
        results = []
        for wallet, profile in self.profiles.items():
            total_buy_vol = sum(t.sol_amount for t in profile.buy_trades)
            if total_buy_vol < MIN_BUY_VOLUME_SOL:
                continue
            buys = len(profile.buy_trades)
            if buys >= self.min_trades:
                if profile._score_dirty:
                    profile.compute_bot_score()
                row = profile.to_dashboard_dict(
                    now=now, min_wallet_age_sec=min_wallet_age_sec,
                )
                if row is not None:
                    results.append(row)
            elif len(profile.trades) >= 3:
                row = profile.to_dashboard_dict(
                    now=now, min_wallet_age_sec=min_wallet_age_sec,
                )
                if row is not None:
                    results.append(row)
        results.sort(key=lambda x: x["bot_score"], reverse=True)
        return results[:limit]

    def get_profiler_stats(self) -> dict:
        """Overall profiler health/progress statistics for the dashboard."""
        now = time.time()
        uptime = now - self.start_time

        scored = [p for p in self.profiles.values()
                  if len(p.buy_trades) >= self.min_trades]
        suspected = [p for p in scored if p._bot_score >= 60]
        confirmed = [p for p in scored if p._bot_score >= 80]

        eps = self.events_processed / uptime if uptime > 0 else 0

        sybil_clusters = self.sybil_detector.get_clusters()

        return {
            "wallets_tracked": len(self.profiles),
            "wallets_scored": len(scored),
            "bots_suspected": len(suspected),
            "bots_confirmed": len(confirmed),
            "events_processed": self.events_processed,
            "events_per_sec": round(eps, 1),
            "uptime_sec": round(uptime),
            "tokens_tracked": len(self.token_snapshots),
            "sybil_clusters": len(sybil_clusters),
            "sybil_wallets": sum(c["size"] for c in sybil_clusters),
        }

    def get_profile(self, wallet: str) -> Optional[WalletProfile]:
        """Get a specific wallet's full profile (for pattern cracker in Stage 2)."""
        return self.profiles.get(wallet)

    def get_sybil_clusters(self) -> List[dict]:
        """Return detected Sybil clusters for dashboard display."""
        if not hasattr(self, 'sybil_detector'):
            return []
        return self.sybil_detector.get_clusters()


# =====================================================
#  FUNDING TRACKER — Detect wallets funded by same source
# =====================================================

class FundingTracker:
    """
    Tracks which wallets were funded by the same parent wallet.
    Wallets sharing a funding source within a time window are likely
    Sybil wallets controlled by the same operator.

    Works asynchronously: wallets are queued for RPC lookup, and
    results are recorded as they arrive. Synthetic co-occurrence
    edges are injected into the SybilDetector's union-find.
    """

    def __init__(self, max_tracked: int = 10000):
        from collections import deque
        # wallet → funding source wallet address
        self._source_map: Dict[str, str] = {}
        # source wallet → set of wallets it funded
        self._source_groups: Dict[str, Set[str]] = defaultdict(set)
        # Wallets pending RPC lookup
        self._pending_lookups: 'deque' = deque(maxlen=5000)
        # Wallets we've already queued (avoid re-queuing)
        self._queued: Set[str] = set()
        self._max_tracked = max_tracked

    def queue_lookup(self, wallet: str):
        """Queue a wallet for funding source RPC lookup (deduped)."""
        if wallet not in self._queued and wallet not in self._source_map:
            self._queued.add(wallet)
            self._pending_lookups.append(wallet)

    def record_source(self, wallet: str, source: str):
        """Record that `wallet` was funded by `source`."""
        if len(self._source_map) >= self._max_tracked:
            return
        self._source_map[wallet] = source
        self._source_groups[source].add(wallet)

    def get_co_funded(self, wallet: str) -> Set[str]:
        """Return wallets sharing the same funding source as `wallet`."""
        source = self._source_map.get(wallet)
        if not source:
            return set()
        return self._source_groups.get(source, set()) - {wallet}

    def get_funding_edges(self) -> Dict[Tuple[str, str], Set[str]]:
        """
        Generate synthetic co-occurrence edges for co-funded wallet pairs.
        Returns edges with weight=10 synthetic mints to exceed min_co_tokens threshold.
        """
        edges: Dict[Tuple[str, str], Set[str]] = {}
        for source, wallets in self._source_groups.items():
            if len(wallets) < 2:
                continue
            wallet_list = sorted(wallets)
            for i in range(len(wallet_list)):
                for j in range(i + 1, len(wallet_list)):
                    edge = (wallet_list[i], wallet_list[j])
                    # 10 synthetic mints to strongly exceed min_co_tokens=3
                    edges[edge] = {f"__funded_{k}" for k in range(10)}
        return edges

    def pop_pending(self, batch_size: int = 5) -> List[str]:
        """Pop up to batch_size wallets from the pending lookup queue."""
        batch = []
        for _ in range(min(batch_size, len(self._pending_lookups))):
            batch.append(self._pending_lookups.popleft())
        return batch

    def stats(self) -> dict:
        """Return tracker stats for dashboard."""
        return {
            "tracked_wallets": len(self._source_map),
            "source_groups": len(self._source_groups),
            "pending_lookups": len(self._pending_lookups),
            "co_funded_pairs": sum(
                len(w) * (len(w) - 1) // 2
                for w in self._source_groups.values() if len(w) >= 2
            ),
        }


# =====================================================
#  SYBIL DETECTOR — Co-occurrence Graph Analysis
# =====================================================

class SybilDetector:
    """
    Detects Sybil clusters (multi-wallet bots) by tracking which wallets
    co-buy the same tokens within tight time windows.

    Logic: If wallet A and wallet B both buy the same token within N seconds
    of each other across M different tokens, they are likely controlled by
    the same operator. The more co-occurrences, the higher the sybil score.

    This runs passively on the same trade stream as the profiler.
    """

    def __init__(self, co_window_sec: float = 5.0, min_co_tokens: int = 3,
                 max_edges: int = 50000):
        self.co_window = co_window_sec
        self.min_co_tokens = min_co_tokens
        self.max_edges = max_edges

        # Per-token: list of (wallet, timestamp) for recent buys
        self._token_buyers: Dict[str, List[Tuple[str, float]]] = defaultdict(list)

        # Co-occurrence edge weights: (walletA, walletB) → (set of shared mints, last_updated)
        self._edges: Dict[Tuple[str, str], Tuple[Set[str], float]] = {}

        # Cluster assignments: wallet → cluster_id
        self._clusters: Dict[str, str] = {}

        self._last_cluster_time: float = 0.0

        # Funding source tracker — detects wallets funded by the same parent
        self.funding = FundingTracker()
        self._seen_wallets: Set[str] = set()

    def on_buy(self, wallet: str, mint: str, timestamp: float):
        """Record a buy and check for co-occurrences with recent buyers."""
        buyers = self._token_buyers[mint]

        # Find wallets that bought this token within the co-occurrence window
        for other_wallet, other_time in buyers:
            if other_wallet == wallet:
                continue
            if abs(timestamp - other_time) <= self.co_window:
                # Record co-occurrence edge (sorted pair for consistency)
                edge = tuple(sorted([wallet, other_wallet]))
                if edge in self._edges:
                    self._edges[edge][0].add(mint)
                    self._edges[edge] = (self._edges[edge][0], timestamp)
                else:
                    self._edges[edge] = ({mint}, timestamp)

        # Add this buy to the token's buyer list
        buyers.append((wallet, timestamp))

        # Queue first-time wallets for funding source lookup
        if wallet not in self._seen_wallets:
            self._seen_wallets.add(wallet)
            self.funding.queue_lookup(wallet)

        # Trim old entries (keep last 60 seconds)
        cutoff = timestamp - 60.0
        trimmed = [(w, t) for w, t in buyers if t > cutoff]
        if trimmed:
            self._token_buyers[mint] = trimmed
        else:
            # Remove empty key to prevent unbounded dict growth
            self._token_buyers.pop(mint, None)

        # Periodic clustering (every 10 seconds)
        if timestamp - self._last_cluster_time >= 10.0:
            self._rebuild_clusters()
            self._last_cluster_time = timestamp

        # Memory cap on edges
        if len(self._edges) > self.max_edges:
            self._prune_edges()

    def _rebuild_clusters(self):
        """Build clusters from co-occurrence + funding edges using union-find."""
        # Prune empty _token_buyers keys accumulated between trims
        empty_mints = [m for m, b in self._token_buyers.items() if not b]
        for m in empty_mints:
            del self._token_buyers[m]

        # Cap _seen_wallets to prevent unbounded growth
        if len(self._seen_wallets) > 20000:
            active_wallets = set()
            for (w1, w2) in self._edges:
                active_wallets.add(w1)
                active_wallets.add(w2)
            active_wallets.update(self._clusters.keys())
            self._seen_wallets = active_wallets

        # Merge funding-source edges into co-occurrence edges
        funding_edges = self.funding.get_funding_edges()
        # Extract mints from timestamped edges: (mints, ts) → mints
        merged_edges: Dict[Tuple[str, str], Set[str]] = {
            edge: mints for edge, (mints, _ts) in self._edges.items()
        }
        for edge, mints in funding_edges.items():
            if edge in merged_edges:
                merged_edges[edge] = merged_edges[edge] | mints
            else:
                merged_edges[edge] = mints

        # Only consider edges with enough shared tokens
        strong_edges = {
            edge: mints for edge, mints in merged_edges.items()
            if len(mints) >= self.min_co_tokens
        }

        if not strong_edges:
            self._clusters.clear()
            return

        # Union-Find
        parent: Dict[str, str] = {}

        def find(x):
            if x not in parent:
                parent[x] = x
            while parent[x] != x:
                parent[x] = parent[parent[x]]
                x = parent[x]
            return x

        def union(a, b):
            ra, rb = find(a), find(b)
            if ra != rb:
                parent[ra] = rb

        for (w1, w2), mints in strong_edges.items():
            union(w1, w2)

        # Build cluster map
        clusters: Dict[str, List[str]] = defaultdict(list)
        for wallet in parent:
            root = find(wallet)
            clusters[root].append(wallet)

        # Assign cluster IDs and sybil scores
        self._clusters.clear()
        for root, members in clusters.items():
            if len(members) < 2:
                continue
            cluster_id = root[:8]
            for wallet in members:
                self._clusters[wallet] = cluster_id

    def _prune_edges(self):
        """Prune stale and weak edges to stay under memory cap."""
        import time as _time
        now = _time.time()
        stale_cutoff = now - 3600  # 1 hour TTL for edges

        # Phase 1: Remove stale edges regardless of weight
        stale = [e for e, (_mints, ts) in self._edges.items() if ts < stale_cutoff]
        for e in stale:
            del self._edges[e]

        # Phase 2: If still over cap, remove weakest
        if len(self._edges) > self.max_edges:
            sorted_edges = sorted(self._edges.items(), key=lambda x: len(x[1][0]))
            to_remove = len(self._edges) - int(self.max_edges * 0.7)
            for edge, _ in sorted_edges[:to_remove]:
                del self._edges[edge]

    def get_sybil_score(self, wallet: str) -> float:
        """Get a wallet's sybil score (0-100), including funding source analysis."""
        co_funded = self.funding.get_co_funded(wallet)
        funding_bonus = min(30, len(co_funded) * 10)

        if wallet not in self._clusters:
            return min(100, funding_bonus)

        cluster_id = self._clusters[wallet]
        # Count cluster members
        cluster_size = sum(
            1 for w, c in self._clusters.items() if c == cluster_id
        )

        # Find max edge weight for this wallet
        max_shared = 0
        for (w1, w2), (mints, _ts) in self._edges.items():
            if wallet in (w1, w2):
                max_shared = max(max_shared, len(mints))

        # Score: cluster size + co-occurrence depth + funding bonus
        size_score = min(50, (cluster_size - 1) * 15)
        depth_score = min(50, max(0, (max_shared - 2)) * 12)
        return min(100, size_score + depth_score + funding_bonus)

    def get_cluster_id(self, wallet: str) -> Optional[str]:
        """Get the cluster ID for a wallet, or None."""
        return self._clusters.get(wallet)

    def get_clusters(self) -> List[dict]:
        """Return all detected clusters for dashboard display."""
        if not self._clusters:
            return []

        # Group wallets by cluster
        groups: Dict[str, List[str]] = defaultdict(list)
        for wallet, cid in self._clusters.items():
            groups[cid].append(wallet)

        results = []
        for cid, members in groups.items():
            if len(members) < 2:
                continue

            # Find max co-occurrence depth in this cluster
            max_shared = 0
            for (w1, w2), (mints, _ts) in self._edges.items():
                if w1 in members or w2 in members:
                    max_shared = max(max_shared, len(mints))

            results.append({
                "cluster_id": cid,
                "wallets": [w[:8] + "..." + w[-4:] for w in members],
                "size": len(members),
                "max_co_tokens": max_shared,
                "score": self.get_sybil_score(members[0]),
            })

        results.sort(key=lambda x: x["size"], reverse=True)
        return results
