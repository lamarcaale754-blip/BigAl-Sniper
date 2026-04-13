"""
PATTERN CRACKER — Stage 2: The Brain
Reverse-engineers a target bot's entry filters by analyzing its trade history.
Uses Random Forest classification to identify the exact volume, buyer count,
bonding curve %, and timing thresholds that trigger the bot to buy.

Input:  WalletProfile from Stage 1 (profiler)
Output: BotSignature dict consumed by Stage 3 (honeypot planner)
"""

import time
import statistics
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

try:
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.tree import export_text
    import numpy as np
    HAS_ML = True
except ImportError:
    HAS_ML = False


@dataclass
class BotSignature:
    """Reverse-engineered bot filter thresholds."""
    wallet: str
    bot_score: float

    # Entry triggers (what conditions make this bot buy)
    min_volume_sol: float = 0.0       # min buy volume before bot enters
    max_volume_sol: float = 10.0      # max volume (bot stops buying above this)
    min_buyers: int = 0               # min unique buyer count
    max_buyers: int = 50
    min_bc_pct: float = 0.0           # bonding curve range
    max_bc_pct: float = 100.0
    avg_entry_time_sec: float = 0.0   # avg seconds after token creation
    min_entry_time_sec: float = 0.0
    max_entry_time_sec: float = 60.0
    avg_sol_amount: float = 0.0       # typical buy size
    confidence: float = 0.0           # 0-1, how confident we are in these filters
    feature_importance: Dict[str, float] = field(default_factory=dict)

    # Solved timestamp
    solved_at: float = 0.0

    def to_dict(self) -> dict:
        return {
            "wallet": self.wallet[:8] + "..." + self.wallet[-4:],
            "wallet_full": self.wallet,
            "bot_score": self.bot_score,
            "min_volume_sol": round(self.min_volume_sol, 4),
            "max_volume_sol": round(self.max_volume_sol, 4),
            "min_buyers": self.min_buyers,
            "max_buyers": self.max_buyers,
            "min_bc_pct": round(self.min_bc_pct, 2),
            "max_bc_pct": round(self.max_bc_pct, 2),
            "avg_entry_time": round(self.avg_entry_time_sec, 1),
            "entry_window": [round(self.min_entry_time_sec, 1), round(self.max_entry_time_sec, 1)],
            "avg_sol_amount": round(self.avg_sol_amount, 4),
            "confidence": round(self.confidence, 2),
            "feature_importance": {k: round(v, 3) for k, v in self.feature_importance.items()},
            "solved_at": self.solved_at,
        }


class PatternCracker:
    """
    Analyzes a target bot's trade history against all observed tokens
    to determine what market conditions trigger it to buy.

    Approach:
    1. Collect all tokens the profiler observed
    2. Label: did the target bot buy this token? (1/0)
    3. Features: volume, buyers, BC%, timing at moment of first observed buy
    4. Train RandomForest → extract thresholds from feature importances + ranges
    """

    def __init__(self):
        self.solved_signatures: Dict[str, BotSignature] = {}
        self._last_solve_attempt: Dict[str, float] = {}

    def crack_bot(self, wallet: str, profiler) -> Optional[BotSignature]:
        """
        Reverse-engineer a bot's entry filters.

        Args:
            wallet: Full wallet address of the suspected bot
            profiler: WalletProfiler instance with observed data

        Returns:
            BotSignature if enough data, None otherwise
        """
        profile = profiler.get_profile(wallet)
        if not profile or len(profile.buy_trades) < 10:
            return None

        # Rate-limit solve attempts (expensive operation)
        now = time.time()
        last = self._last_solve_attempt.get(wallet, 0)
        if now - last < 30:  # max once per 30s per wallet
            return self.solved_signatures.get(wallet)
        self._last_solve_attempt[wallet] = now

        # ── Build dataset: all tokens observed vs which ones this bot bought ──
        bot_tokens = set(t.token_mint for t in profile.buy_trades)
        all_tokens = profiler.token_snapshots

        if len(all_tokens) < 20 or len(bot_tokens) < 5:
            return None

        # Build feature matrix
        rows = []
        labels = []

        for mint, snap in all_tokens.items():
            # Compute features at moment the token was ~3 seconds old
            total_vol = snap.buy_volume
            n_buyers = len(snap.unique_buyers)
            denom = 85.0 - snap.initial_vsol
            bc_pct = max(0, (snap.last_vsol - snap.initial_vsol) / denom * 100) if denom > 0 else 0

            rows.append([total_vol, n_buyers, bc_pct])
            labels.append(1 if mint in bot_tokens else 0)

        if sum(labels) < 5 or sum(labels) == len(labels):
            return None  # not enough positive/negative examples

        # ── Solve with heuristics (always available) ──
        sig = self._solve_heuristic(wallet, profile, rows, labels)

        # ── Upgrade with ML if available ──
        if HAS_ML and len(rows) >= 30:
            ml_sig = self._solve_ml(wallet, profile, rows, labels)
            if ml_sig and ml_sig.confidence > sig.confidence:
                sig = ml_sig

        sig.solved_at = now
        self.solved_signatures[wallet] = sig
        return sig

    def _solve_heuristic(self, wallet, profile, rows, labels) -> BotSignature:
        """Statistical approach — works without scikit-learn."""
        buys = profile.buy_trades

        # Extract stats from bot's actual trades
        volumes = [r[0] for r, l in zip(rows, labels) if l == 1]
        buyers = [r[1] for r, l in zip(rows, labels) if l == 1]
        bc_pcts = [r[2] for r, l in zip(rows, labels) if l == 1]
        timings = [t.time_after_create for t in buys if t.time_after_create > 0]
        amounts = [t.sol_amount for t in buys if t.sol_amount > 0]

        sig = BotSignature(
            wallet=wallet,
            bot_score=profile._bot_score,
        )

        if volumes:
            sig.min_volume_sol = max(0, statistics.mean(volumes) - statistics.stdev(volumes)) if len(volumes) > 1 else min(volumes)
            sig.max_volume_sol = statistics.mean(volumes) + 2 * (statistics.stdev(volumes) if len(volumes) > 1 else 0)
        if buyers:
            sig.min_buyers = max(0, int(min(buyers)))
            sig.max_buyers = int(max(buyers)) + 2
        if bc_pcts:
            sig.min_bc_pct = max(0, min(bc_pcts) - 1)
            sig.max_bc_pct = max(bc_pcts) + 2
        if timings:
            sig.avg_entry_time_sec = statistics.mean(timings)
            sig.min_entry_time_sec = max(0, min(timings) - 1)
            sig.max_entry_time_sec = max(timings) + 2
        if amounts:
            sig.avg_sol_amount = statistics.mean(amounts)

        # Confidence based on data quantity
        n_positive = sum(labels)
        n_total = len(labels)
        sig.confidence = min(0.7, n_positive / max(1, n_total) * 2 + len(buys) / 100)

        return sig

    def _solve_ml(self, wallet, profile, rows, labels) -> Optional[BotSignature]:
        """ML approach — uses RandomForest for non-linear filter detection."""
        if not HAS_ML:
            return None

        X = np.array(rows)
        y = np.array(labels)

        try:
            model = RandomForestClassifier(
                n_estimators=50,
                max_depth=5,
                min_samples_leaf=3,
                random_state=42,
            )
            model.fit(X, y)

            # Feature importance
            feature_names = ["volume", "buyers", "bc_pct"]
            importances = dict(zip(feature_names, model.feature_importances_))

            # Extract ranges from positive-class samples
            pos_mask = y == 1
            X_pos = X[pos_mask]

            buys = profile.buy_trades
            timings = [t.time_after_create for t in buys if t.time_after_create > 0]
            amounts = [t.sol_amount for t in buys if t.sol_amount > 0]

            sig = BotSignature(
                wallet=wallet,
                bot_score=profile._bot_score,
                min_volume_sol=float(np.percentile(X_pos[:, 0], 10)),
                max_volume_sol=float(np.percentile(X_pos[:, 0], 95)),
                min_buyers=int(np.percentile(X_pos[:, 1], 10)),
                max_buyers=int(np.percentile(X_pos[:, 1], 95)) + 1,
                min_bc_pct=float(np.percentile(X_pos[:, 2], 10)),
                max_bc_pct=float(np.percentile(X_pos[:, 2], 95)),
                feature_importance=importances,
            )

            if timings:
                sig.avg_entry_time_sec = statistics.mean(timings)
                sig.min_entry_time_sec = max(0, np.percentile(timings, 5))
                sig.max_entry_time_sec = np.percentile(timings, 95)
            if amounts:
                sig.avg_sol_amount = statistics.mean(amounts)

            # Confidence = model accuracy on training data (rough proxy)
            accuracy = model.score(X, y)
            sig.confidence = min(0.95, accuracy)

            return sig

        except Exception as e:
            print(f"[PatternCracker] ML solve failed: {e}")
            return None

    def get_solved_signatures(self) -> List[dict]:
        """Return all solved bot signatures for dashboard."""
        return [sig.to_dict() for sig in self.solved_signatures.values()]

    def get_signature(self, wallet: str) -> Optional[BotSignature]:
        """Get a specific bot's solved signature."""
        return self.solved_signatures.get(wallet)
