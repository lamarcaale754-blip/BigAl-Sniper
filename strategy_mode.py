"""
Strategy Mode — Parameter selection layer (independent of Trading Mode).

Three modes:
  MANUAL    — all values from dashboard/config, AdaptiveState ignored
  ADAPTIVE  — all values computed from AdaptiveState rolling data
  HYBRID    — adaptive defaults with per-parameter manual overrides

Single source of truth: AdaptiveState holds all rolling data.
Single gatekeeper: resolve_parameters() decides final values.
"""

import time
import statistics
from enum import Enum
from collections import deque
from dataclasses import dataclass, field
from typing import Optional


class StrategyMode(Enum):
    MANUAL = "manual"
    ADAPTIVE = "adaptive"
    HYBRID = "hybrid"


# ─────────────────────────────────────────────
#  ADAPTIVE STATE — single source of truth
# ─────────────────────────────────────────────

@dataclass
class AdaptiveState:
    """Centralized rolling data for all adaptive calculations.

    Initialized ONCE inside the main bot, persisted across runtime.
    All adaptive logic lives here — no duplication elsewhere.
    """

    # Rolling liquidity from recent successful launches (vSOL at entry)
    liquidity_window: deque = field(default_factory=lambda: deque(maxlen=50))

    # vSOL snapshots with timestamps for slope calculation
    vsol_samples: deque = field(default_factory=lambda: deque(maxlen=30))

    # Recent prioritization fees in lamports
    fee_samples: deque = field(default_factory=lambda: deque(maxlen=100))

    # Circuit breaker (structured for extension)
    circuit_open: bool = False
    circuit_open_until: float = 0.0

    # Minimum sample counts before adaptive values are trusted
    MIN_LIQUIDITY_SAMPLES: int = field(default=5, repr=False)
    MIN_FEE_SAMPLES: int = field(default=3, repr=False)
    MIN_VSOL_SAMPLES: int = field(default=3, repr=False)

    # ── Data ingestion ───────────────────────

    def update_liquidity(self, vsol: float):
        """Record vSOL from a token launch / successful entry."""
        if vsol > 0:
            self.liquidity_window.append(vsol)

    def update_vsol_slope(self, vsol: float, timestamp: Optional[float] = None):
        """Record a vSOL sample with timestamp for slope calculation."""
        ts = timestamp if timestamp is not None else time.time()
        self.vsol_samples.append((ts, vsol))

    def update_fees(self, fee_lamports: int):
        """Record a prioritization fee sample (in lamports)."""
        if fee_lamports >= 0:
            self.fee_samples.append(fee_lamports)

    # ── Circuit breaker ──────────────────────

    def trip_circuit(self, duration_sec: float = 30.0):
        """Open circuit breaker for a duration."""
        self.circuit_open = True
        self.circuit_open_until = time.time() + duration_sec

    def check_circuit(self) -> bool:
        """Returns True if circuit is open (trading should pause)."""
        if self.circuit_open and time.time() >= self.circuit_open_until:
            self.circuit_open = False
        return self.circuit_open

    # ── Data readiness checks ────────────────

    @property
    def has_liquidity_data(self) -> bool:
        return len(self.liquidity_window) >= self.MIN_LIQUIDITY_SAMPLES

    @property
    def has_fee_data(self) -> bool:
        return len(self.fee_samples) >= self.MIN_FEE_SAMPLES

    @property
    def has_vsol_data(self) -> bool:
        return len(self.vsol_samples) >= self.MIN_VSOL_SAMPLES

    # ── Computed adaptive values ─────────────

    @property
    def liquidity_median(self) -> Optional[float]:
        """Median vSOL across recent successful launches."""
        if not self.has_liquidity_data:
            return None
        return statistics.median(self.liquidity_window)

    @property
    def fee_p75(self) -> Optional[float]:
        """75th percentile of recent priority fees, in SOL."""
        if not self.has_fee_data:
            return None
        sorted_fees = sorted(self.fee_samples)
        idx = int(len(sorted_fees) * 0.75)
        return sorted_fees[min(idx, len(sorted_fees) - 1)] / 1e9

    @property
    def vsol_slope(self) -> Optional[float]:
        """Rate of change of vSOL (SOL per second) across tracked pools."""
        if not self.has_vsol_data:
            return None
        samples = list(self.vsol_samples)
        dt = samples[-1][0] - samples[0][0]
        if dt <= 0:
            return 0.0
        dv = samples[-1][1] - samples[0][1]
        return dv / dt

    @property
    def adaptive_tip(self) -> Optional[float]:
        """Adaptive priority fee from fee distribution."""
        return self.fee_p75

    @property
    def adaptive_min_liquidity(self) -> Optional[float]:
        """Adaptive min liquidity: 60% of rolling median.

        Filters obvious duds without being so tight it misses early movers.
        """
        med = self.liquidity_median
        if med is None:
            return None
        return max(0.1, med * 0.6)

    @property
    def adaptive_entry_progress(self) -> Optional[float]:
        """Adaptive BC progress threshold based on vSOL slope.

        Positive slope (pools filling) → slightly higher min BC to avoid
        entering too early. Flat/negative → keep low.
        """
        slope = self.vsol_slope
        if slope is None:
            return None
        return min(3.0, max(0.5, 0.5 + slope * 2.0))


# ─────────────────────────────────────────────
#  PARAMETER RESOLVER — the single gatekeeper
# ─────────────────────────────────────────────

def resolve_parameters(config: dict, adaptive_state: AdaptiveState) -> dict:
    """Resolve final trading parameters based on strategy mode.

    This is the ONLY place where final trading parameters are decided.

    Returns:
        dict with keys:
            tip: float            — priority fee in SOL
            min_liquidity: float  — minimum pool vSOL for entry
            entry_progress: float — minimum bonding curve progress %
    """
    mode = config.get("strategy_mode", StrategyMode.MANUAL)
    if isinstance(mode, str):
        mode = StrategyMode(mode)

    # Manual values (always available as fallback)
    manual = {
        "tip": config.get("manual_tip", 0.002),
        "min_liquidity": config.get("manual_min_liquidity",
                                    config.get("min_pool", 0.3)),
        "entry_progress": config.get("manual_entry_progress",
                                     config.get("bc_min", 0.5)),
    }

    if mode == StrategyMode.MANUAL:
        return manual

    # Adaptive values — fall back to manual if insufficient data
    adaptive = {
        "tip": (adaptive_state.adaptive_tip
                if adaptive_state.adaptive_tip is not None
                else manual["tip"]),
        "min_liquidity": (adaptive_state.adaptive_min_liquidity
                          if adaptive_state.adaptive_min_liquidity is not None
                          else manual["min_liquidity"]),
        "entry_progress": (adaptive_state.adaptive_entry_progress
                           if adaptive_state.adaptive_entry_progress is not None
                           else manual["entry_progress"]),
    }

    if mode == StrategyMode.ADAPTIVE:
        return adaptive

    # HYBRID: start with adaptive, override individual params if flagged
    resolved = dict(adaptive)
    if config.get("override_tip", False):
        resolved["tip"] = manual["tip"]
    if config.get("override_liquidity", False):
        resolved["min_liquidity"] = manual["min_liquidity"]
    if config.get("override_entry", False):
        resolved["entry_progress"] = manual["entry_progress"]

    return resolved
