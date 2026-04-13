#!/usr/bin/env python3
"""
BIGAL SNIPER — CUSTOMIZABLE EDITION
Real-time parameter tuning via dashboard sliders.
All trading parameters adjustable without restart.
Built-in HTTP server serves dashboard as a local app.
Presets: Aggressive, Conservative, Balanced, Scalper + custom save/load.
"""

import asyncio
import json
import time
import csv
import os
import sys
import subprocess
import signal
import shutil
import webbrowser
import threading
from http.server import HTTPServer, SimpleHTTPRequestHandler
from dataclasses import dataclass, field
from typing import Dict, Optional, List, Set
from datetime import datetime

import websockets
import aiohttp
import zmq

from solders.keypair import Keypair
from solders.transaction import VersionedTransaction
from solana.rpc.api import Client
from solana.rpc.types import TxOpts
from solana.rpc.commitment import Processed
from base58 import b58decode, b58encode

from executor import (ExecutorBackend, PaperExecutor, LegacyExecutor, ZmqExecutor,
                      resolved_buy, resolved_sell)
from strategy_mode import StrategyMode, AdaptiveState, resolve_parameters
from wallet_profiler import WalletProfiler
from rug_orchestrator import RugOrchestrator
from wallet_developer import WalletDeveloper

# Directory where this script lives (for resolving relative paths)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))


# =====================================================
#  PROCESS MANAGER — Auto-spawn Rust executor & sidecar
# =====================================================

class ProcessManager:
    """Manages Rust executor and Node.js gRPC sidecar as child processes."""

    def __init__(self):
        self.rust_proc: Optional[subprocess.Popen] = None
        self.sidecar_proc: Optional[subprocess.Popen] = None
        self._rust_log = None
        self._sidecar_log = None

    # ── CPU pinning (Linux only) ─────────────────────────

    @staticmethod
    def _pin_to_cpu(proc: subprocess.Popen, cpu: int):
        """Pin a child process to a specific CPU core (Linux only)."""
        if sys.platform != "linux":
            return
        try:
            os.sched_setaffinity(proc.pid, {cpu})
            print(f"  📌 PID {proc.pid} pinned to CPU {cpu}")
        except (OSError, AttributeError):
            pass  # not available on this kernel or permissions insufficient

    def _available_cpus(self) -> list:
        """Return list of available CPU cores."""
        try:
            return sorted(os.sched_getaffinity(0))
        except (OSError, AttributeError):
            return []

    # ── Rust executor ──────────────────────────────────

    def find_rust_binary(self) -> Optional[str]:
        """Locate the Rust executor binary relative to script dir."""
        if sys.platform == "win32":
            name = "bigal-executor.exe"
        else:
            name = "bigal-executor"
        path = os.path.join(SCRIPT_DIR, "rust", "bigal-executor", "target", "release", name)
        return path if os.path.isfile(path) else None

    def start_rust(self, env: dict) -> str:
        """Spawn the Rust executor. Returns status message."""
        if self.rust_proc and self.rust_proc.poll() is None:
            return "Rust executor already running"

        binary = self.find_rust_binary()
        if not binary:
            return (
                "Rust binary not found. Run:\n"
                "  cd rust/bigal-executor && cargo build --release"
            )

        log_path = os.path.join(SCRIPT_DIR, "rust_executor.log")
        self._rust_log = open(log_path, "w")
        env = dict(env)
        if sys.platform == "linux":
            # Match Python ZmqBridge IPC under /tmp/ (Rust executor reads these on Unix)
            env.setdefault("ZMQ_SIGNAL_IPC", ZMQ_SIGNAL_IPC)
            env.setdefault("ZMQ_RESULT_IPC", ZMQ_RESULT_IPC)
        self.rust_proc = subprocess.Popen(
            [binary],
            env=env,
            stdout=self._rust_log,
            stderr=subprocess.STDOUT,
            cwd=os.path.join(SCRIPT_DIR, "rust", "bigal-executor"),
        )

        # Pin Rust executor: logical core 3 on a 4-core box (AWS us-east-1 c6g.xlarge pattern)
        cpus = self._available_cpus()
        if len(cpus) >= 4:
            self._pin_to_cpu(self.rust_proc, cpus[3])

        return "ok"

    def is_rust_alive(self) -> bool:
        return self.rust_proc is not None and self.rust_proc.poll() is None

    # ── Node.js gRPC sidecar ──────────────────────────

    def find_node(self) -> Optional[str]:
        """Check if node is available on PATH."""
        return shutil.which("node")

    def start_sidecar(self, helius_api_key: str) -> str:
        """Spawn the Node.js gRPC sidecar. Returns status message."""
        if self.sidecar_proc and self.sidecar_proc.poll() is None:
            return "Sidecar already running"

        sidecar_dir = os.path.join(SCRIPT_DIR, "sidecar")
        script_path = os.path.join(sidecar_dir, "sidecar_grpc.js")
        if not os.path.isfile(script_path):
            return "Sidecar script not found at sidecar/sidecar_grpc.js"

        node_bin = self.find_node()
        if not node_bin:
            return "node not found on PATH — install Node.js to enable gRPC sidecar"

        modules_dir = os.path.join(sidecar_dir, "node_modules")
        if not os.path.isdir(modules_dir):
            return "sidecar/node_modules missing — run: cd sidecar && npm install"

        env = os.environ.copy()
        env["HELIUS_API_KEY"] = helius_api_key

        log_path = os.path.join(SCRIPT_DIR, "sidecar.log")
        self._sidecar_log = open(log_path, "w")
        self.sidecar_proc = subprocess.Popen(
            [node_bin, script_path],
            env=env,
            stdout=self._sidecar_log,
            stderr=subprocess.STDOUT,
            cwd=sidecar_dir,
        )

        # Pin sidecar to core 1 — reserve core 2 for Python main, core 3 for Rust
        cpus = self._available_cpus()
        if len(cpus) >= 4:
            self._pin_to_cpu(self.sidecar_proc, cpus[1])

        return "ok"

    def pin_python_main(self):
        """Pin the strategy process to core 2 on 4+ core Linux (pairs with Rust on core 3)."""
        if sys.platform != "linux":
            return
        cpus = self._available_cpus()
        if len(cpus) >= 4:
            try:
                os.sched_setaffinity(0, {cpus[2]})
                print(f"📌 Python main pinned to CPU {cpus[2]}")
            except (OSError, AttributeError):
                pass

    def is_sidecar_alive(self) -> bool:
        return self.sidecar_proc is not None and self.sidecar_proc.poll() is None

    # ── Shutdown ───────────────────────────────────────

    def stop_all(self):
        """Gracefully terminate all child processes."""
        for label, proc in [("Rust", self.rust_proc), ("Sidecar", self.sidecar_proc)]:
            if proc and proc.poll() is None:
                try:
                    proc.terminate()
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=2)
                except Exception:
                    pass
                print(f"  ✓ {label} process stopped")
        self.rust_proc = None
        self.sidecar_proc = None
        for log in [self._rust_log, self._sidecar_log]:
            if log:
                try:
                    log.close()
                except Exception:
                    pass
        self._rust_log = None
        self._sidecar_log = None


proc_mgr = ProcessManager()

# Serialize dashboard mode changes — concurrent mode_select (multiple tabs) can
# unlink ZMQ IPC while another bridge is bound, breaking Rust PULL ↔ Python PUSH.
mode_select_lock = asyncio.Lock()


# =====================================================
#  TTL CACHE — bounded memory with time-based eviction
# =====================================================

class TTLCache:
    """LRU + TTL cache. Entries expire after ttl_seconds; oldest evicted at maxsize."""

    def __init__(self, ttl_seconds: float, maxsize: int = 10000):
        from collections import OrderedDict
        self._cache: 'OrderedDict[str, tuple]' = OrderedDict()
        self._ttl = ttl_seconds
        self._maxsize = maxsize

    def get(self, key, default=None):
        entry = self._cache.get(key)
        if entry is None:
            return default
        value, ts = entry
        if time.time() - ts > self._ttl:
            del self._cache[key]
            return default
        return value

    def set(self, key, value):
        self._cache[key] = (value, time.time())
        self._cache.move_to_end(key)
        if len(self._cache) > self._maxsize:
            self._cache.popitem(last=False)

    def contains(self, key) -> bool:
        return self.get(key) is not None

    def __len__(self):
        return len(self._cache)

    def prune(self) -> int:
        """Remove all expired entries. Returns count of pruned entries."""
        now = time.time()
        expired = [k for k, (v, ts) in self._cache.items() if now - ts > self._ttl]
        for k in expired:
            del self._cache[k]
        return len(expired)

# =====================================================
#  MUTABLE CONFIG — updated in real-time from dashboard
# =====================================================

CONFIG = {
    # Exit
    "tp1": 12.0, "tp2": 25.0, "sl": 15.0,
    "max_hold": 60.0,
    "trail_min": 10.0, "trail_peak": 5.0,
    # Entry
    "entry_buyers": 4, "entry_vol": 0.8,
    "sell_ratio": 0.50, "net_buy": 0.15,
    "bc_min": 0.5, "bc_max": 22.0,
    "max_pump": 80.0, "min_age": 2.0,
    "min_pool": 0.3,
    # Risk
    "max_positions": 10, "trade_size": 0.05,
    # Rust executor sell min-out (percent). Wider = fewer reverts on fast dumps; max 35.
    "rust_sell_slippage_pct": 18,
    "cooldown_losses": 8, "cooldown_sec": 10.0,
    # Moonbag
    "moonbag_pct": 10.0, "moonbag_expire": 120.0,
    "moonbag_sl": 15.0,       # hard SL for moonbags (% below creation price)
    "moonbag_trail": 20.0,    # trail from moonbag peak (% drop from peak sells)
    # Timing
    "candidate_age": 60.0,
    # Rug
    "rug_drop": 35.0, "rug_min_vsol": 0.3,
    # Velocity check
    "velocity_check": 1,      # 1=on, 0=off — instant sell on sudden price crash
    "velocity_drop": 6.0,     # max allowed % drop in a single tick before instant sell
    # Anti-manipulation
    "anti_manip": 1,          # 1=on, 0=off — detect fake volume patterns
    "min_wallet_diversity": 0.5,  # min ratio of unique buyer SOL amounts (0-1)
    "max_repeat_wallets": 3,  # if same wallet appears in N recent candidates, skip
    "random_skip_pct": 10,    # randomly skip X% of qualifying entries (unpredictability)
    # Strategy mode (independent of trading mode)
    "strategy_mode": "manual",       # "manual" | "adaptive" | "hybrid"
    "manual_tip": 0.002,             # manual priority fee (SOL)
    "manual_min_liquidity": 0.3,     # manual min pool vSOL
    "manual_entry_progress": 0.5,    # manual min BC progress %
    "override_tip": False,           # hybrid: use manual tip instead of adaptive
    "override_liquidity": False,     # hybrid: use manual min_liquidity instead of adaptive
    "override_entry": False,         # hybrid: use manual entry_progress instead of adaptive
}

PRESETS = {
    "aggressive": {
        "tp1": 10, "tp2": 20, "sl": 18, "max_hold": 90,
        "trail_min": 8, "trail_peak": 4,
        "entry_buyers": 2, "entry_vol": 0.5,
        "sell_ratio": 0.60, "net_buy": 0.10,
        "bc_min": 0.5, "bc_max": 35, "max_pump": 90, "min_age": 1, "min_pool": 0.2,
        "max_positions": 12, "trade_size": 0.02,
        "cooldown_losses": 10, "cooldown_sec": 5,
        "moonbag_pct": 10, "moonbag_expire": 90, "moonbag_sl": 20, "moonbag_trail": 25, "candidate_age": 45,
        "rug_drop": 35, "rug_min_vsol": 0.3,
        "velocity_check": 1, "velocity_drop": 6, "anti_manip": 1, "min_wallet_diversity": 0.4, "max_repeat_wallets": 4, "random_skip_pct": 5,
    },
    "balanced": {
        "tp1": 12, "tp2": 25, "sl": 15, "max_hold": 60,
        "trail_min": 10, "trail_peak": 5,
        "entry_buyers": 4, "entry_vol": 0.8,
        "sell_ratio": 0.50, "net_buy": 0.15,
        "bc_min": 0.5, "bc_max": 22, "max_pump": 80, "min_age": 2, "min_pool": 0.3,
        "max_positions": 10, "trade_size": 0.05,
        "cooldown_losses": 8, "cooldown_sec": 10,
        "moonbag_pct": 10, "moonbag_expire": 120, "moonbag_sl": 15, "moonbag_trail": 20, "candidate_age": 60,
        "rug_drop": 35, "rug_min_vsol": 0.3,
        "velocity_check": 1, "velocity_drop": 6, "anti_manip": 1, "min_wallet_diversity": 0.5, "max_repeat_wallets": 3, "random_skip_pct": 10,
    },
    "conservative": {
        "tp1": 15, "tp2": 35, "sl": 10, "max_hold": 45,
        "trail_min": 12, "trail_peak": 8,
        "entry_buyers": 5, "entry_vol": 1.2,
        "sell_ratio": 0.30, "net_buy": 0.5,
        "bc_min": 2.0, "bc_max": 15, "max_pump": 50, "min_age": 5, "min_pool": 0.8,
        "max_positions": 5, "trade_size": 0.02,
        "cooldown_losses": 3, "cooldown_sec": 45,
        "moonbag_pct": 10, "moonbag_expire": 180, "moonbag_sl": 10, "moonbag_trail": 15, "candidate_age": 60,
        "rug_drop": 30, "rug_min_vsol": 0.3,
        "velocity_check": 1, "velocity_drop": 4, "anti_manip": 1, "min_wallet_diversity": 0.6, "max_repeat_wallets": 2, "random_skip_pct": 15,
    },
    "scalper": {
        "tp1": 8, "tp2": 15, "sl": 12, "max_hold": 30,
        "trail_min": 6, "trail_peak": 3,
        "entry_buyers": 2, "entry_vol": 0.4,
        "sell_ratio": 0.60, "net_buy": 0.10,
        "bc_min": 0.5, "bc_max": 40, "max_pump": 95, "min_age": 1, "min_pool": 0.2,
        "max_positions": 15, "trade_size": 0.02,
        "cooldown_losses": 12, "cooldown_sec": 5,
        "moonbag_pct": 0, "moonbag_expire": 60, "moonbag_sl": 15, "moonbag_trail": 20, "candidate_age": 30,
        "rug_drop": 35, "rug_min_vsol": 0.3,
        "velocity_check": 1, "velocity_drop": 6, "anti_manip": 1, "min_wallet_diversity": 0.4, "max_repeat_wallets": 4, "random_skip_pct": 5,
    },
    "momentum": {
        "tp1": 15, "tp2": 40, "sl": 18, "max_hold": 90,
        "trail_min": 12, "trail_peak": 6,
        "entry_buyers": 3, "entry_vol": 1.0,
        "sell_ratio": 0.40, "net_buy": 0.3,
        "bc_min": 1.0, "bc_max": 30, "max_pump": 70, "min_age": 3, "min_pool": 0.5,
        "max_positions": 8, "trade_size": 0.02,
        "cooldown_losses": 5, "cooldown_sec": 15,
        "moonbag_pct": 15, "moonbag_expire": 180, "moonbag_sl": 15, "moonbag_trail": 20, "candidate_age": 60,
        "rug_drop": 30, "rug_min_vsol": 0.3,
        "velocity_check": 1, "velocity_drop": 6, "anti_manip": 1, "min_wallet_diversity": 0.5, "max_repeat_wallets": 3, "random_skip_pct": 10,
    },
    # Data-driven preset based on actual trade log analysis:
    # Winners hit TP in <1s, losers bleed for 60s or dump instantly.
    # Strategy: fast TP, tight trail to lock gains, shorter hold to cut bleeders.
    "data_optimized": {
        "tp1": 13, "tp2": 25, "sl": 15, "max_hold": 40,
        "trail_min": 8, "trail_peak": 4,
        "entry_buyers": 4, "entry_vol": 0.8,
        "sell_ratio": 0.45, "net_buy": 0.15,
        "bc_min": 0.5, "bc_max": 22, "max_pump": 80, "min_age": 2, "min_pool": 0.3,
        "max_positions": 10, "trade_size": 0.05,
        "cooldown_losses": 8, "cooldown_sec": 10,
        "moonbag_pct": 10, "moonbag_expire": 120, "moonbag_sl": 15, "moonbag_trail": 20, "candidate_age": 60,
        "rug_drop": 35, "rug_min_vsol": 0.3,
        "velocity_check": 1, "velocity_drop": 6, "anti_manip": 1, "min_wallet_diversity": 0.5, "max_repeat_wallets": 3, "random_skip_pct": 10,
    },
}

CUSTOM_PRESETS_FILE = "custom_presets.json"

# =====================================================
#  FIXED CONFIG (not adjustable via dashboard)
# =====================================================

WS_URL = "wss://pumpportal.fun/api/data"
# Helius Gatekeeper HTTP RPC (beta) — JSON-RPC same as mainnet; Rust/Python both use HELIUS_RPC_URL.
_HELIUS_GATEKEEPER_BETA = "https://beta.helius-rpc.com/?api-key="
# Prefer shell-exported or systemd-injected HELIUS_RPC_URL (e.g. `source .env && export ... && python ...`).
HELIUS_RPC_URL = os.environ.get(
    "HELIUS_RPC_URL",
    _HELIUS_GATEKEEPER_BETA + "YOUR_API_KEY_HERE",
)

# AWS us-east-1: Jito NY block engine; Helius mainnet RPC (anycast, low latency to US East PoPs)
JITO_BLOCK_ENGINE_US_EAST = "https://ny.mainnet.block-engine.jito.wtf"


def helius_api_key_from_rpc_url(url: str) -> str:
    """Extract Helius API key from a standard Helius HTTP RPC URL, or empty string."""
    if not url or "api-key=" not in url:
        return ""
    return url.split("api-key=", 1)[-1].split("&")[0].strip()


def helius_mainnet_rpc_url(api_key: str) -> str:
    """Build Helius Gatekeeper (beta) HTTP RPC URL from an API key."""
    return f"{_HELIUS_GATEKEEPER_BETA}{api_key.strip()}"


def _startup_hints() -> dict:
    """Non-secret flags for the dashboard: what is already configured on this machine."""
    url = HELIUS_RPC_URL or ""
    helius_ready = bool(url and "YOUR_API_KEY_HERE" not in url and url.startswith("http"))
    kpath = os.path.join(SCRIPT_DIR, "keypair.txt")
    return {
        "helius_ready": helius_ready,
        "wallet_env": bool(os.environ.get("BIGAL_PRIVATE_KEY", "").strip()),
        "keypair_file": os.path.isfile(kpath),
        "grpc_env": bool(os.environ.get("GRPC_ENDPOINT", "").strip()),
        "grpc_token_env": bool(os.environ.get("GRPC_X_TOKEN", "").strip()),
        "jito_env": bool(os.environ.get("JITO_BLOCK_ENGINE", "").strip()),
    }

DASHBOARD_WS_PORT = 8765
HTTP_PORT = 8766
# HTTP + WebSocket bind: 127.0.0.1 = local-only; 0.0.0.0 = allow browser on another machine via http://SERVER_IP:8766 (open SG 8765–8766).
DASHBOARD_BIND_HOST = os.environ.get("DASHBOARD_BIND_HOST", "127.0.0.1").strip() or "127.0.0.1"
DASHBOARD_HTML = "bigal_dashboard_customizable.html"

START_PAPER_BALANCE_SOL = 10.0
POSITION_CHECK_INTERVAL = float(os.environ.get("POSITION_CHECK_INTERVAL", "0.5"))
ENTRY_WINDOW_SEC = 10.0
SKIP_CREATOR_AS_BUYER = True
BC_GRADUATION_SOL = 85.0

MIN_NAME_LENGTH = 3
MAX_NAME_LENGTH = 40
MIN_SYMBOL_LENGTH = 2
MAX_SYMBOL_LENGTH = 10

PUMPPORTAL_TRADE_LOCAL = "https://pumpportal.fun/api/trade-local"
# Buys: tight min-out is fine (you control size). Sells: curve moves during the hold — 3% sell
# slippage caused mass Rust sell reverts + Legacy fallback (15% there masked the issue).
PUMP_SLIPPAGE_PCT_BY_MODE = {"zmq_rust": 3, "legacy": 15, "paper": 10}

def _effective_slippage() -> int:
    """Return slippage % for BUYS in the current executor mode."""
    return PUMP_SLIPPAGE_PCT_BY_MODE.get(EXECUTOR_MODE, 10)


def _effective_sell_slippage() -> int:
    """Return slippage % for SELLS. Rust path needs a wide min-SOL-out like competitive bots."""
    if EXECUTOR_MODE == "zmq_rust":
        v = CONFIG.get("rust_sell_slippage_pct")
        if v is not None:
            return max(5, min(35, int(v)))
        return 18
    return PUMP_SLIPPAGE_PCT_BY_MODE.get(EXECUTOR_MODE, 10)

PUMP_PRIORITY_FEE_SOL = 0.002
MIN_TRADE_SIZE_SOL = 0.05  # floor for live modes — below this, fees eat >25% of capital

# ZMQ IPC ports (Python ↔ Rust executor)
ZMQ_SIGNAL_PORT = 5555   # fallback if IPC not available
ZMQ_RESULT_PORT = 5556
ZMQ_SIGNAL_IPC = "ipc:///tmp/bigal_signal.ipc"
ZMQ_RESULT_IPC = "ipc:///tmp/bigal_result.ipc"

BLACKLIST_CREATORS_FILE = "blacklist_creators.json"
TRADE_JOURNAL_FILE = "trade_journal.csv"

BLACKLIST_WORDS = [
    "rug", "scam", "test", "bot", "hamster",
    "hitler", "nazi", "trump", "elon", "claim",
    "airdrop", "free", "send", "buy buy"
]

# =====================================================
#  STATE
# =====================================================

@dataclass
class Position:
    mint: str
    name: str
    symbol: str
    size_sol: float
    entry_price: float
    entry_value_sol: float
    entry_time: float
    entry_vsol: float
    last_price: float
    max_price: float
    last_vsol: float
    creator: str = ""
    partial_sold: bool = False
    remaining_size_sol: float = 0.0
    is_moonbag: bool = False
    buy_tx_sig: Optional[str] = None
    last_update_time: float = 0.0
    prev_price: float = 0.0  # previous tick price for velocity check
    closing: bool = False  # guard against double-close race

    def __post_init__(self):
        if self.remaining_size_sol == 0.0:
            self.remaining_size_sol = self.size_sol
        if self.last_update_time == 0.0:
            self.last_update_time = self.entry_time
        if self.prev_price == 0.0:
            self.prev_price = self.entry_price

    def to_dict(self):
        pnl = (self.last_price / self.entry_price - 1.0) * 100.0 if self.entry_price > 0 else 0
        hold = time.time() - self.entry_time
        return {
            "mint": self.mint[:12], "name": self.name, "symbol": self.symbol,
            "size_sol": round(self.remaining_size_sol, 4),
            "pnl_pct": round(pnl, 2), "hold_time": round(hold, 1),
            "partial_sold": self.partial_sold, "is_moonbag": self.is_moonbag,
        }

@dataclass
class Candidate:
    mint: str
    name: str
    symbol: str
    creator: str
    created_at: float
    initial_vsol: float
    initial_vtokens: float
    initial_price: float
    last_vsol: float
    last_vtokens: float = 0.0
    unique_buyers: Set[str] = field(default_factory=set)
    buy_events: List[tuple] = field(default_factory=list)
    sell_events: List[tuple] = field(default_factory=list)

    def bc_progress(self):
        denom = BC_GRADUATION_SOL - self.initial_vsol
        if denom <= 0:
            return 100.0
        return max(0.0, (self.last_vsol - self.initial_vsol) / denom * 100.0)


positions: Dict[str, Position] = {}
candidates: Dict[str, Candidate] = {}
seen_names = TTLCache(ttl_seconds=120.0, maxsize=2000)
moonbags: Dict[str, Position] = {}
recent_entry_wallets = TTLCache(ttl_seconds=300.0, maxsize=5000)
_grpc_dedup = TTLCache(ttl_seconds=5.0, maxsize=1000)  # Cross-source dedup for gRPC + WS
import random

paper_balance: float = START_PAPER_BALANCE_SOL
# On-chain wallet baseline when entering a live session (RPC getBalance); used for true wallet delta vs fees.
session_wallet_start_sol: Optional[float] = None
realized_pnl: float = 0.0
wins: int = 0
losses: int = 0
consecutive_losses: int = 0
cooldown_until: float = 0.0

# Lock to prevent double-sell race conditions across async tasks
state_lock = asyncio.Lock()

MODE_LIVE: bool = False
MODE_RUG_PULLER: bool = False
RUG_PULLER_LIVE: bool = False
LIVE_TX_ENABLED: bool = True
LIVE_CLIENT: Optional[Client] = None
LIVE_KEYPAIR: Optional[Keypair] = None
executor: Optional[ExecutorBackend] = None
EXECUTOR_MODE: str = "paper"  # "paper" | "legacy" | "zmq_rust"
TRADING_PAUSED: bool = False
adaptive_state: AdaptiveState = AdaptiveState()  # strategy mode — initialized once, persisted

last_dashboard_time: float = 0.0
subscribed_trades: Dict[str, bool] = {}
creator_blacklist: Dict[str, dict] = {}
aio_session: Optional[aiohttp.ClientSession] = None
trade_history: List[dict] = []
profiler: Optional[WalletProfiler] = None
orchestrator: Optional[RugOrchestrator] = None
wallet_dev: Optional[WalletDeveloper] = None
wallet_dev_task: Optional[asyncio.Task] = None

# Rug Puller session settings (dashboard "Rug Puller Settings")
RUG_SETTINGS: Dict[str, object] = {
    "auto_plan_threshold": 70.0,
    "aggression_level": "standard",
    "min_wallet_age_minutes": 0.0,
    "auto_approve": False,
}


def get_rug_bot_suspects_for_dashboard(limit: int = 50) -> List[dict]:
    """Profiler rows for rug UI, respecting min wallet age filter."""
    if not profiler or not MODE_RUG_PULLER:
        return []
    try:
        mins = float(RUG_SETTINGS.get("min_wallet_age_minutes", 0) or 0)
    except (TypeError, ValueError):
        mins = 0.0
    return profiler.get_all_scored(
        limit=limit, min_wallet_age_sec=max(0.0, mins) * 60.0
    )


def apply_rug_settings_update(data: dict) -> Dict[str, object]:
    """Merge validated keys into RUG_SETTINGS. Returns the updated dict."""
    global RUG_SETTINGS
    if not data:
        return dict(RUG_SETTINGS)
    if "auto_plan_threshold" in data:
        try:
            v = float(data["auto_plan_threshold"])
            RUG_SETTINGS["auto_plan_threshold"] = max(0.0, min(100.0, v))
        except (TypeError, ValueError):
            pass
    if "aggression_level" in data:
        lvl = str(data["aggression_level"]).lower().strip()
        if lvl in ("conservative", "standard", "degenerate"):
            RUG_SETTINGS["aggression_level"] = lvl
    if "min_wallet_age_minutes" in data:
        try:
            v = float(data["min_wallet_age_minutes"])
            RUG_SETTINGS["min_wallet_age_minutes"] = max(0.0, min(60.0, v))
        except (TypeError, ValueError):
            pass
    if "auto_approve" in data:
        RUG_SETTINGS["auto_approve"] = bool(data["auto_approve"])
    # Keep orchestrator's reference in sync
    if orchestrator is not None:
        orchestrator.set_rug_settings(RUG_SETTINGS)
    return dict(RUG_SETTINGS)


# =====================================================
#  CONFIG MANAGEMENT
# =====================================================

def handle_config_update(data):
    changed = {}
    for key, val in data.items():
        if key in CONFIG:
            try:
                CONFIG[key] = type(CONFIG[key])(val)
                changed[key] = CONFIG[key]
            except (ValueError, TypeError):
                pass
    if changed:
        print(f"⚙ Config updated: {changed}")
    return changed

def apply_preset(name):
    preset = PRESETS.get(name) or load_custom_presets().get(name)
    if preset:
        for k, v in preset.items():
            if k in CONFIG:
                CONFIG[k] = type(CONFIG[k])(v)
        print(f"⚙ Preset applied: {name}")
        return True
    return False

def _atomic_json_write(path, data):
    """Write JSON atomically: temp file → os.replace (crash-safe)."""
    import tempfile
    tmp_fd, tmp_path = tempfile.mkstemp(
        dir=os.path.dirname(os.path.abspath(path)) or ".",
        suffix=".tmp"
    )
    try:
        with os.fdopen(tmp_fd, "w") as tmp_f:
            json.dump(data, tmp_f, indent=2)
        os.replace(tmp_path, path)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise

def save_custom_preset(name, data):
    presets = load_custom_presets()
    presets[name] = {k: CONFIG[k] for k in CONFIG}
    if data:
        for k, v in data.items():
            if k in presets[name]:
                presets[name][k] = type(CONFIG.get(k, v))(v)
    try:
        _atomic_json_write(CUSTOM_PRESETS_FILE, presets)
        print(f"⚙ Custom preset saved: {name}")
        return True
    except Exception as e:
        print(f"❌ Save preset failed: {e}")
        return False

def delete_custom_preset(name):
    presets = load_custom_presets()
    if name in presets:
        del presets[name]
        try:
            _atomic_json_write(CUSTOM_PRESETS_FILE, presets)
            return True
        except Exception:
            pass
    return False

def load_custom_presets():
    if os.path.exists(CUSTOM_PRESETS_FILE):
        try:
            with open(CUSTOM_PRESETS_FILE, "r") as f:
                return json.load(f)
        except Exception as e:
            print(f"⚠ Preset file corrupted ({CUSTOM_PRESETS_FILE}): {e}")
    return {}

def get_all_presets():
    all_p = {k: v for k, v in PRESETS.items()}
    all_p.update(load_custom_presets())
    return all_p

# =====================================================
#  MANUAL SELL
# =====================================================

async def manual_sell_position(mint):
    global positions, moonbags, paper_balance, realized_pnl, wins, losses
    global consecutive_losses, trade_history

    # Check positions first, then moonbags
    pos = positions.get(mint) or moonbags.get(mint)
    if not pos:
        # mint might be truncated (12 chars from dashboard), try matching
        for m, p in list(positions.items()):
            if m.startswith(mint) or m[:12] == mint:
                pos = p; mint = m; break
        if not pos:
            for m, p in list(moonbags.items()):
                if m.startswith(mint) or m[:12] == mint:
                    pos = p; mint = m; break
    if not pos:
        print(f"⚠ Manual sell: mint {mint[:12]} not found")
        return

    is_mb = pos.is_moonbag
    source = moonbags if is_mb else positions

    # Prevent double-sell race with monitor/velocity/TP paths
    async with state_lock:
        if mint not in source or pos.closing:
            print(f"  Manual sell: {mint[:12]} already closing")
            return
        pos.closing = True

    now = time.time()
    active = pos.remaining_size_sol
    pnl_pct = (pos.last_price / pos.entry_price - 1.0) * 100.0 if pos.entry_price > 0 else 0
    pnl_sol = active * (pnl_pct / 100.0)
    hold = now - pos.entry_time

    realized_pnl += pnl_sol
    if pnl_sol >= 0:
        wins += 1; consecutive_losses = 0
    else:
        losses += 1; consecutive_losses += 1

    if MODE_LIVE:
        await live_sell_pump(mint)

    paper_balance += max(active + pnl_sol, 0.0)
    log_trade(pos.entry_time, now, mint, pos.name, pos.symbol, pos.creator,
              "SELL", active, pos.entry_price, pos.last_price, pnl_sol, pnl_pct, "Manual")

    tag = " [MB]" if is_mb else ""
    log_msg = f"🔴 MANUAL SELL {pos.name} ({pos.symbol}){tag} | PnL: {pnl_pct:+.1f}% | Hold: {hold:.1f}s"
    print(log_msg)
    level = "exit-win" if pnl_sol >= 0 else "exit-loss"
    await broadcast_log(log_msg, level)

    trade_history.append({
        "time": datetime.now().strftime("%H:%M:%S"), "timestamp": now * 1000,
        "name": pos.name, "symbol": pos.symbol,
        "pnl_pct": round(pnl_pct, 2), "pnl_sol": round(pnl_sol, 6),
        "cum_pnl": round(realized_pnl, 6), "balance": round(paper_balance, 4),
        "hold_time": round(hold, 1), "reason": "Manual",
    })
    await send_to_dashboard({"type": "trade", "data": trade_history[-1]})
    source.pop(mint, None)
    print(f"✅ Closed {pos.name} | Bal: {paper_balance:.4f} | W/L: {wins}/{losses}")
    await broadcast_log(f"✅ Manual closed {pos.name} | Bal: {paper_balance:.4f}", level)
    await broadcast_stats()

# =====================================================
#  DASHBOARD WEBSOCKET
# =====================================================

dashboard_clients: Set = set()
mode_selected_event: Optional[asyncio.Event] = None

async def _stop_wallet_dev_service():
    """Stop background wallet warming (Rug Puller live)."""
    global wallet_dev, wallet_dev_task
    if wallet_dev_task and not wallet_dev_task.done():
        wallet_dev_task.cancel()
        try:
            await wallet_dev_task
        except asyncio.CancelledError:
            pass
    wallet_dev_task = None
    if wallet_dev:
        await wallet_dev.close()
        wallet_dev = None


async def _start_wallet_dev_service():
    """Start wallet pool warming when Rug Puller live + funded RPC + wallet."""
    global wallet_dev, wallet_dev_task
    await _stop_wallet_dev_service()
    if not (MODE_RUG_PULLER and RUG_PULLER_LIVE and LIVE_KEYPAIR and HELIUS_RPC_URL):
        return
    if "YOUR_API_KEY_HERE" in HELIUS_RPC_URL:
        print("⚠ Wallet Dev skipped — configure Helius API key")
        return
    try:
        wallet_dev = WalletDeveloper(HELIUS_RPC_URL, LIVE_KEYPAIR, pool_size=4)
        wallet_dev_task = asyncio.create_task(wallet_dev.run_forever())
        print("🔧 Wallet Developer service started (pool warming)")
    except Exception as e:
        print(f"⚠ Wallet Developer failed to start: {e}")


def _wallet_dev_snapshot():
    if wallet_dev:
        return wallet_dev.get_pool_snapshot()
    return {"enabled": False, "pool_size": 0, "entries": []}


async def prepare_mode_switch_session(websocket):
    """
    Stop Rug/Sniper session (orchestrator, Rust, sidecar) so the user can
    pick a different product mode without restarting the Python process.
    """
    global orchestrator, executor, EXECUTOR_MODE, MODE_LIVE, MODE_RUG_PULLER, RUG_PULLER_LIVE
    await _stop_wallet_dev_service()
    if orchestrator:
        try:
            await orchestrator.stop()
        except Exception as e:
            print(f"⚠ Orchestrator stop: {e}")
        orchestrator = None
    if executor and getattr(executor, "name", None) == "zmq_rust":
        try:
            executor.close()
        except Exception as e:
            print(f"⚠ ZmqExecutor close (mode switch): {e}")
    proc_mgr.stop_all()
    executor = PaperExecutor()
    EXECUTOR_MODE = "paper"
    MODE_LIVE = False
    MODE_RUG_PULLER = False
    RUG_PULLER_LIVE = False
    await send_to_dashboard({"type": "mode_switch_ready", "data": {}}, target=websocket)
    await broadcast_log("Session stopped — select Sniper or Rug Puller in the startup modal", "warn")


async def handle_validate_field(msg, websocket):
    """Validate a single config field and send result back to dashboard."""
    field = msg.get("field", "")
    value = msg.get("value", "").strip()

    try:
        if field == "private_key":
            if not value:
                env_k = os.environ.get("BIGAL_PRIVATE_KEY", "").strip()
                if env_k:
                    try:
                        if env_k.startswith("["):
                            kp = Keypair.from_bytes(bytes(json.loads(env_k)))
                        else:
                            kp = Keypair.from_bytes(b58decode(env_k))
                        pubkey = str(kp.pubkey())
                        await send_to_dashboard({
                            "type": "validation_result",
                            "data": {"field": "private_key", "status": "ok",
                                     "message": f"BIGAL_PRIVATE_KEY in environment: {pubkey[:8]}...{pubkey[-4:]}"}
                        }, target=websocket)
                    except Exception as e:
                        await send_to_dashboard({
                            "type": "validation_result",
                            "data": {"field": "private_key", "status": "fail",
                                     "message": f"BIGAL_PRIVATE_KEY invalid: {e}"}
                        }, target=websocket)
                elif os.path.exists(os.path.join(SCRIPT_DIR, "keypair.txt")):
                    try:
                        kp = load_keypair_from_file(os.path.join(SCRIPT_DIR, "keypair.txt"))
                        pubkey = str(kp.pubkey())
                        await send_to_dashboard({
                            "type": "validation_result",
                            "data": {"field": "private_key", "status": "ok",
                                     "message": f"keypair.txt found: {pubkey[:8]}...{pubkey[-4:]}"}
                        }, target=websocket)
                    except Exception as e:
                        await send_to_dashboard({
                            "type": "validation_result",
                            "data": {"field": "private_key", "status": "fail",
                                     "message": f"keypair.txt invalid: {e}"}
                        }, target=websocket)
                else:
                    await send_to_dashboard({
                        "type": "validation_result",
                        "data": {"field": "private_key", "status": "fail",
                                 "message": "Set BIGAL_PRIVATE_KEY, keypair.txt, or paste a key above"}
                    }, target=websocket)
            else:
                # Validate provided key format
                try:
                    if value.startswith("["):
                        kp = Keypair.from_bytes(bytes(json.loads(value)))
                    else:
                        kp = Keypair.from_bytes(b58decode(value))
                    pubkey = str(kp.pubkey())
                    await send_to_dashboard({
                        "type": "validation_result",
                        "data": {"field": "private_key", "status": "ok",
                                 "message": f"Valid wallet: {pubkey[:8]}...{pubkey[-4:]}"}
                    }, target=websocket)
                except Exception:
                    await send_to_dashboard({
                        "type": "validation_result",
                        "data": {"field": "private_key", "status": "fail",
                                 "message": "Invalid key format — use base58 or JSON array"}
                    }, target=websocket)

        elif field == "rpc_key":
            if not value:
                url = os.environ.get("HELIUS_RPC_URL") or HELIUS_RPC_URL
                if url and "YOUR_API_KEY_HERE" not in url:
                    await send_to_dashboard({
                        "type": "validation_result",
                        "data": {"field": "rpc_key", "status": "ok",
                                 "message": "Using HELIUS_RPC_URL from environment (field optional)"}
                    }, target=websocket)
                else:
                    await send_to_dashboard({
                        "type": "validation_result",
                        "data": {"field": "rpc_key", "status": "ok",
                                 "message": "Paste a Helius API key or set HELIUS_RPC_URL before launch"}
                    }, target=websocket)
            else:
                # Test the RPC endpoint with a simple getHealth call
                test_url = helius_mainnet_rpc_url(value)
                try:
                    async with aio_session.post(test_url, json={
                        "jsonrpc": "2.0", "id": 1, "method": "getHealth"
                    }, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            if data.get("result") == "ok":
                                await send_to_dashboard({
                                    "type": "validation_result",
                                    "data": {"field": "rpc_key", "status": "ok",
                                             "message": "Helius RPC connected"}
                                }, target=websocket)
                            else:
                                await send_to_dashboard({
                                    "type": "validation_result",
                                    "data": {"field": "rpc_key", "status": "fail",
                                             "message": f"RPC unhealthy: {data.get('error', 'unknown')}"}
                                }, target=websocket)
                        else:
                            await send_to_dashboard({
                                "type": "validation_result",
                                "data": {"field": "rpc_key", "status": "fail",
                                         "message": f"HTTP {resp.status} — check your API key"}
                            }, target=websocket)
                except Exception as e:
                    await send_to_dashboard({
                        "type": "validation_result",
                        "data": {"field": "rpc_key", "status": "fail",
                                 "message": f"Connection failed: {e}"}
                    }, target=websocket)

        elif field == "rust_executor":
            # Rust executor auto-starts on mode submit — just confirm
            await send_to_dashboard({
                "type": "validation_result",
                "data": {"field": "rust_executor", "status": "ok",
                         "message": "Will auto-start on submit"}
            }, target=websocket)

    except Exception as e:
        print(f"⚠ Validation error for {field}: {e}")

async def handle_mode_select(msg, websocket):
    """Handle mode selection from dashboard UI.
    All globals are set atomically (no awaits between state changes)."""
    async with mode_select_lock:
        await _handle_mode_select_impl(msg, websocket)


async def _handle_mode_select_impl(msg, websocket):
    global MODE_LIVE, LIVE_CLIENT, LIVE_KEYPAIR, executor, EXECUTOR_MODE
    global paper_balance, mode_selected_event, MODE_RUG_PULLER, profiler, RUG_PULLER_LIVE
    global orchestrator, session_wallet_start_sol

    mode = msg.get("mode", "paper")
    private_key = msg.get("private_key", "").strip()
    rpc_key = msg.get("rpc_key", "").strip()
    grpc_endpoint = msg.get("grpc_endpoint", "").strip()
    grpc_token = msg.get("grpc_token", "").strip()
    jito_engine = (msg.get("jito_engine") or "").strip()

    if mode != "rug_puller_live":
        await _stop_wallet_dev_service()

    if mode in ("zmq_rust", "rug_puller_live") and not jito_engine:
        jito_engine = JITO_BLOCK_ENGINE_US_EAST

    # Update Helius RPC URL if a custom key was provided
    global HELIUS_RPC_URL
    if rpc_key:
        HELIUS_RPC_URL = helius_mainnet_rpc_url(rpc_key)
        print(f"📡 RPC updated with custom Helius key")

    # Set env vars for Rust executor (reads these on startup)
    os.environ["HELIUS_RPC_URL"] = HELIUS_RPC_URL
    # Sell-side token balance polling uses JSON-RPC (not Yellowstone gRPC). Keep read URL aligned with
    # your paid Helius HTTP endpoint so post-buy balances index as fast as buys.
    if rpc_key:
        os.environ["HELIUS_READ_RPC_URL"] = HELIUS_RPC_URL
    elif not (os.environ.get("HELIUS_READ_RPC_URL") or "").strip():
        os.environ["HELIUS_READ_RPC_URL"] = HELIUS_RPC_URL
    if private_key:
        os.environ["BIGAL_PRIVATE_KEY"] = private_key
    if grpc_endpoint:
        os.environ["GRPC_ENDPOINT"] = grpc_endpoint
        print(f"📡 gRPC endpoint configured: {grpc_endpoint[:40]}...")
    if grpc_token:
        os.environ["GRPC_X_TOKEN"] = grpc_token
    if jito_engine:
        os.environ["JITO_BLOCK_ENGINE"] = jito_engine
        os.environ["JITO_BUNDLE_URL"] = f"{jito_engine.rstrip('/')}/api/v1/bundles"
        print(f"📡 Jito block engine: {jito_engine}")

    try:
        # --- Phase 1: Build new executor without touching globals ---
        new_executor = None
        new_mode_live = False
        new_executor_mode = "paper"
        new_balance = paper_balance
        new_rug_puller = False
        new_rug_puller_live = False

        if mode == "paper":
            new_executor = PaperExecutor()
            print("📝 Paper mode selected via dashboard")
        elif mode == "rug_puller":
            new_executor = PaperExecutor()
            new_rug_puller = True
            if profiler is None:
                profiler = WalletProfiler()
            orchestrator = RugOrchestrator(profiler, is_live=False,
                                          creator_blacklist=creator_blacklist,
                                          rug_settings=RUG_SETTINGS)
            print("☠ Auto Rug Puller mode selected via dashboard")
        elif mode == "analysis":
            # Analysis mode: full Stage 1-5 pipeline with PaperExecutor
            # Sniper + Rug Puller both active, all simulated
            new_executor = PaperExecutor()
            new_rug_puller = True
            if profiler is None:
                profiler = WalletProfiler()
            orchestrator = RugOrchestrator(profiler, is_live=False,
                                          creator_blacklist=creator_blacklist,
                                          rug_settings=RUG_SETTINGS)
            print("🔬 Analysis mode selected — full pipeline with simulated execution")
        elif mode in ("rug_puller_test", "rug_puller_live"):
            new_rug_puller = True
            new_rug_puller_live = (mode == "rug_puller_live")
            if profiler is None:
                profiler = WalletProfiler()

            if new_rug_puller_live and private_key:
                try:
                    wallet_input = private_key.strip()
                    if wallet_input.startswith("["):
                        LIVE_KEYPAIR = Keypair.from_bytes(bytes(json.loads(wallet_input)))
                    else:
                        LIVE_KEYPAIR = Keypair.from_bytes(b58decode(wallet_input))
                    LIVE_CLIENT = Client(HELIUS_RPC_URL)
                    print(f"✅ Wallet stored for rug puller stages 4-5: {LIVE_KEYPAIR.pubkey()}")
                except Exception as e:
                    print(f"⚠ Wallet not loaded (not needed for Stage 1): {e}")

            if new_rug_puller_live and LIVE_KEYPAIR:
                # Live rug puller: build ZmqExecutor (same as zmq_rust sniper mode)
                legacy = LegacyExecutor(LIVE_KEYPAIR, HELIUS_RPC_URL, PUMPPORTAL_TRADE_LOCAL)

                if executor and getattr(executor, "name", None) == "zmq_rust":
                    try:
                        executor.close()
                    except Exception as e:
                        print(f"⚠ ZmqExecutor close before Rug Puller live Rust: {e}")
                proc_mgr.stop_all()
                await asyncio.sleep(0.25)

                # Bind ZMQ socket BEFORE spawning Rust (so IPC file exists)
                from zmq_bridge import ZmqBridge as _ProbeBridge
                _probe = _ProbeBridge(signal_port=ZMQ_SIGNAL_PORT,
                                      result_port=ZMQ_RESULT_PORT,
                                      signal_ipc=ZMQ_SIGNAL_IPC,
                                      result_ipc=ZMQ_RESULT_IPC)

                rust_env = os.environ.copy()
                rust_status = proc_mgr.start_rust(rust_env)
                await send_to_dashboard({
                    "type": "validation_result",
                    "data": {"field": "rust_executor", "status": "pending",
                             "message": rust_status}
                }, target=websocket)
                rust_ready = False
                for _attempt in range(16):
                    await asyncio.sleep(0.5)
                    if _probe.probe_rust():
                        rust_ready = True
                        break
                if rust_ready:
                    # Reuse the probe bridge — closing and rebinding causes
                    # the pure-Rust zeromq PULL socket to lose its connection
                    new_executor = ZmqExecutor(
                        legacy_fallback=legacy,
                        bridge=_probe,
                    )
                    new_executor.set_adaptive_state(adaptive_state)
                    new_executor_mode = "zmq_rust"
                    print("⚡ Rug Puller (LIVE) with Rust executor ready")

                    # Auto-spawn gRPC sidecar for low-latency event streaming
                    sidecar_helius_rp = (
                        rpc_key.strip() if rpc_key
                        else (os.environ.get("HELIUS_API_KEY", "") or "").strip()
                        or helius_api_key_from_rpc_url(HELIUS_RPC_URL)
                    )
                    if sidecar_helius_rp and "YOUR_API_KEY_HERE" not in (HELIUS_RPC_URL or ""):
                        sidecar_status = proc_mgr.start_sidecar(sidecar_helius_rp)
                        if sidecar_status == "ok":
                            print("✅ gRPC sidecar auto-started for rug puller")
                        else:
                            print(f"⚠ gRPC sidecar skipped: {sidecar_status}")
                else:
                    _probe.close()
                    new_executor = legacy
                    new_executor_mode = "legacy"
                    print("⚠ Rust executor not ready — rug puller using legacy fallback")
            else:
                # Test mode or no wallet: use PaperExecutor
                new_executor = PaperExecutor()

            sub = "LIVE" if new_rug_puller_live else "TEST"
            print(f"☠ Rug Puller ({sub}) selected via dashboard")

            # Initialize orchestrator (Stages 2-5)
            orchestrator = RugOrchestrator(profiler, is_live=new_rug_puller_live,
                                          creator_blacklist=creator_blacklist,
                                          rug_settings=RUG_SETTINGS)
            if new_rug_puller_live and LIVE_KEYPAIR:
                orchestrator.set_master_wallet(LIVE_KEYPAIR)
        else:
            new_mode_live = True
            loaded_keypair = None
            loaded_client = None

            # Try provided private key first
            if private_key:
                wallet_input = private_key.strip()
                try:
                    if wallet_input.startswith("["):
                        loaded_keypair = Keypair.from_bytes(bytes(json.loads(wallet_input)))
                    else:
                        loaded_keypair = Keypair.from_bytes(b58decode(wallet_input))
                    loaded_client = Client(HELIUS_RPC_URL)
                    print(f"✅ Wallet loaded from dashboard: {loaded_keypair.pubkey()}")
                except Exception:
                    await send_to_dashboard({
                        "type": "mode_error",
                        "data": {"error": "Invalid private key format"}
                    }, target=websocket)
                    return

            if not loaded_keypair:
                env_k = os.environ.get("BIGAL_PRIVATE_KEY", "").strip()
                if env_k:
                    try:
                        if env_k.startswith("["):
                            loaded_keypair = Keypair.from_bytes(bytes(json.loads(env_k)))
                        else:
                            loaded_keypair = Keypair.from_bytes(b58decode(env_k))
                        loaded_client = Client(HELIUS_RPC_URL)
                        print(f"✅ Wallet loaded from BIGAL_PRIVATE_KEY: {loaded_keypair.pubkey()}")
                    except Exception as e:
                        print(f"⚠ BIGAL_PRIVATE_KEY load failed: {e}")

            # Fallback to keypair.txt
            if not loaded_keypair and os.path.exists(os.path.join(SCRIPT_DIR, "keypair.txt")):
                try:
                    loaded_keypair = load_keypair_from_file(os.path.join(SCRIPT_DIR, "keypair.txt"))
                    loaded_client = Client(HELIUS_RPC_URL)
                    print(f"✅ Wallet loaded from keypair.txt: {loaded_keypair.pubkey()}")
                except Exception as e:
                    print(f"⚠ keypair.txt load failed: {e}")

            # No key available
            if not loaded_keypair:
                await send_to_dashboard({
                    "type": "mode_error",
                    "data": {"error": "No wallet found — paste a key, set BIGAL_PRIVATE_KEY, or add keypair.txt"}
                }, target=websocket)
                return

            # Build executor (no globals touched yet)
            legacy = LegacyExecutor(loaded_keypair, HELIUS_RPC_URL, PUMPPORTAL_TRADE_LOCAL)
            if mode == "zmq_rust":
                new_executor_mode = "zmq_rust"

                # Release any prior Python-side ZMQ bind and kill orphan Rust before rebinding IPC
                if executor and getattr(executor, "name", None) == "zmq_rust":
                    try:
                        executor.close()
                    except Exception as e:
                        print(f"⚠ ZmqExecutor close before Rust mode: {e}")
                proc_mgr.stop_all()
                await asyncio.sleep(0.25)

                # ── Bind ZMQ socket BEFORE spawning Rust (so IPC file exists) ──
                from zmq_bridge import ZmqBridge
                probe_bridge = ZmqBridge(
                    signal_port=ZMQ_SIGNAL_PORT, result_port=ZMQ_RESULT_PORT,
                    signal_ipc=ZMQ_SIGNAL_IPC, result_ipc=ZMQ_RESULT_IPC,
                )

                # ── Auto-spawn Rust executor ──
                await send_to_dashboard({
                    "type": "validation_result",
                    "data": {"field": "rust_executor", "status": "pending",
                             "message": "Starting Rust executor..."}
                }, target=websocket)

                rust_env = os.environ.copy()
                rust_status = proc_mgr.start_rust(rust_env)
                if rust_status != "ok":
                    probe_bridge.close()
                    await send_to_dashboard({
                        "type": "mode_error",
                        "data": {"error": rust_status}
                    }, target=websocket)
                    return
                rust_ready = False
                for attempt in range(16):  # 16 × 500ms = 8s
                    await asyncio.sleep(0.5)
                    if not proc_mgr.is_rust_alive():
                        probe_bridge.close()
                        await send_to_dashboard({
                            "type": "mode_error",
                            "data": {"error": "Rust executor crashed on startup — check rust_executor.log"}
                        }, target=websocket)
                        return
                    if probe_bridge.probe_rust(timeout_ms=1000):
                        rust_ready = True
                        break
                if not rust_ready:
                    probe_bridge.close()
                    proc_mgr.stop_all()
                    await send_to_dashboard({
                        "type": "mode_error",
                        "data": {"error": "Rust executor started but not responding on ZMQ — check rust_executor.log"}
                    }, target=websocket)
                    return

                print("✅ Rust executor auto-started and responding")
                await send_to_dashboard({
                    "type": "validation_result",
                    "data": {"field": "rust_executor", "status": "ok",
                             "message": "Rust executor running"}
                }, target=websocket)

                # ── Auto-spawn gRPC sidecar (non-critical) — key from dashboard, HELIUS_API_KEY, or RPC URL
                sidecar_helius = (
                    rpc_key.strip()
                    if rpc_key
                    else (os.environ.get("HELIUS_API_KEY", "") or "").strip()
                    or helius_api_key_from_rpc_url(HELIUS_RPC_URL)
                )
                if sidecar_helius and "YOUR_API_KEY_HERE" not in (HELIUS_RPC_URL or ""):
                    sidecar_status = proc_mgr.start_sidecar(sidecar_helius)
                    if sidecar_status == "ok":
                        print("✅ gRPC sidecar auto-started")
                        await send_to_dashboard({
                            "type": "validation_result",
                            "data": {"field": "sidecar", "status": "ok",
                                     "message": "gRPC sidecar running"}
                        }, target=websocket)
                    else:
                        print(f"⚠ gRPC sidecar skipped: {sidecar_status}")
                        await send_to_dashboard({
                            "type": "validation_result",
                            "data": {"field": "sidecar", "status": "warn",
                                     "message": sidecar_status}
                        }, target=websocket)

                # Reuse the probe bridge — closing and rebinding causes
                # the pure-Rust zeromq PULL socket to lose its connection
                new_executor = ZmqExecutor(
                    legacy_fallback=legacy,
                    bridge=probe_bridge,
                )
                new_executor.set_adaptive_state(adaptive_state)
                print("⚡ Rust executor mode via dashboard")
            else:
                new_executor_mode = "legacy"
                new_executor = legacy
                print("📡 Legacy executor mode via dashboard")

            # Store keypair for later reference
            LIVE_KEYPAIR = loaded_keypair
            LIVE_CLIENT = loaded_client

        # --- Phase 2: Atomic global swap (no awaits in this block) ---
        MODE_LIVE = new_mode_live
        EXECUTOR_MODE = new_executor_mode
        executor = new_executor
        MODE_RUG_PULLER = new_rug_puller
        RUG_PULLER_LIVE = new_rug_puller_live

        # --- Phase 3: Non-critical async work (globals already consistent) ---
        track_wallet = (
            (MODE_LIVE or (MODE_RUG_PULLER and RUG_PULLER_LIVE))
            and executor is not None
            and getattr(executor, "name", None) != "paper"
        )
        if track_wallet:
            bal = await get_live_balance()
            if bal is not None:
                session_wallet_start_sol = bal
                if MODE_LIVE:
                    paper_balance = bal
                    new_balance = bal
        else:
            session_wallet_start_sol = None

        # Build mode label
        if MODE_RUG_PULLER:
            mode_label = "RUG PULLER (LIVE)" if RUG_PULLER_LIVE else "RUG PULLER (TEST)"
        elif EXECUTOR_MODE == "zmq_rust":
            mode_label = "LIVE (RUST)"
        elif MODE_LIVE:
            mode_label = "LIVE (LEGACY)"
        else:
            mode_label = "PAPER"

        # Send acknowledgment (if this fails, mode is still set correctly)
        try:
            await send_to_dashboard({
                "type": "mode_ack",
                "data": {
                    "mode": mode_label,
                    "executor_type": EXECUTOR_MODE,
                    "balance": round(new_balance, 6),
                }
            }, target=websocket)
        except Exception as e:
            print(f"⚠ Failed to send mode_ack: {e}")

        print(f"✅ Mode confirmed: {mode_label}")

    except Exception as e:
        print(f"❌ Mode select error: {e}")
        # Ensure we have a valid executor even on error
        if executor is None:
            executor = PaperExecutor()
            EXECUTOR_MODE = "paper"
            MODE_LIVE = False
        try:
            await send_to_dashboard({
                "type": "mode_error",
                "data": {"error": str(e)}
            }, target=websocket)
        except Exception:
            pass
    finally:
        # Start orchestrator if rug puller mode
        if orchestrator and MODE_RUG_PULLER:
            orchestrator.set_executor(executor)
            await orchestrator.start()

        if MODE_RUG_PULLER and RUG_PULLER_LIVE:
            await _start_wallet_dev_service()
            # Wire wallet developer to puppet master for pre-warmed wallets
            if wallet_dev and orchestrator:
                orchestrator.puppet_master.set_wallet_developer(wallet_dev)

        # Always fire the event so run_bot never hangs
        if mode_selected_event and not mode_selected_event.is_set():
            mode_selected_event.set()

async def dashboard_handler(websocket):
    global orchestrator, TRADING_PAUSED
    dashboard_clients.add(websocket)
    print(f"📊 Dashboard connected ({len(dashboard_clients)})")
    all_pos = {**positions, **moonbags}
    wsnap = await wallet_chain_snapshot()
    await send_to_dashboard({
        "type": "snapshot",
        "data": {
            "balance": paper_balance, "pnl": realized_pnl,
            "wins": wins, "losses": losses,
            "positions": [p.to_dict() for p in all_pos.values()],
            "trade_history": trade_history[-100:],
            "blacklisted_creators": len(creator_blacklist),
            "mode": ("RUG PULLER (LIVE)" if RUG_PULLER_LIVE else "RUG PULLER (TEST)") if MODE_RUG_PULLER else ("LIVE (RUST)" if EXECUTOR_MODE == "zmq_rust" else ("LIVE (LEGACY)" if MODE_LIVE else "PAPER")),
            "executor_type": EXECUTOR_MODE,
            "paused": TRADING_PAUSED,
            "is_rug_puller": MODE_RUG_PULLER,
            "rug_puller_live": RUG_PULLER_LIVE,
            "profiler_stats": profiler.get_profiler_stats() if profiler else None,
            "bot_suspects": get_rug_bot_suspects_for_dashboard(50) if profiler and MODE_RUG_PULLER else [],
            "rug_settings": dict(RUG_SETTINGS),
            "config": dict(CONFIG),
            "presets": {k: True for k in get_all_presets()},
            "custom_presets": list(load_custom_presets().keys()),
            "wallet_dev": _wallet_dev_snapshot(),
            "startup_hints": _startup_hints(),
            **wsnap,
        }
    }, target=websocket)
    try:
        async for raw in websocket:
            try:
                msg = json.loads(raw)
                msg_type = msg.get("type")
                if msg_type == "config_update":
                    changed = handle_config_update(msg.get("data", {}))
                    if changed:
                        await broadcast_config()
                        await broadcast_log(f"⚙ Config: {changed}", "warn")
                elif msg_type == "preset":
                    if apply_preset(msg.get("name", "")):
                        await broadcast_config()
                        await broadcast_log(f"⚙ Preset: {msg.get('name')}", "warn")
                elif msg_type == "save_preset":
                    name = msg.get("name", "").strip()
                    if name:
                        save_custom_preset(name, msg.get("data"))
                        await send_to_dashboard({
                            "type": "presets_update",
                            "data": {"presets": {k: True for k in get_all_presets()},
                                     "custom_presets": list(load_custom_presets().keys())}
                        })
                        await broadcast_log(f"⚙ Saved preset: {name}", "warn")
                elif msg_type == "delete_preset":
                    name = msg.get("name", "")
                    if delete_custom_preset(name):
                        await send_to_dashboard({
                            "type": "presets_update",
                            "data": {"presets": {k: True for k in get_all_presets()},
                                     "custom_presets": list(load_custom_presets().keys())}
                        })
                elif msg_type == "manual_sell":
                    mint = msg.get("mint", "")
                    await manual_sell_position(mint)
                elif msg_type == "mode_select":
                    await handle_mode_select(msg, websocket)
                elif msg_type == "validate_field":
                    await handle_validate_field(msg, websocket)
                elif msg_type == "get_profiler_data":
                    if profiler:
                        await send_to_dashboard({
                            "type": "bot_suspects",
                            "data": get_rug_bot_suspects_for_dashboard(50)
                        }, target=websocket)
                        await send_to_dashboard({
                            "type": "profiler_stats",
                            "data": profiler.get_profiler_stats()
                        }, target=websocket)
                elif msg_type == "crack_bot":
                    wallet = msg.get("wallet", "")
                    if orchestrator and wallet:
                        sig = orchestrator.crack_specific_bot(wallet)
                        await send_to_dashboard({
                            "type": "bot_cracked",
                            "data": sig
                        }, target=websocket)
                elif msg_type == "approve_plan":
                    plan_id = msg.get("plan_id", "")
                    if orchestrator and plan_id:
                        orchestrator.approve_plan(plan_id)
                        await broadcast_log(f"☠ Plan {plan_id} APPROVED for deployment", "warn")
                        await send_to_dashboard({
                            "type": "orchestrator_status",
                            "data": orchestrator.get_status()
                        })
                elif msg_type == "reject_plan":
                    plan_id = msg.get("plan_id", "")
                    if orchestrator and plan_id:
                        orchestrator.reject_plan(plan_id)
                        await send_to_dashboard({
                            "type": "orchestrator_status",
                            "data": orchestrator.get_status()
                        })
                elif msg_type == "get_orchestrator_status":
                    if orchestrator:
                        await send_to_dashboard({
                            "type": "orchestrator_status",
                            "data": orchestrator.get_status()
                        }, target=websocket)
                elif msg_type == "prepare_mode_switch":
                    await prepare_mode_switch_session(websocket)
                elif msg_type == "plan_suspect":
                    wallet = (msg.get("wallet") or "").strip()
                    res = {
                        "ok": False,
                        "error": "Rug Puller mode is not active",
                        "wallet_requested": wallet,
                    }
                    try:
                        if not MODE_RUG_PULLER:
                            pass
                        elif profiler is None:
                            res = {
                                "ok": False,
                                "error": "Profiler not initialized",
                                "wallet_requested": wallet,
                            }
                        else:
                            if orchestrator is None:
                                orchestrator = RugOrchestrator(
                                    profiler, is_live=RUG_PULLER_LIVE,
                                    creator_blacklist=creator_blacklist,
                                    rug_settings=RUG_SETTINGS,
                                )
                                orchestrator.set_executor(executor)
                                if RUG_PULLER_LIVE and LIVE_KEYPAIR:
                                    orchestrator.set_master_wallet(LIVE_KEYPAIR)
                            if not orchestrator.running:
                                await orchestrator.start()
                            force = bool(msg.get("force", True))
                            res = orchestrator.generate_plan_for_wallet(
                                wallet, force=force
                            )
                    except Exception as e:
                        print(f"⚠ plan_suspect: {e}")
                        res = {
                            "ok": False,
                            "error": str(e),
                            "wallet_requested": wallet,
                        }
                    await send_to_dashboard({
                        "type": "plan_suspect_result",
                        "data": res,
                    }, target=websocket)
                    if res.get("ok") and orchestrator:
                        st = orchestrator.get_status()
                        plan_dict = res.get("plan") or {}
                        await send_to_dashboard({
                            "type": "plan_created",
                            "data": {
                                "plan": plan_dict,
                                "plans": st.get("plans", []),
                                "focus_plan_id": plan_dict.get("plan_id"),
                            },
                        })
                        await send_to_dashboard({
                            "type": "orchestrator_status",
                            "data": st,
                        })
                elif msg_type == "get_wallet_dev":
                    await send_to_dashboard({
                        "type": "wallet_dev",
                        "data": _wallet_dev_snapshot(),
                    }, target=websocket)
                elif msg_type == "update_rug_settings":
                    apply_rug_settings_update(msg.get("data") or {})
                    await send_to_dashboard({
                        "type": "rug_settings_saved",
                        "data": dict(RUG_SETTINGS),
                    })
                    await send_to_dashboard({
                        "type": "toast",
                        "data": {
                            "message": "Rug settings saved",
                            "level": "ok",
                            "ttl_ms": 2200,
                        },
                    })
                    if MODE_RUG_PULLER and profiler:
                        await send_to_dashboard({
                            "type": "bot_suspects",
                            "data": get_rug_bot_suspects_for_dashboard(50),
                        })
                elif msg_type == "pause_trading":
                    TRADING_PAUSED = bool(msg.get("paused", False))
                    state_label = "PAUSED" if TRADING_PAUSED else "RESUMED"
                    print(f"{'⏸' if TRADING_PAUSED else '▶'} Trading {state_label}")
                    await broadcast_log(f"{'⏸' if TRADING_PAUSED else '▶'} Trading {state_label}", "warn")
                    await send_to_dashboard({"type": "pause_status", "data": {"paused": TRADING_PAUSED}})
            except Exception as e:
                print(f"⚠ Dashboard msg error: {e}")
    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        dashboard_clients.discard(websocket)

async def send_to_dashboard(event, target=None):
    msg = json.dumps(event)
    targets = [target] if target else list(dashboard_clients)
    if not targets:
        return

    async def _safe_send(ws):
        try:
            await asyncio.wait_for(ws.send(msg), timeout=2.0)
        except (asyncio.TimeoutError, Exception):
            dashboard_clients.discard(ws)

    await asyncio.gather(*[_safe_send(ws) for ws in targets])

async def broadcast_config():
    await send_to_dashboard({"type": "config", "data": dict(CONFIG)})


_wallet_snapshot_cache: dict = {"chain_balance": None, "wallet_delta_sol": None, "session_wallet_baseline": None}
_wallet_snapshot_ts: float = 0.0
_WALLET_SNAPSHOT_TTL = float(os.environ.get("WALLET_SNAPSHOT_TTL_SEC", "5"))


async def wallet_chain_snapshot(force_rpc: bool = False) -> dict:
    """Cached RPC wallet balance — only calls RPC every WALLET_SNAPSHOT_TTL_SEC (default 5s).
    Trade-event broadcasts use the cache; the ticker forces a refresh."""
    global _wallet_snapshot_cache, _wallet_snapshot_ts
    now = time.time()
    if not force_rpc and (now - _wallet_snapshot_ts) < _WALLET_SNAPSHOT_TTL:
        return _wallet_snapshot_cache

    track = (
        (MODE_LIVE or (MODE_RUG_PULLER and RUG_PULLER_LIVE))
        and executor is not None
        and getattr(executor, "name", None) != "paper"
    )
    if not track:
        _wallet_snapshot_cache = {
            "chain_balance": None,
            "wallet_delta_sol": None,
            "session_wallet_baseline": None,
        }
        return _wallet_snapshot_cache
    bal = await get_live_balance()
    baseline = session_wallet_start_sol
    dlt = None
    if bal is not None and baseline is not None:
        dlt = bal - baseline
    _wallet_snapshot_cache = {
        "chain_balance": round(bal, 6) if bal is not None else None,
        "wallet_delta_sol": round(dlt, 6) if dlt is not None else None,
        "session_wallet_baseline": round(baseline, 6) if baseline is not None else None,
    }
    _wallet_snapshot_ts = now
    return _wallet_snapshot_cache


_last_broadcast_stats_time: float = 0.0

async def broadcast_stats():
    global _last_broadcast_stats_time
    now = time.time()
    if now - _last_broadcast_stats_time < 0.15:
        return  # rate limit: max ~6 broadcasts per second
    _last_broadcast_stats_time = now
    wr = (wins / (wins + losses) * 100) if (wins + losses) > 0 else 0
    all_pos = {**positions, **moonbags}
    latency = executor.latency_dict() if executor else {"zmq_ms": 0, "build_ms": 0, "land_ms": 0, "total_ms": 0}
    wsnap = await wallet_chain_snapshot()
    await send_to_dashboard({
        "type": "stats",
        "data": {
            "balance": round(paper_balance, 6), "pnl": round(realized_pnl, 6),
            "wins": wins, "losses": losses, "win_rate": round(wr, 1),
            "open_positions": len(positions),
            "moonbags": len(moonbags),
            "candidates": len(candidates),
            "blacklisted": len(creator_blacklist),
            "consecutive_losses": consecutive_losses,
            "cooldown_remaining": max(0, round(cooldown_until - time.time(), 1)),
            "positions": [p.to_dict() for p in all_pos.values()],
            "latency": latency,
            "executor_type": EXECUTOR_MODE,
            "paused": TRADING_PAUSED,
            "is_rug_puller": MODE_RUG_PULLER,
            "rug_puller_live": RUG_PULLER_LIVE,
            "profiler_stats": profiler.get_profiler_stats() if profiler else None,
            "wallet_dev": _wallet_dev_snapshot(),
            **wsnap,
        }
    })

async def broadcast_log(msg, level="info"):
    await send_to_dashboard({
        "type": "log",
        "data": {"msg": msg, "level": level, "time": datetime.now().strftime("%H:%M:%S")}
    })

# =====================================================
#  PERSISTENCE
# =====================================================

def load_creator_blacklist():
    global creator_blacklist
    if os.path.exists(BLACKLIST_CREATORS_FILE):
        try:
            with open(BLACKLIST_CREATORS_FILE, "r") as f:
                creator_blacklist = json.load(f)
            print(f"📋 Loaded {len(creator_blacklist)} blacklisted creators.")
        except Exception:
            creator_blacklist = {}

def save_creator_blacklist():
    try:
        with open(BLACKLIST_CREATORS_FILE, "w") as f:
            json.dump(creator_blacklist, f, indent=2)
    except Exception:
        pass

def add_to_creator_blacklist(creator, token_name):
    global creator_blacklist
    if not creator:
        return
    if creator in creator_blacklist:
        creator_blacklist[creator]["losses"] += 1
        creator_blacklist[creator]["tokens"].append(token_name)
    else:
        creator_blacklist[creator] = {"losses": 1, "last_rug": time.time(), "tokens": [token_name]}
    # Keep orchestrator's reference in sync
    if orchestrator is not None:
        orchestrator.set_creator_blacklist(creator_blacklist)
    save_creator_blacklist()

def log_trade(entry_time, exit_time, mint, name, symbol, creator,
              side, size_sol, entry_price, exit_price, pnl_sol, pnl_pct, reason):
    exists = os.path.exists(TRADE_JOURNAL_FILE)
    try:
        with open(TRADE_JOURNAL_FILE, "a", newline="") as f:
            w = csv.writer(f)
            if not exists:
                w.writerow(["entry_time","exit_time","mint","name","symbol","creator",
                            "side","size_sol","entry_price","exit_price","pnl_sol","pnl_pct","reason"])
            w.writerow([
                datetime.fromtimestamp(entry_time).isoformat(),
                datetime.fromtimestamp(exit_time).isoformat(),
                mint, name, symbol, (creator or "")[:12], side,
                f"{size_sol:.6f}", f"{entry_price:.12f}", f"{exit_price:.12f}",
                f"{pnl_sol:.6f}", f"{pnl_pct:.2f}", reason
            ])
    except Exception:
        pass

# =====================================================
#  UTILS
# =====================================================

def is_blacklisted(name, symbol):
    lower = (name or "").lower() + " " + (symbol or "").lower()
    return any(w in lower for w in BLACKLIST_WORDS)

def is_name_quality(name, symbol):
    n = (name or "").strip()
    s = (symbol or "").strip()
    if len(n) < MIN_NAME_LENGTH or len(n) > MAX_NAME_LENGTH:
        return False
    if len(s) < MIN_SYMBOL_LENGTH or len(s) > MAX_SYMBOL_LENGTH:
        return False
    if n.isupper() and len(n) > 10:
        return False
    return True

def load_keypair_from_file(path="keypair.txt"):
    with open(path, "r") as f:
        raw = f.read().strip()
    if raw.startswith("["):
        return Keypair.from_bytes(bytes(json.loads(raw)))
    return Keypair.from_bytes(b58decode(raw))


def live_keypair_b58_for_zmq() -> Optional[str]:
    """Base58 keypair bytes for ZmqExecutor → Rust (must match LegacyExecutor wallet)."""
    if LIVE_KEYPAIR is None:
        return None
    try:
        to_bytes = getattr(LIVE_KEYPAIR, "to_bytes", None)
        raw = to_bytes() if callable(to_bytes) else bytes(LIVE_KEYPAIR)
        return b58encode(raw).decode("ascii")
    except Exception:
        return None

# =====================================================
#  ASYNC TRADE
# =====================================================

async def live_buy_pump(mint, size_sol):
    """Execute a buy via the active executor backend.

    Priority fee resolved at trade time via resolve_parameters().
    """
    if not LIVE_TX_ENABLED:
        return "SIMULATED"
    if executor is None:
        return None
    return await resolved_buy(
        executor,
        mint,
        size_sol,
        _effective_slippage(),
        CONFIG,
        adaptive_state,
        keypair_b58=live_keypair_b58_for_zmq(),
    )

_SELL_PERSIST_TIMEOUT = float(os.environ.get("SELL_PERSIST_TIMEOUT_SEC", "45"))
_SELL_PERSIST_PAUSE = float(os.environ.get("SELL_PERSIST_PAUSE_SEC", "0.3"))


async def live_sell_pump(mint, pct="100%"):
    """Execute a sell via the active executor backend.

    Retries persistently until the sell confirms on chain or a wall-clock
    timeout is reached (SELL_PERSIST_TIMEOUT_SEC, default 45s).  Each round
    goes through the full Rust retry path (build+land × 3 internally).
    Priority fee resolved fresh on every attempt via resolve_parameters().
    """
    if not LIVE_TX_ENABLED and MODE_LIVE:
        return True
    if executor is None:
        return False

    import time as _time
    deadline = _time.monotonic() + _SELL_PERSIST_TIMEOUT
    attempt = 0

    while _time.monotonic() < deadline:
        attempt += 1
        sig = await resolved_sell(
            executor,
            mint,
            pct,
            _effective_sell_slippage(),
            CONFIG,
            adaptive_state,
            keypair_b58=live_keypair_b58_for_zmq(),
        )
        if sig is not None:
            if attempt > 1:
                print(f"[SELL] Confirmed on outer attempt {attempt} for {mint[:8]}...")
            return True

        remaining = deadline - _time.monotonic()
        if remaining <= 0:
            break
        pause = min(_SELL_PERSIST_PAUSE, remaining)
        print(
            f"[SELL] Rust returned None for {mint[:8]}... — retrying "
            f"(attempt {attempt}, {remaining:.1f}s left)"
        )
        await asyncio.sleep(pause)

    print(f"[SELL] FAILED after {attempt} outer attempts / {_SELL_PERSIST_TIMEOUT}s for {mint[:8]}...")
    return False

# =====================================================
#  CACHE JANITOR — Periodic TTLCache pruning
# =====================================================

async def cache_janitor():
    """Prune expired TTLCache entries every 30s to prevent memory bloat during 24/7 operation."""
    while True:
        await asyncio.sleep(30)
        try:
            n1 = seen_names.prune()
            n2 = recent_entry_wallets.prune()
            n3 = _grpc_dedup.prune()
            if n1 + n2 + n3 > 0:
                print(f"🧹 Cache prune: seen_names={n1}, recent_wallets={n2}, grpc_dedup={n3}")
        except Exception as e:
            print(f"⚠ Cache janitor error: {e}")


# =====================================================
#  FUNDING SOURCE RESOLVER — Background Sybil detection
# =====================================================

async def funding_source_resolver():
    """
    Background task: resolves funding sources for wallets queued by
    the SybilDetector's FundingTracker. Uses Helius getSignaturesForAddress
    to find the first incoming SOL transfer (the funding transaction).
    """
    global profiler, aio_session
    while True:
        await asyncio.sleep(2)
        try:
            if not profiler or not MODE_RUG_PULLER or not aio_session:
                continue
            sybil = profiler.sybil_detector
            batch = sybil.funding.pop_pending(batch_size=5)
            if not batch:
                continue
            for wallet in batch:
                try:
                    source = await _fetch_funding_source(wallet)
                    if source:
                        sybil.funding.record_source(wallet, source)
                except Exception as e:
                    pass  # Non-critical — skip silently
        except Exception as e:
            print(f"⚠ Funding resolver error: {e}")


async def _fetch_funding_source(wallet: str) -> Optional[str]:
    """
    Fetch the earliest transaction for a wallet to identify its funding source.
    Returns the sender of the first SOL transfer, or None.
    """
    global aio_session
    if not aio_session:
        return None

    rpc_url = HELIUS_RPC_URL
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getSignaturesForAddress",
        "params": [wallet, {"limit": 1, "commitment": "confirmed"}]
    }

    try:
        async with aio_session.post(rpc_url, json=payload,
                                     timeout=aiohttp.ClientTimeout(total=3)) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
            sigs = data.get("result", [])
            if not sigs:
                return None

            # Get the transaction details to find the sender
            sig = sigs[-1]["signature"]  # oldest (last when limit=1 is the only one)
            tx_payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getTransaction",
                "params": [sig, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}]
            }
            async with aio_session.post(rpc_url, json=tx_payload,
                                         timeout=aiohttp.ClientTimeout(total=3)) as tx_resp:
                if tx_resp.status != 200:
                    return None
                tx_data = await tx_resp.json()
                tx_result = tx_data.get("result")
                if not tx_result:
                    return None

                # Find the account that funded this wallet (first signer that isn't the wallet itself)
                account_keys = tx_result.get("transaction", {}).get("message", {}).get("accountKeys", [])
                for key in account_keys:
                    pubkey = key if isinstance(key, str) else key.get("pubkey", "")
                    signer = key.get("signer", False) if isinstance(key, dict) else False
                    if signer and pubkey != wallet:
                        return pubkey

                return None
    except Exception:
        return None


# =====================================================
#  POSITION MONITOR
# =====================================================

async def position_monitor():
    global positions, moonbags, paper_balance, realized_pnl, wins, losses
    global consecutive_losses, cooldown_until, trade_history

    while True:
        await asyncio.sleep(POSITION_CHECK_INTERVAL)
        now = time.time()
        to_close = []

        for mint, pos in list(positions.items()):
            if pos.closing:
                continue
            hold = now - pos.entry_time
            pnl = (pos.last_price / pos.entry_price - 1.0) * 100.0 if pos.entry_price > 0 else 0

            if hold >= CONFIG["max_hold"]:
                to_close.append((mint, pos, pnl, hold, "Time (monitor)"))
            elif pnl <= -CONFIG["sl"]:
                to_close.append((mint, pos, pnl, hold, "SL (monitor)"))
            elif pos.entry_vsol > 0 and pos.last_vsol > 0:
                rug = (pos.last_vsol / pos.entry_vsol - 1.0) * 100.0
                if rug <= -CONFIG["rug_drop"] or pos.last_vsol < CONFIG["rug_min_vsol"]:
                    to_close.append((mint, pos, pnl, hold, "RUG (monitor)"))
            # Stale price check: if no update in 10s and underwater, force close
            stale = (now - pos.last_update_time) > 10.0
            if stale and pnl < -5.0:
                to_close.append((mint, pos, pnl, hold, "STALE (no updates)"))

        for mint, mb in list(moonbags.items()):
            if mb.closing:
                continue
            hold = now - mb.entry_time
            pnl = (mb.last_price / mb.entry_price - 1.0) * 100.0 if mb.entry_price > 0 else 0
            peak_pnl = (mb.max_price / mb.entry_price - 1.0) * 100.0 if mb.entry_price > 0 else 0

            if hold >= CONFIG["moonbag_expire"]:
                to_close.append((mint, mb, pnl, hold, "Moonbag expire"))
            elif pnl <= -CONFIG["moonbag_sl"]:
                to_close.append((mint, mb, pnl, hold, "Moonbag SL"))
            elif peak_pnl > 0 and mb.last_price <= mb.max_price * (1.0 - CONFIG["moonbag_trail"] / 100.0):
                to_close.append((mint, mb, pnl, hold, "Moonbag trail"))

        for mint, pos, pnl_pct, hold, reason in to_close:
            is_mb = pos.is_moonbag
            source = moonbags if is_mb else positions
            async with state_lock:
                if mint not in source or pos.closing:
                    continue
                pos.closing = True

            active = pos.remaining_size_sol
            pnl_sol = active * (pnl_pct / 100.0)
            realized_pnl += pnl_sol

            is_profitable = pnl_sol >= 0
            mb_pct = CONFIG["moonbag_pct"]
            should_moonbag = (not is_mb and is_profitable and mb_pct > 0 and "RUG" not in reason)

            if is_profitable:
                wins += 1
                consecutive_losses = 0
            else:
                losses += 1
                consecutive_losses += 1
                if pos.creator and pnl_pct <= -CONFIG["sl"]:
                    add_to_creator_blacklist(pos.creator, pos.name)
                if consecutive_losses >= CONFIG["cooldown_losses"]:
                    cooldown_until = now + CONFIG["cooldown_sec"]

            sell_pct = (100 - mb_pct) if should_moonbag else 100
            sell_size = active * (sell_pct / 100.0)
            moonbag_size = active - sell_size if should_moonbag else 0

            if MODE_LIVE:
                sell_str = f"{sell_pct}%" if sell_pct < 100 else "100%"
                await live_sell_pump(mint, sell_str)

            exit_value = sell_size + (sell_size * (pnl_pct / 100.0))
            paper_balance += max(exit_value, 0.0)

            log_trade(pos.entry_time, now, mint, pos.name, pos.symbol, pos.creator,
                      "SELL", sell_size, pos.entry_price, pos.last_price,
                      sell_size * (pnl_pct / 100.0), pnl_pct, reason)

            mb_note = f" | 🌙 keeping {mb_pct:.0f}% moonbag" if should_moonbag else ""
            log_msg = f"🔔 EXIT {pos.name} ({pos.symbol}) – {reason} | PnL: {pnl_pct:+.1f}% | Hold: {hold:.1f}s{mb_note}"
            print(log_msg)
            level = "exit-win" if is_profitable else "exit-loss"
            await broadcast_log(log_msg, level)

            trade_history.append({
                "time": datetime.now().strftime("%H:%M:%S"), "timestamp": now * 1000,
                "name": pos.name, "symbol": pos.symbol,
                "pnl_pct": round(pnl_pct, 2),
                "pnl_sol": round(sell_size * (pnl_pct / 100.0), 6),
                "cum_pnl": round(realized_pnl, 6), "balance": round(paper_balance, 4),
                "hold_time": round(hold, 1), "reason": reason + (" 🌙" if should_moonbag else ""),
            })
            await send_to_dashboard({"type": "trade", "data": trade_history[-1]})
            source.pop(mint, None)

            if should_moonbag and moonbag_size > 0:
                moonbags[mint] = Position(
                    mint=mint, name=pos.name, symbol=pos.symbol,
                    size_sol=moonbag_size, entry_price=pos.entry_price,
                    entry_value_sol=moonbag_size, entry_time=pos.entry_time,
                    entry_vsol=pos.entry_vsol, last_price=pos.last_price,
                    max_price=pos.max_price, last_vsol=pos.last_vsol,
                    creator=pos.creator, is_moonbag=True,
                    remaining_size_sol=moonbag_size,
                    last_update_time=now,
                )
                print(f"🌙 Moonbag created: {pos.name} ({moonbag_size:.4f} SOL)")

            print(f"✅ Closed {pos.name} | Bal: {paper_balance:.4f} | W/L: {wins}/{losses}")
            await broadcast_log(f"✅ Closed {pos.name} | Bal: {paper_balance:.4f}", level)

        if to_close:
            await broadcast_stats()

# =====================================================
#  gRPC SIDECAR ZMQ LISTENER
# =====================================================

async def grpc_zmq_listener():
    """
    Listen for gRPC sightings from the Node.js sidecar via ZMQ SUB.
    Converts sidecar messages to PumpPortal-compatible format and
    dispatches to handle_new_token / handle_trade_event.
    Runs in parallel with PumpPortal WS for redundancy.
    """
    import sys
    grpc_port = 5558
    if sys.platform == "win32":
        grpc_addr = f"tcp://127.0.0.1:{grpc_port}"
    else:
        grpc_addr = "ipc:///tmp/bigal_grpc_sidecar.ipc"

    ctx = zmq.Context.instance()
    sub = ctx.socket(zmq.SUB)
    sub.setsockopt(zmq.RCVHWM, 500)
    sub.setsockopt_string(zmq.SUBSCRIBE, "")

    try:
        sub.connect(grpc_addr)
    except Exception as e:
        print(f"⚠ gRPC sidecar ZMQ connect failed ({grpc_addr}): {e}")
        print("  → Sidecar not running? Start with: cd sidecar && npm start")
        return

    print(f"📡 gRPC sidecar listener connected: {grpc_addr}")
    await broadcast_log("📡 gRPC sidecar listener connected", "info")

    poller = zmq.Poller()
    poller.register(sub, zmq.POLLIN)

    grpc_events = 0
    while True:
        try:
            # Non-blocking poll with 100ms timeout to keep asyncio responsive
            socks = dict(poller.poll(100))
            if sub not in socks:
                await asyncio.sleep(0)  # yield to event loop
                continue

            raw = sub.recv_string(zmq.NOBLOCK)
            msg = json.loads(raw)

            # Dedup: skip if PumpPortal already delivered this event
            mint = msg.get("mint")
            tx_type = msg.get("txType")
            dedup_key = f"{mint}:{tx_type}:{msg.get('traderPublicKey', '')}"
            if _grpc_dedup.contains(dedup_key):
                continue
            _grpc_dedup.set(dedup_key, True)

            grpc_events += 1
            if grpc_events % 50 == 0:
                print(f"⚡ gRPC sidecar: {grpc_events} events processed")

            # Dispatch to existing handlers (ws=None since gRPC doesn't need WS subscriptions)
            if tx_type == "create":
                await handle_new_token(msg, ws=None)
            elif tx_type in ("buy", "sell"):
                await handle_trade_event(msg)

        except zmq.ZMQError as e:
            if e.errno == zmq.EAGAIN:
                await asyncio.sleep(0)
                continue
            print(f"⚠ gRPC ZMQ error: {e}")
            await asyncio.sleep(1)
        except json.JSONDecodeError:
            continue
        except Exception as e:
            print(f"⚠ gRPC listener error [{type(e).__name__}]: {e}")
            await asyncio.sleep(0.1)


# =====================================================
#  CORE BOT LOGIC
# =====================================================

async def handle_new_token(msg, ws):
    global candidates, subscribed_trades

    mint = msg.get("mint")
    if not mint:
        return
    if msg.get("txType") != "create":
        return

    # In rug puller mode, feed ALL tokens to profiler before any filtering
    if MODE_RUG_PULLER and profiler is not None:
        profiler.on_token_create(msg)
        # Subscribe to trades for this token so profiler sees all activity
        if mint not in subscribed_trades and ws is not None:
            await ws.send(json.dumps({"method": "subscribeTokenTrade", "keys": [mint]}))
            subscribed_trades[mint] = True
        name = msg.get("name", "Unknown")
        symbol = msg.get("symbol", "???")
        try:
            vsol = float(msg.get("vSolInBondingCurve", 0))
        except Exception:
            vsol = 0
        log_msg = f"🆕 {name} ({symbol}) | vSOL: {vsol:.2f}"
        print(log_msg)
        await broadcast_log(log_msg, "info")
        return  # skip sniper candidate logic entirely

    if mint in candidates or mint in positions or mint in moonbags:
        return

    name = msg.get("name", "Unknown")
    symbol = msg.get("symbol", "???")

    if is_blacklisted(name, symbol):
        return
    if not is_name_quality(name, symbol):
        return

    now = time.time()
    name_key = name.strip().lower()
    if seen_names.contains(name_key):
        return
    seen_names.set(name_key, True)

    creator = msg.get("traderPublicKey") or msg.get("user") or msg.get("owner") or ""
    if creator and creator in creator_blacklist:
        return

    try:
        vsol = float(msg.get("vSolInBondingCurve", 0))
        vtokens = float(msg.get("vTokensInBondingCurve", 0))
    except Exception:
        return

    # Feed liquidity + vSOL slope data to AdaptiveState
    if vsol > 0:
        adaptive_state.update_liquidity(vsol)
        adaptive_state.update_vsol_slope(vsol, now)

    params = resolve_parameters(CONFIG, adaptive_state)
    if vsol < params["min_liquidity"] or vtokens <= 0:
        return

    candidates[mint] = Candidate(
        mint=mint, name=name, symbol=symbol, creator=creator,
        created_at=now, initial_vsol=vsol, initial_vtokens=vtokens,
        initial_price=vsol / vtokens, last_vsol=vsol, last_vtokens=vtokens,
    )

    # Feed to wallet profiler (always, regardless of mode)
    if profiler is not None:
        profiler.on_token_create(msg)
    if mint not in subscribed_trades and ws is not None:
        await ws.send(json.dumps({"method": "subscribeTokenTrade", "keys": [mint]}))
        subscribed_trades[mint] = True

    log_msg = f"🆕 {name} ({symbol}) | vSOL: {vsol:.2f}"
    print(log_msg)
    await broadcast_log(log_msg, "info")


async def maybe_open_from_candidate(mint, msg):
    global candidates, positions, paper_balance, cooldown_until

    cand = candidates.get(mint)
    if not cand:
        return

    now = time.time()
    if now < cooldown_until:
        return
    if TRADING_PAUSED:
        return

    try:
        vsol = float(msg.get("vSolInBondingCurve", 0))
        vtokens = float(msg.get("vTokensInBondingCurve", 0))
        if vsol <= 0 or vtokens <= 0:
            return
        price = vsol / vtokens
    except Exception:
        return

    if now - cand.created_at > CONFIG["candidate_age"]:
        candidates.pop(mint, None)
        return

    tx_type = msg.get("txType")
    trader = msg.get("traderPublicKey") or msg.get("user") or msg.get("owner") or "unknown"

    sol_amount = 0.0
    try:
        sol_amount = float(msg.get("solAmount", 0) or 0)
    except Exception:
        pass
    if sol_amount <= 0:
        sol_amount = abs(vsol - cand.last_vsol)
    cand.last_vsol = vsol
    cand.last_vtokens = vtokens

    if tx_type == "buy":
        if SKIP_CREATOR_AS_BUYER and trader == cand.creator and cand.creator:
            return
        cand.unique_buyers.add(str(trader))
        if sol_amount > 0:
            cand.buy_events.append((now, sol_amount))
    elif tx_type == "sell":
        if sol_amount > 0:
            cand.sell_events.append((now, sol_amount))

    cutoff = now - ENTRY_WINDOW_SEC
    cand.buy_events = [(t, s) for t, s in cand.buy_events if t >= cutoff]
    cand.sell_events = [(t, s) for t, s in cand.sell_events if t >= cutoff]

    buy_vol = sum(s for _, s in cand.buy_events)
    sell_vol = sum(s for _, s in cand.sell_events)
    total = buy_vol + sell_vol
    net = buy_vol - sell_vol
    sr = sell_vol / total if total > 0 else 0
    buyers = len(cand.unique_buyers)

    bc_prog = cand.bc_progress()
    min_buyers = int(CONFIG["entry_buyers"])

    if tx_type == "buy":
        print(f"👤 {cand.name} ({cand.symbol}) buyers={buyers}/{min_buyers} "
              f"vol={buy_vol:.2f} sell%={sr:.0%} BC={bc_prog:.1f}%")
        await broadcast_log(
            f"👤 {cand.name} ({cand.symbol}) buyers={buyers}/{min_buyers} "
            f"vol={buy_vol:.2f} sell%={sr:.0%} BC={bc_prog:.1f}%", "info")

    # Resolve strategy-mode parameters for entry filters
    strat_params = resolve_parameters(CONFIG, adaptive_state)

    if vsol < strat_params["min_liquidity"]:
        return

    pump = (price / cand.initial_price - 1.0) * 100.0
    if pump > CONFIG["max_pump"]:
        candidates.pop(mint, None)
        return

    if bc_prog < strat_params["entry_progress"]:
        return
    if bc_prog > CONFIG["bc_max"]:
        candidates.pop(mint, None)
        return

    if sr > CONFIG["sell_ratio"] or net < CONFIG["net_buy"]:
        return

    age = now - cand.created_at
    if age < CONFIG["min_age"]:
        return

    trade_size = CONFIG["trade_size"]
    if MODE_LIVE and trade_size < MIN_TRADE_SIZE_SOL:
        trade_size = MIN_TRADE_SIZE_SOL
    if buyers >= min_buyers and buy_vol >= CONFIG["entry_vol"] and mint not in positions:
        if len(positions) >= int(CONFIG["max_positions"]) or paper_balance < trade_size:
            return

        # --- ANTI-MANIPULATION CHECKS ---
        if CONFIG["anti_manip"]:
            # 1. Wallet diversity: check if buy amounts are suspiciously uniform
            buy_amounts = [s for _, s in cand.buy_events]
            if len(buy_amounts) >= 2:
                unique_amounts = len(set(round(a, 4) for a in buy_amounts))
                diversity = unique_amounts / len(buy_amounts)
                if diversity < CONFIG["min_wallet_diversity"]:
                    print(f"🛡 SKIP {cand.name} — fake volume (diversity={diversity:.2f})")
                    await broadcast_log(f"🛡 SKIP {cand.name} — low wallet diversity", "warn")
                    return

            # 2. Repeat wallet detection: same wallets across recent entries
            buyer_list = list(cand.unique_buyers)
            max_repeats = int(CONFIG["max_repeat_wallets"])
            for wallet in buyer_list:
                count = recent_entry_wallets.get(wallet, 0)
                if count is not None and count >= max_repeats:
                    print(f"🛡 SKIP {cand.name} — repeat wallet {wallet[:8]}...")
                    await broadcast_log(f"🛡 SKIP {cand.name} — repeat wallet detected", "warn")
                    return

            # 3. Random skip: make bot unpredictable so filters can't be reverse-engineered
            skip_pct = CONFIG["random_skip_pct"]
            if skip_pct > 0 and random.randint(1, 100) <= skip_pct:
                print(f"🎲 Random skip: {cand.name} (anti-snipe)")
                await broadcast_log(f"🎲 Random skip: {cand.name}", "info")
                return

            # Track wallets for repeat detection
            for wallet in buyer_list:
                prev = recent_entry_wallets.get(wallet, 0) or 0
                recent_entry_wallets.set(wallet, prev + 1)

        pos = Position(
            mint=mint, name=cand.name, symbol=cand.symbol,
            size_sol=trade_size, entry_price=price,
            entry_value_sol=trade_size, entry_time=now,
            entry_vsol=vsol, last_price=price, max_price=price,
            last_vsol=vsol, creator=cand.creator,
        )
        paper_balance -= trade_size
        positions[mint] = pos

        log_msg = (f"🟢 ENTRY {cand.name} ({cand.symbol}) | {buyers} buyers | "
                   f"{buy_vol:.2f} SOL | pump={pump:.1f}% | BC={bc_prog:.1f}% | age={age:.1f}s")
        print(log_msg)
        await broadcast_log(log_msg, "entry")
        await send_to_dashboard({"type": "entry", "data": {
            "name": cand.name, "symbol": cand.symbol, "mint": mint[:12],
            "size": trade_size, "buyers": buyers,
            "pump_pct": round(pump, 1), "bc_progress": round(bc_prog, 1)}})

        if MODE_LIVE:
            sig = await live_buy_pump(mint, trade_size)
            if sig is None:
                paper_balance += trade_size
                positions.pop(mint, None)
                return
            pos.buy_tx_sig = sig

        candidates.pop(mint, None)
        await broadcast_stats()


async def handle_trade_event(msg):
    global positions, moonbags, paper_balance, realized_pnl, wins, losses
    global consecutive_losses, cooldown_until, trade_history

    mint = msg.get("mint")
    if not mint:
        return

    # Feed EVERY trade to the wallet profiler (regardless of mode)
    if profiler is not None:
        profiler.on_trade(msg)

    # In rug puller mode, check if target bot hit a honeypot, then skip sniper logic
    if MODE_RUG_PULLER:
        if orchestrator:
            kill_signal = orchestrator.on_trade_event(msg)
            if kill_signal:
                # TARGET BOT DETECTED — trigger kill chain via executor
                print(f"☠ KILL CHAIN TRIGGERED: {kill_signal['plan_id']} | "
                      f"pool={kill_signal['sol_in_pool']:.4f} SOL")
                await broadcast_log(
                    f"☠ KILL CHAIN: target bot bought honeypot! Executing rug...", "exit-win")
                if executor and RUG_PULLER_LIVE:
                    if hasattr(executor, 'kill'):
                        plan = orchestrator.planner.plans.get(kill_signal["plan_id"])
                        tip_sol = (plan.jito_tip_lamports / 1e9) if plan else 0.005
                        kb58 = live_keypair_b58_for_zmq()
                        await executor.kill(kill_signal["mint"], tip_sol, keypair_b58=kb58)
                    else:
                        await executor.sell(
                            kill_signal["mint"], "100%", 15, 0.005,
                            keypair_b58=live_keypair_b58_for_zmq(),
                        )
        return

    if mint in candidates and mint not in positions:
        await maybe_open_from_candidate(mint, msg)
        if mint not in positions:
            return

    # Real-time moonbag checks — every tick, not just every 1.5s
    if mint in moonbags:
        try:
            mb = moonbags[mint]
            vsol = float(msg.get("vSolInBondingCurve", 0))
            vtokens = float(msg.get("vTokensInBondingCurve", 0))
            if vsol > 0 and vtokens > 0:
                price_mb = vsol / vtokens
                old_price_mb = mb.last_price
                mb.last_price = price_mb
                mb.last_vsol = vsol
                mb.last_update_time = time.time()
                if price_mb > mb.max_price:
                    mb.max_price = price_mb

                if mb.closing:
                    pass  # already being closed, skip exit checks
                else:
                    # Check moonbag exit conditions in real-time
                    mb_pnl = (price_mb / mb.entry_price - 1.0) * 100.0 if mb.entry_price > 0 else 0
                    mb_hold = time.time() - mb.entry_time
                    mb_reason = None

                    # Moonbag SL
                    if mb_pnl <= -CONFIG["moonbag_sl"]:
                        mb_reason = "Moonbag SL"
                    # Moonbag trail
                    elif mb.max_price > mb.entry_price:
                        mb_peak = (mb.max_price / mb.entry_price - 1.0) * 100.0
                        if mb_peak > 0 and price_mb <= mb.max_price * (1.0 - CONFIG["moonbag_trail"] / 100.0):
                            mb_reason = "Moonbag trail"
                    # Moonbag velocity
                    elif CONFIG["velocity_check"] and old_price_mb > 0:
                        mb_tick = (price_mb / old_price_mb - 1.0) * 100.0
                        if mb_tick <= -CONFIG["velocity_drop"]:
                            mb_reason = "Moonbag velocity"
                    # Moonbag expire
                    elif mb_hold >= CONFIG["moonbag_expire"]:
                        mb_reason = "Moonbag expire"

                    if mb_reason:
                        mb.closing = True  # lock before await to prevent double-sell
                        active_mb = mb.remaining_size_sol
                        pnl_sol_mb = active_mb * (mb_pnl / 100.0)
                        realized_pnl += pnl_sol_mb
                        if pnl_sol_mb >= 0:
                            wins += 1; consecutive_losses = 0
                        else:
                            losses += 1; consecutive_losses += 1
                        if MODE_LIVE: await live_sell_pump(mint)
                        paper_balance += max(active_mb + pnl_sol_mb, 0.0)
                        log_trade(mb.entry_time, time.time(), mint, mb.name, mb.symbol, mb.creator,
                                  "SELL", active_mb, mb.entry_price, price_mb, pnl_sol_mb, mb_pnl, mb_reason)
                        log_msg = f"🔔 EXIT {mb.name} ({mb.symbol}) – {mb_reason} | PnL: {mb_pnl:+.1f}% | Hold: {mb_hold:.1f}s"
                        print(log_msg)
                        level = "exit-win" if pnl_sol_mb >= 0 else "exit-loss"
                        await broadcast_log(log_msg, level)
                        trade_history.append({
                            "time": datetime.now().strftime("%H:%M:%S"), "timestamp": time.time() * 1000,
                            "name": mb.name, "symbol": mb.symbol,
                            "pnl_pct": round(mb_pnl, 2), "pnl_sol": round(pnl_sol_mb, 6),
                            "cum_pnl": round(realized_pnl, 6), "balance": round(paper_balance, 4),
                            "hold_time": round(mb_hold, 1), "reason": mb_reason,
                        })
                        await send_to_dashboard({"type": "trade", "data": trade_history[-1]})
                        moonbags.pop(mint, None)
                        print(f"✅ Closed {mb.name} | Bal: {paper_balance:.4f} | W/L: {wins}/{losses}")
                        await broadcast_log(f"✅ Closed {mb.name} | Bal: {paper_balance:.4f}", level)
                        await broadcast_stats()
        except Exception:
            pass

    if mint not in positions:
        return

    pos = positions[mint]
    if pos.closing:
        return  # already being closed by another coroutine

    try:
        vsol = float(msg.get("vSolInBondingCurve", 0))
        vtokens = float(msg.get("vTokensInBondingCurve", 0))
        if vsol <= 0 or vtokens <= 0:
            return
        price = vsol / vtokens
    except Exception:
        return

    # Velocity check — detect sudden price crash
    if CONFIG["velocity_check"] and pos.prev_price > 0:
        tick_drop = (price / pos.prev_price - 1.0) * 100.0
        if tick_drop <= -CONFIG["velocity_drop"]:
            async with state_lock:
                if pos.closing:
                    return
                pos.closing = True
            now = time.time()
            pnl_pct = (price / pos.entry_price - 1.0) * 100.0
            hold = now - pos.entry_time
            active = pos.remaining_size_sol
            pnl_sol = active * (pnl_pct / 100.0)
            realized_pnl += pnl_sol
            if pnl_sol >= 0:
                wins += 1; consecutive_losses = 0
            else:
                losses += 1; consecutive_losses += 1
                if pos.creator: add_to_creator_blacklist(pos.creator, pos.name)
                if consecutive_losses >= CONFIG["cooldown_losses"]:
                    cooldown_until = now + CONFIG["cooldown_sec"]
            if MODE_LIVE: await live_sell_pump(mint)
            paper_balance += max(active + pnl_sol, 0.0)
            log_trade(pos.entry_time, now, mint, pos.name, pos.symbol, pos.creator,
                      "SELL", active, pos.entry_price, price, pnl_sol, pnl_pct, "VelocityDump")
            log_msg = f"⚡ VELOCITY EXIT {pos.name} ({pos.symbol}) – dropped {tick_drop:.1f}% in one tick | PnL: {pnl_pct:+.1f}%"
            print(log_msg)
            level = "exit-win" if pnl_sol >= 0 else "exit-loss"
            await broadcast_log(log_msg, level)
            trade_history.append({
                "time": datetime.now().strftime("%H:%M:%S"), "timestamp": now * 1000,
                "name": pos.name, "symbol": pos.symbol,
                "pnl_pct": round(pnl_pct, 2), "pnl_sol": round(pnl_sol, 6),
                "cum_pnl": round(realized_pnl, 6), "balance": round(paper_balance, 4),
                "hold_time": round(hold, 1), "reason": "VelocityDump",
            })
            await send_to_dashboard({"type": "trade", "data": trade_history[-1]})
            positions.pop(mint, None)
            print(f"✅ Closed {pos.name} | Bal: {paper_balance:.4f} | W/L: {wins}/{losses}")
            await broadcast_log(f"✅ Closed {pos.name} | Bal: {paper_balance:.4f}", level)
            await broadcast_stats()
            return

    pos.prev_price = price
    pos.last_price = price
    pos.last_vsol = vsol
    pos.last_update_time = time.time()
    if price > pos.max_price:
        pos.max_price = price

    # Feed vSOL slope data to AdaptiveState on every trade tick
    adaptive_state.update_vsol_slope(vsol)

    now = time.time()
    pnl_pct = (price / pos.entry_price - 1.0) * 100.0
    hold = now - pos.entry_time
    peak = (pos.max_price / pos.entry_price - 1.0) * 100.0

    rug_drop = (vsol / pos.entry_vsol - 1.0) * 100.0 if pos.entry_vsol > 0 else 0
    rug = rug_drop <= -CONFIG["rug_drop"] or vsol < CONFIG["rug_min_vsol"]

    # Partial TP1
    if not pos.partial_sold and pnl_pct >= CONFIG["tp1"]:
        async with state_lock:
            if pos.closing:
                return
            pos.closing = True
        if MODE_LIVE:
            await live_sell_pump(mint, "50%")
        partial_pnl = (pos.size_sol * 0.5) * (pnl_pct / 100.0)
        realized_pnl += partial_pnl
        pos.partial_sold = True
        pos.remaining_size_sol = pos.size_sol * 0.5
        paper_balance += (pos.size_sol * 0.5) + partial_pnl

        log_msg = f"💰 TP1 {pos.name} ({pos.symbol}) at +{pnl_pct:.1f}%"
        print(log_msg)
        await broadcast_log(log_msg, "exit-win")
        trade_history.append({
            "time": datetime.now().strftime("%H:%M:%S"), "timestamp": now * 1000,
            "name": pos.name, "symbol": pos.symbol,
            "pnl_pct": round(pnl_pct, 2), "pnl_sol": round(partial_pnl, 6),
            "cum_pnl": round(realized_pnl, 6), "balance": round(paper_balance, 4),
            "hold_time": round(hold, 1), "reason": "TP1",
        })
        await send_to_dashboard({"type": "trade", "data": trade_history[-1]})
        await broadcast_stats()
        pos.closing = False  # unlock — TP1 is partial, position stays open
        return

    # Full exit
    tp2 = pnl_pct >= CONFIG["tp2"]
    sl = pnl_pct <= -CONFIG["sl"]
    timed = hold >= CONFIG["max_hold"]
    trail_on = peak >= CONFIG["trail_min"]
    trailed = trail_on and price <= pos.max_price * (1.0 - CONFIG["trail_peak"] / 100.0)

    if not (rug or tp2 or sl or timed or trailed):
        return

    async with state_lock:
        if pos.closing:
            return
        pos.closing = True
    reason = "RUG" if rug else "TP2" if tp2 else "Trail" if trailed else "SL" if sl else "Time"
    active = pos.remaining_size_sol
    pnl_sol = active * (pnl_pct / 100.0)
    is_profitable = pnl_sol >= 0

    mb_pct = CONFIG["moonbag_pct"]
    should_moonbag = is_profitable and mb_pct > 0 and "RUG" not in reason
    sell_pct = (100 - mb_pct) if should_moonbag else 100
    sell_size = active * (sell_pct / 100.0)
    moonbag_size = active - sell_size if should_moonbag else 0

    realized_pnl += sell_size * (pnl_pct / 100.0)

    if is_profitable:
        wins += 1
        consecutive_losses = 0
    else:
        losses += 1
        consecutive_losses += 1
        if pos.creator and (rug or pnl_pct <= -CONFIG["sl"]):
            add_to_creator_blacklist(pos.creator, pos.name)
        if consecutive_losses >= CONFIG["cooldown_losses"]:
            cooldown_until = now + CONFIG["cooldown_sec"]

    if MODE_LIVE:
        sell_str = f"{sell_pct}%" if sell_pct < 100 else "100%"
        await live_sell_pump(mint, sell_str)

    paper_balance += max(sell_size + sell_size * (pnl_pct / 100.0), 0.0)
    log_trade(pos.entry_time, now, mint, pos.name, pos.symbol, pos.creator,
              "SELL", sell_size, pos.entry_price, price,
              sell_size * (pnl_pct / 100.0), pnl_pct, reason)

    mb_note = f" | 🌙 {mb_pct:.0f}% moonbag" if should_moonbag else ""
    log_msg = f"🔔 EXIT {pos.name} ({pos.symbol}) – {reason} | PnL: {pnl_pct:+.1f}% | Hold: {hold:.1f}s{mb_note}"
    print(log_msg)
    level = "exit-win" if is_profitable else "exit-loss"
    await broadcast_log(log_msg, level)

    trade_history.append({
        "time": datetime.now().strftime("%H:%M:%S"), "timestamp": now * 1000,
        "name": pos.name, "symbol": pos.symbol,
        "pnl_pct": round(pnl_pct, 2),
        "pnl_sol": round(sell_size * (pnl_pct / 100.0), 6),
        "cum_pnl": round(realized_pnl, 6), "balance": round(paper_balance, 4),
        "hold_time": round(hold, 1), "reason": reason + (" 🌙" if should_moonbag else ""),
    })
    await send_to_dashboard({"type": "trade", "data": trade_history[-1]})
    positions.pop(mint, None)

    if should_moonbag and moonbag_size > 0:
        moonbags[mint] = Position(
            mint=mint, name=pos.name, symbol=pos.symbol,
            size_sol=moonbag_size, entry_price=pos.entry_price,
            entry_value_sol=moonbag_size, entry_time=pos.entry_time,
            entry_vsol=pos.entry_vsol, last_price=price,
            max_price=pos.max_price, last_vsol=vsol,
            creator=pos.creator, is_moonbag=True,
            remaining_size_sol=moonbag_size,
            last_update_time=now,
        )
        print(f"🌙 Moonbag: {pos.name} ({moonbag_size:.4f} SOL)")

    print(f"✅ Closed {pos.name} | Bal: {paper_balance:.4f} | W/L: {wins}/{losses}")
    await broadcast_log(f"✅ Closed {pos.name} | Bal: {paper_balance:.4f}", level)
    await broadcast_stats()

# =====================================================
#  HTTP SERVER + DASHBOARD TICKER + CLI
# =====================================================

def start_http_server():
    """Serve dashboard HTML via HTTP so it works like a local app."""
    serve_dir = os.path.dirname(os.path.abspath(__file__))
    class Handler(SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=serve_dir, **kwargs)
        def end_headers(self):
            # Prevent browser from caching dashboard HTML
            self.send_header("Cache-Control", "no-store, no-cache, must-revalidate")
            self.send_header("Pragma", "no-cache")
            self.send_header("Expires", "0")
            super().end_headers()
        def log_message(self, format, *args):
            pass
    server = HTTPServer((DASHBOARD_BIND_HOST, HTTP_PORT), Handler)
    server.serve_forever()

async def dashboard_ticker():
    global paper_balance
    balance_check_counter = 0
    while True:
        await asyncio.sleep(1)
        if dashboard_clients:
            balance_check_counter += 1
            if MODE_LIVE and balance_check_counter >= 10:
                balance_check_counter = 0
                bal = await get_live_balance()
                if bal is not None:
                    paper_balance = bal
            await broadcast_stats()
            # Broadcast profiler + orchestrator data in rug puller mode
            if MODE_RUG_PULLER and profiler:
                await send_to_dashboard({
                    "type": "bot_suspects",
                    "data": get_rug_bot_suspects_for_dashboard(50)
                })
                if orchestrator:
                    await send_to_dashboard({
                        "type": "orchestrator_status",
                        "data": orchestrator.get_status()
                    })
                await send_to_dashboard({
                    "type": "wallet_dev",
                    "data": _wallet_dev_snapshot(),
                })

def print_cli():
    global last_dashboard_time
    now = time.time()
    if now - last_dashboard_time < 10:
        return
    last_dashboard_time = now
    wr = (wins / (wins + losses) * 100) if (wins + losses) > 0 else 0
    print(f"\n--- CUSTOM {'LIVE' if MODE_LIVE else 'PAPER'} | Bal: {paper_balance:.4f} | "
          f"PnL: {realized_pnl:+.4f} | W/L: {wins}/{losses} ({wr:.0f}%) | "
          f"Pos: {len(positions)} | MB: {len(moonbags)} | BL: {len(creator_blacklist)} ---\n")

# =====================================================
#  EMERGENCY + MAIN
# =====================================================

async def emergency_close_all():
    global positions, moonbags, paper_balance, realized_pnl, wins, losses
    for src in [positions, moonbags]:
        for mint, pos in list(src.items()):
            # Prevent double-sell race with concurrent monitor/manual paths
            async with state_lock:
                if pos.closing:
                    continue
                pos.closing = True
            active = pos.remaining_size_sol
            pnl = (pos.last_price / pos.entry_price - 1.0) * 100.0 if pos.entry_price > 0 else 0
            pnl_sol = active * (pnl / 100.0)
            if MODE_LIVE:
                await live_sell_pump(mint)
            realized_pnl += pnl_sol
            if pnl_sol >= 0: wins += 1
            else: losses += 1
            paper_balance += max(active + pnl_sol, 0.0)
            src.pop(mint, None)
            print(f"✅ Emergency closed {pos.name} | PnL: {pnl_sol:+.4f} SOL")
    # Stop child processes (Rust executor, gRPC sidecar)
    proc_mgr.stop_all()

async def get_live_balance():
    """Fetch actual SOL balance from chain via executor."""
    if executor is None:
        return None
    try:
        return await executor.get_balance()
    except Exception:
        return None

def load_wallet_from_input(wallet_input):
    """Load keypair from base58 private key or JSON array."""
    global LIVE_KEYPAIR, LIVE_CLIENT
    wallet_input = wallet_input.strip()
    try:
        if wallet_input.startswith("["):
            LIVE_KEYPAIR = Keypair.from_bytes(bytes(json.loads(wallet_input)))
        else:
            LIVE_KEYPAIR = Keypair.from_bytes(b58decode(wallet_input))
        LIVE_CLIENT = Client(HELIUS_RPC_URL)
        return str(LIVE_KEYPAIR.pubkey())
    except Exception as e:
        print(f"❌ Invalid wallet: {e}")
        return None

async def run_bot():
    global MODE_LIVE, LIVE_CLIENT, LIVE_KEYPAIR, aio_session, paper_balance
    global executor, EXECUTOR_MODE, mode_selected_event

    load_creator_blacklist()

    aio_session = aiohttp.ClientSession()

    # Start HTTP server for dashboard
    http_thread = threading.Thread(target=start_http_server, daemon=True)
    http_thread.start()

    print(f"\n{'='*65}")
    print(f" BIGAL SNIPER — CUSTOMIZABLE EDITION")
    print(f" Dashboard HTTP: http://{DASHBOARD_BIND_HOST}:{HTTP_PORT}  (open from your PC if bind is 0.0.0.0: use http://<server-public-ip>:{HTTP_PORT})")
    print(f" Waiting for mode selection in dashboard...")
    print(f"{'='*65}\n")

    srv = await websockets.serve(dashboard_handler, DASHBOARD_BIND_HOST, DASHBOARD_WS_PORT)
    print(f"📊 Dashboard: http://{DASHBOARD_BIND_HOST}:{HTTP_PORT}/{DASHBOARD_HTML}")

    cache_bust = int(time.time())
    url = f"http://127.0.0.1:{HTTP_PORT}/{DASHBOARD_HTML}?v={cache_bust}"
    webbrowser.open(url)

    # Wait for mode selection from dashboard UI
    mode_selected_event = asyncio.Event()
    print("⏳ Waiting for mode selection in dashboard...")
    await mode_selected_event.wait()

    proc_mgr.pin_python_main()

    mode_label = "PAPER"
    if MODE_RUG_PULLER:
        mode_label = "RUG PULLER (LIVE)" if RUG_PULLER_LIVE else "RUG PULLER (TEST)"
    elif MODE_LIVE:
        mode_label = "LIVE (RUST)" if EXECUTOR_MODE == "zmq_rust" else "LIVE (LEGACY)"
    print(f"✅ Mode: {mode_label} — starting PumpPortal stream...")

    asyncio.create_task(dashboard_ticker())
    asyncio.create_task(position_monitor())
    asyncio.create_task(cache_janitor())
    asyncio.create_task(funding_source_resolver())
    asyncio.create_task(grpc_zmq_listener())

    try:
        while True:
            try:
                async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=10) as ws:
                    await ws.send(json.dumps({"method": "subscribeNewToken"}))
                    # Re-subscribe to any active candidate trades
                    for mint in list(candidates.keys()):
                        await ws.send(json.dumps({"method": "subscribeTokenTrade", "keys": [mint]}))
                    for mint in list(positions.keys()):
                        await ws.send(json.dumps({"method": "subscribeTokenTrade", "keys": [mint]}))
                    for mint in list(moonbags.keys()):
                        await ws.send(json.dumps({"method": "subscribeTokenTrade", "keys": [mint]}))
                    print("📡 Subscribed: NEW TOKENS\n")
                    await broadcast_log("📡 Connected to PumpPortal", "info")

                    async for raw in ws:
                        try:
                            msg = json.loads(raw)
                        except Exception:
                            continue
                        if not isinstance(msg, dict):
                            continue
                        try:
                            tx = msg.get("txType")
                            if tx == "create":
                                await handle_new_token(msg, ws)
                            elif tx in ("buy", "sell"):
                                await handle_trade_event(msg)
                            print_cli()
                        except (websockets.exceptions.ConnectionClosedError,
                                websockets.exceptions.ConnectionClosed):
                            # ws.send() inside handlers failed — break to reconnect
                            print("⚠ PumpPortal WS closed during handler — reconnecting")
                            break
                        except Exception as e:
                            # Don't let a single message crash the loop
                            import traceback
                            print(f"⚠ Handler error [{type(e).__name__}]: {e}")
                            traceback.print_exc()
                            await broadcast_log(f"⚠ Handler error [{type(e).__name__}]: {e}", "warn")
            except (websockets.exceptions.ConnectionClosedError,
                    websockets.exceptions.ConnectionClosed,
                    websockets.exceptions.InvalidStatusCode,
                    ConnectionError, OSError) as e:
                print(f"\n⚠ WebSocket disconnected: {e}")
                await broadcast_log("⚠ WebSocket disconnected — reconnecting...", "warn")
                await asyncio.sleep(3)
            except Exception as e:
                print(f"\n❌ Unexpected error: {e}")
                await broadcast_log(f"❌ Error: {e} — reconnecting...", "warn")
                await asyncio.sleep(5)
    finally:
        if aio_session:
            await aio_session.close()
        srv.close()
        await srv.wait_closed()
        proc_mgr.stop_all()

def main():
    # Pin Python bot to a dedicated CPU core on Linux (avoids contention with Rust/Node)
    # Layout: Core 0 = OS, Core 1 = Python, Core 2 = Node sidecar, Core 3 = Rust
    try:
        cpus = sorted(os.sched_getaffinity(0))
        if len(cpus) >= 4:
            os.sched_setaffinity(0, {cpus[1]})
            print(f"📌 Python bot pinned to CPU {cpus[1]}")
    except (OSError, AttributeError):
        pass  # Windows or insufficient permissions

    try:
        asyncio.run(run_bot())
    except KeyboardInterrupt:
        print("\n⚠ CTRL+C — closing...")
        try:
            asyncio.run(emergency_close_all())
        except Exception as e:
            print(f"❌ {e}")
        print("👋 BigAl Customizable exited.")

if __name__ == "__main__":
    main()
