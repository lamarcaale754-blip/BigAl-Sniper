"""
ExecutorBackend — Pluggable trade execution for BigAl Sniper.

Three backends:
  PaperExecutor   — instant "PAPER_SIM", no real tx (paper mode)
  LegacyExecutor  — wraps existing PumpPortal HTTP trade API (fallback)
  ZmqExecutor     — sends signals to Rust executor over ZeroMQ

ZMQ: on Linux, Python binds/connects via `ipc:///tmp/bigal_*.ipc` (see zmq_bridge).
Rust reads `ZMQ_SIGNAL_IPC` / `ZMQ_RESULT_IPC` for the same paths on Unix.
Set `JITO_BLOCK_ENGINE` to the NY (US East) Jito URL for `us-east-1` hosts.

Sell path tuning (ghost balance / RPC lag):
  ZMQ_SELL_RUST_ATTEMPTS — Python↔Rust sell retries before Legacy (default 8).
  ZMQ_SELL_BACKOFF_MAX — max seconds between those retries (default 6.0).
Rust executor: HELIUS_READ_RPC_URL — same paid Helius HTTP URL as buys for token balance polls;
  BIGAL_SELL_BALANCE_RETRIES / BIGAL_SELL_BALANCE_POLL_MS — patience inside each Rust sell build.
  ZMQ_SELL_WAIT_SEC — max wait per Rust sell build (default 90; must exceed internal balance polling).
  ZMQ_SELL_PREFLIGHT_MS — optional sleep before first sell attempt (default 0) to reduce RPC indexing races.
"""

import asyncio
import json
import os
import time
import uuid
from abc import ABC, abstractmethod
from typing import Optional, Dict

from strategy_mode import resolve_parameters, AdaptiveState

# Default Unix IPC under /tmp (Windows uses TCP 5555/5556 via zmq_bridge._ipc_addr)
DEFAULT_ZMQ_SIGNAL_IPC = "ipc:///tmp/bigal_signal.ipc"
DEFAULT_ZMQ_RESULT_IPC = "ipc:///tmp/bigal_result.ipc"

# Rust sell attempts before Legacy (each attempt is a full ZMQ round-trip).
_RUST_SELL_ATTEMPTS = max(1, int(os.environ.get("ZMQ_SELL_RUST_ATTEMPTS", "8")))
_RUST_SELL_BACKOFF_MAX = float(os.environ.get("ZMQ_SELL_BACKOFF_MAX", "6.0"))
_ZMQ_SELL_WAIT_SEC = float(os.environ.get("ZMQ_SELL_WAIT_SEC", "90"))
_ZMQ_SELL_PREFLIGHT_MS = max(0, int(os.environ.get("ZMQ_SELL_PREFLIGHT_MS", "0")))


def _rust_sell_should_retry(result: Optional[dict]) -> bool:
    """Whether to try another Rust sell. Legacy is last resort."""
    if result is None:
        return True
    if result.get("status") == "confirmed":
        return False
    err = (result.get("error") or "").lower()
    if not err:
        return True
    permanent = (
        "invalid mint",
        "signer error",
        "unknown action",
        "missing config",
    )
    if any(s in err for s in permanent):
        return False
    return True


class ExecutorBackend(ABC):
    """Abstract interface for trade execution."""

    name: str = "base"

    # Latency tracking (updated after each trade)
    last_zmq_latency_ms: float = 0
    last_build_latency_ms: float = 0
    last_land_latency_ms: float = 0
    last_total_latency_ms: float = 0

    @abstractmethod
    async def buy(self, mint: str, size_sol: float,
                  slippage: int, priority_fee: float,
                  keypair_b58: Optional[str] = None) -> Optional[str]:
        """Execute a buy. Returns tx signature or None on failure.
        keypair_b58: optional base58-encoded keypair for puppet wallet signing."""

    @abstractmethod
    async def sell(self, mint: str, pct: str,
                   slippage: int, priority_fee: float,
                   keypair_b58: Optional[str] = None) -> Optional[str]:
        """Execute a sell. pct is e.g. '50%' or '100%'. Returns tx sig or None.
        keypair_b58: optional base58-encoded keypair for puppet wallet signing."""

    @abstractmethod
    async def get_balance(self) -> Optional[float]:
        """Fetch wallet SOL balance."""

    @abstractmethod
    def is_connected(self) -> bool:
        """Whether the executor backend is currently reachable."""

    def latency_dict(self) -> Dict[str, float]:
        return {
            "zmq_ms": self.last_zmq_latency_ms,
            "build_ms": self.last_build_latency_ms,
            "land_ms": self.last_land_latency_ms,
            "total_ms": self.last_total_latency_ms,
        }


# ────────��────────────────────────────────────
#  PAPER EXECUTOR
# ─────────────────────────────────────────────

class PaperExecutor(ExecutorBackend):
    """Instant simulated execution for paper trading mode."""

    name = "paper"

    async def buy(self, mint, size_sol, slippage, priority_fee, keypair_b58=None):
        return "PAPER_SIM"

    async def sell(self, mint, pct, slippage, priority_fee, keypair_b58=None):
        return "PAPER_SIM"

    async def get_balance(self):
        return None  # paper mode tracks balance internally

    def is_connected(self):
        return True


# ─────────────────────────────────────────────
#  LEGACY EXECUTOR (PumpPortal HTTP)
# ─────────────────────────────────────────────

class LegacyExecutor(ExecutorBackend):
    """Wraps the original pumpportal_trade_async flow."""

    name = "legacy"

    def __init__(self, keypair, rpc_url, trade_url):
        from solders.keypair import Keypair as SoldersKeypair
        from solders.transaction import VersionedTransaction
        from solana.rpc.async_api import AsyncClient
        from solana.rpc.types import TxOpts
        from solana.rpc.commitment import Processed

        self._keypair = keypair
        self._client = AsyncClient(rpc_url)
        self._trade_url = trade_url
        self._aio_session = None
        self._VersionedTransaction = VersionedTransaction
        self._TxOpts = TxOpts
        self._Processed = Processed

    async def _ensure_session(self):
        if self._aio_session is None or self._aio_session.closed:
            import aiohttp
            self._aio_session = aiohttp.ClientSession()

    async def _trade(self, action, mint, amount, denominated_in_sol=True,
                     slippage=10, priority_fee=0.002):
        await self._ensure_session()
        t0 = time.monotonic()

        body = {
            "publicKey": str(self._keypair.pubkey()),
            "action": action,
            "mint": mint,
            "denominatedInSol": "true" if denominated_in_sol else "false",
            "amount": amount,
            "slippage": slippage,
            "priorityFee": priority_fee,
            "pool": "pump",
        }
        try:
            import aiohttp
            async with self._aio_session.post(
                self._trade_url, json=body,
                timeout=aiohttp.ClientTimeout(total=8)
            ) as resp:
                if resp.status != 200:
                    return None
                tx_bytes = await resp.read()
                if not tx_bytes:
                    return None
        except Exception as e:
            print(f"[LegacyExecutor] HTTP error: {e}")
            return None

        try:
            utx = self._VersionedTransaction.from_bytes(tx_bytes)
            stx = self._VersionedTransaction(utx.message, [self._keypair])

            resp = await self._client.send_raw_transaction(
                bytes(stx),
                opts=self._TxOpts(
                    skip_preflight=True,
                    preflight_commitment=self._Processed,
                    max_retries=3,
                ),
            )
            sig = None
            if isinstance(resp, dict):
                sig = (resp.get("result") or {}).get("value") if isinstance(resp.get("result"), dict) else resp.get("result")
                if sig is None:
                    sig = resp.get("value")
            else:
                sig = getattr(resp, "value", None)
                if sig is None:
                    inner = getattr(resp, "result", None)
                    if inner is not None:
                        sig = getattr(inner, "value", inner)
            elapsed = (time.monotonic() - t0) * 1000
            self.last_total_latency_ms = elapsed
            if sig is not None:
                sig_str = str(sig)
                print(f"[LegacyExecutor] TX: https://solscan.io/tx/{sig_str}")
                return sig_str
            return None
        except Exception as e:
            print(f"[LegacyExecutor] Sign/send error: {e!r}")
            return None

    async def buy(self, mint, size_sol, slippage, priority_fee, keypair_b58=None):
        return await self._trade("buy", mint, size_sol,
                                  denominated_in_sol=True,
                                  slippage=slippage,
                                  priority_fee=priority_fee)

    async def sell(self, mint, pct, slippage, priority_fee, keypair_b58=None):
        sig = await self._trade("sell", mint, pct,
                                 denominated_in_sol=False,
                                 slippage=slippage,
                                 priority_fee=priority_fee)
        return sig

    async def get_balance(self):
        try:
            resp = await self._client.get_balance(self._keypair.pubkey())
            lamports = None
            if resp is None:
                return None
            if hasattr(resp, "value"):
                lamports = resp.value
            elif isinstance(resp, dict):
                lamports = (resp.get("result") or {}).get("value")
            else:
                inner = getattr(resp, "result", None)
                if inner is not None and hasattr(inner, "value"):
                    lamports = inner.value
                elif isinstance(inner, dict):
                    lamports = inner.get("value")
            if lamports is not None:
                return int(lamports) / 1e9
        except Exception as e:
            print(f"[LegacyExecutor] Balance error: {e!r}")
        return None

    def is_connected(self):
        return self._keypair is not None

    async def close(self):
        if self._aio_session and not self._aio_session.closed:
            await self._aio_session.close()


# ─────────────────────────────────────────────
#  ZMQ EXECUTOR (Rust bridge)
# ─────────────────────────────────────────────

class ZmqExecutor(ExecutorBackend):
    """Sends trade signals to Rust executor via ZeroMQ.
    Falls back to LegacyExecutor if Rust is not responding."""

    name = "zmq_rust"

    def __init__(self, legacy_fallback: Optional[LegacyExecutor] = None,
                 signal_port: int = 5555, result_port: int = 5556,
                 signal_ipc: str = DEFAULT_ZMQ_SIGNAL_IPC,
                 result_ipc: str = DEFAULT_ZMQ_RESULT_IPC,
                 timeout_sec: float = 30.0,
                 bridge=None):
        if bridge is not None:
            self._bridge = bridge
        else:
            from zmq_bridge import ZmqBridge
            self._bridge = ZmqBridge(signal_port=signal_port, result_port=result_port,
                                     signal_ipc=signal_ipc, result_ipc=result_ipc,
                                     cleanup_stale=False)
        self._fallback = legacy_fallback
        self._timeout = timeout_sec
        # Rust handles one signal at a time; without this, concurrent buy/sell coroutines
        # queue in ZMQ while each asyncio.wait_for uses the same wall-clock deadline → false timeouts → Legacy (high fixed fee).
        self._signal_lock = asyncio.Lock()
        self._connected = False
        self._seq = 0
        self._adaptive_state: Optional[AdaptiveState] = None

    def set_adaptive_state(self, state: AdaptiveState):
        """Wire adaptive state so Rust fee data feeds back into Python."""
        self._adaptive_state = state

    def _log_rust_rejection(self, action: str, result: Optional[dict]) -> None:
        """Explain why we might fall back to Legacy (helps tune timeouts vs real chain errors)."""
        if result is None:
            return
        err = result.get("error")
        status = result.get("status")
        if err:
            print(f"[ZmqExecutor] Rust {action} returned status={status!r}: {err}")
        elif status and status != "confirmed":
            print(f"[ZmqExecutor] Rust {action} status={status!r} (no error field)")

    async def _send_signal(self, action: str, mint: str, amount,
                           amount_type: str, slippage_bps: int,
                           priority: str = "normal",
                           priority_fee_lamports: int = 0,
                           keypair_b58: Optional[str] = None,
                           config: Optional[str] = None,
                           timeout: Optional[float] = None) -> Optional[dict]:
        """Send a trade signal and wait for result from Rust."""
        wait_timeout = timeout if timeout is not None else self._timeout
        async with self._signal_lock:
            signal_id = str(uuid.uuid4())
            self._seq += 1
            signal = {
                "id": signal_id,
                "seq": self._seq,
                "action": action,
                "mint": mint,
                "amount": amount,
                "amount_type": amount_type,
                "slippage_bps": slippage_bps,
                "priority": priority,
                "priority_fee_lamports": priority_fee_lamports,
                "timestamp_ms": int(time.time() * 1000),
            }
            if keypair_b58:
                signal["keypair_b58"] = keypair_b58
            if config:
                signal["config"] = json.loads(config) if isinstance(config, str) else config

            t0 = time.monotonic()
            sent = self._bridge.send_signal(signal)
            if not sent:
                print("[ZmqExecutor] Failed to send signal, using fallback")
                self._connected = False
                return None

            result = await self._bridge.wait_for_result(signal_id, timeout=wait_timeout)
            elapsed = (time.monotonic() - t0) * 1000

            if result is None:
                print(
                    f"[ZmqExecutor] Timeout ({wait_timeout}s) waiting for Rust {action} result — "
                    "queue stalled or executor hung; using fallback"
                )
                self._connected = False
                return None

            self._connected = True

            lat = result.get("latency", {})
            self.last_zmq_latency_ms = lat.get("zmq_recv_ms", 0)
            self.last_build_latency_ms = lat.get("build_ms", 0)
            self.last_land_latency_ms = lat.get("land_ms", 0)
            self.last_total_latency_ms = lat.get("total_ms", elapsed)

            if self._adaptive_state:
                fee_used = lat.get("fee_used_lamports", 0)
                if fee_used > 0:
                    self._adaptive_state.update_fees(fee_used)

            return result

    async def buy(self, mint, size_sol, slippage, priority_fee, keypair_b58=None):
        fee_lamports = int(priority_fee * 1e9) if priority_fee > 0 else 0
        result = await self._send_signal("buy", mint, size_sol, "sol",
                                          int(slippage * 100),
                                          priority_fee_lamports=fee_lamports,
                                          keypair_b58=keypair_b58)
        if result and result.get("status") == "confirmed":
            sig = result.get("signature")
            method = result.get("method", "unknown")
            lat = result.get("latency", {}).get("total_ms", 0)
            print(f"[ZmqExecutor] BUY via {method} in {lat:.0f}ms: {sig}")
            return sig

        self._log_rust_rejection("buy", result)
        if self._fallback:
            print("[ZmqExecutor] Falling back to legacy executor (buy)")
            return await self._fallback.buy(mint, size_sol, slippage, priority_fee)
        return None

    async def sell(self, mint, pct, slippage, priority_fee, keypair_b58=None):
        """Sell via Rust first; retry transient failures before Legacy (fee savings)."""
        pct_val = float(str(pct).replace("%", "")) if isinstance(pct, str) else float(pct)
        fee_lamports = int(priority_fee * 1e9) if priority_fee > 0 else 0
        last_result: Optional[dict] = None

        if _ZMQ_SELL_PREFLIGHT_MS > 0:
            await asyncio.sleep(_ZMQ_SELL_PREFLIGHT_MS / 1000.0)

        for attempt in range(1, _RUST_SELL_ATTEMPTS + 1):
            if attempt > 1:
                if not _rust_sell_should_retry(last_result):
                    break
                delay = min(
                    _RUST_SELL_BACKOFF_MAX,
                    1.5 * (1.5 ** (attempt - 2)),
                )
                print(
                    f"[ZmqExecutor] Waiting for RPC to index tokens — SELL Rust retry "
                    f"{attempt}/{_RUST_SELL_ATTEMPTS} after {delay:.1f}s (avoiding Legacy)"
                )
                await asyncio.sleep(delay)

            result = await self._send_signal(
                "sell",
                mint,
                pct_val,
                "pct",
                int(slippage * 100),
                priority="urgent",
                priority_fee_lamports=fee_lamports,
                keypair_b58=keypair_b58,
                timeout=_ZMQ_SELL_WAIT_SEC,
            )
            last_result = result

            if result and result.get("status") == "confirmed":
                sig = result.get("signature")
                method = result.get("method", "unknown")
                lat = result.get("latency", {}).get("total_ms", 0)
                if attempt > 1:
                    print(
                        f"[ZmqExecutor] SELL via {method} in {lat:.0f}ms (attempt {attempt}): {sig}"
                    )
                else:
                    print(f"[ZmqExecutor] SELL via {method} in {lat:.0f}ms: {sig}")
                return sig

        self._log_rust_rejection("sell", last_result)
        if self._fallback:
            print(
                "[ZmqExecutor] Legacy sell fallback after "
                f"{_RUST_SELL_ATTEMPTS} Rust attempt(s) (should be rare)"
            )
            return await self._fallback.sell(mint, pct, slippage, priority_fee)
        return None

    async def get_balance(self):
        # Balance check goes through legacy RPC (not latency-critical)
        if self._fallback:
            return await self._fallback.get_balance()
        return None

    async def create_token(self, name: str, symbol: str, uri: str,
                           priority_fee: float = 0.003,
                           keypair_b58: Optional[str] = None) -> Optional[str]:
        """Create a Pump.fun token via Rust executor. Returns mint address or None."""
        fee_lamports = int(priority_fee * 1e9) if priority_fee > 0 else 0
        config_payload = json.dumps({"name": name, "symbol": symbol, "uri": uri})
        result = await self._send_signal(
            "create", "", 0, "sol", 0,
            priority="normal",
            priority_fee_lamports=fee_lamports,
            keypair_b58=keypair_b58,
            config=config_payload,
        )
        if result and result.get("status") == "confirmed":
            mint_address = result.get("signature")  # mint pubkey returned as signature
            method = result.get("method", "unknown")
            lat = result.get("latency", {}).get("total_ms", 0)
            print(f"[ZmqExecutor] CREATE via {method} in {lat:.0f}ms: {mint_address}")
            return mint_address
        return None

    async def kill(self, mint: str, tip_sol: float = 0.0,
                   keypair_b58: Optional[str] = None) -> Optional[str]:
        """Execute kill chain via Rust Jito bundle. Returns tx sig or None."""
        result = await self._send_signal(
            "kill", mint, tip_sol, "sol",
            slippage_bps=2000,
            priority="urgent",
            keypair_b58=keypair_b58,
            timeout=_ZMQ_SELL_WAIT_SEC,
        )
        if result and result.get("status") == "confirmed":
            sig = result.get("signature")
            method = result.get("method", "unknown")
            lat = result.get("latency", {}).get("total_ms", 0)
            print(f"[ZmqExecutor] KILL via {method} in {lat:.0f}ms: {sig}")
            return sig
        self._log_rust_rejection("kill", result)
        if self._fallback:
            print("[ZmqExecutor] Kill failed, falling back to legacy 100% sell")
            return await self._fallback.sell(mint, "100%", 15, 0.01)
        return None

    def is_connected(self):
        return self._connected

    def close(self):
        self._bridge.close()


# ─────────────────────────────────────────────
#  RESOLVED EXECUTION — parameter gatekeeper
# ─────────────────────────────────────────────

async def resolved_buy(backend: ExecutorBackend, mint: str, size_sol: float,
                       slippage: int, config: dict,
                       adaptive_state: AdaptiveState,
                       keypair_b58: Optional[str] = None) -> Optional[str]:
    """Resolve parameters at trade time, then execute buy.

    This is the single call site where resolve_parameters() is invoked
    for buys. Pass ``keypair_b58`` for ZmqExecutor so Rust signs the same wallet as Legacy
    (dashboard / LIVE_KEYPAIR), not only ``BIGAL_PRIVATE_KEY`` in the Rust process env.
    """
    params = resolve_parameters(config, adaptive_state)
    return await backend.buy(
        mint, size_sol, slippage, params["tip"], keypair_b58=keypair_b58
    )


async def resolved_sell(backend: ExecutorBackend, mint: str, pct: str,
                        slippage: int, config: dict,
                        adaptive_state: AdaptiveState,
                        keypair_b58: Optional[str] = None) -> Optional[str]:
    """Resolve parameters at trade time, then execute sell.

    Pass ``keypair_b58`` for ZmqExecutor so Rust uses the same signer as Legacy (see resolved_buy).
    """
    params = resolve_parameters(config, adaptive_state)
    return await backend.sell(
        mint, pct, slippage, params["tip"], keypair_b58=keypair_b58
    )
