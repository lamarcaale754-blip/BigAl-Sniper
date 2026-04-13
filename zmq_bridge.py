"""
ZmqBridge — ZeroMQ communication layer between Python (strategy) and Rust (executor).

Sockets (IPC preferred, TCP fallback):
  PUSH → ipc:///tmp/bigal_signal.ipc  (Python sends trade signals to Rust)
  SUB  ← ipc:///tmp/bigal_result.ipc  (Rust publishes execution results back)

IPC (Unix Domain Sockets) strips TCP/IP stack overhead, cutting IPC latency
from milliseconds to microseconds on the same machine.
"""

import asyncio
import json
import os
import sys
import time
import threading
from typing import Optional, Dict

import zmq


def _ipc_addr(path: str, port: int) -> str:
    """Return IPC address on Unix, TCP fallback on Windows."""
    if sys.platform == "win32":
        return f"tcp://127.0.0.1:{port}"
    return path


class ZmqBridge:
    """Manages ZMQ sockets for Python ↔ Rust IPC."""

    @staticmethod
    def _cleanup_stale_ipc(addr: str):
        """Remove stale IPC socket file left by a previous crash (no-op for TCP)."""
        if not addr.startswith("ipc://"):
            return
        path = addr[len("ipc://"):]
        if os.path.exists(path):
            try:
                os.unlink(path)
                print(f"[ZmqBridge] Cleaned up stale IPC socket: {path}")
            except OSError as e:
                print(f"[ZmqBridge] Warning: could not remove stale IPC {path}: {e}")

    def __init__(self, signal_port: int = 5555, result_port: int = 5556,
                 signal_ipc: str = "ipc:///tmp/bigal_signal.ipc",
                 result_ipc: str = "ipc:///tmp/bigal_result.ipc",
                 cleanup_stale: bool = True):
        self._signal_port = signal_port
        self._result_port = result_port

        self._signal_addr = _ipc_addr(signal_ipc, signal_port)
        self._result_addr = _ipc_addr(result_ipc, result_port)

        # Pre-flight: remove stale IPC socket files from previous crash
        # Skip when Rust is already connected (e.g. post-probe bridge swap)
        if cleanup_stale:
            ZmqBridge._cleanup_stale_ipc(self._signal_addr)
            ZmqBridge._cleanup_stale_ipc(self._result_addr)

        self._ctx = zmq.Context.instance()

        # PUSH socket: Python → Rust (trade signals)
        self._push = self._ctx.socket(zmq.PUSH)
        self._push.setsockopt(zmq.SNDHWM, 100)
        self._push.setsockopt(zmq.LINGER, 1000)
        self._push.setsockopt(zmq.SNDTIMEO, 1000)  # 1s send timeout
        self._push.bind(self._signal_addr)

        # SUB socket: Rust → Python (execution results)
        self._sub = self._ctx.socket(zmq.SUB)
        self._sub.setsockopt(zmq.RCVHWM, 100)
        self._sub.setsockopt_string(zmq.SUBSCRIBE, "")  # subscribe to all
        self._sub.connect(self._result_addr)

        # Pending results: id → asyncio.Future
        self._pending: Dict[str, asyncio.Future] = {}
        self._lock = threading.Lock()

        # Background listener thread for SUB socket
        self._running = True
        self._listener = threading.Thread(target=self._listen_loop, daemon=True)
        self._listener.start()

        print(f"[ZmqBridge] PUSH bound on {self._signal_addr}, SUB connected to {self._result_addr}")

    def send_signal(self, signal: dict) -> bool:
        """Send a trade signal to Rust. Returns True if sent successfully."""
        try:
            msg = json.dumps(signal)
            self._push.send_string(msg)  # respects SNDTIMEO (1s wait for Rust)
            return True
        except zmq.Again:
            print("[ZmqBridge] PUSH send timeout (Rust not consuming)")
            return False
        except zmq.ZMQError as e:
            print(f"[ZmqBridge] PUSH error: {e}")
            return False

    async def wait_for_result(self, signal_id: str,
                               timeout: float = 3.0) -> Optional[dict]:
        """Wait for an execution result matching signal_id."""
        loop = asyncio.get_running_loop()
        future = loop.create_future()

        with self._lock:
            self._pending[signal_id] = future

        try:
            result = await asyncio.wait_for(future, timeout=timeout)
            return result
        except asyncio.TimeoutError:
            return None
        finally:
            with self._lock:
                self._pending.pop(signal_id, None)

    def _listen_loop(self):
        """Background thread: reads execution results from Rust SUB socket."""
        poller = zmq.Poller()
        poller.register(self._sub, zmq.POLLIN)
        poll_ms = int(os.environ.get("ZMQ_POLL_TIMEOUT_MS", "10"))

        while self._running:
            try:
                socks = dict(poller.poll(timeout=poll_ms))
                if self._sub in socks:
                    msg = self._sub.recv_string(zmq.NOBLOCK)
                    result = json.loads(msg)
                    signal_id = result.get("id")

                    if signal_id:
                        with self._lock:
                            future = self._pending.pop(signal_id, None)
                        if future and not future.done():
                            try:
                                future.get_loop().call_soon_threadsafe(
                                    future.set_result, result
                                )
                            except RuntimeError:
                                pass  # event loop closed during shutdown
            except zmq.ZMQError:
                if self._running:
                    time.sleep(0.1)
            except json.JSONDecodeError as e:
                print(f"[ZmqBridge] Bad JSON from Rust: {e}")
            except Exception as e:
                if self._running:
                    print(f"[ZmqBridge] Listener error: {e}")
                    time.sleep(0.1)

    def probe_rust(self, timeout_ms: int = 2000) -> bool:
        """Check if Rust executor is alive by sending a ping signal.
        Uses a separate temporary SUB socket to avoid racing with _listen_loop."""
        ping_id = f"ping-{int(time.time()*1000)}"
        signal = {
            "id": ping_id,
            "action": "ping",
            "timestamp_ms": int(time.time() * 1000),
        }
        probe_sub = None
        try:
            # Create a dedicated SUB socket for this probe only
            probe_sub = self._ctx.socket(zmq.SUB)
            probe_sub.setsockopt_string(zmq.SUBSCRIBE, "")
            probe_sub.connect(self._result_addr)
            time.sleep(0.05)  # brief settle for SUB subscription

            self._push.send_string(json.dumps(signal), zmq.NOBLOCK)

            poller = zmq.Poller()
            poller.register(probe_sub, zmq.POLLIN)
            deadline = time.monotonic() + (timeout_ms / 1000)
            while time.monotonic() < deadline:
                remaining = max(1, int((deadline - time.monotonic()) * 1000))
                socks = dict(poller.poll(timeout=remaining))
                if probe_sub in socks:
                    msg = probe_sub.recv_string(zmq.NOBLOCK)
                    result = json.loads(msg)
                    if result.get("id") == ping_id:
                        return True
                    # Non-matching messages are ignored (the main _listen_loop
                    # SUB socket also receives them via PUB fan-out)
            return False
        except Exception:
            return False
        finally:
            if probe_sub:
                probe_sub.close()

    def close(self, cleanup_ipc: bool = False):
        """Shut down sockets and listener thread.
        cleanup_ipc: only delete IPC files on final shutdown, not between probe/executor swaps."""
        self._running = False
        # Cancel all pending futures so awaiting coroutines unblock
        with self._lock:
            for future in self._pending.values():
                if not future.done():
                    try:
                        future.get_loop().call_soon_threadsafe(future.cancel)
                    except RuntimeError:
                        pass
            self._pending.clear()
        if self._listener.is_alive():
            self._listener.join(timeout=2)
        self._push.close()
        self._sub.close()
        if cleanup_ipc:
            for addr in (self._signal_addr, self._result_addr):
                if addr.startswith("ipc://"):
                    path = addr[len("ipc://"):]
                    try:
                        os.unlink(path)
                    except OSError:
                        pass
        print("[ZmqBridge] Closed")
