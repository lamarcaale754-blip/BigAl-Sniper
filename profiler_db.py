import sqlite3
import threading
import time
from typing import Iterable, Tuple


DB_PATH = "wallet_profiler.db"
_INIT_LOCK = threading.Lock()
_INITIALIZED = False


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=5.0)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def init_profiler_db() -> None:
    global _INITIALIZED
    if _INITIALIZED:
        return
    with _INIT_LOCK:
        if _INITIALIZED:
            return
        with _connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS flagged_bots (
                    address TEXT PRIMARY KEY,
                    confidence REAL NOT NULL,
                    win_rate REAL NOT NULL,
                    updated_at_ms REAL NOT NULL
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_flagged_bots_win_rate ON flagged_bots(win_rate)"
            )
            conn.commit()
        _INITIALIZED = True


def upsert_flagged_bot(address: str, confidence: float, win_rate: float) -> None:
    if not address:
        return
    init_profiler_db()
    ts_ms = time.time() * 1000.0
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO flagged_bots(address, confidence, win_rate, updated_at_ms)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(address) DO UPDATE SET
                confidence = excluded.confidence,
                win_rate = excluded.win_rate,
                updated_at_ms = excluded.updated_at_ms
            """,
            (address, float(confidence), float(win_rate), ts_ms),
        )
        conn.commit()


def profitable_bots_in_buyers(
    buyers: Iterable[str], min_win_rate: float = 0.60
) -> Tuple[int, float]:
    buyer_list = [b for b in set(buyers or []) if b]
    if not buyer_list:
        return 0, 0.0

    init_profiler_db()
    placeholders = ",".join("?" for _ in buyer_list)
    query = (
        f"SELECT COUNT(*), COALESCE(MAX(confidence), 0.0) "
        f"FROM flagged_bots WHERE win_rate >= ? AND address IN ({placeholders})"
    )
    with _connect() as conn:
        row = conn.execute(query, [float(min_win_rate), *buyer_list]).fetchone()
    if not row:
        return 0, 0.0
    return int(row[0] or 0), float(row[1] or 0.0)
