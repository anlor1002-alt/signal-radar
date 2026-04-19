"""
Signal Radar — Persistent SQLite Database Layer

Async wrapper around aiosqlite for user tracking and keyword monitoring.
All operations are non-blocking to keep the Telegram bot responsive.
"""

from __future__ import annotations

import os
from datetime import datetime

import aiosqlite

DB_PATH = os.getenv("SQLITE_DB_PATH", "signal_radar.db")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    chat_id     TEXT PRIMARY KEY,
    joined_date TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS tracked_keywords (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    chat_id          TEXT    NOT NULL,
    keyword          TEXT    NOT NULL,
    domain           TEXT    DEFAULT 'General',
    last_status      TEXT    DEFAULT 'UNKNOWN',
    last_wow_growth  REAL    DEFAULT 0.0,
    last_confidence  INTEGER DEFAULT 0,
    updated_at       TEXT    NOT NULL,
    FOREIGN KEY (chat_id) REFERENCES users(chat_id),
    UNIQUE(chat_id, keyword)
);
"""


# ---------------------------------------------------------------------------
# Initialisation
# ---------------------------------------------------------------------------

async def init_db() -> None:
    """Create tables if they don't exist. Call once on bot startup."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript(_SCHEMA)
        await db.commit()
    print(f"[DB] Initialised at {DB_PATH}")


# ---------------------------------------------------------------------------
# User helpers
# ---------------------------------------------------------------------------

async def register_user(chat_id: int | str) -> None:
    """Insert a user row if not already present."""
    chat_id = str(chat_id)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR IGNORE INTO users (chat_id, joined_date) VALUES (?, ?)",
            (chat_id, datetime.utcnow().isoformat()),
        )
        await db.commit()


# ---------------------------------------------------------------------------
# Keyword tracking
# ---------------------------------------------------------------------------

async def add_keyword(
    chat_id: int | str,
    keyword: str,
    domain: str = "General",
) -> bool:
    """Add a keyword to a user's tracking list. Returns True if inserted."""
    chat_id = str(chat_id)
    now = datetime.utcnow().isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        try:
            await db.execute(
                "INSERT INTO tracked_keywords (chat_id, keyword, domain, updated_at) "
                "VALUES (?, ?, ?, ?)",
                (chat_id, keyword, domain, now),
            )
            await db.commit()
            return True
        except aiosqlite.IntegrityError:
            return False  # already tracked


async def remove_keyword(chat_id: int | str, keyword: str) -> bool:
    """Remove a keyword from a user's tracking list. Returns True if deleted."""
    chat_id = str(chat_id)
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "DELETE FROM tracked_keywords WHERE chat_id = ? AND keyword = ?",
            (chat_id, keyword),
        )
        await db.commit()
        return cursor.rowcount > 0


async def get_user_keywords(chat_id: int | str) -> list[dict]:
    """Return all tracked keywords for a user as a list of dicts."""
    chat_id = str(chat_id)
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT keyword, domain, last_status, last_wow_growth, "
            "last_confidence, updated_at "
            "FROM tracked_keywords WHERE chat_id = ? ORDER BY keyword",
            (chat_id,),
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]


async def get_all_tracked_keywords() -> list[dict]:
    """Return every tracked keyword across all users (for background scanner)."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT id, chat_id, keyword, domain, last_status, "
            "last_wow_growth, last_confidence, updated_at "
            "FROM tracked_keywords"
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]


async def update_keyword_status(
    keyword_id: int,
    status: str,
    wow_growth: float,
    confidence: int,
    domain: str | None = None,
) -> None:
    """Update the status fields of a tracked keyword after a scan."""
    now = datetime.utcnow().isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        if domain:
            await db.execute(
                "UPDATE tracked_keywords "
                "SET last_status = ?, last_wow_growth = ?, last_confidence = ?, "
                "    domain = ?, updated_at = ? "
                "WHERE id = ?",
                (status, wow_growth, confidence, domain, now, keyword_id),
            )
        else:
            await db.execute(
                "UPDATE tracked_keywords "
                "SET last_status = ?, last_wow_growth = ?, last_confidence = ?, "
                "    updated_at = ? "
                "WHERE id = ?",
                (status, wow_growth, confidence, now, keyword_id),
            )
        await db.commit()
