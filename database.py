"""
Signal Radar — Persistent SQLite Database Layer

Async wrapper around aiosqlite for user tracking and keyword monitoring.
All operations are non-blocking to keep the Telegram bot responsive.
"""

from __future__ import annotations

import csv
import io
import os
from datetime import datetime

import aiosqlite

DB_PATH = os.getenv("SQLITE_DB_PATH", "signal_radar.db")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    chat_id     TEXT PRIMARY KEY,
    joined_date TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS projects (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    chat_id     TEXT    NOT NULL,
    name        TEXT    NOT NULL,
    scan_freq   TEXT    DEFAULT 'daily',
    created_at  TEXT    NOT NULL,
    FOREIGN KEY (chat_id) REFERENCES users(chat_id),
    UNIQUE(chat_id, name)
);

CREATE TABLE IF NOT EXISTS tracked_keywords (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    chat_id          TEXT    NOT NULL,
    keyword          TEXT    NOT NULL,
    geo              TEXT    DEFAULT 'VN',
    domain           TEXT    DEFAULT 'General',
    project_id       INTEGER DEFAULT NULL,
    last_status      TEXT    DEFAULT 'UNKNOWN',
    last_wow_growth  REAL    DEFAULT 0.0,
    last_confidence  INTEGER DEFAULT 0,
    last_alert_at    TEXT    DEFAULT NULL,
    updated_at       TEXT    NOT NULL,
    FOREIGN KEY (chat_id) REFERENCES users(chat_id),
    FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE SET NULL,
    UNIQUE(chat_id, keyword, geo)
);

CREATE TABLE IF NOT EXISTS scan_history (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    keyword         TEXT    NOT NULL,
    chat_id         TEXT    NOT NULL,
    geo             TEXT    DEFAULT 'VN',
    domain          TEXT    DEFAULT 'General',
    status          TEXT    NOT NULL,
    wow_growth      REAL    DEFAULT 0.0,
    confidence      INTEGER DEFAULT 0,
    interest        REAL    DEFAULT 0.0,
    acceleration    REAL    DEFAULT 0.0,
    consistency     REAL    DEFAULT 0.0,
    peak_position   REAL    DEFAULT 0.0,
    action_label    TEXT    DEFAULT '',
    action_reason   TEXT    DEFAULT '',
    scanned_at      TEXT    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_history_chat_kw
    ON scan_history (chat_id, keyword);

CREATE INDEX IF NOT EXISTS idx_history_scanned
    ON scan_history (scanned_at);
"""

# Incremental migrations for existing databases
_MIGRATIONS = [
    "ALTER TABLE scan_history ADD COLUMN action_label TEXT DEFAULT ''",
    "ALTER TABLE scan_history ADD COLUMN action_reason TEXT DEFAULT ''",
    "ALTER TABLE scan_history ADD COLUMN geo TEXT DEFAULT 'VN'",
    "ALTER TABLE tracked_keywords ADD COLUMN last_alert_at TEXT DEFAULT NULL",
]


async def init_db() -> None:
    """Create tables if they don't exist, then run incremental migrations."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript(_SCHEMA)
        # Run simple column-add migrations
        for sql in _MIGRATIONS:
            try:
                await db.execute(sql)
            except aiosqlite.OperationalError:
                pass  # column already exists
        # Run table migration for multi-geo support
        await _migrate_tracked_keywords_v2(db)
        # Create index on project_id (safe now — column guaranteed to exist after migration)
        try:
            await db.execute(
                "CREATE INDEX IF NOT EXISTS idx_tracked_project ON tracked_keywords (project_id)"
            )
        except aiosqlite.OperationalError:
            pass
        await db.commit()
    print(f"[DB] Initialised at {DB_PATH}")


async def _migrate_tracked_keywords_v2(db: aiosqlite.Connection) -> None:
    """Migrate tracked_keywords to support geo + project_id columns.

    Handles the UNIQUE constraint change from (chat_id, keyword)
    to (chat_id, keyword, geo) via table recreation.
    """
    # Check if geo column already exists
    cursor = await db.execute("PRAGMA table_info(tracked_keywords)")
    cols = {row[1] for row in await cursor.fetchall()}

    if "geo" in cols and "project_id" in cols:
        return  # already migrated

    await db.executescript("""
        CREATE TABLE IF NOT EXISTS tracked_keywords_v2 (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id          TEXT    NOT NULL,
            keyword          TEXT    NOT NULL,
            geo              TEXT    DEFAULT 'VN',
            domain           TEXT    DEFAULT 'General',
            project_id       INTEGER DEFAULT NULL,
            last_status      TEXT    DEFAULT 'UNKNOWN',
            last_wow_growth  REAL    DEFAULT 0.0,
            last_confidence  INTEGER DEFAULT 0,
            last_alert_at    TEXT    DEFAULT NULL,
            updated_at       TEXT    NOT NULL,
            UNIQUE(chat_id, keyword, geo)
        );

        INSERT OR IGNORE INTO tracked_keywords_v2
            (id, chat_id, keyword, domain, last_status, last_wow_growth,
             last_confidence, last_alert_at, updated_at)
            SELECT id, chat_id, keyword, domain, last_status, last_wow_growth,
                   last_confidence, last_alert_at, updated_at
            FROM tracked_keywords;

        DROP TABLE IF EXISTS tracked_keywords;

        ALTER TABLE tracked_keywords_v2 RENAME TO tracked_keywords;
    """)
    print("[DB] Migrated tracked_keywords to v2 (geo + project_id).")


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
# Project CRUD
# ---------------------------------------------------------------------------

async def create_project(chat_id: int | str, name: str, scan_freq: str = "daily") -> bool:
    """Create a project. Returns True if created, False if name exists."""
    chat_id = str(chat_id)
    now = datetime.utcnow().isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        try:
            await db.execute(
                "INSERT INTO projects (chat_id, name, scan_freq, created_at) VALUES (?, ?, ?, ?)",
                (chat_id, name, scan_freq, now),
            )
            await db.commit()
            return True
        except aiosqlite.IntegrityError:
            return False


async def get_user_projects(chat_id: int | str) -> list[dict]:
    """Return all projects for a user."""
    chat_id = str(chat_id)
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT p.id, p.name, p.scan_freq, p.created_at, "
            "  (SELECT COUNT(*) FROM tracked_keywords WHERE project_id = p.id) AS kw_count "
            "FROM projects p WHERE p.chat_id = ? ORDER BY p.name",
            (chat_id,),
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]


async def get_project(chat_id: int | str, name: str) -> dict | None:
    """Return a single project by name for a user."""
    chat_id = str(chat_id)
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT id, name, scan_freq, created_at FROM projects WHERE chat_id = ? AND name = ?",
            (chat_id, name),
        )
        row = await cursor.fetchone()
        return dict(row) if row else None


async def delete_project(chat_id: int | str, name: str) -> bool:
    """Delete a project. Keywords in it get project_id set to NULL. Returns True if deleted."""
    chat_id = str(chat_id)
    async with aiosqlite.connect(DB_PATH) as db:
        # Get project id first
        cursor = await db.execute(
            "SELECT id FROM projects WHERE chat_id = ? AND name = ?",
            (chat_id, name),
        )
        row = await cursor.fetchone()
        if not row:
            return False
        project_id = row[0]
        # Unlink keywords
        await db.execute(
            "UPDATE tracked_keywords SET project_id = NULL WHERE project_id = ?",
            (project_id,),
        )
        # Delete project
        await db.execute(
            "DELETE FROM projects WHERE id = ?",
            (project_id,),
        )
        await db.commit()
        return True


async def get_twice_daily_projects() -> list[dict]:
    """Return all projects with scan_freq='twice_daily'."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT id, chat_id, name, scan_freq FROM projects WHERE scan_freq = 'twice_daily'"
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Keyword tracking
# ---------------------------------------------------------------------------

async def add_keyword(
    chat_id: int | str,
    keyword: str,
    domain: str = "General",
    geo: str = "VN",
    project_id: int | None = None,
) -> bool:
    """Add a keyword to a user's tracking list. Returns True if inserted."""
    chat_id = str(chat_id)
    now = datetime.utcnow().isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        try:
            await db.execute(
                "INSERT INTO tracked_keywords "
                "(chat_id, keyword, geo, domain, project_id, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (chat_id, keyword, geo, domain, project_id, now),
            )
            await db.commit()
            return True
        except aiosqlite.IntegrityError:
            return False  # already tracked in this geo


async def remove_keyword(chat_id: int | str, keyword: str, geo: str | None = None) -> bool:
    """Remove a keyword from a user's tracking list. If geo is None, removes all geos."""
    chat_id = str(chat_id)
    async with aiosqlite.connect(DB_PATH) as db:
        if geo:
            cursor = await db.execute(
                "DELETE FROM tracked_keywords WHERE chat_id = ? AND keyword = ? AND geo = ?",
                (chat_id, keyword, geo),
            )
        else:
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
            "SELECT id, keyword, geo, domain, project_id, last_status, last_wow_growth, "
            "last_confidence, updated_at "
            "FROM tracked_keywords WHERE chat_id = ? ORDER BY keyword, geo",
            (chat_id,),
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]


async def get_project_keywords(chat_id: int | str, project_name: str) -> list[dict]:
    """Return all tracked keywords for a specific project."""
    chat_id = str(chat_id)
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT tk.id, tk.keyword, tk.geo, tk.domain, tk.project_id, "
            " tk.last_status, tk.last_wow_growth, tk.last_confidence, tk.updated_at "
            "FROM tracked_keywords tk "
            "INNER JOIN projects p ON tk.project_id = p.id "
            "WHERE tk.chat_id = ? AND p.name = ? "
            "ORDER BY tk.keyword, tk.geo",
            (chat_id, project_name),
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]


async def get_all_tracked_keywords(geo_filter: str | None = None) -> list[dict]:
    """Return every tracked keyword across all users (for background scanner)."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        if geo_filter:
            cursor = await db.execute(
                "SELECT id, chat_id, keyword, geo, domain, project_id, last_status, "
                "last_wow_growth, last_confidence, updated_at "
                "FROM tracked_keywords WHERE geo = ?",
                (geo_filter,),
            )
        else:
            cursor = await db.execute(
                "SELECT id, chat_id, keyword, geo, domain, project_id, last_status, "
                "last_wow_growth, last_confidence, updated_at "
                "FROM tracked_keywords"
            )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]


async def get_keywords_for_project_ids(project_ids: list[int]) -> list[dict]:
    """Return tracked keywords belonging to any of the given project IDs."""
    if not project_ids:
        return []
    placeholders = ",".join("?" for _ in project_ids)
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            f"SELECT id, chat_id, keyword, geo, domain, project_id, last_status, "
            f"last_wow_growth, last_confidence, updated_at "
            f"FROM tracked_keywords WHERE project_id IN ({placeholders})",
            project_ids,
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


# ---------------------------------------------------------------------------
# Scan History
# ---------------------------------------------------------------------------

async def insert_scan_history(
    keyword: str,
    chat_id: int | str,
    domain: str,
    status: str,
    wow_growth: float,
    confidence: int,
    interest: float,
    acceleration: float,
    consistency: float,
    peak_position: float,
    action_label: str = "",
    action_reason: str = "",
    geo: str = "VN",
) -> None:
    """Save a snapshot of a keyword scan into history."""
    chat_id = str(chat_id)
    now = datetime.utcnow().isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO scan_history "
            "(keyword, chat_id, geo, domain, status, wow_growth, confidence, "
            " interest, acceleration, consistency, peak_position, "
            " action_label, action_reason, scanned_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (keyword, chat_id, geo, domain, status, wow_growth, confidence,
             interest, acceleration, consistency, peak_position,
             action_label, action_reason, now),
        )
        await db.commit()


async def get_keyword_history(
    chat_id: int | str,
    keyword: str,
    limit: int = 7,
    geo: str | None = None,
) -> list[dict]:
    """Return recent scan history rows for a user + keyword, newest first."""
    chat_id = str(chat_id)
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        if geo:
            cursor = await db.execute(
                "SELECT status, wow_growth, confidence, interest, "
                " acceleration, consistency, peak_position, "
                " action_label, action_reason, geo, scanned_at "
                "FROM scan_history "
                "WHERE chat_id = ? AND keyword = ? AND geo = ? "
                "ORDER BY scanned_at DESC LIMIT ?",
                (chat_id, keyword, geo, limit),
            )
        else:
            cursor = await db.execute(
                "SELECT status, wow_growth, confidence, interest, "
                " acceleration, consistency, peak_position, "
                " action_label, action_reason, geo, scanned_at "
                "FROM scan_history "
                "WHERE chat_id = ? AND keyword = ? "
                "ORDER BY scanned_at DESC LIMIT ?",
                (chat_id, keyword, limit),
            )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]


async def get_latest_user_snapshots(chat_id: int | str) -> list[dict]:
    """Return the most recent history row per keyword+geo for a user (for digest)."""
    chat_id = str(chat_id)
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT sh.keyword, sh.geo, sh.domain, sh.status, sh.wow_growth, "
            " sh.confidence, sh.interest, sh.acceleration, "
            " sh.consistency, sh.peak_position, sh.scanned_at "
            "FROM scan_history sh "
            "INNER JOIN ("
            "  SELECT keyword, geo, MAX(scanned_at) AS max_at "
            "  FROM scan_history WHERE chat_id = ? "
            "  GROUP BY keyword, geo"
            ") latest ON sh.keyword = latest.keyword "
            "         AND sh.geo = latest.geo "
            "         AND sh.scanned_at = latest.max_at "
            "WHERE sh.chat_id = ? "
            "ORDER BY sh.scanned_at DESC",
            (chat_id, chat_id),
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Alert Cooldown
# ---------------------------------------------------------------------------

async def update_alert_time(keyword_id: int) -> None:
    """Set last_alert_at to now for a tracked keyword."""
    now = datetime.utcnow().isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE tracked_keywords SET last_alert_at = ? WHERE id = ?",
            (now, keyword_id),
        )
        await db.commit()


async def get_last_alert_time(keyword_id: int) -> str | None:
    """Return last_alert_at ISO string for a tracked keyword, or None."""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT last_alert_at FROM tracked_keywords WHERE id = ?",
            (keyword_id,),
        )
        row = await cursor.fetchone()
        return row[0] if row else None


# ---------------------------------------------------------------------------
# Export
# ---------------------------------------------------------------------------

async def export_user_history_csv(
    chat_id: int | str,
    keyword: str | None = None,
    project_name: str | None = None,
) -> str:
    """Generate CSV content of scan history for a user.

    Filters: keyword, project_name, or all if neither specified.
    Returns CSV string.
    """
    chat_id = str(chat_id)
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row

        if keyword:
            cursor = await db.execute(
                "SELECT keyword, geo, domain, status, wow_growth, confidence, "
                "interest, acceleration, consistency, peak_position, "
                "action_label, action_reason, scanned_at "
                "FROM scan_history WHERE chat_id = ? AND keyword = ? "
                "ORDER BY scanned_at DESC",
                (chat_id, keyword),
            )
        elif project_name:
            cursor = await db.execute(
                "SELECT sh.keyword, sh.geo, sh.domain, sh.status, sh.wow_growth, "
                "sh.confidence, sh.interest, sh.acceleration, sh.consistency, "
                "sh.peak_position, sh.action_label, sh.action_reason, sh.scanned_at "
                "FROM scan_history sh "
                "INNER JOIN tracked_keywords tk ON sh.keyword = tk.keyword AND sh.geo = tk.geo "
                "INNER JOIN projects p ON tk.project_id = p.id "
                "WHERE sh.chat_id = ? AND p.name = ? "
                "ORDER BY sh.scanned_at DESC",
                (chat_id, project_name),
            )
        else:
            cursor = await db.execute(
                "SELECT keyword, geo, domain, status, wow_growth, confidence, "
                "interest, acceleration, consistency, peak_position, "
                "action_label, action_reason, scanned_at "
                "FROM scan_history WHERE chat_id = ? "
                "ORDER BY scanned_at DESC",
                (chat_id,),
            )

        rows = await cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(columns)
    for row in rows:
        writer.writerow([row[col] for col in columns])

    return output.getvalue()
