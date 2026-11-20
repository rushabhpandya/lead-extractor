"""
services/storage.py — Lightweight SQLite Persistent Lead Store.
Operates in WAL mode with normalized indexing and 7-day TTL expiration.
"""

from __future__ import annotations

import json
import logging
import re
import sqlite3
import time
from pathlib import Path
from typing import List, Optional, Set, Dict, Any

logger = logging.getLogger("storage")

DB_PATH = Path(__file__).parent.parent / "leads_store.db"

# Schema version: incremented whenever enrichment signals, geo-filters, or schema change
SCHEMA_VERSION = 2


def normalize_token(text: str) -> str:
    """Normalize text into alphanumeric lowercase for robust index lookup."""
    if not text:
        return ""
    return re.sub(r"\W+", "", text.strip().lower())


def get_connection(db_path: Optional[Path] = None) -> sqlite3.Connection:
    path = db_path or DB_PATH
    conn = sqlite3.connect(str(path), timeout=10.0)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def init_db(db_path: Optional[Path] = None) -> None:
    """
    Initialize SQLite database schema and indexes.
    Enforces schema versioning: flushes stale cached rows if schema_version is older.
    """
    try:
        with get_connection(db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                """
            )
            cursor = conn.cursor()
            cursor.execute("SELECT value FROM meta WHERE key = 'schema_version';")
            row = cursor.fetchone()
            stored_version = int(row[0]) if row and row[0].isdigit() else 0

            if stored_version < SCHEMA_VERSION:
                logger.info(
                    "SQLite schema version upgrade (%d -> %d). Flushing stale cached rows...",
                    stored_version, SCHEMA_VERSION,
                )
                conn.execute("DROP TABLE IF EXISTS leads;")
                conn.execute(
                    "INSERT OR REPLACE INTO meta (key, value) VALUES ('schema_version', ?);",
                    (str(SCHEMA_VERSION),),
                )
                conn.commit()

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS leads (
                    phone_e164 TEXT PRIMARY KEY,
                    norm_category TEXT NOT NULL,
                    norm_location TEXT NOT NULL,
                    data TEXT NOT NULL,
                    scraped_at REAL NOT NULL
                );
                """
            )
            conn.execute(
                "INSERT OR REPLACE INTO meta (key, value) VALUES ('schema_version', ?);",
                (str(SCHEMA_VERSION),),
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_leads_cat_loc
                ON leads (norm_category, norm_location);
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_leads_scraped_at
                ON leads (scraped_at);
                """
            )
            conn.commit()
    except Exception as exc:
        logger.error("Failed to initialize SQLite store: %s", exc)


# Auto-initialize on module load
init_db()


def save_lead_to_db(lead: dict, db_path: Optional[Path] = None) -> bool:
    """
    Persist a verified lead into SQLite store keyed by phone_e164.
    Silently ignores records lacking a valid phone_e164.
    """
    phone_e164 = lead.get("phone_e164")
    if not phone_e164 or not str(phone_e164).strip():
        return False

    cat = lead.get("category", "")
    loc = lead.get("city", "") or lead.get("address", "")
    norm_cat = normalize_token(cat)
    norm_loc = normalize_token(loc)
    now = time.time()

    # Ensure source tag reflects SQLite when reloaded later
    lead_copy = dict(lead)
    if "source" not in lead_copy or not lead_copy["source"]:
        lead_copy["source"] = "sqlite"

    try:
        data_json = json.dumps(lead_copy)
        with get_connection(db_path) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO leads (phone_e164, norm_category, norm_location, data, scraped_at)
                VALUES (?, ?, ?, ?, ?);
                """,
                (phone_e164, norm_cat, norm_loc, data_json, now),
            )
            conn.commit()
        return True
    except Exception as exc:
        logger.debug("Error saving lead %s to SQLite: %s", phone_e164, exc)
        return False


def save_leads_batch(leads: List[dict], db_path: Optional[Path] = None) -> int:
    """Batch-save multiple verified leads into SQLite store."""
    if not leads:
        return 0

    now = time.time()
    rows = []
    for lead in leads:
        phone_e164 = lead.get("phone_e164")
        if not phone_e164 or not str(phone_e164).strip():
            continue
        cat = lead.get("category", "")
        loc = lead.get("city", "") or lead.get("address", "")
        norm_cat = normalize_token(cat)
        norm_loc = normalize_token(loc)
        lead_copy = dict(lead)
        lead_copy["source"] = lead_copy.get("source") or "sqlite"
        rows.append((phone_e164, norm_cat, norm_loc, json.dumps(lead_copy), now))

    if not rows:
        return 0

    try:
        with get_connection(db_path) as conn:
            conn.executemany(
                """
                INSERT OR REPLACE INTO leads (phone_e164, norm_category, norm_location, data, scraped_at)
                VALUES (?, ?, ?, ?, ?);
                """,
                rows,
            )
            conn.commit()
        return len(rows)
    except Exception as exc:
        logger.error("Batch save to SQLite failed: %s", exc)
        return 0


def get_cached_leads_from_db(
    category: str,
    location: str,
    ttl_days: float = 7.0,
    db_path: Optional[Path] = None,
) -> List[dict]:
    """
    Retrieve non-expired matching leads from the SQLite store.
    Validates records against 7-day TTL and normalized category + location.
    """
    norm_cat = normalize_token(category)
    norm_loc = normalize_token(location)
    cutoff = time.time() - (ttl_days * 86400.0)

    results: List[dict] = []
    try:
        with get_connection(db_path) as conn:
            cursor = conn.cursor()
            # Match on category and location (exact or substring)
            query = """
                SELECT data FROM leads
                WHERE scraped_at >= ?
                  AND norm_category = ?
                  AND (
                      norm_location = ?
                      OR norm_location LIKE ?
                      OR ? LIKE '%' || norm_location || '%'
                  )
                ORDER BY scraped_at DESC;
            """
            cursor.execute(query, (cutoff, norm_cat, norm_loc, f"%{norm_loc}%", norm_loc))
            rows = cursor.fetchall()
            for (data_json,) in rows:
                try:
                    lead = json.loads(data_json)
                    results.append(lead)
                except Exception:
                    continue
    except Exception as exc:
        logger.debug("Failed to query SQLite cache: %s", exc)

    return results


def get_all_cached_phones(db_path: Optional[Path] = None) -> Set[str]:
    """Return all E.164 phone numbers currently stored in SQLite."""
    phones: Set[str] = set()
    try:
        with get_connection(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT phone_e164 FROM leads;")
            for (phone,) in cursor.fetchall():
                if phone:
                    phones.add(phone)
    except Exception as exc:
        logger.debug("Failed to fetch cached phones: %s", exc)
    return phones
