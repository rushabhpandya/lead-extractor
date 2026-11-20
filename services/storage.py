"""
services/storage.py — Lightweight SQLite Persistent Lead Store.
Operates in WAL mode with normalized indexing, schema versioning, and 7-day TTL expiration.
Faithfully serializes and rehydrates complete LeadRecord payloads.
"""

from __future__ import annotations

import json
import logging
import re
import sqlite3
import time
from pathlib import Path
from typing import List, Optional, Set, Dict, Any

from schemas.lead import LeadRecord

logger = logging.getLogger("storage")

DB_PATH = Path(__file__).parent.parent / "leads_store.db"

# Schema version: incremented whenever enrichment signals, geo-filters, or schema change
CURRENT_SCHEMA_VERSION = 4
SCHEMA_VERSION = CURRENT_SCHEMA_VERSION


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


def init_db(db_path: Optional[Path] = None, force_reset: bool = False) -> None:
    """
    Initialize SQLite database schema and indexes.
    Enforces automatic cache invalidation: drops and recreates leads table if schema_version
    is missing or mismatched, purging stale records whenever filtering or enrichment logic changes.
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

            if force_reset or stored_version != CURRENT_SCHEMA_VERSION:
                logger.info(
                    "SQLite schema version mismatch/upgrade (%d -> %d). Flushing stale cached rows...",
                    stored_version, CURRENT_SCHEMA_VERSION,
                )
                conn.execute("DROP TABLE IF EXISTS leads;")
                conn.execute(
                    "INSERT OR REPLACE INTO meta (key, value) VALUES ('schema_version', ?);",
                    (str(CURRENT_SCHEMA_VERSION),),
                )
                conn.commit()

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS leads (
                    phone_e164 TEXT PRIMARY KEY,
                    category_norm TEXT NOT NULL,
                    location_norm TEXT NOT NULL,
                    schema_version INTEGER NOT NULL DEFAULT 4,
                    data TEXT NOT NULL,
                    scraped_at REAL NOT NULL
                );
                """
            )
            # Remove any stray rows with old schema versions if table existed
            conn.execute(
                "DELETE FROM leads WHERE schema_version != ?;",
                (CURRENT_SCHEMA_VERSION,),
            )
            conn.execute(
                "INSERT OR REPLACE INTO meta (key, value) VALUES ('schema_version', ?);",
                (str(CURRENT_SCHEMA_VERSION),),
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_leads_cat_loc
                ON leads (category_norm, location_norm);
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_leads_scraped_at
                ON leads (scraped_at);
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_leads_schema_version
                ON leads (schema_version);
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
    Faithfully serializes complete LeadRecord payload so no enrichment fields are lost.
    """
    phone_e164 = lead.get("phone_e164")
    if not phone_e164 or not str(phone_e164).strip():
        return False

    cat = lead.get("category", "")
    loc = lead.get("city", "") or lead.get("address", "")
    category_norm = normalize_token(cat)
    location_norm = normalize_token(loc)
    now = time.time()

    try:
        lead_copy = dict(lead)
        lead_copy["source"] = lead_copy.get("source") or "sqlite"
        record = LeadRecord.model_validate(lead_copy)
        data_json = json.dumps(record.to_legacy_dict())

        with get_connection(db_path) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO leads (phone_e164, category_norm, location_norm, schema_version, data, scraped_at)
                VALUES (?, ?, ?, ?, ?, ?);
                """,
                (phone_e164, category_norm, location_norm, CURRENT_SCHEMA_VERSION, data_json, now),
            )
            conn.commit()
        return True
    except Exception as exc:
        logger.debug("Error saving lead %s to SQLite: %s", phone_e164, exc)
        return False


def save_leads_batch(leads: List[dict], db_path: Optional[Path] = None) -> int:
    """
    Batch-save multiple verified leads into SQLite store with full schema serialization.
    """
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
        category_norm = normalize_token(cat)
        location_norm = normalize_token(loc)
        try:
            lead_copy = dict(lead)
            lead_copy["source"] = lead_copy.get("source") or "sqlite"
            record = LeadRecord.model_validate(lead_copy)
            data_json = json.dumps(record.to_legacy_dict())
            rows.append((phone_e164, category_norm, location_norm, CURRENT_SCHEMA_VERSION, data_json, now))
        except Exception:
            continue

    if not rows:
        return 0

    try:
        with get_connection(db_path) as conn:
            conn.executemany(
                """
                INSERT OR REPLACE INTO leads (phone_e164, category_norm, location_norm, schema_version, data, scraped_at)
                VALUES (?, ?, ?, ?, ?, ?);
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
    Validates records against 7-day TTL, schema_version, and normalized category + location.
    Faithfully rehydrates full LeadRecord schema so no enrichment fields are lost.
    """
    category_norm = normalize_token(category)
    location_norm = normalize_token(location)
    cutoff = time.time() - (ttl_days * 86400.0)

    results: List[dict] = []
    try:
        with get_connection(db_path) as conn:
            cursor = conn.cursor()
            query = """
                SELECT data FROM leads
                WHERE scraped_at >= ?
                  AND schema_version = ?
                  AND category_norm = ?
                  AND (
                      location_norm = ?
                      OR location_norm LIKE ?
                      OR ? LIKE '%' || location_norm || '%'
                  )
                ORDER BY scraped_at DESC;
            """
            cursor.execute(
                query,
                (cutoff, CURRENT_SCHEMA_VERSION, category_norm, location_norm, f"%{location_norm}%", location_norm),
            )
            rows = cursor.fetchall()
            for (data_json,) in rows:
                try:
                    lead_dict = json.loads(data_json)
                    record = LeadRecord.model_validate(lead_dict)
                    results.append(record.to_legacy_dict())
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
            cursor.execute("SELECT phone_e164 FROM leads WHERE schema_version = ?;", (CURRENT_SCHEMA_VERSION,))
            for (phone,) in cursor.fetchall():
                if phone:
                    phones.add(phone)
    except Exception as exc:
        logger.debug("Failed to fetch cached phones: %s", exc)
    return phones
