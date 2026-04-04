"""
Supabase client wrapper and common database helpers.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Optional

from supabase import Client, create_client

from shared.config import SUPABASE_KEY, SUPABASE_URL

_client: Optional[Client] = None


def get_client() -> Client:
    """Return a singleton Supabase client."""
    global _client
    if _client is None:
        if not SUPABASE_URL or not SUPABASE_KEY:
            raise RuntimeError(
                "SUPABASE_URL and SUPABASE_KEY must be set in the environment."
            )
        _client = create_client(SUPABASE_URL, SUPABASE_KEY)
    return _client


def log_agent_run(
    agent_name: str,
    status: str,
    records_pulled: int = 0,
    records_new: int = 0,
    error_message: Optional[str] = None,
) -> dict:
    """Insert a row into agent_runs and return it."""
    now = datetime.now(timezone.utc).isoformat()
    row = {
        "id": str(uuid.uuid4()),
        "agent_name": agent_name,
        "started_at": now,
        "finished_at": now,
        "status": status,
        "records_pulled": records_pulled,
        "records_new": records_new,
        "error_message": error_message,
    }
    result = get_client().table("agent_runs").insert(row).execute()
    return result.data[0] if result.data else row


def get_last_successful_run(agent_name: str) -> Optional[str]:
    """Return the finished_at timestamp of the most recent successful run, or None."""
    result = (
        get_client()
        .table("agent_runs")
        .select("finished_at")
        .eq("agent_name", agent_name)
        .eq("status", "success")
        .order("finished_at", desc=True)
        .limit(1)
        .execute()
    )
    if result.data:
        return result.data[0]["finished_at"]
    return None
