"""
Helper for creating signals in the signals table.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from shared.db import get_client


def create_signal(
    agent_name: str,
    signal_type: str,
    severity: str,
    title: str,
    summary: str,
    location: Optional[str] = None,
    submarket: Optional[str] = None,
    property_id: Optional[str] = None,
    data: Optional[dict[str, Any]] = None,
    source_url: Optional[str] = None,
) -> dict:
    """Insert a signal row and return it.

    Parameters
    ----------
    location : str | None
        A WKT POINT string, e.g. ``"POINT(-95.3698 29.7604)"``.
        Supabase/PostGIS will cast it to GEOGRAPHY automatically.
    """
    if data is None:
        data = {}

    row: dict[str, Any] = {
        "id": str(uuid.uuid4()),
        "agent_name": agent_name,
        "signal_type": signal_type,
        "severity": severity,
        "title": title,
        "summary": summary,
        "submarket": submarket,
        "data": data,
        "source_url": source_url,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "acknowledged": False,
    }

    if property_id is not None:
        row["property_id"] = property_id
    if location is not None:
        row["location"] = location

    result = get_client().table("signals").insert(row).execute()
    return result.data[0] if result.data else row
