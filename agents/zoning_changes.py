"""
Zoning Changes Agent
--------------------
Monitors Austin's open data portal for zoning change applications
relevant to industrial / warehouse use, and creates watch or alert
signals for the Bullpen dashboard.
"""

from __future__ import annotations

import uuid
from datetime import date, timedelta
from typing import Any

import httpx
from prefect import flow, task

from shared.db import log_agent_run
from shared.signals import create_signal

AGENT_NAME = "zoning_changes"

CITY_APIS: dict[str, dict[str, str]] = {
    "austin": {
        "url": "https://data.austintexas.gov/resource/n7sc-v9nz.json",
        "date_field": "submitted_date",
        "description_field": "description",
        "case_field": "case_number",
        "address_field": "address",
        "status_field": "status",
    },
}

INDUSTRIAL_KEYWORDS: list[str] = [
    "industrial",
    "warehouse",
    "manufacturing",
    "logistics",
    "distribution",
    "flex",
    "storage",
    "I-RR",
    "LI-",
    "HI-",
    "M-1",
    "M-2",
    "IP-",
    "W/",
    "cold storage",
    "data center",
    "truck",
    "freight",
]

# Lower-cased version for matching
_KEYWORDS_LOWER: list[str] = [kw.lower() for kw in INDUSTRIAL_KEYWORDS]


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

@task(name="fetch_zoning_cases", retries=2, retry_delay_seconds=10)
def fetch_zoning_cases(city: str, config: dict[str, str]) -> list[dict[str, Any]]:
    """Fetch recent zoning change cases from the Socrata API."""
    since = (date.today() - timedelta(days=90)).isoformat()
    date_field = config["date_field"]
    params = {
        "$where": f"{date_field} > '{since}T00:00:00.000'",
        "$order": f"{date_field} DESC",
        "$limit": 500,
    }
    try:
        resp = httpx.get(config["url"], params=params, timeout=60)
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:
        print(f"[{AGENT_NAME}] Error fetching {city} zoning cases: {exc}")
        return []


@task(name="filter_industrial_zoning")
def filter_industrial_zoning(
    cases: list[dict[str, Any]], config: dict[str, str]
) -> list[dict[str, Any]]:
    """Keep cases whose description or zoning codes match industrial keywords."""
    description_field = config["description_field"]
    filtered: list[dict[str, Any]] = []

    for case in cases:
        # Build a combined text blob from all relevant fields
        parts: list[str] = [
            str(case.get(description_field, "")),
            str(case.get("current_zoning", "")),
            str(case.get("proposed_zoning", "")),
        ]
        # Also scan every string value for zoning codes
        for value in case.values():
            if isinstance(value, str):
                parts.append(value)

        full_text = " ".join(parts).lower()

        if any(kw in full_text for kw in _KEYWORDS_LOWER):
            filtered.append(case)

    return filtered


@task(name="signal_zoning_changes")
def signal_zoning_changes(
    cases: list[dict[str, Any]], city: str, config: dict[str, str]
) -> int:
    """Create a signal for each filtered zoning case. Returns signal count."""
    description_field = config["description_field"]
    case_field = config["case_field"]
    address_field = config["address_field"]
    status_field = config["status_field"]
    date_field = config["date_field"]

    count = 0

    for case in cases:
        case_number = case.get(case_field, "N/A")
        description = str(case.get(description_field, ""))
        address = case.get(address_field, "N/A")
        status = case.get(status_field, "N/A")
        submitted_date = case.get(date_field, "")[:10] if case.get(date_field) else None
        current_zoning = case.get("current_zoning", "N/A")
        proposed_zoning = case.get("proposed_zoning", "N/A")

        lat = case.get("latitude")
        lng = case.get("longitude")
        location_wkt = f"POINT({lng} {lat})" if lat and lng else None

        # Determine severity
        desc_lower = description.lower()
        rezone_to_industrial = any(
            phrase in desc_lower
            for phrase in ["rezone", "change to i-", "change to li", "change to hi"]
        )
        if rezone_to_industrial:
            severity = "alert"
        else:
            severity = "watch"

        source_url = (
            config.get("link_url")
            or f"{config['url']}?{case_field}={case_number}"
        )

        signal_title = f"Zoning case: {case_number} — {description[:60]}"

        summary = (
            f"Case {case_number} at {address}, {city.title()}. "
            f"Current zoning: {current_zoning}. Proposed zoning: {proposed_zoning}. "
            f"Status: {status}. Submitted: {submitted_date or 'N/A'}. "
            f"{description}"
        )

        create_signal(
            agent_name=AGENT_NAME,
            signal_type="zoning_change",
            severity=severity,
            title=signal_title,
            summary=summary,
            location=location_wkt,
            submarket="Austin East",
            data={
                "case_number": case_number,
                "current_zoning": current_zoning,
                "proposed_zoning": proposed_zoning,
                "address": address,
                "status": status,
                "submitted_date": submitted_date,
                "city": city,
            },
            source_url=source_url,
        )
        count += 1

    return count


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------

@flow(name="zoning_changes_agent", log_prints=True)
def zoning_changes_agent() -> dict[str, Any]:
    """Orchestrate: fetch zoning cases, filter, signal, log."""
    total_pulled = 0
    total_signals = 0

    try:
        for city, config in CITY_APIS.items():
            raw_cases = fetch_zoning_cases(city, config)
            total_pulled += len(raw_cases)
            print(f"Fetched {len(raw_cases)} zoning cases from {city}")

            industrial = filter_industrial_zoning(raw_cases, config)
            print(f"Filtered to {len(industrial)} industrial-related zoning cases")

            signals = signal_zoning_changes(industrial, city, config)
            total_signals += signals
            print(f"Created {signals} signals for {city}")

        log_agent_run(
            agent_name=AGENT_NAME,
            status="success",
            records_pulled=total_pulled,
            records_new=total_signals,
        )
        return {"status": "success", "pulled": total_pulled, "signals": total_signals}

    except Exception as exc:
        log_agent_run(
            agent_name=AGENT_NAME,
            status="failed",
            records_pulled=total_pulled,
            records_new=total_signals,
            error_message=str(exc),
        )
        raise


if __name__ == "__main__":
    zoning_changes_agent()
