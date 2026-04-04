"""
Job Demand Agent
----------------
Scrapes warehouse / logistics / industrial job postings across Texas
using python-jobspy (Indeed), stores them in the job_postings table,
and creates signals when hiring clusters are detected.
"""

from __future__ import annotations

import hashlib
import uuid
from collections import Counter
from datetime import datetime, timezone
from typing import Any

import pandas as pd
from jobspy import scrape_jobs
from prefect import flow, task

from shared.db import get_client, log_agent_run
from shared.signals import create_signal

AGENT_NAME = "job_demand"

TARGET_SEARCHES: list[dict[str, str]] = [
    {"search_term": "warehouse operations", "location": "Houston, TX"},
    {"search_term": "logistics manager", "location": "Dallas, TX"},
    {"search_term": "distribution center", "location": "Austin, TX"},
    {"search_term": "manufacturing technician", "location": "San Antonio, TX"},
    {"search_term": "forklift operator", "location": "Houston, TX"},
    {"search_term": "supply chain", "location": "Fort Worth, TX"},
    {"search_term": "warehouse associate", "location": "Dallas, TX"},
    {"search_term": "freight handler", "location": "Houston, TX"},
]

CLUSTER_THRESHOLD = 8
CLUSTER_ALERT_THRESHOLD = 12


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_unique_key(title: str, company: str, date_posted: str | None) -> str:
    """Generate a deterministic unique key from job attributes."""
    raw = f"{title or ''}|{company or ''}|{date_posted or ''}"
    return hashlib.sha256(raw.encode()).hexdigest()[:32]


def _parse_city(location_str: str | None) -> str:
    """Extract city name from a location string like 'Houston, TX'."""
    if not location_str:
        return "Unknown"
    # Take the part before the first comma, strip whitespace
    city = location_str.split(",")[0].strip()
    return city if city else "Unknown"


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

@task(name="scrape_job_listings", retries=1, retry_delay_seconds=30)
def scrape_job_listings(search_term: str, location: str) -> list[dict[str, Any]]:
    """Scrape Indeed for job listings matching the given search in a location.

    Returns a list of dicts (one per job). Returns an empty list if
    the scrape fails (e.g. rate-limiting).
    """
    try:
        jobs_df: pd.DataFrame = scrape_jobs(
            site_name=["indeed"],
            search_term=search_term,
            location=location,
            results_wanted=25,
            hours_old=72,
            country_indeed="USA",
        )

        if jobs_df is None or jobs_df.empty:
            return []

        # Normalise NaN / NaT to None so JSON serialisation works
        jobs_df = jobs_df.where(jobs_df.notnull(), None)
        records = jobs_df.to_dict(orient="records")

        # Attach the search metadata to each record for traceability
        for rec in records:
            rec["_search_term"] = search_term
            rec["_search_location"] = location

        return records

    except Exception as exc:  # noqa: BLE001
        print(
            f"[{AGENT_NAME}] scrape failed for '{search_term}' in {location}: {exc}"
        )
        return []


@task(name="store_jobs")
def store_jobs(all_jobs: list[dict[str, Any]]) -> int:
    """Write job records to the Supabase job_postings table via upsert.

    Returns the number of rows written.
    """
    if not all_jobs:
        return 0

    client = get_client()
    rows: list[dict[str, Any]] = []

    for job in all_jobs:
        title = str(job.get("title") or "")
        company = str(job.get("company") or "")
        date_posted = job.get("date_posted")
        if isinstance(date_posted, datetime):
            date_posted = date_posted.date().isoformat()
        elif date_posted is not None:
            date_posted = str(date_posted)[:10]

        unique_key = _make_unique_key(title, company, date_posted)

        # Build a JSON-safe copy of the full record for raw_data
        raw_data: dict[str, Any] = {}
        for k, v in job.items():
            if isinstance(v, (datetime, pd.Timestamp)):
                raw_data[k] = str(v)
            elif hasattr(v, 'isoformat'):  # date, time, etc.
                raw_data[k] = str(v)
            elif isinstance(v, float) and pd.isna(v):
                raw_data[k] = None
            elif isinstance(v, (list, dict, str, int, float, bool, type(None))):
                raw_data[k] = v
            else:
                raw_data[k] = str(v)

        job_url = str(job.get("job_url") or job.get("link") or "")

        row: dict[str, Any] = {
            "id": str(uuid.uuid4()),
            "unique_key": unique_key,
            "title": title,
            "company": company,
            "company_industry": str(job.get("company_industry") or ""),
            "city": _parse_city(
                str(job.get("location") or job.get("_search_location") or "")
            ),
            "job_type": str(job.get("job_type") or ""),
            "date_posted": date_posted,
            "source": "indeed",
            "url": job_url,
            "raw_data": raw_data,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        rows.append(row)

    # Dedupe rows by unique_key before upserting (same job can appear in multiple searches)
    seen: dict[str, dict[str, Any]] = {}
    for row in rows:
        seen[row["unique_key"]] = row
    deduped = list(seen.values())

    # Upsert in batches to avoid payload limits
    written = 0
    batch_size = 50
    for i in range(0, len(deduped), batch_size):
        batch = deduped[i : i + batch_size]
        result = (
            client.table("job_postings")
            .upsert(batch, on_conflict="unique_key")
            .execute()
        )
        written += len(result.data) if result.data else 0
    return written


@task(name="detect_clusters")
def detect_clusters(all_jobs: list[dict[str, Any]]) -> int:
    """Detect hiring clusters by city and create signals.

    Returns the number of signals created.
    """
    if not all_jobs:
        return 0

    # Group jobs by city
    city_jobs: dict[str, list[dict[str, Any]]] = {}
    for job in all_jobs:
        city = _parse_city(
            str(job.get("location") or job.get("_search_location") or "")
        )
        city_jobs.setdefault(city, []).append(job)

    signals_created = 0

    for city, jobs in city_jobs.items():
        count = len(jobs)
        if count < CLUSTER_THRESHOLD:
            continue

        severity = "alert" if count >= CLUSTER_ALERT_THRESHOLD else "watch"

        # Find top companies
        company_counts = Counter(
            str(j.get("company") or "Unknown") for j in jobs
        )
        top_companies = [
            name for name, _ in company_counts.most_common(5) if name != "Unknown"
        ]

        top_str = ", ".join(top_companies) if top_companies else "various employers"
        title = f"Hiring surge: {count} warehouse/logistics jobs in {city}"
        summary = (
            f"Detected {count} new job postings in {city} over the past 72 hours. "
            f"Top companies hiring: {top_str}."
        )

        # Build Indeed search link for this cluster
        search_url = f"https://www.indeed.com/jobs?q=warehouse+logistics&l={city.replace(' ', '+')}%2C+TX"

        # Collect individual job URLs for the data payload
        job_urls = [str(j.get("job_url") or j.get("link") or "") for j in jobs[:10] if j.get("job_url") or j.get("link")]

        create_signal(
            agent_name=AGENT_NAME,
            signal_type="job_cluster",
            severity=severity,
            title=title,
            summary=summary,
            source_url=search_url,
            data={
                "count": count,
                "city": city,
                "top_companies": top_companies,
                "sample_job_urls": job_urls,
            },
        )
        signals_created += 1
        print(f"[{AGENT_NAME}] Signal created: {title}")

    return signals_created


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------

@flow(name="job_demand_agent", log_prints=True)
def job_demand_agent() -> dict[str, Any]:
    """Orchestrate: scrape job listings, store, detect clusters, log run."""
    total_pulled = 0
    total_new = 0

    try:
        all_jobs: list[dict[str, Any]] = []

        for search in TARGET_SEARCHES:
            print(
                f"Scraping '{search['search_term']}' in {search['location']}..."
            )
            jobs = scrape_job_listings(
                search_term=search["search_term"],
                location=search["location"],
            )
            print(f"  -> {len(jobs)} results")
            all_jobs.extend(jobs)

        total_pulled = len(all_jobs)
        print(f"Total jobs scraped: {total_pulled}")

        total_new = store_jobs(all_jobs)
        print(f"Stored {total_new} job postings")

        signals = detect_clusters(all_jobs)
        print(f"Created {signals} cluster signals")

        log_agent_run(
            agent_name=AGENT_NAME,
            status="success",
            records_pulled=total_pulled,
            records_new=total_new,
        )
        return {"status": "success", "pulled": total_pulled, "new": total_new, "signals": signals}

    except Exception as exc:
        log_agent_run(
            agent_name=AGENT_NAME,
            status="failed",
            records_pulled=total_pulled,
            records_new=total_new,
            error_message=str(exc),
        )
        raise


if __name__ == "__main__":
    job_demand_agent()
