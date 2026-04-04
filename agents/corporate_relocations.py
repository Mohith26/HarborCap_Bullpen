"""
Corporate Relocations Agent
-----------------------------
Monitors Texas Governor press releases and BusinessInTexas.com for
corporate relocation, expansion, and facility announcements. Extracts
dollar amounts and job counts from article titles, then creates signals
for significant economic development activity that may drive industrial
real estate demand.
"""

from __future__ import annotations

import hashlib
import re
import uuid
from typing import Any, Optional

import httpx
from prefect import flow, task

from shared.db import log_agent_run
from shared.signals import create_signal

AGENT_NAME = "corporate_relocations"

NEWS_URLS: list[str] = [
    "https://gov.texas.gov/news",
    "https://businessintexas.com/news/",
]

# Keywords indicating potential corporate relocation / expansion
KEYWORDS: list[str] = [
    "relocat",
    "expand",
    "headquarter",
    "new facility",
    "distribution",
    "warehouse",
    "manufacturing",
    "million",
    "jobs",
    "texas",
]

# Regex patterns for extracting economic data from titles
DOLLAR_RE = re.compile(r"\$[\d,.]+ (?:million|billion)", re.IGNORECASE)
JOBS_RE = re.compile(r"(\d[\d,]*)\s+(?:jobs|employees|positions)", re.IGNORECASE)

# Regex to extract <a> tags with href and title text from HTML
LINK_RE = re.compile(
    r'<a\s[^>]*href=["\']([^"\']+)["\'][^>]*>(.*?)</a>',
    re.IGNORECASE | re.DOTALL,
)

# Strip HTML tags from extracted text
TAG_RE = re.compile(r"<[^>]+>")


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

@task(name="fetch_press_releases", retries=2, retry_delay_seconds=30)
def fetch_press_releases(url: str) -> Optional[str]:
    """GET a news page and return its HTML text. Returns None on error."""
    try:
        resp = httpx.get(
            url,
            timeout=30,
            follow_redirects=True,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (compatible; HarborCapBullpen/1.0; "
                    "+https://harborcap.com)"
                ),
            },
        )
        resp.raise_for_status()
        return resp.text
    except Exception as exc:
        print(f"  Error fetching {url}: {exc}")
        return None


@task(name="parse_announcements")
def parse_announcements(
    html: str, source_url: str
) -> list[dict[str, Any]]:
    """Extract article links from HTML using regex, then filter for
    relocation/expansion keywords.

    Returns a list of dicts with keys: title, url, source_url.
    """
    matches = LINK_RE.findall(html)
    announcements: list[dict[str, Any]] = []
    seen_urls: set[str] = set()

    for href, raw_title in matches:
        # Clean up title text (strip inner HTML tags and whitespace)
        title = TAG_RE.sub("", raw_title).strip()

        if not title or len(title) < 10:
            continue

        # Resolve relative URLs
        if href.startswith("/"):
            # Extract base domain from source_url
            from urllib.parse import urlparse

            parsed = urlparse(source_url)
            href = f"{parsed.scheme}://{parsed.netloc}{href}"

        # Deduplicate by URL
        if href in seen_urls:
            continue

        # Check for keyword match (case-insensitive)
        title_lower = title.lower()
        if any(kw in title_lower for kw in KEYWORDS):
            seen_urls.add(href)
            announcements.append(
                {
                    "title": title,
                    "url": href,
                    "source_url": source_url,
                }
            )

    return announcements


@task(name="signal_announcements")
def signal_announcements(announcements: list[dict[str, Any]]) -> int:
    """Create signals for each matching announcement. Deduplicates by
    hashing the title to avoid repeat signals on subsequent runs.

    Returns the number of signals created.
    """
    signal_count = 0
    seen_hashes: set[str] = set()

    for ann in announcements:
        title = ann["title"]
        article_url = ann["url"]

        # Deduplicate by title hash
        title_hash = hashlib.sha256(title.encode("utf-8")).hexdigest()[:16]
        if title_hash in seen_hashes:
            continue
        seen_hashes.add(title_hash)

        # Extract dollar amounts and job counts
        dollar_match = DOLLAR_RE.search(title)
        jobs_match = JOBS_RE.search(title)

        dollar_text = dollar_match.group(0) if dollar_match else None
        jobs_count: int | None = None
        if jobs_match:
            jobs_count = int(jobs_match.group(1).replace(",", ""))

        # Parse dollar amount for severity thresholds
        dollar_millions: float = 0.0
        if dollar_text:
            amount_str = re.sub(r"[,$]", "", dollar_text.split()[0])
            try:
                amount = float(amount_str)
            except ValueError:
                amount = 0.0
            if "billion" in dollar_text.lower():
                dollar_millions = amount * 1000
            else:
                dollar_millions = amount

        # Determine severity
        severity = "info"  # default for any keyword match
        if dollar_millions > 100 or (jobs_count is not None and jobs_count > 500):
            severity = "alert"
        elif dollar_millions > 25 or (jobs_count is not None and jobs_count > 100):
            severity = "watch"

        # Build summary
        details: list[str] = []
        if dollar_text:
            details.append(f"Investment: {dollar_text}")
        if jobs_count is not None:
            details.append(f"Jobs: {jobs_count:,}")
        detail_str = ". ".join(details) if details else "Keyword match in title"

        create_signal(
            agent_name=AGENT_NAME,
            signal_type="corporate_relocation",
            severity=severity,
            title=f"Corp relocation: {title[:80]}",
            summary=(
                f"Announcement detected: \"{title}\". {detail_str}. "
                f"Source: {ann['source_url']}"
            ),
            data={
                "title": title,
                "article_url": article_url,
                "dollar_amount": dollar_text,
                "dollar_millions": dollar_millions,
                "jobs_count": jobs_count,
                "title_hash": title_hash,
            },
            source_url=article_url,
        )
        signal_count += 1

    return signal_count


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------

@flow(name="corporate_relocations_agent", log_prints=True)
def corporate_relocations_agent() -> dict[str, Any]:
    """Scrape Texas news sources for corporate relocation announcements."""
    total_pulled = 0
    total_new = 0
    total_signals = 0

    try:
        all_announcements: list[dict[str, Any]] = []

        for url in NEWS_URLS:
            print(f"\nFetching {url} …")
            html = fetch_press_releases(url)

            if html is None:
                print(f"  Skipped (fetch failed)")
                continue

            total_pulled += 1
            announcements = parse_announcements(html, url)
            print(f"  Found {len(announcements)} matching announcements")
            all_announcements.extend(announcements)

        total_new = len(all_announcements)

        if all_announcements:
            total_signals = signal_announcements(all_announcements)
            print(f"\nCreated {total_signals} signals from {total_new} announcements")
        else:
            print("\nNo matching announcements found")

        log_agent_run(
            agent_name=AGENT_NAME,
            status="success",
            records_pulled=total_pulled,
            records_new=total_new,
        )
        print(
            f"\nAgent complete: {total_pulled} pages fetched, "
            f"{total_new} announcements, {total_signals} signals"
        )
        return {
            "status": "success",
            "pulled": total_pulled,
            "new": total_new,
            "signals": total_signals,
        }

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
    corporate_relocations_agent()
