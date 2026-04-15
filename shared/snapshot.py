"""
Webpage → PDF snapshot utility.

Renders a URL as a PDF with Playwright (headless chromium), uploads it to the
Supabase Storage bucket named ``snapshots``, and returns a public URL.

Design:
- SHA256(url) is the dedup key for a given agent within a given month.
- File path: ``{agent_name}/{YYYY-MM}/{sha256(url)}.pdf``
- Idempotent: if the file already exists, we return the existing public URL
  without re-rendering (saves time + bandwidth).
- All failures are caught. The function returns ``None`` instead of raising,
  so a snapshot failure never blocks signal creation.
"""

from __future__ import annotations

import hashlib
import logging
from datetime import datetime, timezone
from typing import Optional

from shared.db import get_client

logger = logging.getLogger("bullpen.snapshot")

_BUCKET = "snapshots"

# Playwright import is lazy — the module must be importable on machines
# that don't have playwright installed (e.g. the dashboard build doesn't).
_playwright_ctx = None


def _file_key(url: str, agent_name: str) -> str:
    """Build the deterministic object key for this URL in the current month."""
    month = datetime.now(timezone.utc).strftime("%Y-%m")
    url_hash = hashlib.sha256(url.encode("utf-8")).hexdigest()[:24]
    safe_agent = agent_name.replace("/", "_")
    return f"{safe_agent}/{month}/{url_hash}.pdf"


def _existing_public_url(client, key: str) -> Optional[str]:
    """If a file already exists at *key* in the snapshots bucket, return its public URL."""
    try:
        # Listing with a prefix and checking names is the most portable way.
        folder = "/".join(key.split("/")[:-1])
        filename = key.split("/")[-1]
        listed = client.storage.from_(_BUCKET).list(folder)
        if isinstance(listed, list) and any(item.get("name") == filename for item in listed):
            return client.storage.from_(_BUCKET).get_public_url(key)
    except Exception:
        pass
    return None


def _render_pdf(url: str, timeout_ms: int = 30000) -> Optional[bytes]:
    """Launch headless chromium, visit *url*, return PDF bytes or None."""
    try:
        from playwright.sync_api import sync_playwright  # type: ignore
    except ImportError:
        logger.warning("playwright not installed; skipping snapshot for %s", url)
        return None

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
            try:
                context = browser.new_context(
                    viewport={"width": 1280, "height": 900},
                    user_agent=(
                        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/131.0.0.0 Safari/537.36 BullpenSnapshot/1.0"
                    ),
                )
                page = context.new_page()
                page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
                # Best-effort wait for network to settle, but don't block forever
                try:
                    page.wait_for_load_state("networkidle", timeout=5000)
                except Exception:
                    pass
                pdf_bytes = page.pdf(format="Letter", print_background=True)
                return pdf_bytes
            finally:
                browser.close()
    except Exception as exc:
        logger.warning("Playwright render failed for %s: %s", url, exc)
        return None


def capture_snapshot(url: str, agent_name: str) -> Optional[str]:
    """
    Capture ``url`` as a PDF and upload it to Supabase Storage.

    Returns the public URL of the stored PDF, or None if any step failed.
    Never raises — callers can safely use it from signal-creation paths.
    """
    if not url:
        return None

    try:
        client = get_client()
    except Exception as exc:
        logger.warning("snapshot: cannot get Supabase client: %s", exc)
        return None

    key = _file_key(url, agent_name)

    # Dedup: if the file already exists this month, reuse it.
    existing = _existing_public_url(client, key)
    if existing:
        return existing

    pdf_bytes = _render_pdf(url)
    if not pdf_bytes:
        return None

    try:
        client.storage.from_(_BUCKET).upload(
            key,
            pdf_bytes,
            {"content-type": "application/pdf", "upsert": "true"},
        )
        return client.storage.from_(_BUCKET).get_public_url(key)
    except Exception as exc:
        logger.warning("snapshot upload failed for %s: %s", url, exc)
        return None
