"""
End-to-end test runner for all Bullpen agents.

Hits the Railway-hosted scheduler API to trigger each agent in order,
polls /status until it finishes, then queries Supabase for the agent's
latest agent_runs row. Writes a results report to scripts/test_results.md.

Usage:
    python scripts/test_all_agents.py
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

import httpx

# Ensure repo root on path so we can import shared modules
REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from shared.db import get_client  # noqa: E402

SCHEDULER_API = os.environ.get(
    "SCHEDULER_API_URL",
    "https://worker-production-0d4c.up.railway.app",
)

# Tested in order: fast → slow, cheap → expensive.
AGENTS = [
    "materials_price",
    "freight_volume",
    "census_migration",
    "census_demographics",
    "environmental_risk",
    "flood_risk",
    "txdot_infra",
    "toll_traffic",
    "building_permits",
    "ercot_load",
    "corporate_relocations",
    "hud_vacancy",
    "irs_migration",
    "sales_tax_revenue",
    "zoning_changes",
    "biz_registration",
    "job_demand",
]

MAX_POLL_SECONDS = 600  # 10 min max per agent
POLL_INTERVAL = 5


def trigger(agent: str) -> bool:
    """POST /agents/{agent}/run. Returns True on 200."""
    try:
        resp = httpx.post(f"{SCHEDULER_API}/agents/{agent}/run", timeout=30)
        return resp.status_code == 200
    except Exception as exc:
        print(f"  trigger error: {exc}")
        return False


def wait_until_done(agent: str) -> bool:
    """Poll /agents/{agent}/status until is_running is false. Returns True on completion."""
    deadline = time.time() + MAX_POLL_SECONDS
    while time.time() < deadline:
        try:
            resp = httpx.get(f"{SCHEDULER_API}/agents/{agent}/status", timeout=15)
            if resp.status_code == 200:
                if not resp.json().get("is_running"):
                    return True
        except Exception:
            pass
        time.sleep(POLL_INTERVAL)
    return False


def latest_run(agent: str) -> dict | None:
    """Fetch the most recent agent_runs row for the given agent."""
    try:
        client = get_client()
        result = (
            client.table("agent_runs")
            .select("agent_name,status,records_pulled,records_new,error_message,started_at,finished_at")
            .eq("agent_name", agent)
            .order("started_at", desc=True)
            .limit(1)
            .execute()
        )
        return result.data[0] if result.data else None
    except Exception as exc:
        return {"status": "db_query_error", "error_message": str(exc)}


def main():
    print(f"Testing {len(AGENTS)} agents against {SCHEDULER_API}")
    print("=" * 72)

    # Health check first
    try:
        health = httpx.get(f"{SCHEDULER_API}/health", timeout=10).json()
        print(f"Scheduler health: {health}")
    except Exception as exc:
        print(f"Scheduler unreachable: {exc}")
        return

    results: list[dict] = []

    for i, agent in enumerate(AGENTS, 1):
        print(f"\n[{i}/{len(AGENTS)}] {agent}")
        start = time.time()
        triggered = trigger(agent)
        if not triggered:
            results.append({"agent": agent, "status": "trigger_failed", "duration_s": 0})
            continue

        print(f"  triggered, polling /status...")
        done = wait_until_done(agent)
        duration = round(time.time() - start, 1)

        if not done:
            results.append({"agent": agent, "status": "timeout", "duration_s": duration})
            print(f"  ✗ TIMEOUT after {duration}s")
            continue

        # Give Supabase a moment to be consistent
        time.sleep(2)
        run = latest_run(agent)
        if run is None:
            results.append({"agent": agent, "status": "no_run_row", "duration_s": duration})
            print(f"  ✗ no agent_runs row after {duration}s")
            continue

        row = {
            "agent": agent,
            "status": run.get("status"),
            "records_pulled": run.get("records_pulled"),
            "records_new": run.get("records_new"),
            "error_message": (run.get("error_message") or "")[:200],
            "duration_s": duration,
        }
        results.append(row)
        mark = "✓" if run.get("status") == "success" else "✗"
        print(f"  {mark} {run.get('status')} — pulled={run.get('records_pulled')} new={run.get('records_new')} in {duration}s")
        if run.get("error_message"):
            print(f"    error: {run.get('error_message')[:200]}")

    # Write markdown report
    report_path = REPO_ROOT / "scripts" / "test_results.md"
    lines = [
        "# Bullpen Agent Test Results",
        "",
        f"Tested against: `{SCHEDULER_API}`",
        "",
        "| # | Agent | Status | Pulled | New | Duration | Error |",
        "|---|-------|--------|-------:|----:|---------:|-------|",
    ]
    for i, r in enumerate(results, 1):
        err = r.get("error_message") or ""
        lines.append(
            f"| {i} | {r['agent']} | {r.get('status','')} | {r.get('records_pulled','')} | "
            f"{r.get('records_new','')} | {r.get('duration_s','')}s | {err[:80]} |"
        )
    report_path.write_text("\n".join(lines) + "\n")
    print(f"\nResults written to {report_path}")


if __name__ == "__main__":
    main()
