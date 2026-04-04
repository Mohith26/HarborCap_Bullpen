"""
Bullpen Agent Scheduler
-----------------------
Long-running process that executes all agents on their configured schedules
AND exposes an HTTP API for on-demand agent triggers from the dashboard.

Usage:
    python scheduler.py              # Start scheduler + API server
    python scheduler.py --run-all    # Run all agents once immediately, then exit
"""

import logging
import os
import sys
import threading

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

from agents.biz_registration import biz_registration_agent
from agents.building_permits import building_permits_agent
from agents.census_demographics import census_demographics_agent
from agents.census_migration import census_migration_agent
from agents.corporate_relocations import corporate_relocations_agent
from agents.environmental_risk import environmental_risk_agent
from agents.ercot_load import ercot_load_agent
from agents.flood_risk import flood_risk_agent
from agents.freight_volume import freight_volume_agent
from agents.hud_vacancy import hud_vacancy_agent
from agents.irs_migration import irs_migration_agent
from agents.job_demand import job_demand_agent
from agents.materials_price import materials_price_agent
from agents.sales_tax_revenue import sales_tax_revenue_agent
from agents.toll_traffic import toll_traffic_agent
from agents.txdot_infra import txdot_infra_agent
from agents.zoning_changes import zoning_changes_agent

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("bullpen.scheduler")

TIMEZONE = "US/Central"

# Map of agent_name -> (flow_function, cron_expression)
AGENTS = {
    "corporate_relocations": (corporate_relocations_agent, "0 * * * *"),
    "biz_registration":      (biz_registration_agent,      "0 6 * * *"),
    "building_permits":      (building_permits_agent,      "10 6 * * *"),
    "job_demand":            (job_demand_agent,            "20 6 * * *"),
    "zoning_changes":        (zoning_changes_agent,        "30 6 * * *"),
    "environmental_risk":    (environmental_risk_agent,    "0 7 * * *"),
    "flood_risk":            (flood_risk_agent,            "15 7 * * *"),
    "txdot_infra":           (txdot_infra_agent,           "0 5 * * 1"),
    "toll_traffic":          (toll_traffic_agent,          "15 5 * * 1"),
    "census_demographics":   (census_demographics_agent,   "30 5 * * 1"),
    "materials_price":       (materials_price_agent,       "0 4 1 * *"),
    "freight_volume":        (freight_volume_agent,        "15 4 1 * *"),
    "ercot_load":            (ercot_load_agent,            "30 4 1 * *"),
    "sales_tax_revenue":     (sales_tax_revenue_agent,     "45 4 1 * *"),
    "census_migration":      (census_migration_agent,      "0 4 15 * *"),
    "irs_migration":         (irs_migration_agent,         "15 4 15 * *"),
    "hud_vacancy":           (hud_vacancy_agent,           "30 4 15 * *"),
}

# Track which agents are currently running
_running: set[str] = set()
_lock = threading.Lock()


def run_agent(agent_fn, name: str):
    """Wrapper that catches exceptions so the scheduler keeps running."""
    with _lock:
        if name in _running:
            logger.warning(f"⏭ Agent {name} already running, skipping")
            return
        _running.add(name)

    logger.info(f"▶ Starting agent: {name}")
    try:
        result = agent_fn()
        logger.info(f"✓ Agent {name} completed: {result}")
    except Exception as e:
        logger.error(f"✗ Agent {name} failed: {e}", exc_info=True)
    finally:
        with _lock:
            _running.discard(name)


def run_all_once():
    """Run every agent once sequentially, then exit."""
    logger.info(f"Running all {len(AGENTS)} agents once...")
    for name, (agent_fn, _) in AGENTS.items():
        run_agent(agent_fn, name)
    logger.info("All agents completed.")


# ---------------------------------------------------------------------------
# APScheduler
# ---------------------------------------------------------------------------
scheduler = BackgroundScheduler(timezone=TIMEZONE)


def start_scheduler():
    """Register all agents with APScheduler cron triggers."""
    for name, (agent_fn, cron_expr) in AGENTS.items():
        scheduler.add_job(
            run_agent,
            CronTrigger.from_crontab(cron_expr, timezone=TIMEZONE),
            args=[agent_fn, name],
            id=name,
            name=name,
            misfire_grace_time=3600,
            max_instances=1,
        )
        logger.info(f"  Registered: {name:30s} → {cron_expr}")

    scheduler.start()
    logger.info(f"Scheduler started with {len(AGENTS)} agents.")


# ---------------------------------------------------------------------------
# FastAPI HTTP API for on-demand triggers
# ---------------------------------------------------------------------------
app = FastAPI(title="Bullpen Scheduler API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
def health():
    return {"status": "ok", "agents": len(AGENTS), "running": list(_running)}


@app.get("/agents")
def list_agents():
    """List all agents with their schedule and running status."""
    result = []
    for name, (_, cron_expr) in AGENTS.items():
        job = scheduler.get_job(name)
        next_run = str(job.next_run_time) if job and job.next_run_time else None
        result.append({
            "name": name,
            "cron": cron_expr,
            "next_run": next_run,
            "is_running": name in _running,
        })
    return {"agents": result}


@app.post("/agents/{agent_name}/run")
def trigger_agent(agent_name: str):
    """Trigger an agent to run immediately in a background thread."""
    if agent_name not in AGENTS:
        raise HTTPException(status_code=404, detail=f"Unknown agent: {agent_name}")

    with _lock:
        if agent_name in _running:
            raise HTTPException(status_code=409, detail=f"Agent {agent_name} is already running")

    agent_fn, _ = AGENTS[agent_name]
    thread = threading.Thread(target=run_agent, args=[agent_fn, agent_name], daemon=True)
    thread.start()

    return {"status": "started", "agent": agent_name}


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    if "--run-all" in sys.argv:
        run_all_once()
    else:
        start_scheduler()
        port = int(os.environ.get("PORT", 8000))
        logger.info(f"Starting API server on port {port}")
        uvicorn.run(app, host="0.0.0.0", port=port)
