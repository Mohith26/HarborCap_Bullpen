"""
Bullpen Agent Scheduler
-----------------------
Long-running process that executes all agents on their configured schedules.
Uses APScheduler with CronTrigger (all times in US/Central).

Usage:
    python scheduler.py              # Start the scheduler (runs forever)
    python scheduler.py --run-all    # Run all agents once immediately, then exit
"""

import logging
import sys

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

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

# (flow_function, cron_expression, agent_id)
SCHEDULE = [
    # ── Hourly ──────────────────────────────────────────────
    (corporate_relocations_agent, "0 * * * *", "corporate_relocations"),

    # ── Daily 6 AM CT (staggered 10 min apart) ─────────────
    (biz_registration_agent, "0 6 * * *", "biz_registration"),
    (building_permits_agent, "10 6 * * *", "building_permits"),
    (job_demand_agent, "20 6 * * *", "job_demand"),
    (zoning_changes_agent, "30 6 * * *", "zoning_changes"),

    # ── Daily 7 AM CT ──────────────────────────────────────
    (environmental_risk_agent, "0 7 * * *", "environmental_risk"),
    (flood_risk_agent, "15 7 * * *", "flood_risk"),

    # ── Weekly Monday 5 AM CT ──────────────────────────────
    (txdot_infra_agent, "0 5 * * 1", "txdot_infra"),
    (toll_traffic_agent, "15 5 * * 1", "toll_traffic"),
    (census_demographics_agent, "30 5 * * 1", "census_demographics"),

    # ── Monthly 1st at 4 AM CT ─────────────────────────────
    (materials_price_agent, "0 4 1 * *", "materials_price"),
    (freight_volume_agent, "15 4 1 * *", "freight_volume"),
    (ercot_load_agent, "30 4 1 * *", "ercot_load"),
    (sales_tax_revenue_agent, "45 4 1 * *", "sales_tax_revenue"),

    # ── Monthly 15th at 4 AM CT ────────────────────────────
    (census_migration_agent, "0 4 15 * *", "census_migration"),
    (irs_migration_agent, "15 4 15 * *", "irs_migration"),
    (hud_vacancy_agent, "30 4 15 * *", "hud_vacancy"),
]


def run_agent(agent_fn, name: str):
    """Wrapper that catches exceptions so the scheduler keeps running."""
    logger.info(f"▶ Starting agent: {name}")
    try:
        result = agent_fn()
        logger.info(f"✓ Agent {name} completed: {result}")
    except Exception as e:
        logger.error(f"✗ Agent {name} failed: {e}", exc_info=True)


def run_all_once():
    """Run every agent once sequentially, then exit."""
    logger.info(f"Running all {len(SCHEDULE)} agents once...")
    for agent_fn, _, name in SCHEDULE:
        run_agent(agent_fn, name)
    logger.info("All agents completed.")


def start_scheduler():
    """Register all agents with APScheduler and block forever."""
    scheduler = BlockingScheduler(timezone=TIMEZONE)

    for agent_fn, cron_expr, name in SCHEDULE:
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

    logger.info(f"Scheduler started with {len(SCHEDULE)} agents. Waiting for triggers...")
    scheduler.start()


if __name__ == "__main__":
    if "--run-all" in sys.argv:
        run_all_once()
    else:
        start_scheduler()
