"""
Manual Agent Runner
-------------------
Run any agent on demand.

Usage:
    python run_agent.py <agent_name>
    python run_agent.py building_permits
    python run_agent.py --list
"""

import sys

AGENTS = {
    "biz_registration": ("agents.biz_registration", "biz_registration_agent"),
    "building_permits": ("agents.building_permits", "building_permits_agent"),
    "census_demographics": ("agents.census_demographics", "census_demographics_agent"),
    "census_migration": ("agents.census_migration", "census_migration_agent"),
    "corporate_relocations": ("agents.corporate_relocations", "corporate_relocations_agent"),
    "environmental_risk": ("agents.environmental_risk", "environmental_risk_agent"),
    "ercot_load": ("agents.ercot_load", "ercot_load_agent"),
    "flood_risk": ("agents.flood_risk", "flood_risk_agent"),
    "freight_volume": ("agents.freight_volume", "freight_volume_agent"),
    "hud_vacancy": ("agents.hud_vacancy", "hud_vacancy_agent"),
    "irs_migration": ("agents.irs_migration", "irs_migration_agent"),
    "job_demand": ("agents.job_demand", "job_demand_agent"),
    "materials_price": ("agents.materials_price", "materials_price_agent"),
    "sales_tax_revenue": ("agents.sales_tax_revenue", "sales_tax_revenue_agent"),
    "toll_traffic": ("agents.toll_traffic", "toll_traffic_agent"),
    "txdot_infra": ("agents.txdot_infra", "txdot_infra_agent"),
    "zoning_changes": ("agents.zoning_changes", "zoning_changes_agent"),
}


def main():
    if len(sys.argv) < 2 or sys.argv[1] == "--list":
        print("Available agents:")
        for name in sorted(AGENTS):
            print(f"  {name}")
        print(f"\nUsage: python run_agent.py <agent_name>")
        return

    name = sys.argv[1]
    if name not in AGENTS:
        print(f"Unknown agent: {name}")
        print(f"Run 'python run_agent.py --list' to see available agents.")
        sys.exit(1)

    module_path, fn_name = AGENTS[name]
    print(f"Running {name}...")

    from importlib import import_module
    module = import_module(module_path)
    agent_fn = getattr(module, fn_name)
    result = agent_fn()
    print(f"Done: {result}")


if __name__ == "__main__":
    main()
