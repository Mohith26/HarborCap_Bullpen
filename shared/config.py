"""
Centralized configuration for The Bullpen platform.
All values are read from environment variables with sensible defaults.
"""

import os

from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL: str = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY: str = os.environ.get("SUPABASE_KEY", "")
FRED_API_KEY: str = os.environ.get("FRED_API_KEY", "")
TX_COMPTROLLER_SIFT_KEY: str = os.environ.get("TX_COMPTROLLER_SIFT_KEY", "")
TX_COMPTROLLER_TAX_KEY: str = os.environ.get("TX_COMPTROLLER_TAX_KEY", "")
GOOGLE_MAPS_API_KEY: str = os.environ.get("GOOGLE_MAPS_API_KEY", "")
CENSUS_API_KEY: str = os.environ.get("CENSUS_API_KEY", "")
ERCOT_SUBSCRIPTION_KEY: str = os.environ.get("ERCOT_SUBSCRIPTION_KEY", "")
ERCOT_USERNAME: str = os.environ.get("ERCOT_USERNAME", "")
ERCOT_PASSWORD: str = os.environ.get("ERCOT_PASSWORD", "")
HUD_API_TOKEN: str = os.environ.get("HUD_API_TOKEN", "")

# When True, agents return built-in seed data instead of calling live APIs.
SEED_MODE: bool = os.environ.get("SEED_MODE", "true").lower() in ("true", "1", "yes")
