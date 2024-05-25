"""Modelling sub package."""
import os.path
from datetime import datetime

from association import ROOT_PATH


def create_timestamp() -> str:
    """
    Get current timestamp for output folder name.
    Example 2021-03-12T153748.258Z
    """
    utc_now = datetime.utcnow()
    dt_string = utc_now.strftime("%Y-%m-%dT%H%M%S.%f")[:-3] + "Z"
    return dt_string


OUTPUT_PATH = os.path.join(ROOT_PATH, "output")
RESULT_DELIVERY_PATH = os.path.join(OUTPUT_PATH, "result_delivery")
DATABASE_URI = os.getenv("DATABASE_URI")
DATABASE = "association_prod"

timestamp = create_timestamp()
RULES_PATH = os.path.join(RESULT_DELIVERY_PATH, timestamp, "rules")
RULES_COLLECTION = "rules"
ITEM_SETS_PATH = os.path.join(RESULT_DELIVERY_PATH, timestamp, "item_sets")
ITEM_SETS_COLLECTION = "item_sets"
