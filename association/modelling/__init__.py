"""Modelling sub package."""
import os.path

from association import ROOT_PATH
from association.utils import get_run_date_from_cli

arguments = get_run_date_from_cli()

ANALYSIS_PATH = os.path.join(ROOT_PATH, 'analysis')
PRODUCTS_TO_FILTER_OUT_PATH = os.path.join(
    ANALYSIS_PATH, 'products_to_filter_out'
)
MODEL_PATH = os.path.join(ROOT_PATH, "model", f"run_date={arguments.run_date}")
RULES_PATH = os.path.join(MODEL_PATH, "rules")
ITEM_SETS_PATH = os.path.join(MODEL_PATH, "item_sets")
MODEL_DATA_PATH = os.path.join(MODEL_PATH, "data")
LOOK_BACK_PERIOD = 30
