"""Modelling sub package."""
import os.path

from association import ROOT_PATH
from association.utils import get_run_date_from_cli

arguments = get_run_date_from_cli()

MODEL_PATH = os.path.join(ROOT_PATH, "model", f"run_date={arguments.run_date}")
RULES_PATH = os.path.join(
    MODEL_PATH, "rules", f"run_date={arguments.run_date}"
)
ITEM_SETS_PATH = os.path.join(
    MODEL_PATH, "item_sets", f"run_date={arguments.run_date}"
)
LOOK_BACK_PERIOD = 30
