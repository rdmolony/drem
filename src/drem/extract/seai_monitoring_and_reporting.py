from typing import Dict
from pathlib import Path

import pandas as pd
import prefect


@prefect.task
def extract_seai_monitoring_and_reporting(filepath: Path) -> Dict[str, pd.DataFrame]:

    return pd.read_excel(filepath, sheet_name=None)
