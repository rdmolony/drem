from typing import Dict
from pathlib import Path

import pandas as pd
import prefect


@prefect.task
def extract_measurement_and_verification(filepath: Path) -> Dict[str, pd.DataFrame]:

    return pd.read_excel(filepath, sheet_name=None)
