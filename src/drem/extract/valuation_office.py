from typing import Dict
from pathlib import Path

import pandas as pd
import prefect


@prefect.task
def extract_valuation_office(filepath: Path) -> Dict[str, pd.DataFrame]:

    return pd.read_parquet(filepath)
