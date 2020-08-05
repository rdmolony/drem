from typing import Dict
from pathlib import Path

import pandas as pd
import prefect


@prefect.task(name="Extract Valuation Office Data")
def valuation_office(filepath: Path) -> Dict[str, pd.DataFrame]:

    return pd.read_parquet(filepath)
