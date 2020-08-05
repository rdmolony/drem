from typing import Dict
from pathlib import Path

import pandas as pd
import prefect


@prefect.task
def valuation_office(df: pd.DataFrame, filepath: Path) -> None:
    """Load data to filepath ...

    Parameters
    ----------
    df : pd.DataFrame
    filepath: Path
    """

    df.to_csv(filepath.with_suffix(".csv"))
