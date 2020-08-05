from typing import Dict
from pathlib import Path

import pandas as pd
import prefect


@prefect.task
def seai_monitoring_and_reporting(df: pd.DataFrame, filepath: Path) -> None:
    """Load data to filepath ...

    Parameters
    ----------
    df : pd.DataFrame
    filepath: Path
    """

    df.to_csv(filepath.with_name("mnr_raw").with_suffix(".csv"))
