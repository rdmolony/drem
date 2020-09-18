from pathlib import Path
from typing import List

import pandas as pd

from prefect import task


@task
def _read_html(filepath: Path) -> List[pd.DataFrame]:

    filepath = str(filepath)
    return pd.read_html(filepath)


@task
def _extract_from_list(input_list: List[pd.DataFrame], index: int) -> pd.DataFrame:

    return input_list[index]
