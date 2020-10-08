from pathlib import Path

import numpy as np
import pandas as pd

from prefect import Flow
from prefect import Parameter
from prefect import Task
from prefect import task
from prefect.utilities.debug import raise_on_exception

import drem.utilities.pandas_tasks as pdt


@task
def _bin_year_of_construction_as_in_census(
    ber: pd.DataFrame, target: str, result: str,
) -> pd.DataFrame:

    ber = ber.copy()

    year = ber[target].fillna(0).astype(int).to_numpy()

    conditions = [
        year <= 1919,
        year < 1946,
        year < 1961,
        year < 1971,
        year < 1981,
        year < 1991,
        year < 2001,
        year < 2010,
        year < 2025,
        year == 0,
    ]

    choices = [
        "before 1919",
        "1919 - 1945",
        "1946 - 1960",
        "1961 - 1970",
        "1971 - 1980",
        "1981 - 1990",
        "1991 - 2000",
        "2001 - 2010",
        "2011 or later",
        "not stated",
    ]

    ber.loc[:, result] = np.select(conditions, choices, default="ERROR")

    return ber


with Flow("Cleaning the BER Data...") as flow:

    ber_fpath = Parameter("ber_fpath")

    raw_ber = pdt.read_parquet(ber_fpath)
    get_dublin_rows = pdt.get_rows_where_column_contains_substring(
        raw_ber, target="CountyName", substring="Dublin",
    )
    rename_postcodes = pdt.rename(get_dublin_rows, columns={"CountyName": "postcodes"})
    bin_year_built_into_census_categories = _bin_year_of_construction_as_in_census(
        rename_postcodes, target="Year_of_Construction", result="cso_period_built",
    )


class TransformBER(Task):
    """Clean BER Data in a Prefect flow.

    Args:
        Task (prefect.Task): see https://docs.prefect.io/core/concepts/tasks.html
    """

    def run(self, dirpath: Path, filename: str) -> pd.DataFrame:
        """Run flow.

        Args:
            dirpath (Path): Directory where data is stored
            filename (str): Name of data

        Returns:
            pd.DataFrame: Clean BER Data
        """
        ber_filepath = dirpath / f"{filename}.parquet"
        with raise_on_exception():
            state = flow.run(parameters=dict(ber_fpath=ber_filepath))

        return state.result[bin_year_built_into_census_categories].result


transform_ber = TransformBER()
