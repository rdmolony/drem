from typing import Iterable
from typing import Union

import pandas as pd

from icontract import require
from prefect import task


@task
@require(
    lambda df, column_names: set(column_names).issubset(set(df.columns)),
    "df.columns doesn't contain all names in columns!",
)
def get_columns(df: pd.DataFrame, column_names: Iterable[str]) -> pd.DataFrame:
    """Get DataFrame columns (copy to a new DataFrame).

    Args:
        df (pd.DataFrame): Any single-indexed Pandas DataFrame
        column_names (Iterable[str]): Names of columns to be extracted

    Returns:
        pd.DataFrame: A new DataFrame containing only the specified columns
    """
    return df.copy().loc[:, column_names]


@task
@require(
    lambda df, target: set(target).issubset(set(df.columns)),
    "df.columns doesn't contain all names in columns!",
)
def get_sum_of_columns(
    df: pd.DataFrame, target: Union[str, Iterable[str]], result: str,
) -> pd.DataFrame:
    """Get sum of target DataFrame columns.

    Args:
        df (pd.DataFrame): Any single-indexed Pandas DataFrame
        target (Union[str, Iterable[str]]): Names of columns to be summed
        result (str): Name of result column

    Returns:
        pd.DataFrame: [description]
    """
    df[result] = df[target].copy().sum(axis=1)

    return df
