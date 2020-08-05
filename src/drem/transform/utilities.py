import pandas as pd
from fuzzymatch_records.clean_columns import clean_fuzzy_column
from fuzzymatch_records.parse_addresses import (
    extract_address_numbers,
    extract_dublin_postcodes,
    remove_dublin_postcodes,
)


def _parse_address_column(series: pd.Series, column: str) -> pd.DataFrame:

    import ipdb

    ipdb.set_trace()

    return (
        series.copy()
        .rename(column)
        .to_frame()
        .pipe(extract_dublin_postcodes, column)
        .pipe(remove_dublin_postcodes, column)
        .pipe(extract_address_numbers, column)
    )


def parse_address_column(df: pd.DataFrame, column: str) -> pd.DataFrame:

    df = df.copy()

    address_columns = _parse_address_column(df[column], column)
    df = df.drop(columns=[column])  # address column will be replaced by parsed version

    return pd.concat([df, address_columns], axis="columns")
