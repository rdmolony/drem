import logging
from pathlib import Path
from typing import Dict, List
from collections import defaultdict

import pandas as pd
import prefect
from string_grouper import group_similar_strings
from fuzzymatch_records.clean_columns import clean_fuzzy_column
from fuzzymatch_records.deduplicate import deduplicate_dataframe_columns
from fuzzymatch_records.fuzzymatch import (
    calculate_fuzzymatches_for_min_similarity,
    fuzzymatch_dataframes,
)
from fuzzymatch_records.parse_addresses import (
    extract_address_numbers,
    extract_dublin_postcodes,
    remove_dublin_postcodes,
)


def _parse_address_column(series: pd.Series, column: str) -> pd.DataFrame:

    parsed_column = f"{column}_parsed"
    return (
        series.rename(parsed_column)
        .pipe(clean_fuzzy_column)
        .to_frame()
        .pipe(extract_dublin_postcodes, parsed_column)
        .pipe(remove_dublin_postcodes, parsed_column)
        .pipe(extract_address_numbers, parsed_column)
    )


def parse_address_column(df: pd.DataFrame, column: str) -> pd.DataFrame:

    df = df.copy()
    address_columns = _parse_address_column(df[column], column)
    return pd.concat([df, address_columns], axis="columns")


def clean_valuation_office(valuation_office_raw: pd.DataFrame) -> pd.DataFrame:

    return (
        valuation_office_raw.rename(columns=str.lower)
        .rename(columns=str.strip)
        .assign(
            address=lambda x: x[
                ["address 1", "address 2", "address 3", "address 5", "address 5"]
            ]
            .astype(str)
            .agg(" ".join, axis=1)
            .str.strip()
            .pipe(clean_fuzzy_column)
        )
        .query("area > 0")
        .loc[:, ("address", "x itm", "y itm", "area", "category", "publication date")]
    )


@prefect.task
def transform_valuation_office(
    valuation_office_raw: pd.DataFrame, seai_monitoring_and_reporting: pd.DataFrame,
) -> pd.DataFrame:

    import ipdb

    ipdb.set_trace()

    valuation_office_clean = valuation_office_raw.pipe(clean_valuation_office).pipe(
        parse_address_column, "address"
    )

    valuation_office_fuzzymatched = fuzzymatch_dataframes(
        valuation_office_clean,
        seai_monitoring_and_reporting,
        on_fuzzy_left=["address_parsed"],
        on_fuzzy_right=["Location_parsed"],
    )
