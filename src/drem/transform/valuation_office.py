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


from drem.transform.utilities import parse_address_column


def clean_valuation_office(valuation_office_raw: pd.DataFrame) -> pd.DataFrame:

    return (
        valuation_office_raw.rename(columns=str.lower)
        .rename(columns=str.strip)
        .assign(
            address=lambda x: x[
                ["address 1", "address 2", "address 3", "address 5", "address 5"]
            ]
            .fillna("")
            .astype(str)
            .agg(" ".join, axis=1)
            .str.replace(" +", " ")
            .str.strip()
            .pipe(clean_fuzzy_column)
        )
        .query("area > 0")
        .loc[:, ("address", "x itm", "y itm", "area", "category", "publication date")]
        .assign(category=lambda x: x.category.str.title())
    )


@prefect.task(name="Transform Valuation Office Data")
def valuation_office(
    valuation_office_raw: pd.DataFrame, seai_monitoring_and_reporting: pd.DataFrame,
) -> pd.DataFrame:

    import ipdb

    ipdb.set_trace()

    valuation_office_clean = (
        valuation_office_raw.copy()
        .pipe(clean_valuation_office)
        .pipe(parse_address_column, "address")
    )

    valuation_office_fuzzymatched = fuzzymatch_dataframes(
        valuation_office_clean,
        seai_monitoring_and_reporting,
        on_fuzzy_left=["address_parsed"],
        on_fuzzy_right=["Location_parsed"],
    )
