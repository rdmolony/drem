import logging
from pathlib import Path
from typing import Dict, List
from collections import defaultdict

import pandas as pd
import prefect
from pipey import Pipeable
from string_grouper import group_similar_strings
from fuzzymatch_records.clean_columns import clean_fuzzy_columns
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

from drem.transform.utilities import parse_address_column


def _fuzzymerge(
    mprn: pd.DataFrame,
    gprn: pd.DataFrame,
    fuzzy_columns: List[str],
    merge_columns: List[str],
) -> pd.DataFrame:

    gprn_fuzzymatched = fuzzymatch_dataframes(
        mprn, gprn.copy(), on_fuzzy=fuzzy_columns, min_similarities=[0.8, 0.6],
    )

    import ipdb

    ipdb.set_trace()

    """ ^^^ 0.8 & 0.6 represent tf-idf min_similarity
    Found to be a good fit for each column after inspection with
    calculate_fuzzymatches_for_min_similarity"""

    return pd.merge(
        mprn,
        gprn_fuzzymatched,
        left_on=merge_columns,
        right_on=merge_columns,
        how="left",
        suffixes=("_mprn", "_gprn"),
    )


@prefect.task(name="Transform SEAI's M&R Data")
def seai_monitoring_and_reporting(raw: Dict[str, pd.DataFrame]) -> pd.DataFrame:

    # Parse address column for easy differentiation of building numbers etc.
    # ... tf-idf struggles with 15 blah road and 17 blah road
    mprn = (
        raw["MPRN_data"]
        .copy()
        .pipe(parse_address_column, "Location")
        .pipe(clean_fuzzy_columns, ["PB Name", "Location"])
        .pipe(
            deduplicate_dataframe_columns, columns=["PB Name"], min_similarities=[0.8]
        )
    )
    gprn = (
        raw["GPRN_data"]
        .copy()
        .pipe(parse_address_column, "Location")
        .pipe(clean_fuzzy_columns, ["PB Name", "Location"])
        .pipe(
            deduplicate_dataframe_columns, columns=["PB Name"], min_similarities=[0.8]
        )
    )

    return _fuzzymerge(
        mprn,
        gprn,
        fuzzy_columns=["PB Name", "Location"],
        merge_columns=[
            "PB Name_deduplicated",
            "Location",
            "Consumption Category",
            "Year",
        ],
    )
