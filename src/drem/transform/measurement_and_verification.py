import logging
from pathlib import Path
from typing import Dict, List
from collections import defaultdict

import pandas as pd
import prefect
from string_grouper import group_similar_strings
from fuzzymatch_records.clean_columns import clean_fuzzy_column
from fuzzymatch_records.deduplicate import deduplicate_dataframe
from fuzzymatch_records.fuzzymatch import (
    calculate_fuzzymatches_for_min_similarity,
    fuzzymatch_dataframes,
)
from fuzzymatch_records.parse_addresses import (
    extract_address_numbers,
    extract_dublin_postcodes,
    remove_dublin_postcodes,
)

from drem._filepaths import INTERIM_DIR, MNR_RAW

LOGGER = logging.getLogger(__name__)
STREAM_HANDLER = logging.StreamHandler()
LOGGER.addHandler(STREAM_HANDLER)


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


@prefect.task
def transform_measurement_and_verification(
    measurement_and_verification_raw: Dict[str, pd.DataFrame]
) -> pd.DataFrame:

    # Parse address column for easy differentiation of building numbers etc.
    # ... tf-idf struggles with 15 blah road and 17 blah road
    mprn = measurement_and_verification_raw["MPRN_data"].pipe(
        parse_address_column, "Location"
    )
    gprn = measurement_and_verification_raw["GPRN_data"].pipe(
        parse_address_column, "Location"
    )

    mprn_deduplicated = deduplicate_dataframe(mprn, [("PB Name", 0.8)])
    gprn_deduplicated = deduplicate_dataframe(gprn, [("PB Name", 0.8)])

    gprn_fuzzymatched = fuzzymatch_dataframes(
        mprn_deduplicated,
        gprn_deduplicated,
        [("PB Name_deduplicated", 0.8), ("Location_parsed", 0.6)],
    )

    """ ^^^ 0.8 & 0.6 represent tf-idf min_similarity
    Found to be a good fit for each column after inspection with
    calculate_fuzzymatches_for_min_similarity"""

    return pd.merge(
        mprn_deduplicated,
        gprn_fuzzymatched,
        left_on=["PB Name", "Location_parsed", "Consumption Category", "Year"],
        right_on=[
            "PB Name_deduplicated_fuzzymatched",
            "Location_parsed_fuzzymatched",
            "Consumption Category",
            "Year",
        ],
        how="left",
        suffixes=("_mprn", "_gprn"),
    )
