from pathlib import Path
from typing import Dict, List

import pandas as pd
from pandas.testing import assert_frame_equal
import pytest

from drem.transform import (
    _merge_mprn_and_gprn,
    _clean_merged_data,
)

CWD = Path(__file__).parent
INPUT_DATA = CWD / "input_data"
REF_DATA = CWD / "reference_data"


@pytest.fixture
def mnr_sheets() -> Dict[str, pd.DataFrame]:

    return pd.read_excel(
        INPUT_DATA / "RawSEAIMonitoringAndReporting.ods", sheet_name=None, engine="odf"
    )


def test_merge_mprn_and_gprn(mnr_sheets, ref) -> None:

    input = mnr_sheets

    output = _merge_mprn_and_gprn(mnr_sheets)
    ref.assertDataFrameCorrect(output, "MPRNMergedWithGPRN.csv")

