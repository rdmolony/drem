from pathlib import Path
from typing import Dict, List

import pandas as pd
from pandas.testing import assert_frame_equal
import pytest
from tdda.referencetest import referencepytest, tag

from drem.transform.utilities import parse_address_column

CWD = Path(__file__).parent
INPUT_DATA = CWD / "input_data"
REF_DATA = CWD / "reference_data"


@pytest.fixture
def clean_seai_monitoring_and_reporting() -> Dict[str, pd.DataFrame]:

    return pd.read_csv(REF_DATA / "CleanMonitoringAndReporting.ods",)


@pytest.fixture
def clean_valuation_office() -> Dict[str, pd.DataFrame]:

    return pd.read_csv(REF_DATA / "CleanValuationOffice.csv")


# @pytest.mark.parametrize(
#     "data,address_column,ref_filename",
#     [
#         (
#             "clean_seai_monitoring_and_reporting",
#             "Location",
#             "ParsedSEAIMonitoringAndReporting.csv",
#         ),
#         ("clean_valuation_office", "address", "ParsedValtuationOffice.csv"),
#     ],
# )
# def test_parse_address_column(data, address_column, ref_filename, ref) -> None:

#     input = data
#     column = address_column

#     output = parse_address_column(input, column)
#     ref.assertDataFrameCorrect(output, ref_filename)
