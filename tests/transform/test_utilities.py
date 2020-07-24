from pathlib import Path
from typing import Dict, List

import pandas as pd
from pandas.testing import assert_frame_equal
import pytest

from drem.transform.utilities import parse_address_column

CWD = Path(__file__).parent
DATA = CWD / "data"


@pytest.fixture
def seai_monitoring_and_reporting() -> Dict[str, pd.DataFrame]:

    return pd.read_excel(
        DATA / "CleanMonitoringAndReporting.ods", sheet_name="MPRN_data", engine="odf"
    )


@pytest.fixture
def valuation_office() -> Dict[str, pd.DataFrame]:

    return pd.read_excel(DATA / "CleanValuationOffice.ods", engine="odf")


@pytest.mark.parametrize(
    "data,address_column", [("mprn", "Location"), ("valuation_office", "address")]
)
def test_parse_address_column(data, address_column) -> None:

    input = data
    column = address_column

    parse_address_column(input, column)
