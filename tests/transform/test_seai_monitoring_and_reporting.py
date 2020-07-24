from pathlib import Path
from typing import Dict, List

import pandas as pd
from pandas.testing import assert_frame_equal
import pytest

from drem.transform import transform_seai_monitoring_and_reporting

CWD = Path(__file__).parent
DATA = CWD / "data"


@pytest.fixture
def mnr_sheets() -> Dict[str, pd.DataFrame]:

    return pd.read_excel(
        DATA / "RawMonitoringAndReporting.ods", sheet_name=None, engine="odf"
    )


def test_transform_seai_monitoring_and_reporting(mnr_sheets) -> None:

    input = mnr_sheets

    transform_seai_monitoring_and_reporting.run(mnr_sheets)
