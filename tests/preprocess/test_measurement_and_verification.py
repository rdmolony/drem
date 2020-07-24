from pathlib import Path
from typing import Dict, List

import pandas as pd
from pandas.testing import assert_frame_equal
import pytest

from drem.transform.measurement_and_verification import fuzzymerge_mprn_and_gprn

CWD = Path(__file__).parent
DATA = CWD / "data"


@pytest.fixture
def mnr_sheets() -> Dict[str, pd.DataFrame]:

    return pd.read_excel(
        DATA / "MeasurementAndVerification.ods", sheet_name=None, engine="odf"
    )


def test_fuzzymerge_mprn_and_gprn(monkeypatch, tmpdir, mnr_sheets) -> None:

    input = mnr_sheets
    expected_output = mnr_sheets["gprn_fuzzymatched"]

    output = fuzzymerge_mprn_and_gprn(input)

    assert_frame_equal(output, expected_output, check_dtype=False)
