from typing import Dict, List

import pandas as pd
import pytest

from drem.preprocess import fuzzymerge_mprn_and_gprn


@pytest.fixture
def mnr_sheets() -> Dict[str, pd.DataFrame]:

    return pd.read_excel(
        DATA / "MeasurementAndVerification.ods", sheet_name=None, engine="odf"
    )


def test_fuzzymerge_mprn_and_gprn(monkeypatch, tmpdir, mnr_sheets) -> None:

    monkeypatch.setattr(pd, "read_excel", mnr_sheets)
    input_filepath = tmpdir / "M&R raw.xlsx"
    output_filepath = tmpdir / "M&R clean.xlsx"
    fuzzymerge_mprn_and_gprn(input_filepath, output_filepath)

