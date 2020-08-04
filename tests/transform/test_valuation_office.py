from pathlib import Path
from typing import Dict, List

import pandas as pd
from pandas.testing import assert_frame_equal
import pytest
from tdda.referencetest import referencepytest, tag

from drem.transform import transform_valuation_office
from drem.transform.valuation_office import clean_valuation_office


CWD = Path(__file__).parent
INPUT_DATA = CWD / "input_data"


@pytest.fixture
def raw_valuation_office() -> Dict[str, pd.DataFrame]:

    return pd.read_excel(INPUT_DATA / "RawValuationOffice.ods", engine="odf")


def test_clean_valuation_office(raw_valuation_office, ref) -> None:

    output = raw_valuation_office.pipe(clean_valuation_office)
    ref.assertDataFrameCorrect(output, "CleanValuationOffice.csv")


# def test_fuzzymerge_mprn_and_gprn(valuation_office) -> None:

#     input = valuation_office

#     transform_valuation_office.run(input)

