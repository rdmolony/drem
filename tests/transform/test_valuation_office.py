from pathlib import Path
from typing import Dict, List

import pandas as pd
from pandas.testing import assert_frame_equal
import pytest

from drem.transform import transform_valuation_office
from drem.transform.valuation_office import clean_valuation_office

CWD = Path(__file__).parent
DATA = CWD / "data"


@pytest.fixture
def valuation_office() -> Dict[str, pd.DataFrame]:

    return pd.read_excel(DATA / "RawValuationOffice.ods", engine="odf")


def test_clean_valuation_office(valuation_office) -> None:

    input = valuation_office
    valuation_office.pipe(clean_valuation_office)


def test_clean_valuation_office(valuation_office) -> None:

    input = valuation_office
    valuation_office.pipe(clean_valuation_office)


def test_fuzzymerge_mprn_and_gprn(valuation_office) -> None:

    input = valuation_office

    transform_valuation_office.run(input)

