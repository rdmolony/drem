from typing import Dict
from pathlib import Path

import pandas as pd
import prefect


@prefect.task
def load_seai_monitoring_and_reporting(df: pd.DataFrame, filepath: Path) -> None:
    """Load seai_monitoring_and_reporting to files

    Parameters
    ----------
    df : pd.DataFrame
    filepath: Path
    """

    df.to_csv(filepath.with_name("mnr_raw").with_suffix(".csv"))
    df[
        [
            "PB Name_deduplicated_mprn",
            "address_numbers_mprn",
            "Location_parsed_mprn",
            "County_mprn",
            "PB Name_deduplicated_gprn",
            "address_numbers_gprn",
            "Location_parsed_gprn",
            "County_gprn",
            "Attributable Total Final Consumption (kWh)_mprn",
            "Attributable Total Final Consumption (kWh)_gprn",
            "Consumption Category",
            "Year",
        ]
    ].to_csv(filepath.with_name("mnr_parsed").with_suffix(".csv"))

