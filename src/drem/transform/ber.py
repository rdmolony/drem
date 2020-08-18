import numpy as np
import pandas as pd

from prefect import task


def _extract_dublin_rows(ber_ireland: pd.DataFrame) -> pd.DataFrame:

    dublin_rows = ber_ireland["CountyName"].str.contains("Dublin")
    return ber_ireland[dublin_rows]


def _bin_year_of_construction_as_in_census(ber: pd.DataFrame) -> pd.DataFrame:

    year = ber["Year_of_Construction"].fillna(0).astype(int).to_numpy()

    conditions = [
        year <= 1919,
        year < 1946,
        year < 1961,
        year < 1971,
        year < 1981,
        year < 1991,
        year < 2001,
        year < 2010,
        year < 2025,
        year == 0,
    ]

    choices = [
        "before 1919",
        "1919 - 1945",
        "1946 - 1960",
        "1961 - 1970",
        "1971 - 1980",
        "1981 - 1990",
        "1991 - 2000",
        "2001 - 2010",
        "2011 or later",
        "not stated",
    ]

    ber["period_built"] = np.select(conditions, choices, default="ERROR")

    return ber


@task
def transform_ber(ber_raw: pd.DataFrame) -> pd.DataFrame:
    """Tidy BER data set.

    See tests/functional for more information

    Args:
        ber_raw (pd.DataFrame): Raw Ireland BER Residential Housing Data set

    Returns:
        pd.DataFrame: Clean Dublin BER Residential Housing Data set
    """
    return (
        ber_raw.loc[
            :,
            [
                "CountyName",
                "DwellingTypeDescr",
                "Year_of_Construction",
                "DeliveredEnergyMainSpace",
                "DeliveredEnergyMainWater",
                "DeliveredEnergySecondarySpace",
                "DeliveredEnergySupplementaryWater",
                "DeliveredEnergyPumpsFans",
                "DeliveredLightingEnergy",
                "TotalDeliveredEnergy",
            ],
        ]
        .pipe(_extract_dublin_rows)
        .pipe(_bin_year_of_construction_as_in_census)
        .assign(
            total_heat_demand=lambda x: x[
                [
                    "DeliveredEnergyMainSpace",
                    "DeliveredEnergyMainWater",
                    "DeliveredEnergySecondarySpace",
                    "DeliveredEnergySupplementaryWater",
                ]
            ].sum(axis=1),
        )
        .loc[
            :, ["CountyName", "period_built", "DwellingTypeDescr", "total_heat_demand"],
        ]
    )