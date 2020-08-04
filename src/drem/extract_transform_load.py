from prefect import Flow, task
from prefect_toolkit import run_flow

from drem.extract import extract_seai_monitoring_and_reporting, extract_valuation_office
from drem.transform import (
    transform_seai_monitoring_and_reporting,
    transform_valuation_office,
)
from drem.load import load_seai_monitoring_and_reporting, load_valuation_office
from drem._filepaths import MNR_RAW, MNR_CLEAN, VO_RAW, VO_CLEAN


def etl() -> Flow:

    with Flow("Extract Transform & Load DREM Data") as flow:

        seai_monitoring_and_reporting_raw = extract_seai_monitoring_and_reporting(
            MNR_RAW
        )
        seai_monitoring_and_reporting_clean = transform_seai_monitoring_and_reporting(
            seai_monitoring_and_reporting_raw
        )
        load_seai_monitoring_and_reporting(
            seai_monitoring_and_reporting_clean, MNR_CLEAN
        )

        valuation_office_raw = extract_valuation_office(VO_RAW)
        valuation_office_clean = transform_valuation_office(
            valuation_office_raw, seai_monitoring_and_reporting_clean
        )
        # load_valuation_office(valuation_office_clean, VO_CLEAN)

    return flow

