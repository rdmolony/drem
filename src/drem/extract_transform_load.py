from prefect import Flow, task

from drem.extract import extract_seai_monitoring_and_reporting
from drem.transform import transform_seai_monitoring_and_reporting
from drem.load import load_seai_monitoring_and_reporting
from drem.utilities.flow import run_flow
from drem._filepaths import MNR_RAW, MNR_CLEAN


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

    return flow

