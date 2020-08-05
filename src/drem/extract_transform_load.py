from os import environ

import prefect
from prefect import Flow, task
from prefect.engine.results import LocalResult
from prefect_toolkit import run_flow

from drem import extract
from drem import transform
from drem import load
from drem._filepaths import INTERIM_DIR, MNR_RAW, MNR_CLEAN, VO_RAW, VO_CLEAN

# Enable checkpointing for pipeline-persisted results
prefect.config.flows.checkpointing = True


def etl() -> Flow:

    with Flow(
        "Extract Transform & Load DREM Data", result=LocalResult(str(INTERIM_DIR))
    ) as flow:

        seai_monitoring_and_reporting_raw = extract.seai_monitoring_and_reporting(
            MNR_RAW
        )
        seai_monitoring_and_reporting_clean = transform.seai_monitoring_and_reporting(
            seai_monitoring_and_reporting_raw,
        )
        load.seai_monitoring_and_reporting(
            seai_monitoring_and_reporting_clean, MNR_CLEAN
        )

        valuation_office_raw = extract.valuation_office(VO_RAW)
        valuation_office_clean = transform.valuation_office(
            valuation_office_raw, seai_monitoring_and_reporting_clean,
        )

    return flow

