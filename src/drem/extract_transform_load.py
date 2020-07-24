from prefect import Flow, task

from drem.extract import extract_measurement_and_verification
from drem.transform import transform_measurement_and_verification
from drem.load import load_measurement_and_verification
from drem.utilities.flow import run_flow
from drem._filepaths import MNR_RAW, MNR_CLEAN


def etl() -> Flow:

    with Flow("Extract Transform & Load DREM Data") as flow:

        measurement_and_verification_raw = extract_measurement_and_verification(MNR_RAW)
        measurement_and_verification_clean = transform_measurement_and_verification(
            measurement_and_verification_raw
        )
        load_measurement_and_verification(measurement_and_verification_clean, MNR_CLEAN)

    return flow

