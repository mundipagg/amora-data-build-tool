from amora.tests.assertions import (
    is_not_null,
    that,
    is_non_negative,
    has_accepted_values,
    expression_is_true,
    is_unique,
)
from examples.amora_project.models.health import Health


def test_value_is_not_null():
    assert that(Health.value, is_not_null)


def test_value_is_not_negative():
    assert that(Health.value, is_non_negative)


def test_type_is_not_null():
    assert that(Health.type, is_not_null)


def test_type_has_accepted_values():
    assert that(
        Health.type,
        has_accepted_values,
        values=[
            "ActiveEnergyBurned",
            "BodyMass",
            "BodyMassIndex",
            "DistanceWalkingRunning",
            "FlightsClimbed",
            "HKDataTypeSleepDurationGoal",
            "HeadphoneAudioExposure",
            "HeartRate",
            "Height",
            "SleepAnalysis",
            "StepCount",
            "WalkingAsymmetryPercentage",
            "WalkingDoubleSupportPercentage",
            "WalkingSpeed",
            "WalkingStepLength",
        ],
    )


def test_end_date_after_start_date():
    assert expression_is_true(Health.endDate >= Health.startDate)


def test_id_is_unique():
    assert that(Health.id, is_unique)
