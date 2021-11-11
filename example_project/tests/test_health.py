from amora.tests.assertions import is_not_null, that, is_non_negative, has_accepted_values
from example_project.models.health import Health


def test_value_is_not_null():
    assert that(Health.value, is_not_null)


def test_value_is_not_negative():
    assert that(Health.value, is_non_negative)


def test_type_is_not_null():
    assert that(Health.type, is_not_null)


def test_type_has_accepted_values():
    assert that(Health.type, has_accepted_values, values=[
        "HeartRate",
        "StepCount",
        "DistanceWalkingRunning",
        "FlightsClimbed",
        "HeadphoneAudioExposure",
        "WalkingDoubleSupportPercentage",
        "WalkingSpeed",
    ])