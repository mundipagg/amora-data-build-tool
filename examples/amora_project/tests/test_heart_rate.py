from amora.tests.assertions import (
    expression_is_true,
    has_accepted_values,
    is_non_negative,
    relationship,
    that,
)
from examples.amora_project.models.health import Health
from examples.amora_project.models.heart_rate import HeartRate


def test_relates_to_health():
    assert relationship(from_=HeartRate.id, to=Health.id)


def test_value_is_non_negative():
    assert that(HeartRate.value, is_non_negative)


def test_unit_accepted_values():
    assert that(HeartRate.unit, has_accepted_values, values=["count/min"])


def test_end_date_after_start_date():
    assert expression_is_true(HeartRate.endDate >= HeartRate.startDate)
