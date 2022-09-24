from amora_models.health import Health
from amora_models.steps import Steps

from amora.tests.assertions import (
    expression_is_true,
    has_accepted_values,
    is_non_negative,
    relationship,
    that,
)


def test_relates_to_health():
    assert relationship(from_=Steps.id, to=Health.id)


def test_value_is_non_negative():
    assert that(Steps.value, is_non_negative)


def test_unit_accepted_values():
    assert that(Steps.unit, has_accepted_values, values=["count"])


def test_end_date_after_start_date():
    assert expression_is_true(Steps.endDate >= Steps.startDate)
