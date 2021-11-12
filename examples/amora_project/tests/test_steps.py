from amora.tests.assertions import (
    relationship,
    that,
    is_non_negative,
    has_accepted_values,
    expression_is_true,
)
from examples.amora_project.models.health import Health
from examples.amora_project.models.steps import Steps


def test_relates_to_health():
    assert relationship(from_=Steps.id, to=Health.id)


def test_value_is_non_negative():
    assert that(Steps.value, is_non_negative)


def test_unit_accepted_values():
    assert that(Steps.unit, has_accepted_values, values=["count"])


def test_end_date_after_start_date():
    assert expression_is_true(Steps.endDate >= Steps.startDate)
