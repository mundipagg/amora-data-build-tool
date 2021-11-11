from amora.tests.assertions import expression_is_true
from example_project.models.steps_agg import StepsAgg


def test_sum_of_values_is_greater_than_avg_of_values():
    assert expression_is_true(StepsAgg.sum > StepsAgg.avg, condition=StepsAgg.year == 2021)