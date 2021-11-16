from amora.tests.assertions import expression_is_true, is_non_negative, that
from examples.amora_project.models.steps_agg import StepsAgg


def test_sum_of_values_is_greater_than_avg_of_values():
    assert expression_is_true(
        StepsAgg.sum > StepsAgg.avg, condition=StepsAgg.year == 2021
    )


def test_sum_is_non_negative():
    assert that(StepsAgg.sum, is_non_negative)


def test_avg_is_non_negative():
    assert that(StepsAgg.avg, is_non_negative)


def test_count_is_non_negative():
    assert that(StepsAgg.count, is_non_negative)


def test_theres_no_data_before_year_2019():
    assert expression_is_true(StepsAgg.year > 2018)
