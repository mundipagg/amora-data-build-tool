from amora.tests.assertions import (are_unique_together, expression_is_true,
                                    has_at_least_one_not_null_value,
                                    is_non_negative, that)
from examples.amora_project.models.heart_agg import HeartRateAgg


def test_sum_of_values_is_greater_than_avg_of_values():
    assert expression_is_true(
        HeartRateAgg.sum > HeartRateAgg.avg, condition=HeartRateAgg.year == 2021
    )


def test_sum_is_non_negative():
    assert that(HeartRateAgg.sum, is_non_negative)


def test_avg_is_non_negative():
    assert that(HeartRateAgg.avg, is_non_negative)


def test_count_is_non_negative():
    assert that(HeartRateAgg.count, is_non_negative)


def test_count_has_at_least_one_not_null_value():
    assert that(HeartRateAgg.count, has_at_least_one_not_null_value)


def test_theres_no_data_before_year_2019():
    assert expression_is_true(HeartRateAgg.year > 2018)


def test_year_and_month_columns_are_unique_together():
    assert that([HeartRateAgg.year, HeartRateAgg.month], are_unique_together)
