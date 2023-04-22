from datetime import date, datetime

import pytest
from sqlalchemy import select

from amora.filters import DateFilter, ValueFilter
from amora.providers.bigquery import cte_from_rows
from amora.questions import question

from tests.models.step_count_by_source import how_many_data_points_where_acquired


def test_is_valid_filter_with_a_valid_filter():
    assert ValueFilter(field="source_name").is_valid_for(
        how_many_data_points_where_acquired
    )


def test_is_valid_filter_with_an_invalid_filter():
    filter = DateFilter(field="creationDate")
    assert not filter.is_valid_for(how_many_data_points_where_acquired)


def test_ValueFilter_filter_with_a_valid_filter():
    filter = ValueFilter(field="source_name")
    filtered_question = filter.filter(
        how_many_data_points_where_acquired, value="iPhone"
    )
    assert filtered_question.sql == (
        """WITH `how_many_data_points_where_acquired` AS
  (SELECT
     sum(`step_count_by_source`.`value_count`) AS `total`,
     `step_count_by_source`.`source_name` AS `source_name`
   FROM `amora-data-build-tool.amora`.`step_count_by_source`
   GROUP BY `step_count_by_source`.`source_name`)
SELECT
  `how_many_data_points_where_acquired`.`total`,
  `how_many_data_points_where_acquired`.`source_name`
FROM `how_many_data_points_where_acquired`
WHERE `how_many_data_points_where_acquired`.`source_name` = 'iPhone'"""
    )


def test_DateFilter_filter_with_an_invalid_filter():
    filter = DateFilter(field="creationDate")
    with pytest.raises(ValueError):
        filter.filter(how_many_data_points_where_acquired, value=date.today())


def test_DateFilter_filter_on_date_column():
    @question()
    def a_question():
        cte = cte_from_rows(
            [
                {
                    "name": "Diogo",
                    "count": 0,
                    "date_col": date(2021, 1, 1),
                },
                {
                    "name": "Diogo",
                    "count": 1,
                    "date_col": date(2022, 8, 27),
                },
                {
                    "name": "Diogo",
                    "count": 2,
                    "date_col": date(2022, 8, 28),
                },
                {
                    "name": "Lorena",
                    "count": 1,
                    "date_col": date(2021, 1, 1),
                },
                {
                    "name": "Lorena",
                    "count": 1,
                    "date_col": date(2022, 8, 27),
                },
                {
                    "name": "Lorena",
                    "count": 2,
                    "date_col": date(2022, 8, 28),
                },
            ]
        )
        return select(cte)

    filtered_question = DateFilter(field="date_col").filter(
        a_question, value=date(2022, 8, 28)
    )
    assert filtered_question.answer_df().to_dict(orient="rows") == [
        {
            "name": "Diogo",
            "count": 2,
            "date_col": date(2022, 8, 28),
        },
        {
            "name": "Lorena",
            "count": 2,
            "date_col": date(2022, 8, 28),
        },
    ]


def test_DateFilter_filter_on_datetime_column():
    @question()
    def a_question():
        cte = cte_from_rows(
            [
                {
                    "name": "Diogo",
                    "count": 0,
                    "datetime_col": datetime(2021, 1, 1),
                },
                {
                    "name": "Diogo",
                    "count": 1,
                    "datetime_col": datetime(2022, 8, 27, 23, 00),
                },
                {
                    "name": "Diogo",
                    "count": 2,
                    "datetime_col": datetime(2022, 8, 28, 23, 33),
                },
                {
                    "name": "Lorena",
                    "count": 1,
                    "datetime_col": datetime(2021, 1, 1),
                },
                {
                    "name": "Lorena",
                    "count": 1,
                    "datetime_col": datetime(2022, 8, 27, 23, 00),
                },
                {
                    "name": "Lorena",
                    "count": 2,
                    "datetime_col": datetime(2022, 8, 28, 23, 33),
                },
            ]
        )
        return select(cte)

    date_filter = DateFilter(field="datetime_col")
    filtered_question = date_filter.filter(a_question, value=date(2022, 8, 28))

    assert filtered_question.answer_df().to_dict(orient="rows") == [
        {
            "name": "Diogo",
            "count": 2,
            "datetime_col": datetime(2022, 8, 28, 23, 33),
        },
        {
            "name": "Lorena",
            "count": 2,
            "datetime_col": datetime(2022, 8, 28, 23, 33),
        },
    ]
