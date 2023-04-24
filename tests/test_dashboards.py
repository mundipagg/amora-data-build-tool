from datetime import date

import pytest

from amora.dashboards import Dashboard
from amora.filters import ValueFilter, DateFilter
from tests.models.step_count_by_source import how_many_data_points_where_acquired, \
    how_many_data_points_where_acquired_per_day


@pytest.fixture(scope="module")
def mock_dashboard_with_value_filter():
    return Dashboard(
        uid="mock_dashboard_with_value_filter",
        name="A dashboard with value filter",
        questions=[
            [how_many_data_points_where_acquired],
        ],
        filters=[ValueFilter(field='source_name')]
    )


def test_dashboard_to_markdown(mock_dashboard_with_value_filter):
    md = mock_dashboard_with_value_filter.to_markdown()
    assert md == """# A dashboard with value filter
---
---
## How many data points where acquired?
```sql 
 SELECT
  sum(`step_count_by_source`.`value_count`) AS `total`,
  `step_count_by_source`.`source_name`
FROM `amora-data-build-tool.amora`.`step_count_by_source`
GROUP BY `step_count_by_source`.`source_name` 
```
### Answer
|    |   total | source_name   |
|---:|--------:|:--------------|
|  0 |    6880 | Diogo iPhone  |
|  1 |   11163 | Mi Fit        |
|  2 |     742 | iPhone        |"""


def test_dashboard_with_value_filter(mock_dashboard_with_value_filter):
    filtered_dashboard = mock_dashboard_with_value_filter.filter(source_name="iPhone")
    md = filtered_dashboard.to_markdown()
    assert md == """# My dashboard
---
---
## How many data points where acquired filtered by source name iphone?
```sql 
 WITH `how_many_data_points_where_acquired` AS
  (SELECT
     sum(`step_count_by_source`.`value_count`) AS `total`,
     `step_count_by_source`.`source_name` AS `source_name`
   FROM `amora-data-build-tool.amora`.`step_count_by_source`
   GROUP BY `step_count_by_source`.`source_name`)
SELECT
  `how_many_data_points_where_acquired`.`total`,
  `how_many_data_points_where_acquired`.`source_name`
FROM `how_many_data_points_where_acquired`
WHERE `how_many_data_points_where_acquired`.`source_name` = 'iPhone' 
```
### Answer
|    |   total | source_name   |
|---:|--------:|:--------------|
|  0 |     742 | iPhone        |"""


@pytest.fixture(scope='module')
def mock_dashboard_with_date_filter():
    return Dashboard(
        uid='mock_dashboard_with_date_filter',
        name="A dashboard with date filter",
        questions=[
            [how_many_data_points_where_acquired_per_day]
        ],
        filters=[
            DateFilter(field='day'),
            ValueFilter(field='source_name')
        ]
    )


def test_dashboard_with_date_filter(mock_dashboard_with_date_filter):
    md = mock_dashboard_with_date_filter.filter(day=date(2021, 2, 26)).to_markdown()

    assert md == """# A dashboard with date filter
---
---
## How many data points where acquired per day at 2021-02-26?
```sql 
 WITH `how_many_data_points_where_acquired_per_day` AS
  (SELECT
     sum(`step_count_by_source`.`value_count`) AS `total`,
     `step_count_by_source`.`source_name` AS `source_name`,
     date(`step_count_by_source`.`event_timestamp`) AS `day`
   FROM `amora-data-build-tool.amora`.`step_count_by_source`
   GROUP BY
     `step_count_by_source`.`source_name`,
     `day`)
SELECT
  `how_many_data_points_where_acquired_per_day`.`total`,
  `how_many_data_points_where_acquired_per_day`.`source_name`,
  `how_many_data_points_where_acquired_per_day`.`day`
FROM `how_many_data_points_where_acquired_per_day`
WHERE date(`how_many_data_points_where_acquired_per_day`.`day`) = date(DATE '2021-02-26') 
```
### Answer
|    |   total | source_name   | day        |
|---:|--------:|:--------------|:-----------|
|  0 |      26 | Diogo iPhone  | 2021-02-26 |
|  1 |    1220 | Mi Fit        | 2021-02-26 |"""

    md = mock_dashboard_with_date_filter.filter(day=date(2021, 2, 26), source_name="Diogo iPhone").to_markdown()
    assert md == """# A dashboard with date filter
---
---
## How many data points where acquired per day at 2021-02-26 filtered by source name diogo iphone?
```sql 
 WITH
  `how_many_data_points_where_acquired_per_day` AS
  (SELECT
     sum(`step_count_by_source`.`value_count`) AS `total`,
     `step_count_by_source`.`source_name` AS `source_name`,
     date(`step_count_by_source`.`event_timestamp`) AS `day`
   FROM `amora-data-build-tool.amora`.`step_count_by_source`
   GROUP BY
     `step_count_by_source`.`source_name`,
     `day`),
  `how_many_data_points_where_acquired_per_day_at_2021-02-26` AS
  (SELECT
     `how_many_data_points_where_acquired_per_day`.`total` AS `total`,
     `how_many_data_points_where_acquired_per_day`.`source_name` AS `source_name`,
     `how_many_data_points_where_acquired_per_day`.`day` AS `day`
   FROM `how_many_data_points_where_acquired_per_day`
   WHERE date(`how_many_data_points_where_acquired_per_day`.`day`) = date(DATE '2021-02-26'))
SELECT
  `how_many_data_points_where_acquired_per_day_at_2021-02-26`.`total`,
  `how_many_data_points_where_acquired_per_day_at_2021-02-26`.`source_name`,
  `how_many_data_points_where_acquired_per_day_at_2021-02-26`.`day`
FROM `how_many_data_points_where_acquired_per_day_at_2021-02-26`
WHERE `how_many_data_points_where_acquired_per_day_at_2021-02-26`.`source_name` = 'Diogo iPhone' 
```
### Answer
|    |   total | source_name   | day        |
|---:|--------:|:--------------|:-----------|
|  0 |      26 | Diogo iPhone  | 2021-02-26 |"""