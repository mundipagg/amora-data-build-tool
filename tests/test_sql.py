from amora.sql import format_sql


def test_format_sql():
    sql = """
    SELECT DISTINCT 
        `step_count_by_source`.`source_name`, `step_count_by_source`.`device`
    FROM 
      `amora-data-build-tool.amora`.`step_count_by_source`
    """

    formatted_sql = format_sql(sql)
    assert (
        formatted_sql
        == """
SELECT DISTINCT
  `step_count_by_source`.`source_name`,
  `step_count_by_source`.`device`
FROM `amora-data-build-tool.amora`.`step_count_by_source`"""
    )
