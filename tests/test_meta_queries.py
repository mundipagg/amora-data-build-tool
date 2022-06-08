from pandas import Timestamp

from amora.meta_queries import summarize

from tests.models.health import Health
from tests.models.step_count_by_source import StepCountBySource


def test_summarize():
    summary = summarize(Health)
    assert summary.to_dict() == {
        "min": {0: 0.0},
        "max": {0: 1742.0},
        "unique_count": {0: 8101},
        "avg": {0: 82.16480398039408},
        "stddev": {0: 37.98340562839273},
        "null_percentage": {0: 0.0},
        "column_name": {0: "value"},
        "column_type": {0: "FLOAT"},
    }


def test_summarize_feature_view_model():
    summary = summarize(StepCountBySource)
    assert summary.to_dict(orient="records") == [
        {
            "min": Timestamp("2019-12-09 13:00:00+0000", tz="UTC"),
            "max": Timestamp("2021-07-23 02:00:00+0000", tz="UTC"),
            "unique_count": 3775,
            "avg": None,
            "stddev": None,
            "null_percentage": 0.0,
            "column_name": "event_timestamp",
            "column_type": "TIMESTAMP",
            "is_fv_feature": False,
            "is_fv_entity": False,
            "is_fv_event_timestamp": True,
        },
        {
            "min": 1.0,
            "max": 1742.0,
            "unique_count": 1422,
            "avg": 126.42742197839384,
            "stddev": 175.07633740644545,
            "null_percentage": 0.0,
            "column_name": "value_avg",
            "column_type": "FLOAT",
            "is_fv_feature": True,
            "is_fv_entity": False,
            "is_fv_event_timestamp": False,
        },
        {
            "min": 1.0,
            "max": 132968.0,
            "unique_count": 1130,
            "avg": 663.7570048309184,
            "stddev": 3996.362789365462,
            "null_percentage": 0.0,
            "column_name": "value_sum",
            "column_type": "FLOAT",
            "is_fv_feature": True,
            "is_fv_entity": False,
            "is_fv_event_timestamp": False,
        },
        {
            "min": 1,
            "max": 1374,
            "unique_count": 70,
            "avg": 4.5374396135265735,
            "stddev": 36.39923587492331,
            "null_percentage": 0.0,
            "column_name": "value_count",
            "column_type": "FLOAT",
            "is_fv_feature": True,
            "is_fv_entity": False,
            "is_fv_event_timestamp": False,
        },
        {
            "min": "Diogo iPhone",
            "max": "iPhone",
            "unique_count": 3,
            "avg": None,
            "stddev": None,
            "null_percentage": 0.0,
            "column_name": "source_name",
            "column_type": "VARCHAR",
            "is_fv_feature": False,
            "is_fv_entity": True,
            "is_fv_event_timestamp": False,
        },
    ]
