from amora.meta_queries import summarize

from tests.models.health import Health


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
