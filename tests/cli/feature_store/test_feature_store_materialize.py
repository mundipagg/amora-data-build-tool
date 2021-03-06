from datetime import datetime
from unittest.mock import MagicMock, Mock, patch

from feast import FeatureStore
from typer.testing import CliRunner

from amora.cli import app

runner = CliRunner()


@patch("amora.feature_store.fs", spec=FeatureStore)
@patch("amora.feature_store.registry.get_repo_contents")
def test_feature_store_materialize_without_options(
    get_repo_contents: MagicMock, fs: MagicMock
):
    start_ts = "2020-01-01T00:00:00"
    end_ts = "2022-01-01T00:00:00"

    result = runner.invoke(
        app,
        ["feature-store", "materialize", start_ts, end_ts],
    )

    assert result.exit_code == 0
    assert get_repo_contents.called

    fs.materialize.assert_called_once_with(
        feature_views=[fv.name for fv in get_repo_contents.return_value.feature_views],
        start_date=datetime.fromisoformat(start_ts),
        end_date=datetime.fromisoformat(end_ts),
    )


@patch("amora.feature_store.fs", spec=FeatureStore)
@patch(
    "amora.feature_store.registry.get_repo_contents",
    return_value=MagicMock(feature_views=[Mock(), Mock()]),
)
def test_feature_store_materialize_with_models_option(
    get_repo_contents: MagicMock, fs: MagicMock
):
    # readme: https://python.readthedocs.io/en/latest/library/unittest.mock.html#unittest.mock.Mock
    get_repo_contents.return_value.feature_views[0].name = "step_count_by_source"

    start_ts = "2020-01-01T00:00:00"
    end_ts = "2022-01-01T00:00:00"

    result = runner.invoke(
        app,
        [
            "feature-store",
            "materialize",
            start_ts,
            end_ts,
            "--model",
            "step_count_by_source",
        ],
    )

    assert result.exit_code == 0
    fs.materialize.assert_called_once_with(
        feature_views=["step_count_by_source"],
        start_date=datetime.fromisoformat(start_ts),
        end_date=datetime.fromisoformat(end_ts),
    )
