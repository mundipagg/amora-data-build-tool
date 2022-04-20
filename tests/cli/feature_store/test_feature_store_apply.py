from unittest.mock import patch, MagicMock

from feast import FeatureStore
from typer.testing import CliRunner
from amora.cli import app

runner = CliRunner()


@patch("feast.repo_operations.apply_total_with_repo_instance")
@patch("amora.feature_store.fs", spec=FeatureStore)
@patch("amora.feature_store.registry.get_repo_contents")
def test_feature_store_apply_without_options(
    get_repo_contents: MagicMock,
    fs: MagicMock,
    apply_total_with_repo_instance: MagicMock,
):
    result = runner.invoke(
        app,
        ["feature-store", "apply"],
    )

    assert result.exit_code == 0
    apply_total_with_repo_instance.assert_called_once_with(
        store=fs,
        project=fs.project,
        registry=fs.registry,
        repo=get_repo_contents.return_value,
        skip_source_validation=False,
    )
