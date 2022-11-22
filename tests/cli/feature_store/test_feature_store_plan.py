from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from amora.cli import app

runner = CliRunner()


@patch("feast.repo_operations.apply_total_with_repo_instance")
@patch(
    "amora.feature_store.fs.plan",
    return_value=(
        MagicMock(to_string=lambda: "registry diff"),
        MagicMock(to_string=lambda: "infra diff"),
        MagicMock(),
    ),
)
@patch("amora.feature_store.registry.get_repo_contents")
def test_feature_store_plan(
    get_repo_contents: MagicMock,
    fs_plan: MagicMock,
    apply_total_with_repo_instance: MagicMock,
):
    result = runner.invoke(
        app,
        ["feature-store", "plan"],
    )

    assert result.exit_code == 0
    get_repo_contents.assert_called_once()
    assert (
        result.stdout
        == "Amora: Feature Store :: Registry diff\nregistry diff\nAmora: Feature Store :: Infrastructure diff\ninfra diff\n"
    )
