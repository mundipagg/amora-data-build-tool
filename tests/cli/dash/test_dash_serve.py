from unittest.mock import patch

from typer.testing import CliRunner

from amora.cli import app
from amora.dash.config import settings

runner = CliRunner()


def test_dash_serve():
    with patch("amora.dash.app.dash_app") as dash_app:
        result = runner.invoke(
            app,
            ["dash", "serve"],
        )

        assert result.exit_code == 0
        dash_app.run.assert_called_once_with(
            debug=settings.DEBUG, host=settings.HTTP_HOST, port=settings.HTTP_PORT
        )
