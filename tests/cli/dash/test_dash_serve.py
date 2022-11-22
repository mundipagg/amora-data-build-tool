from unittest.mock import patch

from typer.testing import CliRunner

from amora.cli import app
from amora.dash.config import settings
from amora.dash.gunicorn.config import child_exit, when_ready

runner = CliRunner()


def test_dash_serve():
    with patch("amora.dash.app.dash_app") as dash_app, patch(
        "amora.dash.gunicorn.application.StandaloneApplication"
    ) as StandaloneApplication, patch.multiple(settings, DEBUG=False):
        result = runner.invoke(
            app,
            ["dash", "serve"],
        )

        assert result.exit_code == 0
        dash_app.run.assert_not_called()

        options = {
            "bind": f"{settings.HTTP_HOST}:{settings.HTTP_PORT}",
            "workers": settings.GUNICORN_WORKERS,
            "timeout": settings.GUNICORN_WORKER_TIMEOUT,
            "when_ready": when_ready,
            "child_exit": child_exit,
        }
        StandaloneApplication.assert_called_with(app=dash_app.server, options=options)
        StandaloneApplication.return_value.run.assert_called_once()


def test_dash_serve_with_debug_on():
    with patch("amora.dash.app.dash_app") as dash_app, patch.multiple(
        settings, DEBUG=True
    ):
        result = runner.invoke(
            app,
            ["dash", "serve"],
        )

        assert result.exit_code == 0
        dash_app.run.assert_called_once_with(
            debug=settings.DEBUG, host=settings.HTTP_HOST, port=settings.HTTP_PORT
        )


def test_dash_serve_without_metrics():
    with patch("amora.dash.app.dash_app") as dash_app, patch(
        "amora.dash.gunicorn.application.StandaloneApplication"
    ) as StandaloneApplication, patch.multiple(
        settings, DEBUG=False, METRICS_ENABLED=False
    ):
        result = runner.invoke(
            app,
            ["dash", "serve"],
        )

        assert result.exit_code == 0
        dash_app.run.assert_not_called()

        options = {
            "bind": f"{settings.HTTP_HOST}:{settings.HTTP_PORT}",
            "workers": settings.GUNICORN_WORKERS,
            "timeout": settings.GUNICORN_WORKER_TIMEOUT,
        }
        StandaloneApplication.assert_called_with(app=dash_app.server, options=options)
        StandaloneApplication.return_value.run.assert_called_once()
