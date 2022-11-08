import typer

app = typer.Typer(help="Amora dashboards")


@app.command("serve")
def serve():
    import gunicorn.app.base

    from amora.dash.app import dash_app
    from amora.dash.config import settings
    from amora.dash.gunicorn.config import child_exit, when_ready

    # https://docs.gunicorn.org/en/latest/custom.html#custom-application
    class StandaloneApplication(gunicorn.app.base.BaseApplication):
        def __init__(self, app, options=None):
            self.options = options or {}
            self.application = app
            super().__init__()

        def load_config(self):
            config = {
                key: value
                for key, value in self.options.items()
                if key in self.cfg.settings and value is not None
            }
            for key, value in config.items():
                self.cfg.set(key.lower(), value)

        def load(self):
            return self.application

    if settings.DEBUG:
        dash_app.run(
            debug=settings.DEBUG, host=settings.HTTP_HOST, port=settings.HTTP_PORT
        )
    else:
        options = {
            "bind": f"{settings.HTTP_HOST}:{settings.HTTP_PORT}",
            "workers": settings.GUNICORN_WORKERS,
            "timeout": settings.GUNICORN_WORKER_TIMEOUT,
        }
        if settings.METRICS_ENABLED:
            options.update(
                {
                    "when_ready": when_ready,
                    "child_exit": child_exit,
                }
            )

        StandaloneApplication(dash_app.server, options).run()
