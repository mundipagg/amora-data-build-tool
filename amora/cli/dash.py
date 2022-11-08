import typer

app = typer.Typer(help="Amora dashboards")


@app.command("serve")
def serve():
    from amora.dash.app import dash_app
    from amora.dash.config import settings
    from amora.dash.gunicorn.application import StandaloneApplication
    from amora.dash.gunicorn.config import child_exit, when_ready

    if settings.DEBUG:
        return dash_app.run(
            debug=settings.DEBUG, host=settings.HTTP_HOST, port=settings.HTTP_PORT
        )

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

    StandaloneApplication(app=dash_app.server, options=options).run()
