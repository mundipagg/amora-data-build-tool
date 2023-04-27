import typer

app = typer.Typer(help="Amora Web UI")


@app.command("serve")
def serve():
    """
    Runs Amora's Web UI on the configured host and port.
    Debug mode can be activated with the envvar `AMORA_DASH_DEBUG=1`,
    which runs the webserver (Flask) on development mode. E.g:

    ```
    $ AMORA_DASH_DEBUG=1 amora dash serve

    Dash is running on http://127.0.0.1:8050/
     * Serving Flask app 'amora.dash.app' (lazy loading)
     * Environment: production
       WARNING: This is a development server. Do not use it in a production deployment.
       Use a production WSGI server instead.
     * Debug mode: on
    ```

    The default behaviour is a production ready setup, which runs amora on a multiprocess
    environment using [gunicorn](https://github.com/benoitc/gunicorn)

    ```
    [2022-11-09 09:31:35 -0300] [22863] [INFO] Starting gunicorn 20.1.0
    [2022-11-09 09:31:35 -0300] [22863] [INFO] Listening at: http://127.0.0.1:8050 (22863)
    [2022-11-09 09:31:35 -0300] [22863] [INFO] Using worker: sync
    [2022-11-09 09:31:35 -0300] [22875] [INFO] Booting worker with pid: 22875
    [2022-11-09 09:31:35 -0300] [22876] [INFO] Booting worker with pid: 22876
    ```

    The amount of [worker processes](https://docs.gunicorn.org/en/stable/settings.html#workers)
    can be configured with the envvar `AMORA_DASH_GUNICORN_WORKERS` (default: 2) to match
    the host hardware. The [timeout](https://docs.gunicorn.org/en/stable/settings.html#timeout)
    can be configured with the envvar `AMORA_DASH_GUNICORN_WORKER_TIMEOUT` (default: 30)

    ## Prometheus Metrics

    On the production ready setup, the metrics resource is exposed on a different port,
    by default on `:9090/metrics`, and configurable with `AMORA_DASH_METRICS_PORT`.
    On development/debug, `/metrics` share the same port as the application.

    Metrics can be disabled by setting the `AMORA_DASH_METRICS_ENABLED` to `0` or `False`.

    ```
    # HELP amora_version Multiprocess metric
    # TYPE amora_version gauge
    amora_version{version="0.1.16"} 1.0
    # HELP amora_dash_component_update_duration_seconds Multiprocess metric
    # TYPE amora_dash_component_update_duration_seconds histogram
    amora_dash_component_update_duration_seconds_sum{method="POST",status="200"} 0.030016314999699034
    amora_dash_component_update_duration_seconds_bucket{le="0.005",method="POST",status="200"} 14.0
    amora_dash_component_update_duration_seconds_bucket{le="0.01",method="POST",status="200"} 16.0
    amora_dash_component_update_duration_seconds_bucket{le="0.025",method="POST",status="200"} 16.0
    amora_dash_component_update_duration_seconds_bucket{le="0.05",method="POST",status="200"} 16.0
    amora_dash_component_update_duration_seconds_bucket{le="0.075",method="POST",status="200"} 16.0
    amora_dash_component_update_duration_seconds_bucket{le="0.1",method="POST",status="200"} 16.0
    amora_dash_component_update_duration_seconds_bucket{le="0.25",method="POST",status="200"} 16.0
    amora_dash_component_update_duration_seconds_bucket{le="0.5",method="POST",status="200"} 16.0
    amora_dash_component_update_duration_seconds_bucket{le="0.75",method="POST",status="200"} 16.0
    amora_dash_component_update_duration_seconds_bucket{le="1.0",method="POST",status="200"} 16.0
    amora_dash_component_update_duration_seconds_bucket{le="2.5",method="POST",status="200"} 16.0
    amora_dash_component_update_duration_seconds_bucket{le="5.0",method="POST",status="200"} 16.0
    amora_dash_component_update_duration_seconds_bucket{le="7.5",method="POST",status="200"} 16.0
    amora_dash_component_update_duration_seconds_bucket{le="10.0",method="POST",status="200"} 16.0
    amora_dash_component_update_duration_seconds_bucket{le="+Inf",method="POST",status="200"} 16.0
    amora_dash_component_update_duration_seconds_count{method="POST",status="200"} 16.0
    ...
    ```

    ## User Authentication

    ![amora dash authentication](../static/user-guide/web-ui/web-ui-auth.gif)

    User authentication on the interface can be achieved with [auth0](http://auth0.com)
    by running `amora dash serve` with the following envvars:

    ```
    AMORA_DASH_AUTH0_CLIENT_ID=YOUR_CLIENT_ID
    AMORA_DASH_AUTH0_CLIENT_SECRET=YOUR_CLIENT_SECRET
    AMORA_DASH_AUTH0_DOMAIN=YOUR_DOMAIN
    ```

    ## Theming

    The interface style can be easily replaced by setting the envvar `AMORA_DASH_DBC_THEME`.

    `AMORA_DASH_DBT_THEME=SKETCHY` (Default):

    ![sketchy theme](../static/user-guide/web-ui/web-ui-themes-sketchy.png)

    `AMORA_DASH_DBT_THEME=MINTY`

    ![minty theme](../static/user-guide/web-ui/web-ui-themes-minty.png)

    `AMORA_DASH_DBT_THEME=CYBORG`

    ![cyborg theme](../static/user-guide/web-ui/web-ui-themes-cyborg.png)

    `AMORA_DASH_DBT_THEME=VAPOR`

    ![vapor theme](../static/user-guide/web-ui/web-ui-themes-vapor.png)

    """
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


@app.command(
    "inspect",
    help="Inspect the project data queries and generates a cache to speedup the interface",
)
def inspect():
    from concurrent.futures import ThreadPoolExecutor

    from amora.config import settings as amora_settings
    from amora.dash.config import settings
    from amora.logger import logger
    from amora.meta_queries import summarize
    from amora.models import list_models
    from amora.providers.bigquery import sample
    from amora.questions import QUESTIONS

    if not amora_settings.STORAGE_CACHE_ENABLED:
        logger.debug("Cache disabled. Skipping cache generation.")
        return

    with ThreadPoolExecutor(
        max_workers=settings.THREAD_POOL_EXECUTOR_WORKERS
    ) as executor:
        # cache model summary
        for model, path_ in list_models():
            executor.submit(summarize, model)

        # cache model data sample
        for model, path_ in list_models():
            executor.submit(sample, model)

        # cache data questions
        for question in QUESTIONS:
            executor.submit(question.answer_df)
