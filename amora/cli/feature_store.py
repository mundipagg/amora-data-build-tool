from datetime import datetime
from typing import Optional

import typer

from amora.cli.shared_options import models_option
from amora.cli.type_specs import Models

app = typer.Typer(help="Easily productionize new features from Amora Models")


@app.command(name="plan")
def feature_store_plan():
    """
    Dry-run registering objects to the Feature Registry

    The plan method dry-runs registering one or more definitions (e.g.: Entity, Feature View)
    and produces a list of all the changes that would be introduced in the Feature Registry
    by an `amora feature-store apply` execution.

    The changes computed by the `plan` command are informational, and are not actually applied to the registry.
    """
    from amora.feature_store import fs
    from amora.feature_store.registry import get_repo_contents

    registry_diff, infra_diff, infra = fs._plan(
        desired_repo_contents=get_repo_contents()
    )

    typer.echo("Amora: Feature Store :: Registry diff")
    typer.echo(registry_diff.to_string())

    typer.echo("Amora: Feature Store :: Infrastructure diff")
    typer.echo(infra_diff.to_string())


@app.command(name="apply")
def feature_store_apply():
    """
    1. Scans Python files in your amora project and find all models defined as
    feature views.

    2. Validate your feature definitions

    3. Sync the metadata about feature store objects to the feature registry.
    If a registry does not exist, then it will be instantiated.
    The standard registry is a simple protobuf binary file
    that is stored on disk (locally or in an object store).

    4. Create all necessary feature store infrastructure.
    The exact infrastructure that is deployed or configured depends
    on the provider configuration. For example, setting local as
    your provider will result in a sqlite online store being created.
    """
    from feast.repo_operations import apply_total_with_repo_instance

    from amora.feature_store import fs
    from amora.feature_store.registry import get_repo_contents

    apply_total_with_repo_instance(
        store=fs,
        project=fs.project,
        registry=fs.registry,
        repo=get_repo_contents(),
        skip_source_validation=False,
    )


@app.command(name="materialize")
def feature_store_materialize(
    start_ts: str = typer.Argument(
        None,
        help="Start timestamp on ISO 8601 format. E.g.: '2022-01-01T01:00:00'",
    ),
    end_ts: str = typer.Argument(
        None,
        help="End timestamp on ISO 8601 format. E.g.: '2022-01-02T01:00:00'",
    ),
    models: Optional[Models] = models_option,
):
    """
    Run a (non-incremental) materialization job to ingest data into the online
    store. All data between `start_ts` and `end_ts` will be read from the offline
    store and written into the online store. If you don't specify feature view
    names using `--models`, all registered Feature Views will be materialized.
    """
    from amora.feature_store import fs
    from amora.feature_store.registry import get_repo_contents

    repo_contents = get_repo_contents()

    if models:
        views_to_materialize = [
            fv.name for fv in repo_contents.feature_views if fv.name in models
        ]
    else:
        views_to_materialize = [fv.name for fv in repo_contents.feature_views]

    fs.materialize(
        feature_views=views_to_materialize,
        start_date=datetime.fromisoformat(start_ts),
        end_date=datetime.fromisoformat(end_ts),
    )


@app.command(name="materialize-incremental")
def feature_store_materialize_incremental(
    end_ts: Optional[str] = typer.Argument(
        None,
        help="End timestamp on ISO 8601 format. E.g.: '2022-01-02T01:00:00'. If a date isn't provided, `datetime.utcnow` is used",
    ),
    models: Optional[Models] = models_option,
):
    """
    Load data from feature views into the online store, beginning from either the previous `materialize`
    or `materialize-incremental` end date, or the beginning of time.

    """
    from amora.feature_store import fs
    from amora.feature_store.registry import get_repo_contents

    repo_contents = get_repo_contents()

    if models:
        views_to_materialize = [
            fv.name for fv in repo_contents.feature_views if fv.name in models
        ]
    else:
        views_to_materialize = [fv.name for fv in repo_contents.feature_views]

    if end_ts is not None:
        end_date = datetime.fromisoformat(end_ts)
    else:
        end_date = datetime.utcnow()

    fs.materialize_incremental(
        feature_views=views_to_materialize,
        end_date=end_date,
    )


@app.command(name="serve")
def feature_store_serve():
    """
    Starts the feature server HTTP app.

    Routes:

        - `POST /get-online-features`

        `curl -XPOST -H "Content-type: application/json" -d '{"features": ["step_count_by_source:value_avg", "step_count_by_source:value_sum", "step_count_by_source:value_count"], "entities": {"source_name": ["Mi Fit", "Diogo iPhone", "An invalid source"]}}' 'http://localhost:8666/get-online-features'`

        ```json
        {
          "metadata": {
            "feature_names": [
              "source_name",
              "value_count",
              "value_sum",
              "value_avg"
            ]
          },
          "results": [
            {
              "values": [
                "Mi Fit",
                6.0,
                809.0,
                134.8333282470703
              ],
              "statuses": [
                "PRESENT",
                "PRESENT",
                "PRESENT",
                "PRESENT"
              ],
              "event_timestamps": [
                "1970-01-01T00:00:00Z",
                "2021-07-23T02:00:00Z",
                "2021-07-23T02:00:00Z",
                "2021-07-23T02:00:00Z"
              ]
            },
            {
              "values": [
                "Diogo iPhone",
                2.0,
                17.0,
                8.5
              ],
              "statuses": [
                "PRESENT",
                "PRESENT",
                "PRESENT",
                "PRESENT"
              ],
              "event_timestamps": [
                "1970-01-01T00:00:00Z",
                "2021-07-23T02:00:00Z",
                "2021-07-23T02:00:00Z",
                "2021-07-23T02:00:00Z"
              ]
            },
            {
              "values": [
                "An invalid source",
                null,
                null,
                null
              ],
              "statuses": [
                "PRESENT",
                "NOT_FOUND",
                "NOT_FOUND",
                "NOT_FOUND"
              ],
              "event_timestamps": [
                "1970-01-01T00:00:00Z",
                "2021-07-23T02:00:00Z",
                "2021-07-23T02:00:00Z",
                "2021-07-23T02:00:00Z"
              ]
            }
          ]
        }
        ```

        More on: https://docs.feast.dev/v/v0.9-branch/user-guide/getting-online-features

        - `GET /list-feature-views`. E.g.:

        `curl http://localhost:8666/list-feature-views | jq`

        ```json
        [
            {
                "name": "step_count_by_source",
                "features": [
                    "step_count_by_source:value_avg",
                    "step_count_by_source:value_sum",
                    "step_count_by_source:value_count"
                ],
                "entities": [
                    "source_name"
                ]
            }
        ]
        ```
    """
    import uvicorn
    from feast.feature_server import get_app
    from prometheus_fastapi_instrumentator import Instrumentator

    from amora.feature_store import fs
    from amora.feature_store.config import settings

    app = get_app(store=fs)

    @app.get("/list-feature-views")
    def list_feature_views():
        fvs = fs.list_feature_views()
        return [
            {
                "name": fv.name,
                "features": [f"{fv.name}:{feature.name}" for feature in fv.features],
                "entities": fv.entities,
            }
            for fv in fvs
        ]

    Instrumentator().instrument(app).expose(app)

    uvicorn.run(
        app,
        host=settings.HTTP_SERVER_HOST,
        port=settings.HTTP_SERVER_PORT,
        access_log=settings.HTTP_ACCESS_LOG_ENABLED,
    )
