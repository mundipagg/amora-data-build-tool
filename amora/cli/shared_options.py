import typer

target_option = typer.Option(
    None,
    "--target",
    "-t",
    help="Target connection configuration as defined as an amora.target.Target",
)

models_option = typer.Option(
    [],
    "--model",
    help="A model to be compiled. This option can be passed multiple times.",
)


depth_option = typer.Option(
    None,
    "--depth",
    "-d",
    help="Depth of the materialization DAG. If not passed, the DAG will be materialized until the end.",
)


force_option = typer.Option(
    False,
    "--force",
    help="Flag to force Amora to recompile all models.",
)

depends_option = typer.Option(
    False, "--depends", help="Flag to materialize also the dependents of the model"
)
