import typer
from typing import Optional, List


app = typer.Typer(
    help="Amora Data Build Tool enables engineers to transform data in their warehouses "
    "by defining schemas and writing select statements with SQLAlchemy. Amora handles turning these "
    "select statements into tables and views"
)

Models = List[str]
target_option = lambda: typer.Option(
    ...,
    "--target",
    "-t",
    help="Target connection configuration as defined as an amora.target.Target",
)


@app.command()
def compile(
    models: Optional[Models] = typer.Argument(None),
    target: str = target_option(),
) -> None:
    """
    Generates executable SQL from model files. Compiled SQL files are written to the `./target` directory.
    """
    typer.echo("compile")


@app.command()
def materialize(
    models: Optional[Models] = typer.Argument(None),
    target: str = target_option(),
) -> None:
    """
    Executes the compiled SQL againts the current target database.
    """
    typer.echo("materialize")


def main():
    return app()


if __name__ == "__main__":
    main()
