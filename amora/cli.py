import typer
from typing import Optional, List


app = typer.Typer(
    help="Amora Data Build Tool enables engineers to transform data in their warehouses "
    "by defining schemas and writing select statements with SQLAlchemy. Amora handles turning these "
    "select statements into tables and views"
)

Models = List[str]


@app.command()
def compile(models: Optional[Models] = typer.Argument(None)):
    """
    Generates executable SQL from model files. Compiled SQL files are written to the target/ directory.
    """
    typer.echo("compile")


@app.command()
def materialize(models: Optional[Models] = typer.Argument(None)):
    """
    Executes the compiled SQL againts the curent target database.
    """
    typer.echo("materialize")


def main():
    return app()
