import typer
from typing import Optional, List
from amora.compilation import py_module_for_path

from amora.config import settings
from amora.models import list_model_files, is_py_model, AmoraModel
from amora.compilation import compile_statement


app = typer.Typer(
    help="Amora Data Build Tool enables engineers to transform data in their warehouses "
    "by defining schemas and writing select statements with SQLAlchemy. Amora handles turning these "
    "select statements into tables and views"
)

Models = List[str]
target_option = typer.Option(
    None,
    "--target",
    "-t",
    help="Target connection configuration as defined as an amora.target.Target",
)

models_option = typer.Option(
    [], "--models", help="A list space separated models to be compiled"
)


@app.command()
def compile(
    models: Optional[Models] = models_option,
    target: Optional[str] = target_option,
) -> None:
    """
    Generates executable SQL from model files. Compiled SQL files are written to the `./target` directory.
    """
    for model_file_path in list_model_files():
        if models and model_file_path.stem not in models:
            continue

        try:
            module = py_module_for_path(model_file_path).output
        except AttributeError:
            continue

        # todo: validar que o modulo de fato é um AmoraModel. isinstance não tá funcionando
        # if not isinstance(module, AmoraModel):
        #     continue
        source_sql_statement = module.source()
        if source_sql_statement is None:
            typer.echo(f"⏭ Skipping compilation of model `{model_file_path}`")
            continue

        target_file_path = module.target_path(model_file_path)
        typer.echo(f"🏗 Compiling model `{model_file_path}` -> `{target_file_path}`")

        # todo: remover a necessidade de passar `model_file_path`
        with open(target_file_path, "w") as fp:
            content = compile_statement(source_sql_statement)
            fp.write(content)


@app.command()
def materialize(
    models: Optional[Models] = models_option,
    target: str = target_option,
) -> None:
    """
    Executes the compiled SQL againts the current target database.
    """
    typer.echo("materialize")


def main():
    return app()


if __name__ == "__main__":
    main()
