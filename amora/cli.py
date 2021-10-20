import typer
from typing import Optional, List
from amora.compilation import py_module_for_path

from amora.config import settings
from amora.models import list_model_files, is_py_model, AmoraModel, list_target_files
from amora.compilation import compile_statement
from amora import materialization

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
    [],
    "--model",
    help="A model to be compiled. This option can be passed multiple times.",
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

        if not issubclass(module, AmoraModel):
            continue

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
    draw_dag: bool = typer.Option(False, "--draw-dag"),
) -> None:
    """
    Executes the compiled SQL againts the current target database.
    """
    model_to_task = {}

    for target_file_path in list_target_files():
        if models and target_file_path.stem not in models:
            continue

        task = materialization.Task.for_target(target_file_path)
        model_to_task[task.model.__name__] = task

    dag = materialization.DependencyDAG.from_tasks(tasks=model_to_task.values())

    if draw_dag:
        dag.draw()

    for model in dag:
        try:
            task = model_to_task[model]
        except KeyError:
            typer.echo(f"⚠️  Skipping `{model}`")
            continue
        else:
            # todo: deveria ser `task.module.output.__table__.name` ?
            result = materialization.materialize(
                sql=task.sql_stmt, name=task.target_file_path.stem
            )

            typer.echo(f"✅  Created `{model}` as `{result.full_table_id}`")
            typer.echo(f"    Rows: {result.num_rows}")
            typer.echo(f"    Bytes: {result.num_bytes}")


def main():
    return app()


if __name__ == "__main__":
    main()
