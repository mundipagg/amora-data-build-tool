from pathlib import Path
from amora.compilation import compile, py_module_for_path
from amora.config import settings
from amora.models import list_model_files, is_py_model


def compile_py_model(path: Path) -> str:
    # {settings.dbt_models_path}/a_model/a_model.py -> a_model/a_model.py
    strip_path = settings.MODELS_PATH
    relative_model_path = str(path).split(strip_path)[1][1:]
    # a_model/a_model.py -> ~/project/amora/target/a_model/a_model.sql
    target_file_path = Path(settings.TARGET_PATH).joinpath(
        relative_model_path.replace(".py", ".sql")
    )
    content = compile(module.source())
    target_file_path.write_text(content)

    return content


if __name__ == "__main__":
    for model_file_path in list_model_files():
        module = py_module_for_path(model_file_path)

        if not is_py_model(module):
            continue

        compile_py_model(path=model_file_path)
