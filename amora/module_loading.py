import sys
from importlib import import_module
from typing import Iterable


def cached_import(module_path, class_name):
    # Check whether module is loaded and fully initialized.
    if not (
        (module := sys.modules.get(module_path))
        and (spec := getattr(module, "__spec__", None))
        and getattr(spec, "_initializing", False) is False
    ):
        module = import_module(module_path)
    return getattr(module, class_name)


def import_string(dotted_path):
    """
    Import a dotted module path and return the attribute/class designated by the
    last name in the path. Raise ImportError if the import failed.
    """
    try:
        module_path, class_name = dotted_path.rsplit(".", 1)
    except ValueError as err:
        raise ImportError("%s doesn't look like a module path" % dotted_path) from err

    try:
        return cached_import(module_path, class_name)
    except AttributeError as err:
        raise ImportError(
            'Module "%s" does not define a "%s" attribute/class'
            % (module_path, class_name)
        ) from err


def main():
    from amora.config import settings
    from pprint import pprint
    from importlib.util import module_from_spec, spec_from_file_location
    from pathlib import Path
    import inspect
    from importlib import import_module
    from typing import Union, Iterable
    from types import ModuleType
    from amora.protocols import Compilable, CompilableProtocol
    from amora.models import AmoraModel


    def list_files(path: Union[str, Path], suffix: str) -> Iterable[Path]:
        yield from Path(path).rglob(f"*{suffix}")

    def get_string_module(path: Path, project_path: Path = settings.PROJECT_PATH):
        project_path_str = str(project_path) + "/"
        module_path = str(path).replace(project_path_str, "")
        string_module_path = module_path.replace("/", ".").replace(".py", "")
        return string_module_path

    def _is_amora_model(candidate: ModuleType) -> bool:
        return (
            isinstance(candidate, CompilableProtocol)
            and inspect.isclass(candidate)
            and issubclass(candidate, AmoraModel)
            and hasattr(candidate, "__table__")
        )

    for model_path in list_files(settings.models_path, suffix=".py"):
        string_module_path = get_string_module(model_path)

        module = import_module(string_module_path)
        pprint(f"{string_module_path=}")

        compilables = inspect.getmembers(
            module,
            _is_amora_model,
        )

        for _name, class_ in compilables:
            if inspect.getfile(class_) == str(model_path):
                print(class_)


if __name__ == "__main__":
    main()